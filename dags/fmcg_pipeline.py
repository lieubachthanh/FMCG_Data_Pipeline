"""
Airflow DAG: fmcg_data_pipeline
Full end-to-end FMCG data pipeline orchestration
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# ─── Project paths (set as Airflow vars or env vars) ──────────────────────────
PROJECT_DIR  = os.environ.get("FMCG_PROJECT_DIR", "/opt/fmcg_platform")
RAW_BASE     = os.path.join(PROJECT_DIR, "data_lake", "raw")
CLEAN_BASE   = os.path.join(PROJECT_DIR, "data_lake", "clean")
DW_PATH      = os.path.join(PROJECT_DIR, "data_lake", "fmcg_warehouse.duckdb")
PYTHON_BIN   = os.environ.get("PYTHON_BIN", "python3")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")

# ─── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "fmcg_data_team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ─── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="fmcg_data_pipeline",
    description="End-to-end FMCG data pipeline: generate → clean → warehouse → marts",
    default_args=default_args,
    schedule="0 3 * * *",             # Run at 3 AM daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fmcg", "data-engineering", "production"],
) as dag:

    # ── 1. Generate raw synthetic data ────────────────────────────────────────
    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"RAW_BASE={RAW_BASE} "
            f"{PYTHON_BIN} scripts/generate_data.py"
        ),
        doc_md="""
        **Generate Raw Data**
        Runs the synthetic data generator to produce CSV files for all
        operational source systems (ERP, DMS, POS, Supply Chain, etc.)
        into `data_lake/raw/`.
        """,
    )

    # ── 2. Convert master tables to Parquet (fast, no Spark needed) ───────────
    convert_masters_to_parquet = BashOperator(
        task_id="convert_masters_to_parquet",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"RAW_BASE={RAW_BASE} CLEAN_BASE={CLEAN_BASE} "
            f"{PYTHON_BIN} -c \""
            "import pandas as pd, os; "
            "masters = ["
            "  ('erp/product_master', ['product_id','sku_code']), "
            "  ('erp/distributor_master', ['distributor_id']), "
            "  ('dms/retailer_master', ['retailer_id']), "
            "  ('pos/store_master', ['store_id']), "
            "  ('marketing/promotions', ['promotion_id']), "
            "  ('innovation/innovation_master', ['innovation_id']), "
            "  ('external/market_share', ['year','brand']), "
            "]; "
            "[( "
            "  os.makedirs(f'{os.environ[\\\"CLEAN_BASE\\\"]}/{p}', exist_ok=True), "
            "  pd.read_csv(f'{os.environ[\\\"RAW_BASE\\\"]}/{p}.csv').dropna(subset=k).drop_duplicates().to_parquet(f'{os.environ[\\\"CLEAN_BASE\\\"]}/{p}/data.parquet', index=False) "
            ") for p,k in masters]; "
            "print('Masters converted')\""
        ),
        doc_md="Converts master/dimension source tables to Parquet for faster downstream reads.",
    )

    # ── 3. Spark clean layer: large transactional tables ──────────────────────
    spark_clean_layer = BashOperator(
        task_id="spark_clean_layer",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"RAW_BASE={RAW_BASE} CLEAN_BASE={CLEAN_BASE} "
            f"{SPARK_SUBMIT} "
            "--master local[*] "
            "--driver-memory 2g "
            f"spark_jobs/clean_layer.py"
        ),
        doc_md="""
        **Spark Clean Layer**
        Uses PySpark to process large transactional tables:
        - sellin, sellout, pos_sales, inventory, ecommerce_orders, store_visits
        Applies: schema normalization, type casting, null handling, deduplication.
        Output: Parquet files in `data_lake/clean/`.
        """,
    )

    # ── 4. Build DuckDB dimensions ─────────────────────────────────────────────
    def build_dimensions(**context):
        import importlib.util, sys
        spec = importlib.util.spec_from_file_location(
            "build_warehouse",
            os.path.join(PROJECT_DIR, "warehouse", "build_warehouse.py")
        )
        mod = importlib.util.load_from_spec(spec)
        spec.loader.exec_module(mod)

        os.environ["CLEAN_BASE"] = CLEAN_BASE
        os.environ["DW_PATH"]    = DW_PATH
        conn = mod.get_conn()
        try:
            mod.build_dim_date(conn)
            mod.build_dim_product(conn)
            mod.build_dim_distributor(conn)
            mod.build_dim_store(conn)
            mod.build_dim_promotion(conn)
        finally:
            conn.close()

    build_dimensions_task = PythonOperator(
        task_id="build_dimensions",
        python_callable=build_dimensions,
        doc_md="Builds star-schema dimension tables in DuckDB.",
    )

    # ── 5. Build fact tables ───────────────────────────────────────────────────
    def build_fact_tables(**context):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "build_warehouse",
            os.path.join(PROJECT_DIR, "warehouse", "build_warehouse.py")
        )
        mod = importlib.util.load_from_spec(spec)
        spec.loader.exec_module(mod)

        os.environ["CLEAN_BASE"] = CLEAN_BASE
        os.environ["DW_PATH"]    = DW_PATH
        conn = mod.get_conn()
        try:
            mod.build_fact_sellin(conn)
            mod.build_fact_sellout(conn)
            mod.build_fact_pos_sales(conn)
            mod.build_fact_ecommerce_sales(conn)
            mod.build_fact_inventory(conn)
        finally:
            conn.close()

    build_fact_tables_task = PythonOperator(
        task_id="build_fact_tables",
        python_callable=build_fact_tables,
        doc_md="Builds star-schema fact tables in DuckDB.",
    )

    # ── 6. Build data marts ────────────────────────────────────────────────────
    def build_data_marts(**context):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "build_marts",
            os.path.join(PROJECT_DIR, "data_marts", "build_marts.py")
        )
        mod = importlib.util.load_from_spec(spec)
        spec.loader.exec_module(mod)

        os.environ["DW_PATH"] = DW_PATH
        conn = mod.get_conn()
        try:
            mod.build_sales_mart(conn)
            mod.build_promotion_mart(conn)
            mod.build_innovation_mart(conn)
            mod.build_views(conn)
        finally:
            conn.close()

    build_data_marts_task = PythonOperator(
        task_id="build_data_marts",
        python_callable=build_data_marts,
        doc_md="Builds analytics marts: sales_mart, promotion_mart, innovation_mart, and BI views.",
    )

    # ── 7. Refresh Metabase dashboard (trigger sync) ───────────────────────────
    refresh_dashboard = BashOperator(
        task_id="refresh_dashboard",
        bash_command="""
        # Trigger Metabase sync via API
        METABASE_URL=${METABASE_URL:-http://localhost:3000}
        TOKEN_RESPONSE=$(curl -s -X POST "${METABASE_URL}/api/session" \
            -H "Content-Type: application/json" \
            -d '{"username":"'"${METABASE_EMAIL:-admin@fmcg.local}"'","password":"'"${METABASE_PASSWORD:-admin123}"'"}')
        TOKEN=$(echo $TOKEN_RESPONSE | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))")
        if [ -n "$TOKEN" ]; then
            curl -s -X POST "${METABASE_URL}/api/database/1/sync_schema" \
                -H "X-Metabase-Session: $TOKEN" \
                -H "Content-Type: application/json"
            echo "✓ Metabase sync triggered"
        else
            echo "⚠ Metabase not available, skipping sync"
        fi
        """,
        doc_md="Triggers Metabase schema sync so dashboards reflect latest warehouse data.",
    )

    # ─── Task dependencies ────────────────────────────────────────────────────
    (
        generate_raw_data
        >> convert_masters_to_parquet
        >> spark_clean_layer
        >> build_dimensions_task
        >> build_fact_tables_task
        >> build_data_marts_task
        >> refresh_dashboard
    )