"""
DuckDB Data Warehouse Builder
Builds Star Schema: Dimensions + Fact Tables
Reads from clean Parquet layer
"""

import os
import duckdb
import pandas as pd

CLEAN_BASE = os.environ.get("CLEAN_BASE", "data_lake/clean")
DW_PATH    = os.environ.get("DW_PATH",    "data_lake/fmcg_warehouse.duckdb")


def get_conn():
    conn = duckdb.connect(DW_PATH)
    conn.execute("SET threads TO 4")
    conn.execute("SET memory_limit='2GB'")
    return conn


def parquet_glob(path):
    """Return glob pattern for parquet files in a directory."""
    return f"'{CLEAN_BASE}/{path}/*.parquet'"


# ══════════════════════════════════════════════════
# DIMENSIONS
# ══════════════════════════════════════════════════

def build_dim_date(conn):
    print("Building dim_date...")
    conn.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH dates AS (
            SELECT unnest(generate_series(
                DATE '2023-01-01',
                DATE '2025-12-31',
                INTERVAL '1 day'
            )) AS date
        )
        SELECT
            CAST(strftime(date, '%Y%m%d') AS INTEGER)  AS date_key,
            date,
            YEAR(date)                                  AS year,
            QUARTER(date)                               AS quarter,
            MONTH(date)                                 AS month,
            WEEK(date)                                  AS week,
            DAY(date)                                   AS day,
            DAYOFWEEK(date)                             AS day_of_week,
            DAYNAME(date)                               AS day_name,
            MONTHNAME(date)                             AS month_name,
            CASE WHEN MONTH(date) IN (1,2)       THEN 'Tet Season'
                 WHEN MONTH(date) IN (5,6,7,8)   THEN 'Summer'
                 WHEN MONTH(date) IN (11,12)      THEN 'Year-End'
                 ELSE 'Regular'
            END                                         AS season
        FROM dates
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    print(f"  ✓ dim_date: {count} rows")


def build_dim_product(conn):
    print("Building dim_product...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE dim_product AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
            product_id,
            sku_code,
            sku_name,
            brand,
            category,
            sub_category,
            pack_size,
            launch_date,
            innovation_flag
        FROM read_parquet({parquet_glob('erp/product_master')})
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    print(f"  ✓ dim_product: {count} rows")


def build_dim_distributor(conn):
    print("Building dim_distributor...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE dim_distributor AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY distributor_id) AS distributor_key,
            distributor_id,
            name,
            region,
            province
        FROM read_parquet({parquet_glob('erp/distributor_master')})
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_distributor").fetchone()[0]
    print(f"  ✓ dim_distributor: {count} rows")


def build_dim_store(conn):
    """Unified General Trade + Modern Trade stores."""
    print("Building dim_store...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE dim_store AS
        -- Modern Trade (POS)
        SELECT
            ROW_NUMBER() OVER (ORDER BY store_id) AS store_key,
            store_id,
            store_name,
            'Modern Trade'  AS store_type,
            chain,
            region,
            province,
            NULL            AS distributor_id
        FROM read_parquet({parquet_glob('pos/store_master')})

        UNION ALL

        -- General Trade (Retailers via DMS)
        SELECT
            (SELECT MAX(store_key) FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY store_id) AS store_key
                FROM read_parquet({parquet_glob('pos/store_master')})
            ))
            + ROW_NUMBER() OVER (ORDER BY retailer_id)     AS store_key,
            retailer_id                                     AS store_id,
            name                                            AS store_name,
            store_type,
            NULL                                            AS chain,
            region,
            province,
            distributor_id
        FROM read_parquet({parquet_glob('dms/retailer_master')})
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_store").fetchone()[0]
    print(f"  ✓ dim_store: {count} rows")


def build_dim_promotion(conn):
    print("Building dim_promotion...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE dim_promotion AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY promotion_id) AS promotion_key,
            promotion_id,
            product_id,
            promotion_type,
            discount_percent,
            start_date,
            end_date
        FROM read_parquet({parquet_glob('marketing/promotions')})
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_promotion").fetchone()[0]
    print(f"  ✓ dim_promotion: {count} rows")


# ══════════════════════════════════════════════════
# FACT TABLES
# ══════════════════════════════════════════════════

def build_fact_sellin(conn):
    print("Building fact_sellin...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE fact_sellin AS
        SELECT
            dd.date_key,
            dp.product_key,
            ddist.distributor_key,
            s.qty,
            s.gross_sales,
            s.discount,
            s.net_sales
        FROM read_parquet({parquet_glob('erp/sellin')}) s
        JOIN dim_date        dd    ON dd.date            = s.date
        JOIN dim_product     dp    ON dp.product_id      = s.product_id
        JOIN dim_distributor ddist ON ddist.distributor_id = s.distributor_id
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_sellin").fetchone()[0]
    print(f"  ✓ fact_sellin: {count} rows")


def build_fact_sellout(conn):
    print("Building fact_sellout...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE fact_sellout AS
        SELECT
            dd.date_key,
            ds.store_key,
            dp.product_key,
            so.qty,
            so.revenue
        FROM read_parquet({parquet_glob('dms/sellout')}) so
        JOIN dim_date    dd ON dd.date       = so.date
        JOIN dim_store   ds ON ds.store_id   = so.retailer_id
        JOIN dim_product dp ON dp.product_id = so.product_id
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_sellout").fetchone()[0]
    print(f"  ✓ fact_sellout: {count} rows")


def build_fact_pos_sales(conn):
    print("Building fact_pos_sales...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE fact_pos_sales AS
        SELECT
            dd.date_key,
            ds.store_key,
            dp.product_key,
            p.qty,
            p.revenue
        FROM read_parquet({parquet_glob('pos/pos_sales')}) p
        JOIN dim_date    dd ON dd.date       = p.date
        JOIN dim_store   ds ON ds.store_id   = p.store_id
        JOIN dim_product dp ON dp.product_id = p.product_id
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_pos_sales").fetchone()[0]
    print(f"  ✓ fact_pos_sales: {count} rows")


def build_fact_ecommerce_sales(conn):
    print("Building fact_ecommerce_sales...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE fact_ecommerce_sales AS
        SELECT
            dd.date_key,
            e.platform,
            dp.product_key,
            e.qty,
            e.revenue
        FROM read_parquet({parquet_glob('ecommerce/ecommerce_orders')}) e
        JOIN dim_date    dd ON dd.date       = e.date
        JOIN dim_product dp ON dp.product_id = e.product_id
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_ecommerce_sales").fetchone()[0]
    print(f"  ✓ fact_ecommerce_sales: {count} rows")


def build_fact_inventory(conn):
    print("Building fact_inventory...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE fact_inventory AS
        SELECT
            dd.date_key,
            dp.product_key,
            inv.stock_on_hand
        FROM read_parquet({parquet_glob('supply_chain/inventory')}) inv
        JOIN dim_date    dd ON dd.date       = inv.date
        JOIN dim_product dp ON dp.product_id = inv.product_id
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_inventory").fetchone()[0]
    print(f"  ✓ fact_inventory: {count} rows")


# ══════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════

def run_all():
    conn = get_conn()
    try:
        print("\n=== Building Data Warehouse ===\n")
        print("--- Dimensions ---")
        build_dim_date(conn)
        build_dim_product(conn)
        build_dim_distributor(conn)
        build_dim_store(conn)
        build_dim_promotion(conn)

        print("\n--- Fact Tables ---")
        build_fact_sellin(conn)
        build_fact_sellout(conn)
        build_fact_pos_sales(conn)
        build_fact_ecommerce_sales(conn)
        build_fact_inventory(conn)

        print("\n✅ Data Warehouse built successfully!")
        print(f"   Database: {DW_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    run_all()
