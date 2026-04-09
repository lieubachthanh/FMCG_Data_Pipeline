"""
Metabase Dashboard Setup Script
Auto-creates dashboards via Metabase API after first login.
Run once after Metabase is up: python3 config/setup_metabase.py
"""

import requests
import json
import sys

METABASE_URL = "http://localhost:3000"
EMAIL        = "admin@fmcg.local"
PASSWORD     = "admin123"
DW_PATH      = "/opt/fmcg_platform/data_lake/fmcg_warehouse.duckdb"


def get_token():
    r = requests.post(f"{METABASE_URL}/api/session",
                      json={"username": EMAIL, "password": PASSWORD})
    r.raise_for_status()
    return r.json()["id"]


def headers(token):
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}


def add_duckdb_database(token):
    """Register the DuckDB warehouse as a Metabase data source."""
    payload = {
        "name":    "FMCG DuckDB Warehouse",
        "engine":  "duckdb",
        "details": {"db": DW_PATH, "read_only": True},
    }
    r = requests.post(f"{METABASE_URL}/api/database",
                      headers=headers(token), json=payload)
    if r.status_code in (200, 201):
        db_id = r.json()["id"]
        print(f"✓ Database added (id={db_id})")
        return db_id
    else:
        print(f"⚠ Could not add database: {r.text}")
        return 1  # assume id=1


def create_card(token, db_id, name, description, sql):
    payload = {
        "name":           name,
        "description":    description,
        "display":        "table",
        "dataset_query":  {
            "type":     "native",
            "database": db_id,
            "native":   {"query": sql},
        },
        "visualization_settings": {},
    }
    r = requests.post(f"{METABASE_URL}/api/card",
                      headers=headers(token), json=payload)
    r.raise_for_status()
    card_id = r.json()["id"]
    print(f"  ✓ Card '{name}' created (id={card_id})")
    return card_id


def create_dashboard(token, name, description):
    r = requests.post(f"{METABASE_URL}/api/dashboard",
                      headers=headers(token),
                      json={"name": name, "description": description})
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"✓ Dashboard '{name}' created (id={dash_id})")
    return dash_id


def add_card_to_dashboard(token, dash_id, card_id, row, col, size_x=12, size_y=8):
    r = requests.post(f"{METABASE_URL}/api/dashboard/{dash_id}/cards",
                      headers=headers(token),
                      json={"cardId": card_id, "row": row, "col": col,
                            "size_x": size_x, "size_y": size_y})
    r.raise_for_status()


# ── SQL queries for each dashboard card ──────────────────────────────────────

QUERIES = {
    # Sales Dashboard
    "Monthly Revenue by Channel": (
        "line",
        """
        SELECT year, month, channel,
               SUM(revenue) AS total_revenue
        FROM sales_mart
        GROUP BY year, month, channel
        ORDER BY year, month
        """
    ),
    "SI vs SO Summary": (
        "bar",
        """
        SELECT year, month, brand,
               SUM(CASE WHEN channel='Sell-In'          THEN revenue ELSE 0 END) AS sellin,
               SUM(CASE WHEN channel='Sell-Out GT'       THEN revenue ELSE 0 END) AS sellout_gt,
               SUM(CASE WHEN channel='POS Modern Trade'  THEN revenue ELSE 0 END) AS pos_mt
        FROM sales_mart
        GROUP BY year, month, brand
        ORDER BY year, month
        """
    ),
    "Sales by Region": (
        "bar",
        """
        SELECT year, quarter, region, SUM(revenue) AS revenue
        FROM sales_mart
        WHERE region IS NOT NULL
        GROUP BY year, quarter, region
        ORDER BY year, quarter
        """
    ),
    "Top Brands by Revenue": (
        "row",
        """
        SELECT brand, SUM(revenue) AS total_revenue
        FROM sales_mart
        GROUP BY brand
        ORDER BY total_revenue DESC
        """
    ),
    "Ecommerce Growth by Platform": (
        "line",
        """
        SELECT year, month, platform, SUM(revenue) AS revenue
        FROM fact_ecommerce_sales fe
        JOIN dim_date dd ON fe.date_key = dd.date_key
        JOIN dim_product dp ON fe.product_key = dp.product_key
        GROUP BY year, month, platform
        ORDER BY year, month
        """
    ),

    # Promotion Dashboard
    "Promotion Uplift by Type": (
        "bar",
        """
        SELECT promotion_type,
               AVG(uplift_pct) AS avg_uplift_pct,
               SUM(CASE WHEN on_promotion=1 THEN revenue END) AS promo_revenue,
               SUM(CASE WHEN on_promotion=0 THEN revenue END) AS baseline_revenue
        FROM promotion_mart
        GROUP BY promotion_type
        """
    ),
    "Promotion vs Non-Promotion Sales": (
        "pie",
        """
        SELECT
          CASE WHEN on_promotion=1 THEN 'On Promotion' ELSE 'No Promotion' END AS status,
          SUM(revenue) AS revenue
        FROM promotion_mart
        GROUP BY status
        """
    ),
    "Top Promoted Products": (
        "table",
        """
        SELECT product_id, promotion_type,
               COUNT(*) AS promo_days,
               AVG(uplift_pct) AS avg_uplift,
               SUM(revenue) AS total_promo_revenue
        FROM promotion_mart
        WHERE on_promotion = 1
        GROUP BY product_id, promotion_type
        ORDER BY avg_uplift DESC
        LIMIT 20
        """
    ),

    # Innovation Dashboard
    "New Product Adoption Curve": (
        "line",
        """
        SELECT sku_name, month_since_launch,
               AVG(adoption_rate_pct) AS avg_adoption_rate,
               SUM(revenue) AS revenue
        FROM innovation_mart
        GROUP BY sku_name, month_since_launch
        ORDER BY sku_name, month_since_launch
        """
    ),
    "Innovation Revenue by Brand": (
        "bar",
        """
        SELECT brand, month_since_launch, SUM(revenue) AS revenue
        FROM innovation_mart
        GROUP BY brand, month_since_launch
        ORDER BY brand, month_since_launch
        """
    ),
    "Cohort Revenue after Launch": (
        "table",
        """
        SELECT sku_name, launch_date, channel,
               MAX(CASE WHEN month_since_launch=1  THEN revenue END) AS month_1,
               MAX(CASE WHEN month_since_launch=3  THEN revenue END) AS month_3,
               MAX(CASE WHEN month_since_launch=6  THEN revenue END) AS month_6,
               MAX(CASE WHEN month_since_launch=12 THEN revenue END) AS month_12
        FROM innovation_mart
        GROUP BY sku_name, launch_date, channel
        ORDER BY launch_date
        """
    ),
}

DASHBOARDS = {
    "📊 Sales Dashboard":     ["Monthly Revenue by Channel", "SI vs SO Summary",
                               "Sales by Region", "Top Brands by Revenue",
                               "Ecommerce Growth by Platform"],
    "🎯 Promotion Dashboard": ["Promotion Uplift by Type",
                               "Promotion vs Non-Promotion Sales",
                               "Top Promoted Products"],
    "🚀 Innovation Dashboard":["New Product Adoption Curve",
                               "Innovation Revenue by Brand",
                               "Cohort Revenue after Launch"],
}


def main():
    print("Connecting to Metabase...")
    try:
        token = get_token()
    except Exception as e:
        print(f"❌ Could not connect to Metabase at {METABASE_URL}: {e}")
        sys.exit(1)

    print("Adding DuckDB database...")
    db_id = add_duckdb_database(token)

    print("\nCreating question cards...")
    card_ids = {}
    for card_name, (display, sql) in QUERIES.items():
        try:
            cid = create_card(token, db_id, card_name, "", sql)
            card_ids[card_name] = cid
        except Exception as e:
            print(f"  ⚠ Failed to create card '{card_name}': {e}")

    print("\nCreating dashboards...")
    for dash_name, card_names in DASHBOARDS.items():
        dash_id = create_dashboard(token, dash_name, "")
        row = 0
        for card_name in card_names:
            if card_name in card_ids:
                add_card_to_dashboard(token, dash_id, card_ids[card_name],
                                      row=row, col=0, size_x=18, size_y=8)
                row += 8

    print("\n✅ Metabase setup complete!")
    print(f"   Open: {METABASE_URL}")
    print(f"   Login: {EMAIL} / {PASSWORD}")


if __name__ == "__main__":
    main()
