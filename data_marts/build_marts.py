"""
Data Marts Builder
Builds analytics-ready marts from DuckDB warehouse
: sales_mart, promotion_mart, innovation_mart
"""

import os
import duckdb

DW_PATH = os.environ.get("DW_PATH", "data_lake/fmcg_warehouse.duckdb")


def get_conn():
    return duckdb.connect(DW_PATH)


# ══════════════════════════════════════════════════
# SALES MART
# Unified view across SI / SO / POS / Ecommerce
# ══════════════════════════════════════════════════

def build_sales_mart(conn):
    print("Building sales_mart...")
    conn.execute("""
        CREATE OR REPLACE TABLE sales_mart AS

        -- Sell-In (Company → Distributor)
        SELECT
            dd.date                 AS date,
            dd.year,
            dd.quarter,
            dd.month,
            dd.season,
            'Sell-In'               AS channel,
            dp.brand,
            dp.category,
            dp.sub_category,
            dp.sku_name             AS product_name,
            dp.pack_size,
            ddist.region,
            ddist.province,
            NULL                    AS store_type,
            fs.qty,
            fs.net_sales            AS revenue,
            fs.gross_sales,
            fs.discount
        FROM fact_sellin fs
        JOIN dim_date        dd    ON fs.date_key        = dd.date_key
        JOIN dim_product     dp    ON fs.product_key     = dp.product_key
        JOIN dim_distributor ddist ON fs.distributor_key = ddist.distributor_key

        UNION ALL

        -- Sell-Out General Trade (Distributor → Retailer)
        SELECT
            dd.date,
            dd.year, dd.quarter, dd.month, dd.season,
            'Sell-Out GT'           AS channel,
            dp.brand, dp.category, dp.sub_category, dp.sku_name, dp.pack_size,
            ds.region, ds.province,
            ds.store_type,
            fso.qty,
            fso.revenue,
            fso.revenue             AS gross_sales,
            0.0                     AS discount
        FROM fact_sellout fso
        JOIN dim_date    dd ON fso.date_key    = dd.date_key
        JOIN dim_product dp ON fso.product_key = dp.product_key
        JOIN dim_store   ds ON fso.store_key   = ds.store_key

        UNION ALL

        -- POS Modern Trade
        SELECT
            dd.date,
            dd.year, dd.quarter, dd.month, dd.season,
            'POS Modern Trade'      AS channel,
            dp.brand, dp.category, dp.sub_category, dp.sku_name, dp.pack_size,
            ds.region, ds.province,
            ds.store_type,
            fp.qty,
            fp.revenue,
            fp.revenue              AS gross_sales,
            0.0                     AS discount
        FROM fact_pos_sales fp
        JOIN dim_date    dd ON fp.date_key    = dd.date_key
        JOIN dim_product dp ON fp.product_key = dp.product_key
        JOIN dim_store   ds ON fp.store_key   = ds.store_key

        UNION ALL

        -- Ecommerce
        SELECT
            dd.date,
            dd.year, dd.quarter, dd.month, dd.season,
            'Ecommerce'             AS channel,
            dp.brand, dp.category, dp.sub_category, dp.sku_name, dp.pack_size,
            NULL                    AS region,
            NULL                    AS province,
            'Online'                AS store_type,
            fe.qty,
            fe.revenue,
            fe.revenue              AS gross_sales,
            0.0                     AS discount
        FROM fact_ecommerce_sales fe
        JOIN dim_date    dd ON fe.date_key    = dd.date_key
        JOIN dim_product dp ON fe.product_key = dp.product_key
    """)
    count = conn.execute("SELECT COUNT(*) FROM sales_mart").fetchone()[0]
    print(f"  ✓ sales_mart: {count} rows")


# ══════════════════════════════════════════════════
# PROMOTION MART
# Promotion vs Non-Promotion sales, uplift analysis
# ══════════════════════════════════════════════════

def build_promotion_mart(conn):
    print("Building promotion_mart...")
    conn.execute("""
        CREATE OR REPLACE TABLE promotion_mart AS
        WITH daily_pos AS (
            SELECT
                dd.date,
                dp.product_id,
                dp.brand,
                dp.category,
                SUM(fp.qty)     AS total_qty,
                SUM(fp.revenue) AS total_revenue
            FROM fact_pos_sales fp
            JOIN dim_date    dd ON fp.date_key    = dd.date_key
            JOIN dim_product dp ON fp.product_key = dp.product_key
            GROUP BY dd.date, dp.product_id, dp.brand, dp.category
        ),
        promo_active AS (
            SELECT
                dm.product_id,
                dm.promotion_id,
                dm.promotion_type,
                dm.discount_percent,
                dd.date
            FROM dim_promotion dm
            JOIN dim_date dd ON dd.date BETWEEN dm.start_date AND dm.end_date
        )
        SELECT
            p.date,
            p.product_id,
            p.brand,
            p.category,
            COALESCE(pa.promotion_id,   'NONE')        AS promotion_id,
            COALESCE(pa.promotion_type, 'No Promotion') AS promotion_type,
            COALESCE(pa.discount_percent, 0)            AS discount_percent,
            CASE WHEN pa.promotion_id IS NOT NULL THEN 1 ELSE 0 END AS on_promotion,
            p.total_qty                                 AS qty,
            p.total_revenue                             AS revenue,
            -- Uplift: compare promo day vs avg non-promo for same product (approximation)
            AVG(p.total_revenue) OVER (
                PARTITION BY p.product_id
                ORDER BY p.date
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            )                                           AS baseline_revenue_30d
        FROM daily_pos p
        LEFT JOIN promo_active pa
            ON pa.product_id = p.product_id AND pa.date = p.date
    """)

    # Add uplift calculation
    conn.execute("""
        CREATE OR REPLACE TABLE promotion_mart AS
        SELECT
            *,
            CASE
                WHEN on_promotion = 1 AND baseline_revenue_30d > 0
                THEN ROUND((revenue - baseline_revenue_30d) / baseline_revenue_30d * 100, 2)
                ELSE 0
            END AS uplift_pct
        FROM promotion_mart
    """)
    count = conn.execute("SELECT COUNT(*) FROM promotion_mart").fetchone()[0]
    print(f"  ✓ promotion_mart: {count} rows")


# ══════════════════════════════════════════════════
# INNOVATION MART
# New product cohort analysis by months since launch
# ══════════════════════════════════════════════════

def build_innovation_mart(conn):
    print("Building innovation_mart...")
    conn.execute("""
        CREATE OR REPLACE TABLE innovation_mart AS
        WITH innovation_sales AS (
            -- POS Sales for innovation products
            SELECT
                dd.date,
                dp.product_id,
                dp.brand,
                dp.category,
                dp.sub_category,
                dp.launch_date,
                dp.sku_name,
                SUM(fp.qty)     AS qty,
                SUM(fp.revenue) AS revenue,
                'POS'           AS channel
            FROM fact_pos_sales fp
            JOIN dim_date    dd ON fp.date_key    = dd.date_key
            JOIN dim_product dp ON fp.product_key = dp.product_key
            WHERE dp.innovation_flag = 1
            GROUP BY dd.date, dp.product_id, dp.brand, dp.category,
                     dp.sub_category, dp.launch_date, dp.sku_name

            UNION ALL

            -- Sellout GT Sales for innovation products
            SELECT
                dd.date,
                dp.product_id,
                dp.brand,
                dp.category,
                dp.sub_category,
                dp.launch_date,
                dp.sku_name,
                SUM(fso.qty)     AS qty,
                SUM(fso.revenue) AS revenue,
                'GT'             AS channel
            FROM fact_sellout fso
            JOIN dim_date    dd  ON fso.date_key    = dd.date_key
            JOIN dim_product dp  ON fso.product_key = dp.product_key
            WHERE dp.innovation_flag = 1
            GROUP BY dd.date, dp.product_id, dp.brand, dp.category,
                     dp.sub_category, dp.launch_date, dp.sku_name
        ),
        with_months AS (
            SELECT
                *,
                -- Months since launch (for cohort analysis)
                DATEDIFF('month', launch_date, date) AS month_since_launch,
                -- Running cumulative revenue per product
                SUM(revenue) OVER (
                    PARTITION BY product_id, channel
                    ORDER BY date
                    ROWS UNBOUNDED PRECEDING
                ) AS cumulative_revenue
            FROM innovation_sales
        )
        SELECT
            date,
            product_id,
            brand,
            category,
            sub_category,
            sku_name,
            launch_date,
            channel,
            month_since_launch,
            qty,
            revenue,
            cumulative_revenue,
            -- Adoption rate: ratio vs peak monthly revenue in first year
            ROUND(
                revenue / NULLIF(
                    MAX(revenue) OVER (
                        PARTITION BY product_id, channel
                        ORDER BY date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ), 0
                ) * 100, 2
            ) AS adoption_rate_pct
        FROM with_months
        WHERE month_since_launch >= 0
        ORDER BY product_id, channel, date
    """)
    count = conn.execute("SELECT COUNT(*) FROM innovation_mart").fetchone()[0]
    print(f"  ✓ innovation_mart: {count} rows")


# ══════════════════════════════════════════════════
# CONVENIENCE VIEWS for Metabase / BI
# ══════════════════════════════════════════════════

def build_views(conn):
    print("Building BI views...")

    # Monthly sales summary
    conn.execute("""
        CREATE OR REPLACE VIEW v_monthly_sales AS
        SELECT
            year, month,
            channel,
            brand,
            category,
            region,
            SUM(qty)     AS total_qty,
            SUM(revenue) AS total_revenue
        FROM sales_mart
        GROUP BY year, month, channel, brand, category, region
        ORDER BY year, month
    """)

    # Sell-In vs Sell-Out comparison
    conn.execute("""
        CREATE OR REPLACE VIEW v_si_vs_so AS
        SELECT
            year, month, brand, region,
            SUM(CASE WHEN channel = 'Sell-In'      THEN revenue ELSE 0 END) AS sellin_revenue,
            SUM(CASE WHEN channel = 'Sell-Out GT'  THEN revenue ELSE 0 END) AS sellout_gt_revenue,
            SUM(CASE WHEN channel = 'POS Modern Trade' THEN revenue ELSE 0 END) AS pos_revenue,
            SUM(CASE WHEN channel = 'Ecommerce'    THEN revenue ELSE 0 END) AS ecom_revenue
        FROM sales_mart
        GROUP BY year, month, brand, region
    """)

    # Innovation cohort monthly rollup
    conn.execute("""
        CREATE OR REPLACE VIEW v_innovation_cohort AS
        SELECT
            brand,
            sku_name,
            launch_date,
            month_since_launch,
            SUM(qty)              AS total_qty,
            SUM(revenue)          AS total_revenue,
            AVG(adoption_rate_pct) AS avg_adoption_rate
        FROM innovation_mart
        GROUP BY brand, sku_name, launch_date, month_since_launch
        ORDER BY brand, sku_name, month_since_launch
    """)

    # Promotion effectiveness
    conn.execute("""
        CREATE OR REPLACE VIEW v_promotion_effectiveness AS
        SELECT
            pm.promotion_type,
            dp.brand,
            dp.category,
            COUNT(DISTINCT pm.promotion_id)                            AS num_promotions,
            AVG(pm.discount_percent)                                   AS avg_discount,
            SUM(CASE WHEN pm.on_promotion=1 THEN pm.revenue ELSE 0 END) AS promo_revenue,
            SUM(CASE WHEN pm.on_promotion=0 THEN pm.revenue ELSE 0 END) AS non_promo_revenue,
            AVG(CASE WHEN pm.on_promotion=1 THEN pm.uplift_pct ELSE NULL END) AS avg_uplift_pct
        FROM promotion_mart pm
        JOIN dim_product dp ON pm.product_id = dp.product_id
        GROUP BY pm.promotion_type, dp.brand, dp.category
    """)

    # Regional performance
    conn.execute("""
        CREATE OR REPLACE VIEW v_regional_performance AS
        SELECT
            year, quarter, region,
            channel,
            SUM(qty)     AS total_qty,
            SUM(revenue) AS total_revenue,
            COUNT(DISTINCT product_name) AS active_skus
        FROM sales_mart
        WHERE region IS NOT NULL
        GROUP BY year, quarter, region, channel
    """)

    print("  ✓ BI views created (v_monthly_sales, v_si_vs_so, v_innovation_cohort,")
    print("    v_promotion_effectiveness, v_regional_performance)")


# ══════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════

def run_all():
    conn = get_conn()
    try:
        print("\n=== Building Data Marts ===\n")
        build_sales_mart(conn)
        build_promotion_mart(conn)
        build_innovation_mart(conn)
        build_views(conn)
        print("\n✅ Data Marts built successfully!")
    finally:
        conn.close()


if __name__ == "__main__":
    run_all()
