# 🏭 FMCG Data Engineering Platform

End-to-end data engineering platform for a **non-alcoholic beverages FMCG company** in Vietnam.
Covers **Jan 2023 – Dec 2025** with synthetic but realistic data.

---

## 🏗 Architecture

```
Synthetic Data Generator (Python)
        ↓  CSV
Raw Data Lake  (data_lake/raw/)
        ↓  PySpark
Clean Layer    (data_lake/clean/  — Parquet)
        ↓  DuckDB SQL
Star Schema Warehouse  (fmcg_warehouse.duckdb)
        ↓  SQL
Data Marts  (sales_mart / promotion_mart / innovation_mart)
        ↓  Metabase API
BI Dashboards  (Sales / Promotion / Innovation)
        ↑  Orchestrated by
Apache Airflow DAG  (fmcg_data_pipeline)
```

---

## 📁 Project Structure

```
fmcg_platform/
├── scripts/
│   ├── generate_data.py       # Synthetic data generator
│   └── fallback_clean.py      # Pandas clean (no-Spark fallback)
├── spark_jobs/
│   └── clean_layer.py         # PySpark: raw CSV → clean Parquet
├── warehouse/
│   └── build_warehouse.py     # DuckDB star schema builder
├── data_marts/
│   └── build_marts.py         # Analytics marts + BI views
├── dags/
│   └── fmcg_pipeline.py       # Airflow DAG (full pipeline)
├── config/
│   └── setup_metabase.py      # Auto-creates Metabase dashboards
├── data_lake/
│   ├── raw/                   # CSV source data
│   ├── clean/                 # Parquet clean data
│   ├── mart/                  # (optional export)
│   └── fmcg_warehouse.duckdb  # DuckDB database (auto-created)
├── docker-compose.yml         # Airflow + Metabase
├── requirements.txt
└── run_local.sh               # Run full pipeline locally
```

---

## 🚀 Quick Start

### Option A — Local (No Docker)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the full pipeline
chmod +x run_local.sh
./run_local.sh

# 3. Explore the data
duckdb data_lake/fmcg_warehouse.duckdb
```

### Option B — Docker (Airflow + Metabase)

```bash
# 1. Start services
docker-compose up -d

# 2. Wait ~2 minutes, then open:
#    Airflow  → http://localhost:8080  (admin / admin123)
#    Metabase → http://localhost:3000

# 3. Trigger the DAG (or wait for nightly run at 3 AM)
docker-compose exec airflow-webserver \
    airflow dags trigger fmcg_data_pipeline

# 4. Set up Metabase dashboards
pip install requests
python3 config/setup_metabase.py
```

---

## 📊 Data Sources Simulated

| System | Tables | Description |
|--------|--------|-------------|
| ERP | product_master, distributor_master, sellin | Company → Distributor sales |
| DMS | retailer_master, sellout | Distributor → Retailer sales |
| POS | store_master, pos_sales | Modern Trade (Winmart, BigC, etc.) |
| Supply Chain | inventory | Daily stock snapshots |
| Marketing | promotions | Discount / bundle / display promos |
| Innovation | innovation_master | New product launches |
| SFA | store_visits | Field sales rep visits |
| Ecommerce | ecommerce_orders | Shopee, Lazada, Tiki |
| External | market_share | VietRefresh vs competitors |

---

## ⭐ Data Warehouse — Star Schema

### Dimensions
- `dim_date` — Full calendar with season tags (Tet, Summer, Year-End)
- `dim_product` — SKU, brand, category, innovation flag
- `dim_store` — Unified GT + MT stores
- `dim_distributor` — Region, province
- `dim_promotion` — Type, discount, validity

### Fact Tables
- `fact_sellin` — Sell-in transactions
- `fact_sellout` — Distributor → Retailer sell-out
- `fact_pos_sales` — Modern trade POS
- `fact_ecommerce_sales` — Online platforms
- `fact_inventory` — Weekly stock levels

---

## 📈 Analytics Marts

### `sales_mart`
Unified cross-channel view (SI / SO GT / POS MT / Ecommerce) with region, brand, season.

### `promotion_mart`
Per-product daily sales tagged with active promotions, 30-day baseline, and uplift %.

### `innovation_mart`
Cohort analysis: month-since-launch, adoption rate curve, cumulative revenue per new SKU.

### BI Views
| View | Purpose |
|------|---------|
| `v_monthly_sales` | Monthly revenue by channel and brand |
| `v_si_vs_so` | Sell-In vs Sell-Out gap analysis |
| `v_innovation_cohort` | Month-by-month new product ramp |
| `v_promotion_effectiveness` | Uplift by promotion type |
| `v_regional_performance` | Region × channel quarterly |

---

## 🔄 Airflow Pipeline — `fmcg_data_pipeline`

```
generate_raw_data
        ↓
convert_masters_to_parquet
        ↓
spark_clean_layer
        ↓
build_dimensions
        ↓
build_fact_tables
        ↓
build_data_marts
        ↓
refresh_dashboard
```

Schedule: **Daily at 03:00**

---

## 📉 Realistic Data Patterns

| Pattern | Implementation |
|---------|---------------|
| Seasonality | Tet (Jan–Feb) ×1.6, Summer (May–Aug) ×1.4 |
| Promotion uplift | Compared to 30-day rolling baseline |
| Innovation S-curve | Ramps 10% → 100% over 6 months |
| E-commerce growth | +40% YoY across all platforms |
| Market share shift | VietRefresh gains 2pp/year vs competitors |

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Generation | Python, Pandas, NumPy, Faker |
| Processing | Apache Spark (PySpark) |
| Data Lake | Local filesystem (Parquet) |
| Warehouse | DuckDB |
| Orchestration | Apache Airflow 2.9 |
| BI / Dashboards | Metabase |
| Containers | Docker Compose |

---

## 💡 DuckDB Quick Queries

```sql
-- Monthly revenue by channel
SELECT year, month, channel, SUM(revenue) revenue
FROM sales_mart
GROUP BY 1,2,3 ORDER BY 1,2;

-- Sell-In vs Sell-Out gap
SELECT * FROM v_si_vs_so LIMIT 20;

-- Innovation cohort — month-by-month
SELECT * FROM v_innovation_cohort
WHERE sku_name LIKE '%VietRefresh%'
ORDER BY month_since_launch;

-- Promotion uplift
SELECT * FROM v_promotion_effectiveness ORDER BY avg_uplift_pct DESC;

-- Regional performance
SELECT * FROM v_regional_performance WHERE year = 2025;
```
