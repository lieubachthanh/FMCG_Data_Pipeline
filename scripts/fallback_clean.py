"""
Fallback Clean Layer (Pandas)
Used when spark-submit is not available.
Replicates all Spark clean_layer transformations using pandas + pyarrow.
"""

import os
import pandas as pd

RAW_BASE   = os.environ.get("RAW_BASE",   "data_lake/raw")
CLEAN_BASE = os.environ.get("CLEAN_BASE", "data_lake/clean")


def save_parquet(df: pd.DataFrame, rel_path: str):
    out = os.path.join(CLEAN_BASE, rel_path)
    os.makedirs(out, exist_ok=True)
    df.to_parquet(os.path.join(out, "data.parquet"), index=False)
    print(f"  ✓ {rel_path}: {len(df)} rows")


def read_csv(rel_path: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(RAW_BASE, rel_path))


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    return df


def process(name: str, src: str, dst: str, key_cols: list,
            date_cols: list = None, int_cols: list = None,
            float_cols: list = None, positive_col: str = None):
    df = read_csv(src)
    df = normalize_cols(df)
    if date_cols:
        for c in date_cols:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    if int_cols:
        for c in int_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if float_cols:
        for c in float_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=key_cols).drop_duplicates()
    if positive_col:
        df = df[df[positive_col] > 0]
    save_parquet(df, dst)


print("\n=== Pandas Clean Layer (Spark fallback) ===\n")

# ERP
process("product_master",    "erp/product_master.csv",    "erp/product_master",
        ["product_id","sku_code"], date_cols=["launch_date"], int_cols=["innovation_flag"])

process("distributor_master","erp/distributor_master.csv","erp/distributor_master",
        ["distributor_id"])

process("sellin",            "erp/sellin.csv",            "erp/sellin",
        ["date","distributor_id","product_id"],
        date_cols=["date"], int_cols=["qty"],
        float_cols=["gross_sales","discount","net_sales"], positive_col="qty")

# DMS
process("retailer_master",   "dms/retailer_master.csv",   "dms/retailer_master",
        ["retailer_id","distributor_id"])

process("sellout",           "dms/sellout.csv",           "dms/sellout",
        ["date","retailer_id","product_id"],
        date_cols=["date"], int_cols=["qty"], float_cols=["revenue"], positive_col="qty")

# POS
process("store_master",      "pos/store_master.csv",      "pos/store_master",
        ["store_id"])

process("pos_sales",         "pos/pos_sales.csv",         "pos/pos_sales",
        ["date","store_id","product_id"],
        date_cols=["date"], int_cols=["qty"], float_cols=["revenue"], positive_col="qty")

# Supply Chain
process("inventory",         "supply_chain/inventory.csv","supply_chain/inventory",
        ["date","product_id"], date_cols=["date"], int_cols=["stock_on_hand"])

# Marketing
df_promo = read_csv("marketing/promotions.csv")
df_promo = normalize_cols(df_promo)
df_promo["start_date"] = pd.to_datetime(df_promo["start_date"], errors="coerce").dt.date
df_promo["end_date"]   = pd.to_datetime(df_promo["end_date"],   errors="coerce").dt.date
df_promo["discount_percent"] = pd.to_numeric(df_promo["discount_percent"], errors="coerce")
df_promo = df_promo.dropna(subset=["promotion_id","product_id"]).drop_duplicates()
df_promo = df_promo[df_promo["start_date"] <= df_promo["end_date"]]
save_parquet(df_promo, "marketing/promotions")

# Innovation
process("innovation_master", "innovation/innovation_master.csv","innovation/innovation_master",
        ["innovation_id","product_id"], date_cols=["launch_date"])

# SFA
process("store_visits",      "sfa/store_visits.csv",      "sfa/store_visits",
        ["date","retailer_id","sales_rep_id"], date_cols=["date"])

# Ecommerce
process("ecommerce_orders",  "ecommerce/ecommerce_orders.csv","ecommerce/ecommerce_orders",
        ["date","platform","product_id"],
        date_cols=["date"], int_cols=["qty"], float_cols=["revenue"], positive_col="qty")

# External
process("market_share",      "external/market_share.csv", "external/market_share",
        ["year","brand"], int_cols=["year"], float_cols=["market_share"])

print("\n✅ Pandas clean layer complete!")
