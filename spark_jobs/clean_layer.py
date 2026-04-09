"""
Spark Clean Layer Job
Raw CSV → Clean Parquet
Handles: schema normalization, type casting, null handling, deduplication
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, LongType
)

RAW_BASE  = os.environ.get("RAW_BASE",   "data_lake/raw")
CLEAN_BASE = os.environ.get("CLEAN_BASE", "data_lake/clean")


def get_spark():
    return (
        SparkSession.builder
        .appName("FMCG_CleanLayer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Generic helpers
# ─────────────────────────────────────────────
def read_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def write_parquet(df, path):
    os.makedirs(path, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"  ✓ Written → {path}")


def normalize_columns(df):
    """Lowercase column names, strip whitespace."""
    return df.toDF(*[c.lower().strip().replace(" ", "_") for c in df.columns])


def cast_date(df, col_name):
    return df.withColumn(col_name, F.to_date(F.col(col_name)))


def deduplicate(df):
    return df.dropDuplicates()


def drop_nulls_key(df, key_cols):
    return df.dropna(subset=key_cols)


# ─────────────────────────────────────────────
# ERP
# ─────────────────────────────────────────────
def clean_product_master(spark):
    print("Cleaning product_master...")
    df = read_csv(spark, f"{RAW_BASE}/erp/product_master.csv")
    df = normalize_columns(df)
    df = cast_date(df, "launch_date")
    df = drop_nulls_key(df, ["product_id", "sku_code"])
    df = deduplicate(df)
    df = df.withColumn("innovation_flag", F.col("innovation_flag").cast(IntegerType()))
    write_parquet(df, f"{CLEAN_BASE}/erp/product_master")


def clean_distributor_master(spark):
    print("Cleaning distributor_master...")
    df = read_csv(spark, f"{RAW_BASE}/erp/distributor_master.csv")
    df = normalize_columns(df)
    df = drop_nulls_key(df, ["distributor_id"])
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/erp/distributor_master")


def clean_sellin(spark):
    print("Cleaning sellin...")
    df = read_csv(spark, f"{RAW_BASE}/erp/sellin.csv")
    df = normalize_columns(df)
    df = cast_date(df, "date")
    df = drop_nulls_key(df, ["date", "distributor_id", "product_id"])
    df = df.withColumn("qty",        F.col("qty").cast(IntegerType()))
    df = df.withColumn("gross_sales", F.col("gross_sales").cast(DoubleType()))
    df = df.withColumn("discount",    F.col("discount").cast(DoubleType()))
    df = df.withColumn("net_sales",   F.col("net_sales").cast(DoubleType()))
    df = df.filter(F.col("qty") > 0)
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/erp/sellin")


# ─────────────────────────────────────────────
# DMS
# ─────────────────────────────────────────────
def clean_retailer_master(spark):
    print("Cleaning retailer_master...")
    df = read_csv(spark, f"{RAW_BASE}/dms/retailer_master.csv")
    df = normalize_columns(df)
    df = drop_nulls_key(df, ["retailer_id", "distributor_id"])
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/dms/retailer_master")


def clean_sellout(spark):
    print("Cleaning sellout...")
    df = read_csv(spark, f"{RAW_BASE}/dms/sellout.csv")
    df = normalize_columns(df)
    df = cast_date(df, "date")
    df = drop_nulls_key(df, ["date", "retailer_id", "product_id"])
    df = df.withColumn("qty",     F.col("qty").cast(IntegerType()))
    df = df.withColumn("revenue", F.col("revenue").cast(DoubleType()))
    df = df.filter(F.col("qty") > 0)
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/dms/sellout")


# ─────────────────────────────────────────────
# POS
# ─────────────────────────────────────────────
def clean_store_master(spark):
    print("Cleaning store_master...")
    df = read_csv(spark, f"{RAW_BASE}/pos/store_master.csv")
    df = normalize_columns(df)
    df = drop_nulls_key(df, ["store_id"])
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/pos/store_master")


def clean_pos_sales(spark):
    print("Cleaning pos_sales...")
    df = read_csv(spark, f"{RAW_BASE}/pos/pos_sales.csv")
    df = normalize_columns(df)
    df = cast_date(df, "date")
    df = drop_nulls_key(df, ["date", "store_id", "product_id"])
    df = df.withColumn("qty",     F.col("qty").cast(IntegerType()))
    df = df.withColumn("revenue", F.col("revenue").cast(DoubleType()))
    df = df.filter(F.col("qty") > 0)
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/pos/pos_sales")


# ─────────────────────────────────────────────
# Supply Chain
# ─────────────────────────────────────────────
def clean_inventory(spark):
    print("Cleaning inventory...")
    df = read_csv(spark, f"{RAW_BASE}/supply_chain/inventory.csv")
    df = normalize_columns(df)
    df = cast_date(df, "date")
    df = drop_nulls_key(df, ["date", "product_id"])
    df = df.withColumn("stock_on_hand", F.col("stock_on_hand").cast(IntegerType()))
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/supply_chain/inventory")


# ─────────────────────────────────────────────
# Marketing
# ─────────────────────────────────────────────
def clean_promotions(spark):
    print("Cleaning promotions...")
    df = read_csv(spark, f"{RAW_BASE}/marketing/promotions.csv")
    df = normalize_columns(df)
    df = cast_date(df, "start_date")
    df = cast_date(df, "end_date")
    df = drop_nulls_key(df, ["promotion_id", "product_id"])
    df = df.filter(F.col("start_date") <= F.col("end_date"))
    df = df.withColumn("discount_percent", F.col("discount_percent").cast(DoubleType()))
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/marketing/promotions")


# ─────────────────────────────────────────────
# Innovation
# ─────────────────────────────────────────────
def clean_innovation_master(spark):
    print("Cleaning innovation_master...")
    df = read_csv(spark, f"{RAW_BASE}/innovation/innovation_master.csv")
    df = normalize_columns(df)
    df = cast_date(df, "launch_date")
    df = drop_nulls_key(df, ["innovation_id", "product_id"])
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/innovation/innovation_master")


# ─────────────────────────────────────────────
# SFA
# ─────────────────────────────────────────────
def clean_store_visits(spark):
    print("Cleaning store_visits...")
    df = read_csv(spark, f"{RAW_BASE}/sfa/store_visits.csv")
    df = normalize_columns(df)
    df = cast_date(df, "date")
    df = drop_nulls_key(df, ["date", "retailer_id", "sales_rep_id"])
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/sfa/store_visits")


# ─────────────────────────────────────────────
# Ecommerce
# ─────────────────────────────────────────────
def clean_ecommerce_orders(spark):
    print("Cleaning ecommerce_orders...")
    df = read_csv(spark, f"{RAW_BASE}/ecommerce/ecommerce_orders.csv")
    df = normalize_columns(df)
    df = cast_date(df, "date")
    df = drop_nulls_key(df, ["date", "platform", "product_id"])
    df = df.withColumn("qty",     F.col("qty").cast(IntegerType()))
    df = df.withColumn("revenue", F.col("revenue").cast(DoubleType()))
    df = df.filter(F.col("qty") > 0)
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/ecommerce/ecommerce_orders")


# ─────────────────────────────────────────────
# External
# ─────────────────────────────────────────────
def clean_market_share(spark):
    print("Cleaning market_share...")
    df = read_csv(spark, f"{RAW_BASE}/external/market_share.csv")
    df = normalize_columns(df)
    df = drop_nulls_key(df, ["year", "brand"])
    df = df.withColumn("year",         F.col("year").cast(IntegerType()))
    df = df.withColumn("market_share", F.col("market_share").cast(DoubleType()))
    df = deduplicate(df)
    write_parquet(df, f"{CLEAN_BASE}/external/market_share")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def run_all():
    spark = get_spark()
    try:
        print("\n=== Starting Clean Layer Processing ===\n")
        clean_product_master(spark)
        clean_distributor_master(spark)
        clean_sellin(spark)
        clean_retailer_master(spark)
        clean_sellout(spark)
        clean_store_master(spark)
        clean_pos_sales(spark)
        clean_inventory(spark)
        clean_promotions(spark)
        clean_innovation_master(spark)
        clean_store_visits(spark)
        clean_ecommerce_orders(spark)
        clean_market_share(spark)
        print("\n✅ Clean layer complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_all()
