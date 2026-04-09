"""
FMCG Synthetic Data Generator
Generates realistic data for Jan 2023 - Dec 2025
Non-alcoholic beverages industry
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import date, timedelta
from faker import Faker

fake = Faker("vi_VN")
random.seed(42)
np.random.seed(42)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "data_lake", "raw")

# ─────────────────────────────────────────────
# Date range
# ─────────────────────────────────────────────
START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 12, 31)
ALL_DATES = pd.date_range(START_DATE, END_DATE, freq="D")


# ─────────────────────────────────────────────
# Seasonality helpers
# ─────────────────────────────────────────────
def seasonal_multiplier(dt):
    """Higher demand in summer (May–Aug) and Tet (Jan–Feb)."""
    m = dt.month
    if m in (1, 2):      # Tet
        return 1.6
    if m in (5, 6, 7, 8):  # Summer
        return 1.4
    if m in (11, 12):    # Year-end gifting
        return 1.2
    return 1.0


def ecom_growth_multiplier(dt):
    """E-commerce grows ~40% YoY."""
    base = (dt.year - 2023) * 0.40
    return 1.0 + base


def innovation_adoption(launch_date, current_date):
    """S-curve adoption: low start, ramp up over 6 months."""
    months = (current_date.year - launch_date.year) * 12 + (current_date.month - launch_date.month)
    if months < 0:
        return 0.0
    if months == 0:
        return 0.1
    return min(1.0, 0.1 + 0.15 * months)


# ─────────────────────────────────────────────
# 1. ERP — product_master
# ─────────────────────────────────────────────
BRANDS = ["VietRefresh", "CoolDrink", "PureSip", "ZenWater", "FizzPop"]
CATEGORIES = ["Water", "Juice", "Energy Drink", "Tea", "Soda"]
SUB_CATS = {
    "Water":        ["Still Water", "Sparkling Water"],
    "Juice":        ["Orange Juice", "Mixed Fruit", "Coconut Water"],
    "Energy Drink": ["Classic Energy", "Sugar-Free Energy"],
    "Tea":          ["Green Tea", "Oolong Tea", "Lemon Tea"],
    "Soda":         ["Cola", "Lemon Soda", "Grape Soda"],
}
PACK_SIZES = ["250ml", "330ml", "500ml", "1L", "1.5L", "24-pack"]

products = []
sku_counter = 1000
for brand in BRANDS:
    for cat in CATEGORIES:
        for sub in SUB_CATS[cat][:1]:  # 1 sub-cat per cat per brand
            for pack in random.sample(PACK_SIZES, 2):
                launch = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 6, 1))
                innovation = 1 if launch >= date(2023, 1, 1) else 0
                products.append({
                    "product_id": f"PRD{sku_counter}",
                    "sku_code":   f"SKU{sku_counter}",
                    "sku_name":   f"{brand} {sub} {pack}",
                    "brand":      brand,
                    "category":   cat,
                    "sub_category": sub,
                    "pack_size":  pack,
                    "launch_date": launch,
                    "innovation_flag": innovation,
                })
                sku_counter += 1

df_products = pd.DataFrame(products)
df_products.to_csv(f"{BASE_DIR}/erp/product_master.csv", index=False, encoding="utf-8-sig")
print(f"✓ product_master: {len(df_products)} rows")


# ─────────────────────────────────────────────
# 2. ERP — distributor_master
# ─────────────────────────────────────────────
REGIONS = ["North", "Central", "South"]
PROVINCES = {
    "North":   ["Hanoi", "Hai Phong", "Bac Ninh", "Quang Ninh"],
    "Central": ["Da Nang", "Hue", "Quang Nam", "Binh Dinh"],
    "South":   ["Ho Chi Minh City", "Binh Duong", "Dong Nai", "Can Tho"],
}

distributors = []
for i in range(1, 41):
    region = random.choice(REGIONS)
    distributors.append({
        "distributor_id": f"DIST{i:03d}",
        "name":   fake.company(),
        "region": region,
        "province": random.choice(PROVINCES[region]),
    })
df_dist = pd.DataFrame(distributors)
df_dist.to_csv(f"{BASE_DIR}/erp/distributor_master.csv", index=False, encoding="utf-8-sig")
print(f"✓ distributor_master: {len(df_dist)} rows")


# ─────────────────────────────────────────────
# 3. ERP — sellin (company → distributors)
# ─────────────────────────────────────────────
product_ids = df_products["product_id"].tolist()
product_launches = dict(zip(df_products["product_id"], pd.to_datetime(df_products["launch_date"]).dt.date))
dist_ids = df_dist["distributor_id"].tolist()

sellin_rows = []
for dt in ALL_DATES[::7]:   # weekly sell-in
    d = dt.date()
    mult = seasonal_multiplier(d)
    for dist_id in random.sample(dist_ids, k=min(15, len(dist_ids))):
        for prod_id in random.sample(product_ids, k=random.randint(5, 15)):
            launch = product_launches[prod_id]
            if d < launch:
                continue
            adoption = innovation_adoption(launch, d)
            base_qty = random.randint(50, 500)
            qty = max(1, int(base_qty * mult * adoption))
            gross = round(qty * random.uniform(20_000, 80_000), 0)
            discount = round(gross * random.uniform(0.02, 0.10), 0)
            sellin_rows.append({
                "date": d,
                "distributor_id": dist_id,
                "product_id": prod_id,
                "qty": qty,
                "gross_sales": gross,
                "discount": discount,
                "net_sales": gross - discount,
            })

df_sellin = pd.DataFrame(sellin_rows)
df_sellin.to_csv(f"{BASE_DIR}/erp/sellin.csv", index=False, encoding="utf-8-sig")
print(f"✓ sellin: {len(df_sellin)} rows")


# ─────────────────────────────────────────────
# 4. DMS — retailer_master
# ─────────────────────────────────────────────
STORE_TYPES = ["Convenience", "Grocery", "Supermarket"]
retailers = []
for i in range(1, 501):
    region = random.choice(REGIONS)
    dist_id = random.choice(dist_ids)
    retailers.append({
        "retailer_id": f"RET{i:04d}",
        "name": fake.company() + " Store",
        "distributor_id": dist_id,
        "store_type": random.choice(STORE_TYPES),
        "region": region,
        "province": random.choice(PROVINCES[region]),
    })
df_retailers = pd.DataFrame(retailers)
df_retailers.to_csv(f"{BASE_DIR}/dms/retailer_master.csv", index=False, encoding="utf-8-sig")
print(f"✓ retailer_master: {len(df_retailers)} rows")


# ─────────────────────────────────────────────
# 5. DMS — sellout (distributors → retailers)
# ─────────────────────────────────────────────
retailer_ids = df_retailers["retailer_id"].tolist()
retailer_dist = dict(zip(df_retailers["retailer_id"], df_retailers["distributor_id"]))

sellout_rows = []
for dt in ALL_DATES[::3]:  # every 3 days
    d = dt.date()
    mult = seasonal_multiplier(d)
    for ret_id in random.sample(retailer_ids, k=min(80, len(retailer_ids))):
        for prod_id in random.sample(product_ids, k=random.randint(3, 8)):
            if d < product_launches[prod_id]:
                continue
            adoption = innovation_adoption(product_launches[prod_id], d)
            qty = max(1, int(random.randint(5, 60) * mult * adoption))
            revenue = round(qty * random.uniform(25_000, 90_000), 0)
            sellout_rows.append({
                "date": d,
                "retailer_id": ret_id,
                "distributor_id": retailer_dist[ret_id],
                "product_id": prod_id,
                "qty": qty,
                "revenue": revenue,
            })

df_sellout = pd.DataFrame(sellout_rows)
df_sellout.to_csv(f"{BASE_DIR}/dms/sellout.csv", index=False, encoding="utf-8-sig")
print(f"✓ sellout: {len(df_sellout)} rows")


# ─────────────────────────────────────────────
# 6. POS — store_master (Modern Trade)
# ─────────────────────────────────────────────
CHAINS = ["Winmart", "Coopmart", "Go", "Lotte"]
stores = []
for i in range(1, 201):
    region = random.choice(REGIONS)
    chain = random.choice(CHAINS)
    stores.append({
        "store_id":   f"STR{i:04d}",
        "store_name": f"{chain} {fake.city()}",
        "chain":      chain,
        "region":     region,
        "province":   random.choice(PROVINCES[region]),
    })
df_stores = pd.DataFrame(stores)
df_stores.to_csv(f"{BASE_DIR}/pos/store_master.csv", index=False, encoding="utf-8-sig")
print(f"✓ store_master: {len(df_stores)} rows")


# ─────────────────────────────────────────────
# 7. POS — pos_sales
# ─────────────────────────────────────────────
store_ids = df_stores["store_id"].tolist()
pos_rows = []
for dt in ALL_DATES:
    d = dt.date()
    mult = seasonal_multiplier(d)
    for store_id in random.sample(store_ids, k=min(30, len(store_ids))):
        for prod_id in random.sample(product_ids, k=random.randint(3, 10)):
            if d < product_launches[prod_id]:
                continue
            adoption = innovation_adoption(product_launches[prod_id], d)
            qty = max(1, int(random.randint(2, 30) * mult * adoption))
            revenue = round(qty * random.uniform(28_000, 95_000), 0)
            pos_rows.append({
                "date":       d,
                "store_id":   store_id,
                "product_id": prod_id,
                "qty":        qty,
                "revenue":    revenue,
            })

df_pos = pd.DataFrame(pos_rows)
df_pos.to_csv(f"{BASE_DIR}/pos/pos_sales.csv", index=False, encoding="utf-8-sig")
print(f"✓ pos_sales: {len(df_pos)} rows")


# ─────────────────────────────────────────────
# 8. Supply Chain — inventory
# ─────────────────────────────────────────────
inv_rows = []
for dt in ALL_DATES[::7]:  # weekly snapshot
    d = dt.date()
    for prod_id in product_ids:
        if d < product_launches[prod_id]:
            continue
        inv_rows.append({
            "date":         d,
            "product_id":   prod_id,
            "stock_on_hand": random.randint(100, 10_000),
        })
df_inv = pd.DataFrame(inv_rows)
df_inv.to_csv(f"{BASE_DIR}/supply_chain/inventory.csv", index=False, encoding="utf-8-sig")
print(f"✓ inventory: {len(df_inv)} rows")


# ─────────────────────────────────────────────
# 9. Marketing — promotions
# ─────────────────────────────────────────────
PROMO_TYPES = ["discount", "bundle", "display"]
promos = []
for i in range(1, 201):
    prod_id = random.choice(product_ids)
    start = fake.date_between(start_date=START_DATE, end_date=date(2025, 11, 30))
    end = start + timedelta(days=random.randint(7, 30))
    promos.append({
        "promotion_id":    f"PROMO{i:04d}",
        "product_id":       prod_id,
        "start_date":       start,
        "end_date":         end,
        "promotion_type":   random.choice(PROMO_TYPES),
        "discount_percent": round(random.uniform(5, 30), 1),
    })
df_promos = pd.DataFrame(promos)
df_promos.to_csv(f"{BASE_DIR}/marketing/promotions.csv", index=False, encoding="utf-8-sig")
print(f"✓ promotions: {len(df_promos)} rows")


# ─────────────────────────────────────────────
# 10. Innovation — innovation_master
# ─────────────────────────────────────────────
innovations = []
innovation_products = df_products[df_products["innovation_flag"] == 1]
for i, row in enumerate(innovation_products.itertuples(), 1):
    innovations.append({
        "innovation_id":  f"INN{i:03d}",
        "product_id":     row.product_id,
        "launch_date":    row.launch_date,
        "campaign_name":  f"Campaign_{row.brand}_{row.sub_category.replace(' ', '_')}",
    })
df_innov = pd.DataFrame(innovations)
df_innov.to_csv(f"{BASE_DIR}/innovation/innovation_master.csv", index=False, encoding="utf-8-sig")
print(f"✓ innovation_master: {len(df_innov)} rows")


# ─────────────────────────────────────────────
# 11. SFA — store_visits
# ─────────────────────────────────────────────
VISIT_TYPES = ["merchandising", "order", "audit"]
visit_rows = []
for dt in ALL_DATES[::2]:
    d = dt.date()
    for _ in range(random.randint(20, 60)):
        visit_rows.append({
            "date":        d,
            "retailer_id": random.choice(retailer_ids),
            "sales_rep_id": f"REP{random.randint(1, 50):03d}",
            "visit_type":  random.choice(VISIT_TYPES),
        })
df_visits = pd.DataFrame(visit_rows)
df_visits.to_csv(f"{BASE_DIR}/sfa/store_visits.csv", index=False, encoding="utf-8-sig")
print(f"✓ store_visits: {len(df_visits)} rows")


# ─────────────────────────────────────────────
# 12. Ecommerce — ecommerce_orders
# ─────────────────────────────────────────────
PLATFORMS = ["Shopee", "Lazada", "TikTokShop"]
ecom_rows = []
for dt in ALL_DATES:
    d = dt.date()
    mult = seasonal_multiplier(d) * ecom_growth_multiplier(d)
    for platform in PLATFORMS:
        for prod_id in random.sample(product_ids, k=random.randint(3, 12)):
            if d < product_launches[prod_id]:
                continue
            adoption = innovation_adoption(product_launches[prod_id], d)
            qty = max(1, int(random.randint(1, 20) * mult * adoption))
            revenue = round(qty * random.uniform(30_000, 100_000), 0)
            ecom_rows.append({
                "date":       d,
                "platform":   platform,
                "product_id": prod_id,
                "qty":        qty,
                "revenue":    revenue,
            })
df_ecom = pd.DataFrame(ecom_rows)
df_ecom.to_csv(f"{BASE_DIR}/ecommerce/ecommerce_orders.csv", index=False, encoding="utf-8-sig")
print(f"✓ ecommerce_orders: {len(df_ecom)} rows")


# ─────────────────────────────────────────────
# 13. External — market_share
# ─────────────────────────────────────────────
EXT_BRANDS = ["VietRefresh", "CompetitorA", "CompetitorB"]
mkt_rows = []
for year in [2023, 2024, 2025]:
    shares = [0.42, 0.32, 0.26]
    # VietRefresh gains share each year
    delta = (year - 2023) * 0.02
    shares = [shares[0] + delta, shares[1] - delta * 0.6, shares[2] - delta * 0.4]
    for brand, share in zip(EXT_BRANDS, shares):
        mkt_rows.append({
            "year":         year,
            "brand":        brand,
            "market_share": round(share, 4),
        })
df_mkt = pd.DataFrame(mkt_rows)
df_mkt.to_csv(f"{BASE_DIR}/external/market_share.csv", index=False, encoding="utf-8-sig")
print(f"✓ market_share: {len(df_mkt)} rows")

print("\n✅ All synthetic data generated successfully!")
