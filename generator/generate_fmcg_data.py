import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import random
import os
from datetime import datetime, date

fake = Faker()

START_DATE = "2023-01-01"
END_DATE = "2025-12-31"

dates = pd.date_range(START_DATE, END_DATE)

current_dir = os.path.dirname(os.path.abspath(__file__))
target_path = os.path.join(current_dir, "..", "data_lake", "raw")

N_PRODUCTS = 120
N_DISTRIBUTORS = 40
N_RETAILERS = 4000
N_MT_STORES = 300
N_WAREHOUSES = 8
N_SALES_REPS = 120

regions = ["North","Central","South"]

pack_sizes=[250,330,500,1000]

categories={
"Carbonated":["Cola","Orange Soda","Lemon Soda"],
"Juice":["Orange Juice","Apple Juice","Mango Juice"],
"Energy":["Energy Drink"],
"Tea":["Green Tea","Oolong Tea"]
}

def ensure(p):
    os.makedirs(p,exist_ok=True)


# -----------------------------
# SEASONALITY MODEL
# -----------------------------

def seasonal_multiplier(date):

    m=date.month

    if m in [5,6,7,8]:
        return 1.6

    if m in [1,2]:
        return 1.3

    return 1.0


# -----------------------------
# PRODUCT MASTER
# -----------------------------

def gen_products():

    rows=[]
    pid=1

    for cat,subs in categories.items():

        for sub in subs:

            for _ in range(10):

                launch=fake.date_between("-3y","today")

                rows.append({

                    "product_id":pid,
                    "sku_code":f"SKU{pid:04}",
                    "sku_name":f"{sub} {random.choice(pack_sizes)}ml",
                    "brand":"VietRefresh",
                    "category":cat,
                    "sub_category":sub,
                    "pack_size":random.choice(pack_sizes),
                    "launch_date":launch,
                    "innovation_flag":random.random()<0.15
                })

                pid+=1

    df=pd.DataFrame(rows)

    ensure(target_path+"/erp")

    df.to_csv(target_path+"/erp/product_master.csv",index=False)

    return df


# -----------------------------
# DISTRIBUTORS
# -----------------------------

def gen_distributors():

    rows=[]

    for i in range(1,N_DISTRIBUTORS+1):

        rows.append({
        "distributor_id":i,
        "name":fake.company(),
        "region":random.choice(regions),
        "province":fake.city()
        })

    df=pd.DataFrame(rows)

    df.to_csv(target_path+"/erp/distributor_master.csv",index=False)

    return df


# -----------------------------
# RETAILERS (GT)
# -----------------------------

def gen_retailers(distributors):

    rows=[]

    for i in range(1,N_RETAILERS+1):

        d=distributors.sample(1).iloc[0]

        rows.append({
        "retailer_id":i,
        "name":fake.company(),
        "distributor_id":d.distributor_id,
        "store_type":random.choice(["grocery","mini_mart","kiosk"]),
        "region":d.region,
        "province":d.province
        })

    ensure(target_path+"/dms")

    df=pd.DataFrame(rows)

    df.to_csv(target_path+"/dms/retailer_master.csv",index=False)

    return df


# -----------------------------
# MT STORES
# -----------------------------

def gen_mt_stores():

    chains=["Winmart","Coopmart","BigC","Lotte"]

    rows=[]

    for i in range(1,N_MT_STORES+1):

        rows.append({
        "store_id":i,
        "store_name":fake.company(),
        "chain":random.choice(chains),
        "region":random.choice(regions),
        "province":fake.city()
        })

    ensure(target_path+"/pos")

    df=pd.DataFrame(rows)

    df.to_csv(target_path+"/pos/store_master.csv",index=False)

    return df


# -----------------------------
# SKU DISTRIBUTION MATRIX
# -----------------------------

def build_store_assortment(products,retailers,stores):

    coverage=[]

    for _,p in products.iterrows():

        base=random.uniform(0.2,0.7)

        for _,r in retailers.sample(int(len(retailers)*base)).iterrows():

            coverage.append({
            "store_type":"GT",
            "store_id":r.retailer_id,
            "product_id":p.product_id
            })

        for _,s in stores.sample(int(len(stores)*0.8)).iterrows():

            coverage.append({
            "store_type":"MT",
            "store_id":s.store_id,
            "product_id":p.product_id
            })

    df=pd.DataFrame(coverage)

    ensure(target_path+"/reference")

    df.to_csv(target_path+"/reference/store_assortment.csv",index=False)

    return df


# -----------------------------
# PROMOTION GENERATION
# -----------------------------

def gen_promotions(products):

    rows=[]

    for i in range(80):

        p=products.sample(1).iloc[0]
        start = fake.date_between(
            start_date=date(2023, 1, 1),
            end_date=date(2025, 10, 1))

        end=pd.to_datetime(start)+pd.Timedelta(days=random.randint(7,30))

        rows.append({

        "promotion_id":i+1,
        "product_id":p.product_id,
        "start_date":start,
        "end_date":end,
        "promotion_type":random.choice(["discount","display","bundle"]),
        "discount_percent":random.uniform(0.05,0.25)
        })

    ensure(target_path+"/marketing")

    df=pd.DataFrame(rows)

    df.to_csv(target_path+"/marketing/promotions.csv",index=False)

    return df


# -----------------------------
# INNOVATION MASTER
# -----------------------------

def gen_innovations(products):

    rows=[]

    innovations=products[products.innovation_flag]

    for _,p in innovations.iterrows():

        rows.append({

        "innovation_id":p.product_id,
        "product_id":p.product_id,
        "launch_date":p.launch_date,
        "campaign_name":fake.bs()
        })

    ensure(target_path+"/innovation")

    pd.DataFrame(rows).to_csv(target_path+"/innovation/innovation_master.csv",index=False)


# -----------------------------
# SALES ENGINE
# -----------------------------

def demand_engine(base_qty,date,promo=False):

    q=base_qty

    q*=seasonal_multiplier(date)

    if promo:
        q*=random.uniform(1.3,2.0)

    return int(np.random.poisson(q))


# -----------------------------
# ERP SELLIN
# -----------------------------

def gen_sellin(products,distributors):

    rows=[]

    for d in tqdm(dates):

        for _ in range(random.randint(70,120)):

            p=products.sample(1).iloc[0]
            dist=distributors.sample(1).iloc[0]

            qty=demand_engine(40,d)

            price=0.4*p.pack_size
            discount = random.uniform(0.05,0.15)

            rows.append({

            "date":d,
            "distributor_id":dist.distributor_id,
            "product_id":p.product_id,
            "qty":qty,
            "gross_sales":qty*price,
            "discount":discount,
            "net_sales":qty*price*(1-discount)
            })

    pd.DataFrame(rows).to_csv(target_path+"/erp/sellin.csv",index=False)


# -----------------------------
# DMS SELLOUT
# -----------------------------

def gen_sellout(products,retailers):

    rows=[]

    for d in tqdm(dates):

        for _ in range(random.randint(200,320)):

            p=products.sample(1).iloc[0]
            r=retailers.sample(1).iloc[0]

            qty=demand_engine(10,d)

            price=0.6*p.pack_size

            rows.append({

            "date":d,
            "retailer_id":r.retailer_id,
            "distributor_id":r.distributor_id,
            "product_id":p.product_id,
            "qty":qty,
            "revenue":qty*price
            })

    pd.DataFrame(rows).to_csv(target_path+"/dms/sellout.csv",index=False)


# -----------------------------
# POS SALES
# -----------------------------

def gen_pos(products,stores):

    rows=[]

    for d in tqdm(dates):

        for _ in range(random.randint(120,200)):

            p=products.sample(1).iloc[0]
            s=stores.sample(1).iloc[0]

            qty=demand_engine(6,d)

            price=0.8*p.pack_size

            rows.append({

            "date":d,
            "store_id":s.store_id,
            "product_id":p.product_id,
            "qty":qty,
            "revenue":qty*price
            })

    pd.DataFrame(rows).to_csv(target_path+"/pos/pos_sales.csv",index=False)


# -----------------------------
# INVENTORY
# -----------------------------

def gen_inventory(products):

    rows=[]

    for d in dates:

        for p in products.sample(30).product_id:

            rows.append({

            "date":d,
            "product_id":p,
            "stock_on_hand":np.random.randint(500,6000)
            })

    ensure(target_path+"/supply_chain")

    pd.DataFrame(rows).to_csv(target_path+"/supply_chain/inventory.csv",index=False)


# -----------------------------
# SFA STORE VISITS
# -----------------------------

def gen_store_visits(retailers):

    rows=[]

    for d in dates:

        for _ in range(80):

            r=retailers.sample(1).iloc[0]

            rows.append({

            "date":d,
            "retailer_id":r.retailer_id,
            "sales_rep_id":random.randint(1,N_SALES_REPS),
            "visit_type":random.choice(["merchandising","order","audit"])
            })

    ensure(target_path+"/sfa")

    pd.DataFrame(rows).to_csv(target_path+"/sfa/store_visits.csv",index=False)


# -----------------------------
# ECOMMERCE
# -----------------------------

def gen_ecommerce(products):

    rows=[]

    platforms=["Shopee","Lazada","TikTokShop"]

    for d in dates:

        growth=1+(d.year-2023)*0.3

        for _ in range(int(random.randint(30,60)*growth)):

            p=products.sample(1).iloc[0]

            qty=np.random.poisson(3)

            rows.append({

            "date":d,
            "platform":random.choice(platforms),
            "product_id":p.product_id,
            "qty":qty,
            "revenue":qty*(p.pack_size*0.9)
            })

    ensure(target_path+"/ecommerce")

    pd.DataFrame(rows).to_csv(target_path+"/ecommerce/orders.csv",index=False)


# -----------------------------
# MARKET SHARE
# -----------------------------

def gen_market_data():

    rows=[]

    for y in [2023,2024,2025]:

        shares=np.random.dirichlet(np.ones(3))

        brands=["VietRefresh","CompetitorA","CompetitorB"]

        for i,b in enumerate(brands):

            rows.append({

            "year":y,
            "brand":b,
            "market_share":shares[i]
            })

    ensure(target_path+"/external")

    pd.DataFrame(rows).to_csv(target_path+"/external/market_share.csv",index=False)


# -----------------------------
# MAIN
# -----------------------------

def main():

    products=gen_products()
    distributors=gen_distributors()
    retailers=gen_retailers(distributors)
    stores=gen_mt_stores()

    build_store_assortment(products,retailers,stores)

    gen_promotions(products)

    gen_innovations(products)

    gen_sellin(products,distributors)
    gen_sellout(products,retailers)
    gen_pos(products,stores)

    gen_inventory(products)

    gen_store_visits(retailers)

    gen_ecommerce(products)

    gen_market_data()

    print("ADVANCED FMCG DATA GENERATED")


if __name__=="__main__":
    main()