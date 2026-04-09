
# Metadata Documentation: Operational Data Source Systems
## 1. Overview

This document provides the schema and functional descriptions for the raw data ingested into the Data Lake from various operational source systems.

| System | Function |
|--------|----------|
|ERP  | Finance + Sell-in |
|DMS | Distributor sales|
|POS | Modern trade retail|
|Supply Chain | Inventory & logistics|
|Marketing | Promotions|
|Innovation | New product launches|
|SFA | Field sales operations|
|Ecommerce | Online marketplace|
|External | Market intelligence|


## 2. Data Lake Schema (Raw Layer)
### 2.1 ERP (Finance / Sell-in)
Focuses on product masters and distributor-level purchases from the company.

#### Table: product_master  
    product_id (int): Internal product ID.  
    sku_code (string): SKU code.  
    sku_name (string): Product name.  
    brand (string): Brand name.  
    category (string): Product category.  
    sub_category (string): Sub-category.  
    pack_size (int): Size in ml.  
    launch_date (date): Product launch date.  
    innovation_flag (bool): Flag for new products.  

#### Table: distributor_master
    distributor_id (int): Unique distributor ID.
    name (string): Distributor name.
    region (string): Geographic region.
    province (string): Province.
#### Table: sellin (Distributor purchase from company)
    date (date): Transaction date.
    distributor_id (int): Distributor ID.
    product_id (int): Product ID.
    qty (int): Quantity.
    gross_sales (float): Gross sales value.
    discount (float): Applied discount.
    net_sales (float): Net sales value.

### 2.2 DMS (Distributor Management System)
Tracks sales from distributors to retailers.
#### Table: retailer_master
    retailer_id (int): Retailer ID.
    name (string): Retailer name.
    distributor_id (int): Associated distributor ID.
    store_type (string): Type of store.
    region (string): Region.
    province (string): Province.
#### Table: sellout (Distributor sales to retailers)
    date (date): Transaction date.
    retailer_id (int): Retailer ID.
    distributor_id (int): Distributor ID.
    product_id (int): Product ID.
    qty (int): Quantity.
    revenue (float): Revenue generated.
### 2.3 POS (Modern Trade)
Retail sales data from Modern Trade stores.
#### Table: store_master
    store_id (int): Store ID.
    store_name (string): Name of the store.
    chain (string): Retail chain name.
    region (string): Region.
    province (string): Province.
#### Table: pos_sales
    date (date): Sale date.
    store_id (int): Store ID.
    product_id (int): Product ID.
    qty (int): Quantity.
    revenue (float): Sales revenue.
### 2.4 Supply Chain
Warehouse inventory snapshots.
#### Table: inventory
    date (date): Snapshot date.
    product_id (int): Product ID.
    stock_on_hand (int): Current stock quantity.
### 2.5 Marketing & Trade Promotion
Details regarding promotional activities.
#### Table: promotions
    promotion_id (int): Promotion ID.
    product_id (int): Product ID.
    start_date (date): Start date.
    end_date (date): End date.
    promotion_type (string): Type of promotion.
    discount_percent (float): Discount percentage.
### 2.6 Product Innovation
Tracking new product launches and campaigns.
#### Table: innovation_master
    innovation_id (int): Innovation ID.
    product_id (int): Product ID.
    launch_date (date): Launch date.
    campaign_name (string): Associated campaign name.
### 2.7 Field Sales (SFA)
Store visit records from field sales representatives.
#### Table: store_visits
    date (date): Visit date.
    retailer_id (int): Retailer ID.
    sales_rep_id (int): Sales representative ID.
    visit_type (string): Purpose of the visit.
### 2.8 Ecommerce Platforms
Sales data from online marketplaces.
#### Table: ecommerce_orders
    date (date): Order date.
    platform (string): Marketplace name (e.g., Shopee, Lazada).
    product_id (int): Product ID.
    qty (int): Quantity.
    revenue (float): Order revenue.
### 2.9 External Market Data
Market intelligence and share data.
#### Table: market_share
    year (int): Reporting year.
    brand (string): Brand name.
    market_share (float): Market share percentage.

