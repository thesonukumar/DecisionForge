import pandas as pd
from datetime import datetime

# =========================================================
# FILE PATHS
# =========================================================

SALES_RAW = "data/raw/raw_data.csv"
CUSTOMERS_RAW = "data/raw/customers_raw.csv"
PRODUCTS_RAW = "data/raw/products_raw.csv"
INVENTORY_RAW = "data/raw/inventory_raw.csv"
REGIONS_RAW = "data/raw/regions_raw.csv"

SALES_TRUSTED = "data/trusted/sales_trusted.csv"
CUSTOMERS_TRUSTED = "data/trusted/customers_trusted.csv"
PRODUCTS_TRUSTED = "data/trusted/products_trusted.csv"
INVENTORY_TRUSTED = "data/trusted/inventory_trusted.csv"
REGIONS_TRUSTED = "data/trusted/regions_trusted.csv"

print("=== PHASE 2: DATA TRUST LAYER STARTED ===")

# =========================================================
# STEP 2 — SALES DATA TRUST
# =========================================================

sales = pd.read_csv(SALES_RAW, encoding="latin1")
print(f"Sales raw rows: {len(sales)}")

# ---- Rename columns (adjust LEFT side if needed) ----
sales = sales.rename(columns={
    "Order ID": "order_id",
    "Order Date": "order_date",
    "Sales": "revenue",
    "Quantity": "quantity",
    "Customer ID": "customer_id",
    "Product ID": "product_id",
    "Region": "region"
})

# ---- Datatype fixes ----
sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
sales["revenue"] = pd.to_numeric(sales["revenue"], errors="coerce")
sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce")

# ---- Business rules ----
sales = sales[
    (sales["revenue"] >= 0) &
    (sales["quantity"] > 0) &
    (sales["order_date"] <= pd.Timestamp(datetime.today()))
]

# ---- Missing critical fields ----
sales = sales.dropna(subset=["order_id", "order_date", "revenue"])

# ---- Duplicate removal ----
sales = sales.drop_duplicates(
    subset=["order_id", "order_date", "revenue"],
    keep="last"
)

sales.to_csv(SALES_TRUSTED, index=False)
print(f"Sales trusted rows: {len(sales)}")

# =========================================================
# STEP 3 — CUSTOMERS DATA TRUST
# =========================================================

customers = pd.read_csv(CUSTOMERS_RAW, encoding="latin1")
print(f"Customers raw rows: {len(customers)}")

customers = customers.rename(columns={
    "Customer ID": "customer_id",
    "Customer Name": "customer_name",
    "Region": "region"
})

customers = customers.dropna(subset=["customer_id"])
customers = customers.drop_duplicates(subset=["customer_id"])

customers.to_csv(CUSTOMERS_TRUSTED, index=False)
print(f"Customers trusted rows: {len(customers)}")

# =========================================================
# STEP 4 — PRODUCTS DATA TRUST
# =========================================================

products = pd.read_csv(PRODUCTS_RAW, encoding="latin1")
print(f"Products raw rows: {len(products)}")

products = products.rename(columns={
    "Product ID": "product_id",
    "Product Name": "product_name",
    "Category": "category"
})

products = products.dropna(subset=["product_id"])
products = products.drop_duplicates(subset=["product_id"])

products.to_csv(PRODUCTS_TRUSTED, index=False)
print(f"Products trusted rows: {len(products)}")

# =========================================================
# STEP 5 — INVENTORY DATA TRUST
# =========================================================

inventory = pd.read_csv(INVENTORY_RAW, encoding="latin1")
print(f"Inventory raw rows: {len(inventory)}")

# Standardize column names
inventory.columns = inventory.columns.str.strip().str.lower()

# Rename to standard schema
inventory = inventory.rename(columns={
    "product id": "product_id",
    "stock_level": "stock"
})

# Validate schema
if "product_id" not in inventory.columns or "stock" not in inventory.columns:
    raise Exception(
        f"Inventory schema invalid. Columns present: {list(inventory.columns)}"
    )

# Datatype + business rule
inventory["stock"] = pd.to_numeric(inventory["stock"], errors="coerce")
inventory = inventory[inventory["stock"] >= 0]

inventory = inventory.drop_duplicates(subset=["product_id"])

inventory.to_csv(INVENTORY_TRUSTED, index=False)
print(f"Inventory trusted rows: {len(inventory)}")

# =========================================================
# STEP 6 — REGIONS DATA TRUST
# =========================================================

regions = pd.read_csv(REGIONS_RAW, encoding="latin1")
print(f"Regions raw rows: {len(regions)}")

regions = regions.rename(columns={
    "Region ID": "region_id",
    "Region Name": "region_name"
})

regions = regions.dropna(subset=["region_id"])
regions = regions.drop_duplicates(subset=["region_id"])

regions.to_csv(REGIONS_TRUSTED, index=False)
print(f"Regions trusted rows: {len(regions)}")

print("=== PHASE 2 COMPLETED SUCCESSFULLY ===")
