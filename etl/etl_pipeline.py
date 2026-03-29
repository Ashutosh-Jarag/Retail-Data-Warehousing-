import pandas as pd
import pymysql
import os

print("Connecting...")
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="NewPassword123",
    database="retaildw",
    local_infile=True
)
cursor = conn.cursor()
print("Connected!")

RAW = "data/raw"

def load_table(table, df, cols):
    df = df[cols].copy()
    tmp = os.path.abspath(f"data/raw/tmp_{table}.csv").replace("\\", "/")
    df.to_csv(tmp, index=False, na_rep="NULL")
    col_list = ", ".join(cols)
    cursor.execute(
        f"LOAD DATA LOCAL INFILE '{tmp}' INTO TABLE {table} "
        f"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
        f"LINES TERMINATED BY '\\n' IGNORE 1 ROWS ({col_list})"
    )
    conn.commit()
    print(f"  {table}: {cursor.rowcount} rows")
    os.remove(tmp)

# Clear warehouse tables
print("Clearing warehouse tables...")
cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
for t in ["fact_sales","dim_date","dim_customer","dim_product","dim_payment"]:
    cursor.execute(f"TRUNCATE TABLE {t}")
cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
conn.commit()

# Read OLTP tables
print("Reading OLTP data...")
customers  = pd.read_csv(f"{RAW}/olist_customers_dataset.csv")
products   = pd.read_csv(f"{RAW}/olist_products_dataset.csv")
orders     = pd.read_csv(f"{RAW}/olist_orders_dataset.csv")
items      = pd.read_csv(f"{RAW}/olist_order_items_dataset.csv")
payments   = pd.read_csv(f"{RAW}/olist_order_payments_dataset.csv")

# ── DIM_DATE ──────────────────────────────────────────────
print("Building dim_date...")
orders["order_date"] = pd.to_datetime(orders["order_purchase_timestamp"])
dates = orders["order_date"].dt.date.unique()
dim_date = pd.DataFrame({"full_date": pd.to_datetime(dates)})
dim_date["date_id"]     = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
dim_date["day"]         = dim_date["full_date"].dt.day
dim_date["month"]       = dim_date["full_date"].dt.month
dim_date["quarter"]     = dim_date["full_date"].dt.quarter
dim_date["year"]        = dim_date["full_date"].dt.year
dim_date["month_name"]  = dim_date["full_date"].dt.strftime("%B")
dim_date["day_of_week"] = dim_date["full_date"].dt.strftime("%A")
dim_date["full_date"]   = dim_date["full_date"].dt.strftime("%Y-%m-%d")
dim_date = dim_date.drop_duplicates("date_id")
load_table("dim_date", dim_date,
           ["date_id","full_date","day","month","quarter","year","month_name","day_of_week"])

# ── DIM_CUSTOMER ──────────────────────────────────────────
print("Building dim_customer...")
dim_customer = customers[["customer_unique_id","customer_city","customer_state"]].copy()
dim_customer = dim_customer.rename(columns={
    "customer_unique_id": "customer_id",
    "customer_city":      "city",
    "customer_state":     "state"
})
dim_customer["customer_name"] = "Cust_" + dim_customer["customer_id"].str[:8]
dim_customer = dim_customer.drop_duplicates("customer_id")
load_table("dim_customer", dim_customer,
           ["customer_id","customer_name","city","state"])

# ── DIM_PRODUCT ───────────────────────────────────────────
print("Building dim_product...")
dim_product = products[["product_id","product_category_name","product_weight_g"]].copy()
dim_product = dim_product.rename(columns={
    "product_category_name": "category",
    "product_weight_g":      "weight_g"
})
dim_product["product_name"] = "Prod_" + dim_product["product_id"].str[:8]
dim_product = dim_product.dropna(subset=["product_id"]).drop_duplicates("product_id")
load_table("dim_product", dim_product,
           ["product_id","product_name","category","weight_g"])

# ── DIM_PAYMENT ───────────────────────────────────────────
print("Building dim_payment...")
dim_payment = pd.DataFrame({
    "payment_type_id": [1, 2, 3, 4],
    "payment_type":    ["credit_card", "boleto", "voucher", "debit_card"]
})
load_table("dim_payment", dim_payment, ["payment_type_id","payment_type"])
pay_map = dim_payment.set_index("payment_type")["payment_type_id"].to_dict()

# ── FACT_SALES ────────────────────────────────────────────
print("Building fact_sales...")

# Map order_id -> date_id
orders["order_date"]  = pd.to_datetime(orders["order_purchase_timestamp"])
orders["date_id"]     = orders["order_date"].dt.strftime("%Y%m%d").astype(int)
order_date_map        = orders.set_index("order_id")["date_id"].to_dict()

# Map order_id -> customer unique id
id_map = customers.set_index("customer_id")["customer_unique_id"].to_dict()
orders["customer_uid"] = orders["customer_id"].map(id_map)
order_cust_map = orders.set_index("order_id")["customer_uid"].to_dict()

# Map order_id -> payment_type
pay_agg = payments.groupby("order_id")["payment_type"].first().to_dict()

# Build fact
fact = items[["order_id","product_id","price","freight_value"]].copy()
fact["quantity"]        = 1
fact["date_id"]         = fact["order_id"].map(order_date_map)
fact["customer_id"]     = fact["order_id"].map(order_cust_map)
fact["payment_type"]    = fact["order_id"].map(pay_agg)
fact["payment_type_id"] = fact["payment_type"].map(pay_map).fillna(1).astype(int)
fact["unit_price"]      = fact["price"]
fact["total_amount"]    = fact["unit_price"] + fact["freight_value"]

# Drop rows with missing keys
fact = fact.dropna(subset=["date_id","customer_id","product_id"])
fact["date_id"] = fact["date_id"].astype(int)

# Keep only valid foreign keys
valid_products  = set(dim_product["product_id"])
valid_customers = set(dim_customer["customer_id"])
valid_dates     = set(dim_date["date_id"])
fact = fact[fact["product_id"].isin(valid_products)]
fact = fact[fact["customer_id"].isin(valid_customers)]
fact = fact[fact["date_id"].isin(valid_dates)]

load_table("fact_sales", fact,
           ["date_id","customer_id","product_id","payment_type_id",
            "order_id","quantity","unit_price","freight_value","total_amount"])

cursor.close()
conn.close()
print("\nStar schema warehouse ready!")
print(f"Total fact rows: {len(fact)}")