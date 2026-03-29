import pandas as pd
import pymysql
import os

print("Connecting...")
conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="NewPassword123",
    database="retaildw",
    local_infile=True
)
cursor = conn.cursor()
print("Connected to MySQL!")

RAW = "data/raw"

cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
for t in ["payments", "order_items", "orders", "products", "customers"]:
    cursor.execute(f"TRUNCATE TABLE {t}")
cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
conn.commit()
print("Tables cleared")

def load_table(table, df, cols):
    df = df[cols].copy()
    tmp_path = os.path.abspath(f"data/raw/tmp_{table}.csv").replace("\\", "/")
    df.to_csv(tmp_path, index=False, na_rep="\\N", lineterminator='\n')
    col_list = ", ".join(cols)
    sql = (
        f"LOAD DATA LOCAL INFILE '{tmp_path}' "
        f"INTO TABLE {table} "
        f"FIELDS TERMINATED BY ',' "
        f"OPTIONALLY ENCLOSED BY '\"' "
        f"LINES TERMINATED BY '\\n' "
        f"IGNORE 1 ROWS ({col_list})"
    )
    cursor.execute(sql)
    conn.commit()
    print(f"  {table}: {cursor.rowcount} rows inserted")
    os.remove(tmp_path)

# CUSTOMERS
print("Loading customers...")
df = pd.read_csv(f"{RAW}/olist_customers_dataset.csv")
df = df.drop(columns=["customer_id"])
df = df.rename(columns={
    "customer_unique_id":       "customer_id",
    "customer_city":            "city",
    "customer_state":           "state",
    "customer_zip_code_prefix": "zip_code"
})
df["customer_name"] = "Cust_" + df["customer_id"].str[:8]
df = df.drop_duplicates("customer_id")
load_table("customers", df, ["customer_id", "customer_name", "city", "state", "zip_code"])
customers = df.copy()

# PRODUCTS
print("Loading products...")
df = pd.read_csv(f"{RAW}/olist_products_dataset.csv")
df = df.rename(columns={
    "product_category_name": "category",
    "product_weight_g":      "weight_g"
})
df["product_name"] = "Prod_" + df["product_id"].str[:8]
df["price"] = 0.0
df = df.dropna(subset=["product_id"]).drop_duplicates("product_id")
load_table("products", df, ["product_id", "product_name", "category", "weight_g", "price"])
products = df.copy()

# ORDERS
print("Loading orders...")
raw_c = pd.read_csv(f"{RAW}/olist_customers_dataset.csv")
id_map = raw_c.set_index("customer_id")["customer_unique_id"].to_dict()
df = pd.read_csv(f"{RAW}/olist_orders_dataset.csv")
df = df.rename(columns={
    "order_purchase_timestamp":      "order_date",
    "order_delivered_customer_date": "delivered_date",
    "order_status":                  "status"
})
df["customer_id"] = df["customer_id"].map(id_map)
df = df.dropna(subset=["customer_id"]).drop_duplicates("order_id")
load_table("orders", df, ["order_id", "customer_id", "order_date", "delivered_date", "status"])
valid_orders = df["order_id"].tolist()

# ORDER ITEMS
print("Loading order items...")
df = pd.read_csv(f"{RAW}/olist_order_items_dataset.csv")
df["quantity"] = 1
df = df[df["order_id"].isin(valid_orders)]
df = df[df["product_id"].isin(products["product_id"].tolist())]
load_table("order_items", df, ["order_id", "product_id", "quantity", "price", "freight_value"])

# PAYMENTS
print("Loading payments...")
df = pd.read_csv(f"{RAW}/olist_order_payments_dataset.csv")
df = df.rename(columns={"payment_installments": "installments"})
df = df[df["order_id"].isin(valid_orders)]
load_table("payments", df, ["order_id", "payment_type", "installments", "payment_value"])

cursor.close()
conn.close()
print("\nOLTP database ready!")