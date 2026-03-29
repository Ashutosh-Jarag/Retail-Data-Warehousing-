-- DIMENSION TABLES

CREATE TABLE dim_date (
  date_id   INT PRIMARY KEY,
  full_date DATE,
  day       INT,
  month     INT,
  quarter   INT,
  year      INT,
  month_name VARCHAR(20),
  day_of_week VARCHAR(20)
);

CREATE TABLE dim_customer (
  customer_id   VARCHAR(50) PRIMARY KEY,
  customer_name VARCHAR(100),
  city          VARCHAR(100),
  state         VARCHAR(50)
);

CREATE TABLE dim_product (
  product_id   VARCHAR(50) PRIMARY KEY,
  product_name VARCHAR(200),
  category     VARCHAR(100),
  weight_g     DECIMAL(10,2)
);

CREATE TABLE dim_payment (
  payment_type_id INT AUTO_INCREMENT PRIMARY KEY,
  payment_type    VARCHAR(50)
);

-- FACT TABLE

CREATE TABLE fact_sales (
  sale_id        INT AUTO_INCREMENT PRIMARY KEY,
  date_id        INT,
  customer_id    VARCHAR(50),
  product_id     VARCHAR(50),
  payment_type_id INT,
  order_id       VARCHAR(50),
  quantity       INT,
  unit_price     DECIMAL(10,2),
  freight_value  DECIMAL(10,2),
  total_amount   DECIMAL(10,2),
  FOREIGN KEY (date_id)         REFERENCES dim_date(date_id),
  FOREIGN KEY (customer_id)     REFERENCES dim_customer(customer_id),
  FOREIGN KEY (product_id)      REFERENCES dim_product(product_id),
  FOREIGN KEY (payment_type_id) REFERENCES dim_payment(payment_type_id)
);

exit