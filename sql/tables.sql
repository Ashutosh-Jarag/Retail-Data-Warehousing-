CREATE TABLE customers (
  customer_id   VARCHAR(50) PRIMARY KEY,
  customer_name VARCHAR(100),
  city          VARCHAR(100),
  state         VARCHAR(50),
  zip_code      VARCHAR(20)
);

CREATE TABLE products (
  product_id   VARCHAR(50) PRIMARY KEY,
  product_name VARCHAR(200),
  category     VARCHAR(100),
  weight_g     DECIMAL(10,2),
  price        DECIMAL(10,2)
);

CREATE TABLE orders (
  order_id       VARCHAR(50) PRIMARY KEY,
  customer_id    VARCHAR(50),
  order_date     DATETIME,
  delivered_date DATETIME,
  status         VARCHAR(50),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  item_id       INT AUTO_INCREMENT PRIMARY KEY,
  order_id      VARCHAR(50),
  product_id    VARCHAR(50),
  quantity      INT DEFAULT 1,
  price         DECIMAL(10,2),
  freight_value DECIMAL(10,2),
  FOREIGN KEY (order_id)    REFERENCES orders(order_id),
  FOREIGN KEY (product_id)  REFERENCES products(product_id)
);

CREATE TABLE payments (
  payment_id    INT AUTO_INCREMENT PRIMARY KEY,
  order_id      VARCHAR(50),
  payment_type  VARCHAR(50),
  installments  INT,
  payment_value DECIMAL(10,2),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

SHOW TABLES;
