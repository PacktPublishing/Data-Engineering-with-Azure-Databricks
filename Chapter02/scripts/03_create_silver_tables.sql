
CREATE OR REPLACE TABLE analytics_dev.sales_silver.customers AS
SELECT
  TRIM(customer_id) AS customer_id,
  LOWER(TRIM(email)) AS email,
  TRIM(name) AS name,
  city,
  state,
  country,
  TO_DATE(signup_date, "yyyy-MM-dd") AS signup_date,
  source_file,
  current_timestamp() AS silver_loaded_ts_utc
FROM
  analytics_dev.sales_bronze.customers
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY bronze_loaded_ts_utc DESC) = 1;

CREATE OR REPLACE TABLE analytics_dev.sales_silver.orders AS
SELECT
  TRIM(order_id) AS order_id,
  TRIM(customer_id) AS customer_id,
  TO_DATE(order_date, "yyyy-MM-dd") AS order_date,
  CAST(order_total AS DOUBLE) AS order_total,
  source_file,
  current_timestamp() AS silver_loaded_ts_utc
FROM
  analytics_dev.sales_bronze.orders
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY bronze_loaded_ts_utc DESC) = 1;

CREATE OR REPLACE TABLE analytics_dev.sales_silver.order_items AS
SELECT
  TRIM(order_item_id) AS order_item_id,
  TRIM(order_id) AS order_id,
  TRIM(product_id) AS product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DOUBLE) AS unit_price,
  source_file,
  current_timestamp() AS silver_loaded_ts_utc
FROM
  analytics_dev.sales_bronze.order_items
QUALIFY
  DENSE_RANK() OVER (PARTITION BY TRIM(order_id) ORDER BY bronze_loaded_ts_utc DESC) = 1;

CREATE OR REPLACE TABLE analytics_dev.sales_silver.products AS
SELECT
  TRIM(product_id) AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  CAST(price AS DOUBLE) AS price,
  source_file,
  current_timestamp() AS silver_loaded_ts_utc
FROM
  analytics_dev.sales_bronze.products
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY TRIM(product_id) ORDER BY bronze_loaded_ts_utc DESC) = 1