CREATE OR REPLACE TABLE analytics_dev.sales_gold.customer AS
SELECT
  ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
  customer_id,
  name,
  email,
  city,
  state,
  country,
  signup_date
FROM
  analytics_dev.sales_silver.customers;

CREATE OR REPLACE TABLE analytics_dev.sales_gold.product AS
SELECT
  ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
  product_id,
  product_name,
  category,
  price
FROM
  analytics_dev.sales_silver.products;

CREATE OR REPLACE TABLE analytics_dev.sales_gold.dim_date AS
WITH dates AS (
  SELECT
    EXPLODE(SEQUENCE(TO_DATE('2024-01-01'), TO_DATE('2025-12-31'), INTERVAL 1 DAY)) AS date_value
)
SELECT
  ROW_NUMBER() OVER (ORDER BY date_value) AS date_sk,
  date_value,
  YEAR(date_value) AS year,
  MONTH(date_value) AS month,
  DAY(date_value) AS day,
  QUARTER(date_value) AS quarter,
  WEEKOFYEAR(date_value) AS week_of_year,
  DATE_FORMAT(date_value, 'EEEE') AS day_of_week_name,
  DAYOFWEEK(date_value) AS day_of_week_num,
  CASE
    WHEN DAYOFWEEK(date_value) IN (7, 1) THEN 1
    ELSE 0
  END AS is_weekend
FROM
  dates;

CREATE OR REPLACE TABLE analytics_dev.sales_gold.fact_sales
SELECT
  DENSE_RANK() OVER (ORDER BY oi.order_id, oi.order_item_id) AS sale_id,
  oi.order_id,
  oi.order_item_id,
  d.date_sk,
  p.product_sk,
  c.customer_sk,
  oi.quantity,
  oi.unit_price,
  oi.quantity * oi.unit_price AS line_total
FROM
  analytics_dev.sales_silver.order_items oi
    JOIN analytics_dev.sales_silver.orders o
      ON oi.order_id = o.order_id
    LEFT JOIN analytics_dev.sales_gold.dim_customer c
      ON o.customer_id = c.customer_id
    LEFT JOIN analytics_dev.sales_gold.dim_product p
      ON oi.product_id = p.product_id
    LEFT JOIN analytics_dev.sales_gold.dim_date d
      ON o.order_date = d.date_value;