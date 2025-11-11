CREATE TABLE IF NOT EXISTS analytics_dev.sales_bronze.customers (
  customer_id STRING COMMENT 'Unique customer identifier',
  email STRING COMMENT 'Customer email address',
  name STRING COMMENT 'Full name of the customer',
  city STRING COMMENT 'City of the customer',
  state STRING COMMENT 'State or province of the customer',
  country STRING COMMENT 'Country of the customer',
  signup_date DATE COMMENT 'Date when customer signed up',
  source_file STRING COMMENT 'Original source file for this row',
  bronze_loaded_ts_utc TIMESTAMP COMMENT 'Timestamp (UTC) when this row was loaded into Bronze'
)
COMMENT 'Bronze table containing customer data';

CREATE TABLE IF NOT EXISTS analytics_dev.sales_bronze.order_items (
  order_item_id STRING COMMENT 'Unique identifier for the order item',
  order_id STRING COMMENT 'Reference to the parent order',
  product_id STRING COMMENT 'Reference to the purchased product',
  quantity INT COMMENT 'Quantity of the product in this order item',
  unit_price DOUBLE COMMENT 'Unit price of the product at time of purchase',
  source_file STRING COMMENT 'Original source file for this row',
  bronze_loaded_ts_utc TIMESTAMP COMMENT 'Timestamp (UTC) when this row was loaded into Bronze'
)
COMMENT 'Bronze table containing order item details';

CREATE TABLE IF NOT EXISTS analytics_dev.sales_bronze.orders (
  order_id STRING COMMENT 'Unique identifier for the order',
  customer_id STRING COMMENT 'Reference to the customer who placed the order',
  order_date DATE COMMENT 'Date when the order was placed',
  order_total DOUBLE COMMENT 'Total amount of the order',
  order_status STRING COMMENT 'Current status of the order (e.g., Delivered, Pending)',
  source_file STRING COMMENT 'Original source file for this row',
  bronze_loaded_ts_utc TIMESTAMP COMMENT 'Timestamp (UTC) when this row was loaded into Bronze'
)
COMMENT 'Bronze table containing order header details';

CREATE TABLE IF NOT EXISTS analytics_dev.sales_bronze.products (
  product_id STRING COMMENT 'Unique product identifier',
  product_name STRING COMMENT 'Name of the product',
  category STRING COMMENT 'High-level product category',
  subcategory STRING COMMENT 'Subcategory of the product',
  price DOUBLE COMMENT 'Unit price of the product',
  source_file STRING COMMENT 'Original source file for this row',
  bronze_loaded_ts_utc TIMESTAMP COMMENT 'Timestamp (UTC) when this row was loaded into Bronze'
)
COMMENT 'Bronze table containing product master data';