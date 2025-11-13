COPY INTO analytics_dev.sales_bronze.order_items
FROM (
  SELECT
    order_item_id,
    order_id,
    product_id,
    CAST(quantity AS INT) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    _metadata.file_path AS source_file,
    current_timestamp() AS bronze_loaded_ts_utc
  FROM '/Volumes/analytics_dev/sales_raw/staging/order_items/*/*'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

COPY INTO analytics_dev.sales_bronze.customers
FROM (
  SELECT
    customer_id,
    email,
    name,
    city,
    state,
    country,
    CAST(signup_date AS DATE) AS signup_date,
    _metadata.file_path AS source_file,
    current_timestamp() AS bronze_loaded_ts_utc
  FROM '/Volumes/analytics_dev/sales_raw/staging/customers/*/*'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

COPY INTO analytics_dev.sales_bronze.orders
FROM (
  SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(order_total AS DOUBLE) AS order_total,
    order_status,
    _metadata.file_path AS source_file,
    current_timestamp() AS bronze_loaded_ts_utc
  FROM '/Volumes/analytics_dev/sales_raw/staging/orders/*/*'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

COPY INTO analytics_dev.sales_bronze.products
FROM (
  SELECT
    product_id,
    product_name,
    category,
    subcategory,
    CAST(price AS DOUBLE) AS price,
    _metadata.file_path AS source_file,
    current_timestamp() AS bronze_loaded_ts_utc
  FROM '/Volumes/analytics_dev/sales_raw/staging/products/*/*'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');
