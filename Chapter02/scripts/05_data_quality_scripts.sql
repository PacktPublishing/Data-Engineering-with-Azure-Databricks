-- Check for missing dimension references
SELECT
  COUNT(*) AS MissingDimensionAttributes
FROM analytics_dev.sales_gold.fact_sales
WHERE product_sk IS NULL
  OR customer_sk IS NULL;

-- Check for incomplete dimension data
SELECT
  *
FROM analytics_dev.sales_gold.dim_product
WHERE product_name IS NULL
  OR category IS NULL
  OR price IS NULL;

-- Check for duplicate sales records
SELECT
  sale_id,
  COUNT(sale_id) AS duplicate_count
FROM analytics_dev.sales_gold.fact_sales
GROUP BY sale_id
HAVING COUNT(sale_id) > 1;

-- Data Freshness test
SELECT
  last_altered < CURRENT_TIMESTAMP() - INTERVAL 3 DAY AS is_stale
FROM system.information_schema.tables
WHERE table_name = 'fact_sales'
  AND table_catalog = 'analytics_dev'
  AND table_schema = 'sales_gold';
