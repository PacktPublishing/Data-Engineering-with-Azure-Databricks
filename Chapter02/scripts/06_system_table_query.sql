SELECT
  t1.workspace_id,
  t1.billing_origin_product,
  t1.sku_name,
  SUM(t1.usage_quantity * list_prices.pricing.default) AS list_cost
FROM system.billing.usage AS t1
INNER JOIN system.billing.list_prices AS list_prices
  ON t1.cloud = list_prices.cloud
  AND t1.sku_name = list_prices.sku_name
  AND t1.usage_start_time >= list_prices.price_start_time
  AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
GROUP BY
  t1.workspace_id,
  t1.billing_origin_product,
  t1.sku_name;
