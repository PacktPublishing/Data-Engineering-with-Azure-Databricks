from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

# Customers schema
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("account_creation_date", DateType(), True),
])

# Orders schema
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("payment_method", StringType(), True),
    StructField("days_since_last_purchase", IntegerType(), True),
    StructField("repeat_customer", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
])

# Order items schema
order_items_schema = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("line_total", DoubleType(), True),
])

# Products schema
products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("stock_level", IntegerType(), True),
    StructField("product_launch_date", DateType(), True),
    StructField("supplier_id", StringType(), True),
])

# Schema registry for lookup by table name
TABLE_SCHEMAS = {
    "customers": customers_schema,
    "orders": orders_schema,
    "order_items": order_items_schema,
    "products": products_schema,
}
