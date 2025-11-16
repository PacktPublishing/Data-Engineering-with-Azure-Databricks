# Complete Azure SQL Database to Databricks Batch Load
from pyspark.sql.functions import col, current_timestamp

# Database connection configuration
jdbc_url = "jdbc:sqlserver://yourserver.database.windows.net:1433;database=salesdb;encrypt=true"
connection_properties = {
    "user": dbutils.secrets.get(scope="azure-key-vault", key="sql-username"),
    "password": dbutils.secrets.get(scope="azure-key-vault", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Create schema in Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS sales_db")

print("Loading customers table...")
# Read customers table
customers_df = spark.read.jdbc(
    url=jdbc_url,
    table="sales.customers",
    properties=connection_properties
)

# Add ingestion metadata
customers_enriched = customers_df.withColumn("ingestion_timestamp", current_timestamp())

# Write to Delta Lake
customers_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("sales_db.customers")

print(f"Loaded {customers_enriched.count()} customers")

print("Loading orders table...")
# Read orders table
orders_df = spark.read.jdbc(
    url=jdbc_url,
    table="sales.orders",
    properties=connection_properties
)

# Add metadata
orders_enriched = orders_df.withColumn("ingestion_timestamp", current_timestamp())

# Write to Delta Lake
orders_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("sales_db.orders")

print(f"Loaded {orders_enriched.count()} orders")

# Verify the load
print("\nData verification:")
display(spark.sql("""
    SELECT o.order_id, o.order_date, c.customer_name, o.total_amount, o.order_status
    FROM sales_db.orders o
    JOIN sales_db.customers c ON o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
"""))
