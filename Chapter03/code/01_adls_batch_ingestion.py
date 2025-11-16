# Complete ADLS Batch Ingestion Example
from pyspark.sql.functions import col, current_timestamp, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configure authentication (using service principal)
storage_account = "yourstorageaccount"
container = "rawdata"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", 
               dbutils.secrets.get(scope="azure-key-vault", key="sp-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", 
               dbutils.secrets.get(scope="azure-key-vault", key="sp-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='azure-key-vault', key='tenant-id')}/oauth2/token")

# Define paths
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
sales_csv_path = f"{base_path}/sales/csv/"
products_json_path = f"{base_path}/products/json/"
delta_output_path = f"{base_path}/delta/sales_enriched/"

# Define schema for CSV
sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("sale_date", TimestampType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False)
])

# Read CSV sales data
print("Reading CSV sales data...")
sales_df = spark.read \
    .option("header", "true") \
    .schema(sales_schema) \
    .csv(sales_csv_path)

# Read JSON product data
print("Reading JSON product data...")
products_df = spark.read \
    .option("multiLine", "true") \
    .json(products_json_path)

# Enrich and transform
sales_enriched = sales_df \
    .withColumn("total_amount", col("quantity") * col("unit_price")) \
    .withColumn("sale_year", year("sale_date")) \
    .withColumn("sale_month", month("sale_date")) \
    .withColumn("ingestion_timestamp", current_timestamp())

# Join with product information
sales_with_products = sales_enriched \
    .join(products_df, "product_id", "left")

# Write to Delta Lake with partitioning
print("Writing to Delta Lake...")
sales_with_products.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sale_year", "sale_month") \
    .option("overwriteSchema", "true") \
    .save(delta_output_path)

# Create Delta table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sales_db.sales_enriched
    USING DELTA
    LOCATION '{delta_output_path}'
""")

print("Data loaded successfully!")
display(spark.sql("SELECT * FROM sales_db.sales_enriched LIMIT 10"))
