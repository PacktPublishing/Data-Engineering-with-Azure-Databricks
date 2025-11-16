# Incremental Load using Timestamp Watermark
from pyspark.sql.functions import col, max as max_, current_timestamp
from delta.tables import DeltaTable

jdbc_url = "jdbc:sqlserver://yourserver.database.windows.net:1433;database=salesdb"
connection_properties = {
    "user": dbutils.secrets.get(scope="azure-key-vault", key="sql-username"),
    "password": dbutils.secrets.get(scope="azure-key-vault", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Create watermark table
spark.sql("CREATE SCHEMA IF NOT EXISTS control")
spark.sql("""
    CREATE TABLE IF NOT EXISTS control.watermarks (
        source_table STRING,
        last_watermark_value TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

# Initialize watermark if needed
source_table = "sales.orders"
spark.sql(f"""
    MERGE INTO control.watermarks AS target
    USING (SELECT '{source_table}' AS source_table) AS source
    ON target.source_table = source.source_table
    WHEN NOT MATCHED THEN
        INSERT (source_table, last_watermark_value, updated_at)
        VALUES ('{source_table}', timestamp('1900-01-01'), current_timestamp())
""")

# Get last watermark
last_watermark = spark.sql(f"""
    SELECT last_watermark_value 
    FROM control.watermarks 
    WHERE source_table = '{source_table}'
""").first()[0]

print(f"Last watermark: {last_watermark}")

# Read incremental data from source
incremental_query = f"""
(SELECT * 
 FROM {source_table}
 WHERE updated_date > '{last_watermark}') AS incremental_data
"""

incremental_df = spark.read.jdbc(
    url=jdbc_url,
    table=incremental_query,
    properties=connection_properties
)

record_count = incremental_df.count()

if record_count > 0:
    print(f"Found {record_count} new/updated records")
    
    # Add ingestion metadata
    incremental_enriched = incremental_df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Check if target table exists
    if not spark.catalog.tableExists("sales_db.orders"):
        # First load
        incremental_enriched.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("sales_db.orders")
    else:
        # Merge into existing table
        delta_table = DeltaTable.forName(spark, "sales_db.orders")
        
        delta_table.alias("target").merge(
            incremental_enriched.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    
    # Update watermark
    new_watermark = incremental_enriched.agg(max_("updated_date")).first()[0]
    
    spark.sql(f"""
        UPDATE control.watermarks
        SET last_watermark_value = timestamp('{new_watermark}'),
            updated_at = current_timestamp()
        WHERE source_table = '{source_table}'
    """)
    
    print(f"Incremental load completed. New watermark: {new_watermark}")
else:
    print("No new records to process")
