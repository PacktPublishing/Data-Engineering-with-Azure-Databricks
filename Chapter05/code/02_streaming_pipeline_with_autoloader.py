# Databricks notebook source
# ==========================================
# 1) Create database/schema for tables
# ==========================================
spark.sql("CREATE SCHEMA IF NOT EXISTS iot")

# ==========================================
# 2) Configure Azure Blob access with account key
#    (Replace with your real values)
# ==========================================
account_name   = "streamingdata123"
container_name = "input-files"
# Prefer Databricks Secrets in real projects; plain text here for demo:
account_key    = "YOUR_STORAGE_ACCOUNT_KEY"

spark.conf.set(
    f"fs.azure.account.key.{account_name}.blob.core.windows.net",
    account_key
)

# Input folder in your Blob container where you will drop JSON files
input_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/"



# ==========================================
# 3) Auto Loader metadata paths (DBFS)
#    Change names to "replay" from scratch
# ==========================================
schema_path     = "/tmp/autoloader/schema/iot_incoming_v1"
checkpoint_path = "/tmp/autoloader/checkpoints/iot_autoloader_parsed_v1"



# ==========================================
# 4) Read JSON files with Auto Loader
#    - multiLine: supports JSON arrays and pretty JSON
#    - includeExistingFiles: ingests existing files on first run
#    - rescuedDataColumn: keeps unparsed records for debugging
# ==========================================
from pyspark.sql.functions import col

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("multiLine", "true")
    .load(input_path)
)

# Optional quick sanity check:
# display(raw_df)



# ==========================================
# 5) Normalize schema and add server_ts_utc
#    Assumes fields present in JSON:
#    deviceId, ts, temperature, humidity, pressure, battery, lat, lon, status
# ==========================================
from pyspark.sql.functions import to_timestamp, current_timestamp

parsed_df = (raw_df
    .select(
        col("deviceId").cast("string").alias("deviceId"),
        to_timestamp(col("ts")).alias("local_ts"),
        col("temperature").cast("double").alias("temperature"),
        col("humidity").cast("double").alias("humidity"),
        col("pressure").cast("double").alias("pressure"),
        col("battery").cast("double").alias("battery"),
        col("lat").cast("double").alias("lat"),
        col("lon").cast("double").alias("lon"),
        col("status").cast("string").alias("status"),
        current_timestamp().alias("server_ts_utc"),
        col("_rescued_data")  # keep for troubleshooting; drop later if you want
    )
)




# ==========================================
# 6) Write to a new Delta table (streaming)
#    The table will be created automatically if it doesn't exist.
# ==========================================
q = (parsed_df.writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", checkpoint_path)
     .toTable("iot.autoloader_parsed")
)




# ==========================================
# 7) Verify data (run after you upload files)
#    Drop 1-2 JSON files into the Blob folder:
#    wasbs://<container>@<account>.blob.core.windows.net/iot/incoming
# ==========================================
spark.sql("""
SELECT deviceId, local_ts, temperature, humidity, battery, server_ts_utc, _rescued_data
FROM iot.autoloader_parsed
ORDER BY server_ts_utc DESC
LIMIT 20
""").show(truncate=False)




