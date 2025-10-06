from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, get_json_object, expr, regexp_replace
from pyspark.sql.types import *

# Test that we have right version of Spark, Scala and EventHubs connector
spark_version = spark.version
scala_version = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()

print(f"🔹 Spark version: {spark_version}")
print(f"🔹 Scala version: {scala_version}")

# Verify compatibility with Azure Event Hubs connector
required_scala = "2.12"
if scala_version.startswith(required_scala):
    print(f"✅ Compatible: azure-eventhubs-spark_{required_scala} can be used.")
else:
    print(f"❌ Incompatible: You need azure-eventhubs-spark_{scala_version[:4]} instead.")

try:
    cls = spark._jvm.org.apache.spark.sql.eventhubs.EventHubsSourceProvider
    print("✅ Connector loaded successfully:", cls)
except Exception as e:
    print("❌ Connector not found:", e)

NAMESPACE = "streaming-events-ns"
EVENTHUB = "sensor-data"
KEY_NAME = "databricks-access"
KEY_VALUE = "YOUR_KEY" 

conn = (
    f"Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;"
    f"SharedAccessKeyName={KEY_NAME};"
    f"SharedAccessKey={KEY_VALUE};"
    f"EntityPath={EVENTHUB}"
)

# for 2.3.15+ we should encrypt the connection string
enc = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)

ehConf = {
    "eventhubs.connectionString": enc,
}

# Read streaming data from Event Hubs
df = (spark.readStream
          .format("eventhubs")
          .options(**ehConf)
          .load())


# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS iot")

# schema for ONE event
payload_schema = StructType([
    StructField("deviceId", StringType()),
    StructField("ts", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("battery", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("status", StringType())
])

# parse the whole array
parsed_array = df.select(
    col("enqueuedTime").alias("eh_enqueued_ts"),
    col("partition"),
    from_json(col("body").cast("string"), ArrayType(payload_schema)).alias("arr")
)

# explode into individual events
events = parsed_array.selectExpr("eh_enqueued_ts", "partition", "explode(arr) as event")

# flatten JSON
final = events.select(
    "eh_enqueued_ts", "partition",
    col("event.deviceId"), col("event.ts").alias("local_ts"),
    col("event.temperature"), col("event.humidity"),
    col("event.pressure"), col("event.battery"),
    col("event.lat"), col("event.lon"), col("event.status"),
    current_timestamp().alias("server_ts_utc")
)

q = (final.writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "/tmp/checkpoints/eventhub_array_split")
     .toTable("iot.eventhub_parsed"))

# COMMAND ----------

# test data in the table
display(spark.sql("""
SELECT deviceId, local_ts, temperature, humidity, battery, server_ts_utc
FROM iot.eventhub_parsed
ORDER BY server_ts_utc DESC
"""))

# COMMAND ----------


display(spark.sql("""
SELECT *
FROM iot.eventhub_stream
LIMIT 10
"""))


