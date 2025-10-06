# This example demonstrates the core stateful operations in Spark Structured Streaming

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.window import Window
import json
from datetime import datetime

# Event Hub Configuration
NAMESPACE = "streaming-events-ns"
EVENTHUB = "sensor-data"
KEY_NAME = "databricks-access"
KEY_VALUE = "KEY_VALUE"  

conn = (
    f"Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;"
    f"SharedAccessKeyName={KEY_NAME};"
    f"SharedAccessKey={KEY_VALUE};"
    f"EntityPath={EVENTHUB}"
)

# Encrypt connection string for Event Hubs connector
enc = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)

ehConf = {
    "eventhubs.connectionString": enc,
    "eventhubs.consumerGroup": "$Default",
    # Read from the beginning to capture all events (including those sent before stream started)
    "eventhubs.startingPosition": json.dumps({"offset": "@earliest"})
    # For production, use: json.dumps({"offset": "-1"}) to read only new events
}

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS iot_stateful")

# Simple schema for testing
payload_schema = StructType([
    StructField("deviceId", StringType()),
    StructField("ts", StringType()),
    StructField("temperature", DoubleType()),
    StructField("battery", DoubleType())
])

# Read streaming data from Event Hubs
df = (spark.readStream
          .format("eventhubs")
          .options(**ehConf)
          .load())

# Parse and flatten the data
parsed_array = df.select(
    col("enqueuedTime").alias("eh_enqueued_ts"),
    col("partition"),
    from_json(col("body").cast("string"), ArrayType(payload_schema)).alias("arr")
)

events = parsed_array.selectExpr("eh_enqueued_ts", "partition", "explode(arr) as event")

# Flatten JSON and add processing timestamp
sensor_data = events.select(
    "eh_enqueued_ts", "partition",
    col("event.deviceId"), 
    to_timestamp(col("event.ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_timestamp"),
    col("event.temperature"), 
    col("event.battery")
)

# Add watermarks for stateful processing
sensor_data_with_watermark = sensor_data.withWatermark("event_timestamp", "5 minutes")

print("🚀 Starting Stateful Operations Test...")
print("=" * 60)
print("\n⚙️ Configuration:")
print(f"   Event Hub: {EVENTHUB}")
print(f"   Namespace: {NAMESPACE}")
print(f"   Consumer Group: $Default")
print(f"   Starting Position: @earliest (reads all events from beginning)")
print(f"   Watermark: 5 minutes")
print("\n📋 Alert Thresholds:")
print("   🚨 CRITICAL_TEMPERATURE: > 35°C")
print("   ⚠️  HIGH_TEMPERATURE: > 30°C (but ≤ 35°C)")
print("   🚨 CRITICAL_BATTERY: < 10%")
print("   ⚠️  LOW_BATTERY: < 20% (but ≥ 10%)")
print("=" * 60)

# =============================================================================
# 1. Stateful Aggregations - Device State Tracking
# =============================================================================
print("\n📊 1. Stateful Aggregations - Device State Tracking")

# For streaming, we'll use time-based aggregations with groupBy instead of window functions
# Group by device and time window (5 minute tumbling window)
device_summary = (sensor_data_with_watermark
    .groupBy(
        col("deviceId"),
        window(col("event_timestamp"), "5 minutes")
    )
    .agg(
        count("*").alias("message_count"),
        avg("temperature").alias("avg_temperature"),
        avg("battery").alias("avg_battery"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
        min("battery").alias("min_battery"),
        max("battery").alias("max_battery"),
        sum("temperature").alias("temperature_sum"),
        sum("battery").alias("battery_sum"),
        last("temperature").alias("last_temperature"),
        last("battery").alias("last_battery"),
        first("event_timestamp").alias("window_start_time"),
        last("event_timestamp").alias("window_end_time")
    )
    .select(
        col("deviceId"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("message_count"),
        col("avg_temperature"),
        col("avg_battery"),
        col("min_temperature"),
        col("max_temperature"),
        col("min_battery"),
        col("max_battery"),
        col("last_temperature"),
        col("last_battery"),
        (col("max_temperature") - col("min_temperature")).alias("temperature_range"),
        (col("max_battery") - col("min_battery")).alias("battery_range")
    )
)

# Write device summary to Delta table (this starts the stream automatically)
summary_query = (device_summary
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/device_summary")
    .queryName("device_summary_query")
    .toTable("iot_stateful.device_summary")
)

print("✅ Stateful aggregations configured")

# =============================================================================
# 2. Stateful Alert Generation - Pattern Detection
# =============================================================================
print("\n🚨 2. Stateful Alert Generation - Pattern Detection")

# Generate alerts based on threshold conditions (simple rule-based alerts)
device_alerts = (sensor_data_with_watermark
    .withColumn("alert_type",
        when(col("temperature") > 35, "CRITICAL_TEMPERATURE")
        .when(col("temperature") > 30, "HIGH_TEMPERATURE")
        .when(col("battery") < 10, "CRITICAL_BATTERY")
        .when(col("battery") < 20, "LOW_BATTERY")
        .otherwise("NORMAL")
    )
    .withColumn("alert_message",
        when(col("alert_type") == "CRITICAL_TEMPERATURE", 
             concat(lit("Critical temperature detected: "), col("temperature"), lit("°C")))
        .when(col("alert_type") == "HIGH_TEMPERATURE",
             concat(lit("High temperature warning: "), col("temperature"), lit("°C")))
        .when(col("alert_type") == "CRITICAL_BATTERY",
             concat(lit("Critical battery level: "), col("battery"), lit("%")))
        .when(col("alert_type") == "LOW_BATTERY",
             concat(lit("Low battery warning: "), col("battery"), lit("%")))
        .otherwise("Normal operation")
    )
    .withColumn("severity",
        when(col("alert_type").isin("CRITICAL_TEMPERATURE", "CRITICAL_BATTERY"), "CRITICAL")
        .when(col("alert_type").isin("HIGH_TEMPERATURE", "LOW_BATTERY"), "WARNING")
        .otherwise("NORMAL")
    )
    .filter(col("alert_type") != "NORMAL")  # Only keep alert records
    .select(
        col("deviceId"),
        col("event_timestamp"),
        col("temperature"),
        col("battery"),
        col("alert_type"),
        col("alert_message"),
        col("severity")
    )
)

# Write alerts to Delta table (this starts the stream automatically)
alerts_query = (device_alerts
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/device_alerts")
    .queryName("device_alerts_query")
    .toTable("iot_stateful.device_alerts")
)

print("✅ Stateful alert generation configured")

# =============================================================================
# 3. STREAMING QUERIES STATUS
# =============================================================================
print("\n🚀 Streaming Queries Started!")
print("=" * 60)

# The queries are already running because .toTable() starts them automatically
print("✅ Both stateful operations started successfully!")
print("\n📊 Active Streaming Queries:")

# List active streams
active_streams = spark.streams.active
if active_streams:
    for stream in active_streams:
        print(f"  - Query: {stream.name if stream.name else 'Unnamed'}")
        print(f"    ID: {stream.id}")
        print(f"    Status: {stream.status['message'] if 'message' in stream.status else 'Running'}")
        print()
else:
    print("  ⚠️  No active streams found")

print("🔍 Monitoring Tables:")
print("  - iot_stateful.device_summary (Time-window aggregations)")
print("  - iot_stateful.device_alerts (Real-time threshold alerts)")

print("\n📋 Stateful Operations Demonstrated:")
print("Time-Window Aggregations:")
print("  - 5-minute tumbling windows per device")
print("  - Count, average, min, max calculations")
print("  - Used for summaries, trends, statistics")
print("  - Output mode: 'append'")

print("\nReal-Time Alert Generation:")
print("  - Threshold-based alerting")
print("  - Immediate alert generation")
print("  - Used for monitoring, notifications")
print("  - Output mode: 'append'")

print("\n⏱️  Stateful operations are now processing data in real-time!")
print("💡 Tip: Streams run in the background. Use the queries below to view results.")
print("=" * 60)


# View alerts
display(spark.sql("""
    SELECT deviceId, event_timestamp, temperature, battery,
           alert_type, severity, alert_message
    FROM iot_stateful.device_alerts
    ORDER BY event_timestamp DESC
    LIMIT 20
"""))