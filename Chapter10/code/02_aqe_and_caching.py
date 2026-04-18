"""
Chapter 10: Optimizing Query Performance and Cost Management
File: 02_aqe_and_caching.py

This script demonstrates Adaptive Query Execution (AQE) and caching
techniques in Azure Databricks, including:
- Enabling and verifying AQE features
- Demonstrating AQE's three key runtime optimizations
- Disk Cache vs. Spark Cache comparison
- Storage level selection and cache monitoring
- When NOT to cache (anti-patterns)

Prerequisites:
- Azure Databricks Runtime 15.2 or higher
- Unity Catalog enabled workspace
- The tables created in 01_delta_lake_performance_tuning.py

Run this notebook in Azure Databricks. The `spark` session is pre-configured.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time

print("=== Chapter 10: AQE and Caching Techniques ===\n")

CATALOG = "analytics_dev"
SCHEMA  = "silver"
TABLE   = f"{CATALOG}.{SCHEMA}.sales_events_liquid"


# ---------------------------------------------------------------------------
# 1. Adaptive Query Execution (AQE)
# ---------------------------------------------------------------------------
# AQE is enabled by default in Databricks Runtime 10.0+.
# It re-optimizes query plans at runtime using actual data statistics,
# rather than relying solely on pre-execution estimates.

print("=== Part 1: Adaptive Query Execution (AQE) ===\n")

# Verify AQE is enabled (it should be by default on Databricks)
aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
skew_enabled = spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
coalesce_enabled = spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")

print(f"AQE enabled:                          {aqe_enabled}")
print(f"AQE skew join handling:               {skew_enabled}")
print(f"AQE dynamic partition coalescing:     {coalesce_enabled}")

# Fine-tune AQE thresholds for your workload
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
print("\nAQE thresholds configured for 128MB target partition size.")


# ---------------------------------------------------------------------------
# 1a. AQE Feature 1: Dynamic Partition Coalescing
# ---------------------------------------------------------------------------
# After a shuffle, AQE merges small partitions into larger ones.
# This prevents the overhead of scheduling thousands of tiny tasks.

print("\n--- AQE Feature 1: Dynamic Partition Coalescing ---")

# Set a high initial shuffle partition count (common anti-pattern)
spark.conf.set("spark.sql.shuffle.partitions", "2000")

df = spark.table(TABLE)

# This aggregation will create 2000 shuffle partitions initially,
# but AQE will coalesce them down to a reasonable number at runtime.
start_time = time.time()
result = df.groupBy("region", "product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("revenue_usd").alias("total_revenue"),
    F.avg("unit_price").alias("avg_price")
)
result.write.format("noop").mode("overwrite").save()  # Force execution without writing
coalesce_time = time.time() - start_time

print(f"  Aggregation with 2000 initial partitions completed in {coalesce_time:.2f}s")
print("  AQE dynamically coalesced small partitions during execution.")
print("  Check the Spark UI SQL tab to see the 'AQEShuffleRead' nodes in the plan.")


# ---------------------------------------------------------------------------
# 1b. AQE Feature 2: Dynamic Join Strategy Switching
# ---------------------------------------------------------------------------
# AQE can switch a sort-merge join to a broadcast join at runtime
# if it discovers one side is small enough to broadcast.

print("\n--- AQE Feature 2: Dynamic Join Strategy Switching ---")

# Create a small dimension table that AQE will broadcast
df_products = spark.table(TABLE).select("product_id").distinct().limit(500)
df_products.createOrReplaceTempView("small_products_dim")

# This join will initially be planned as a sort-merge join,
# but AQE will switch it to a broadcast join at runtime.
start_time = time.time()
spark.sql(f"""
    SELECT s.region, s.product_id, SUM(s.revenue_usd) AS total_revenue
    FROM {TABLE} s
    JOIN small_products_dim p ON s.product_id = p.product_id
    GROUP BY s.region, s.product_id
""").write.format("noop").mode("overwrite").save()
join_switch_time = time.time() - start_time

print(f"  Join with dynamic strategy switching completed in {join_switch_time:.2f}s")
print("  AQE switched from sort-merge join to broadcast join at runtime.")
print("  Look for 'BroadcastHashJoin' in the executed plan in the Spark UI.")


# ---------------------------------------------------------------------------
# 1c. AQE Feature 3: Automatic Skew Join Handling
# ---------------------------------------------------------------------------
# AQE detects skewed partitions and splits them into smaller sub-tasks,
# preventing a single task from bottlenecking the entire stage.

print("\n--- AQE Feature 3: Automatic Skew Join Handling ---")

# Create a skewed dataset where 'US' has 80% of the data
df_skewed = df.withColumn(
    "skewed_region",
    F.when(F.rand() < 0.8, F.lit("US")).otherwise(F.col("region"))
)
df_skewed.createOrReplaceTempView("skewed_sales")

# Create a region dimension table
df_regions = spark.createDataFrame(
    [("US", "United States"), ("EU", "Europe"), ("APAC", "Asia Pacific"),
     ("LATAM", "Latin America"), ("MEA", "Middle East & Africa")],
    ["region_code", "region_name"]
)
df_regions.createOrReplaceTempView("region_dim")

start_time = time.time()
spark.sql("""
    SELECT s.skewed_region, r.region_name, COUNT(*) AS order_count, SUM(s.revenue_usd) AS revenue
    FROM skewed_sales s
    JOIN region_dim r ON s.skewed_region = r.region_code
    GROUP BY s.skewed_region, r.region_name
""").write.format("noop").mode("overwrite").save()
skew_join_time = time.time() - start_time

print(f"  Skew join with AQE completed in {skew_join_time:.2f}s")
print("  AQE detected the skewed 'US' partition and split it into sub-tasks.")
print("  Without AQE, the 'US' task would have bottlenecked the entire stage.")


# ---------------------------------------------------------------------------
# 2. Caching Strategies
# ---------------------------------------------------------------------------
print("\n=== Part 2: Caching Strategies ===\n")

# ---------------------------------------------------------------------------
# 2a. Disk Cache (Automatic — Databricks-specific)
# ---------------------------------------------------------------------------
# The Disk Cache is a Databricks-specific feature that automatically caches
# data from remote storage (ADLS, S3) to fast local NVMe SSDs on the cluster.
# It is transparent — no code changes needed.
# Best for: Storage-optimized (L-series) instances with local NVMe SSDs.

print("--- Disk Cache ---")
print("The Disk Cache is automatic and transparent.")
print("To enable it, use Storage Optimized (L-series) instances.")
print("Check cache status in the Spark UI > Storage tab.")

# You can pre-warm the disk cache for a specific table
spark.sql(f"CACHE SELECT * FROM {TABLE}")
print(f"  Pre-warmed disk cache for: {TABLE}")


# ---------------------------------------------------------------------------
# 2b. Spark Cache (Manual — for repeated DataFrame access)
# ---------------------------------------------------------------------------
# Spark Cache stores a DataFrame in memory (or memory+disk) across the
# cluster. Use it when you will access the same DataFrame multiple times
# in the same Spark application.

print("\n--- Spark Cache (Manual) ---")

# Load and filter the DataFrame once
df_us_sales = (
    spark.table(TABLE)
    .filter(F.col("region") == "US")
    .select("customer_id", "product_id", "revenue_usd", "event_date")
)

# Cache it — this is lazy; the cache is populated on the first action
df_us_sales.cache()
print("  DataFrame cached (lazy — will be populated on first action).")

# First access: triggers the cache population
print("  First access (populates cache)...")
start_time = time.time()
count1 = df_us_sales.count()
first_access_time = time.time() - start_time
print(f"  First access: {count1:,} rows in {first_access_time:.2f}s")

# Second access: served from cache
print("  Second access (served from cache)...")
start_time = time.time()
count2 = df_us_sales.count()
second_access_time = time.time() - start_time
print(f"  Second access: {count2:,} rows in {second_access_time:.2f}s")

if first_access_time > 0:
    print(f"  Cache speedup: {first_access_time / second_access_time:.1f}x faster on second access")

# Always unpersist when done to free memory for other operations
df_us_sales.unpersist()
print("  Cache cleared with unpersist().")


# ---------------------------------------------------------------------------
# 2c. Storage Levels
# ---------------------------------------------------------------------------
# Choose the right storage level based on your memory constraints.

print("\n--- Storage Level Selection ---")

df_to_cache = spark.table(TABLE).filter(F.col("region") == "EU")

# MEMORY_ONLY: Fastest but uses the most memory. Data is lost if evicted.
# Use when: You have ample memory and need maximum speed.
df_to_cache.persist(StorageLevel.MEMORY_ONLY)
df_to_cache.count()  # Trigger caching
print("  MEMORY_ONLY: Fastest. Use when memory is plentiful.")
df_to_cache.unpersist()

# MEMORY_AND_DISK: Falls back to disk if memory is full. Safer choice.
# Use when: You want caching but can't guarantee enough memory.
df_to_cache.persist(StorageLevel.MEMORY_AND_DISK)
df_to_cache.count()
print("  MEMORY_AND_DISK: Balanced. Falls back to disk if memory is full.")
df_to_cache.unpersist()

# DISK_ONLY: Stores entirely on disk. Slower but uses no executor memory.
# Use when: The DataFrame is too large to fit in memory.
df_to_cache.persist(StorageLevel.DISK_ONLY)
df_to_cache.count()
print("  DISK_ONLY: Memory-efficient. Use for very large DataFrames.")
df_to_cache.unpersist()


# ---------------------------------------------------------------------------
# 2d. When NOT to Cache (Anti-Patterns)
# ---------------------------------------------------------------------------
print("\n--- When NOT to Cache ---")
print("""
Because caching is often overused as a default optimization strategy,
it's equally important to recognize when it is unnecessary or counterproductive.

Anti-patterns to avoid:

1. Single-use DataFrames: If you only access a DataFrame once, caching
   adds overhead (memory allocation + serialization) with no benefit.
   BAD:  df.cache(); df.write.saveAsTable("my_table")
   GOOD: df.write.saveAsTable("my_table")

2. DataFrames larger than available memory: If the DataFrame doesn't fit
   in memory, Spark will spill to disk or evict other cached data, causing
   more harm than good. Check executor memory before caching large datasets.

3. Frequently updated data: If the underlying data changes often, your
   cached version will become stale. Use cache.invalidate() or unpersist()
   and re-cache after each update.

4. Simple filter + aggregate queries: Delta Lake's data skipping and
   Photon are often faster than caching for simple aggregations on
   well-optimized tables.
""")


# ---------------------------------------------------------------------------
# 3. Photon Vectorized Engine
# ---------------------------------------------------------------------------
print("=== Part 3: Photon Vectorized Engine ===\n")
print("""
Photon is Databricks' native vectorized query engine that accelerates
SQL and DataFrame operations by processing data in columnar batches
rather than row-by-row.

Photon is automatically enabled on Photon-enabled cluster types.
No code changes are required — it works transparently.

Workloads that benefit most from Photon:
- Large aggregations and GROUP BY operations
- Joins on large tables
- Sorting and filtering on large datasets
- Delta Lake MERGE, UPDATE, and DELETE operations

To verify Photon is active, look for 'Photon' nodes in the Spark UI
SQL / DataFrame tab query plan.
""")

# Demonstrate a Photon-accelerated aggregation
print("Running a Photon-accelerated aggregation...")
start_time = time.time()
spark.sql(f"""
    SELECT
        region,
        DATE_TRUNC('month', event_date) AS month,
        COUNT(DISTINCT customer_id)     AS unique_customers,
        COUNT(*)                        AS total_orders,
        SUM(revenue_usd)                AS total_revenue,
        AVG(unit_price)                 AS avg_unit_price,
        PERCENTILE_APPROX(revenue_usd, 0.95) AS p95_order_value
    FROM {TABLE}
    GROUP BY region, DATE_TRUNC('month', event_date)
    ORDER BY region, month
""").show(10, truncate=False)
photon_time = time.time() - start_time
print(f"  Photon-accelerated aggregation completed in {photon_time:.2f}s")

print("\n=== AQE and Caching demonstration complete. ===")
