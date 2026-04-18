"""
Chapter 10: Optimizing Query Performance and Cost Management
File: 04_monitoring_and_debugging.py

This script demonstrates monitoring and debugging techniques for
Spark performance bottlenecks, including:
- Identifying the four key bottleneck types (data skew, large shuffles,
  spill to disk, and inefficient joins)
- Navigating the Spark UI programmatically
- Salting technique for skewed joins
- Using system tables for job monitoring
- Setting up alerting with Lakeflow Jobs SLA

Prerequisites:
- Azure Databricks Runtime 15.2 or higher
- Unity Catalog enabled workspace
- The tables created in 01_delta_lake_performance_tuning.py

Run this notebook in Azure Databricks. The `spark` session is pre-configured.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import time

print("=== Chapter 10: Monitoring and Debugging Performance Bottlenecks ===\n")

CATALOG = "analytics_dev"
SCHEMA  = "silver"
TABLE   = f"{CATALOG}.{SCHEMA}.sales_events_liquid"


# ---------------------------------------------------------------------------
# 1. The Four Key Bottleneck Indicators
# ---------------------------------------------------------------------------
print("=== Part 1: The Four Key Bottleneck Indicators ===\n")

BOTTLENECK_GUIDE = """
When diagnosing performance issues, look for these four indicators in the Spark UI:

1. DATA SKEW
   - Symptom: In the Spark UI > Stages tab, most tasks complete quickly
     but a few "straggler" tasks take 10-100x longer.
   - Root cause: One or more partition keys have disproportionately more
     data than others (e.g., a single customer_id with millions of orders).
   - Fix: Salting (see Part 3), AQE skew join handling, or pre-aggregation.

2. LARGE SHUFFLES
   - Symptom: Stages show very high "Shuffle Write" and "Shuffle Read" sizes
     in the Spark UI > Stages tab. GC time is high.
   - Root cause: Wide transformations (groupBy, join, distinct) on large
     datasets with too many or too few shuffle partitions.
   - Fix: Tune spark.sql.shuffle.partitions, use broadcast joins for small
     tables, or pre-partition data with Liquid Clustering.

3. SPILL TO DISK
   - Symptom: Tasks show non-zero "Spill (Memory)" and "Spill (Disk)" values
     in the Spark UI > Stages > Tasks table.
   - Root cause: A task's data doesn't fit in executor memory, forcing Spark
     to serialize and write intermediate data to disk (very slow).
   - Fix: Increase executor memory, reduce partition size, or use a larger
     instance type (E-series for memory-intensive workloads).

4. INEFFICIENT JOINS
   - Symptom: Long sort-merge join stages visible in the SQL plan.
     High shuffle read/write. No broadcast join in the plan.
   - Root cause: Joining a large table with a small table without broadcasting
     the small table, or joining without proper data co-location.
   - Fix: Use broadcast hints for small tables, enable AQE for automatic
     strategy switching, or pre-partition join keys with Liquid Clustering.
"""
print(BOTTLENECK_GUIDE)


# ---------------------------------------------------------------------------
# 2. Spark UI Navigation Guide
# ---------------------------------------------------------------------------
print("=== Part 2: Spark UI Navigation Guide ===\n")

SPARK_UI_GUIDE = """
How to navigate the Spark UI in Azure Databricks:

1. Open your cluster in the Databricks workspace.
2. Click "Spark UI" in the cluster details page.

Key tabs to use for debugging:

┌─────────────────┬──────────────────────────────────────────────────────────────┐
│ Tab             │ What to Look For                                             │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ Jobs            │ List of all Spark jobs. Click a job to see its stages.       │
│                 │ Look for jobs with unexpectedly long durations.              │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ Stages          │ Breakdown of each stage. Look for:                          │
│                 │ - High "Shuffle Write/Read" (large shuffles)                │
│                 │ - High "Spill (Disk)" (memory pressure)                     │
│                 │ - Long "GC Time" (memory pressure)                          │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ Stages > Tasks  │ Per-task breakdown. Look for:                               │
│                 │ - Straggler tasks (much longer than median = data skew)     │
│                 │ - Non-zero "Spill (Memory)" and "Spill (Disk)" columns      │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ SQL / DataFrame │ Visual query plan. Look for:                                │
│                 │ - "SortMergeJoin" (could be replaced with BroadcastHashJoin)│
│                 │ - "Exchange" nodes (shuffle operations)                     │
│                 │ - "AQEShuffleRead" (AQE is active and coalescing partitions)│
│                 │ - "Photon" nodes (Photon engine is accelerating the query)  │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ Storage         │ Cached DataFrames and RDDs. Check cache hit rates and       │
│                 │ memory/disk usage for cached datasets.                      │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ Executors       │ Per-executor memory usage, GC time, and task counts.        │
│                 │ Uneven task distribution indicates data skew.               │
├─────────────────┼──────────────────────────────────────────────────────────────┤
│ Environment     │ All Spark configuration settings. Verify AQE, shuffle       │
│                 │ partitions, and memory settings are as expected.            │
└─────────────────┴──────────────────────────────────────────────────────────────┘
"""
print(SPARK_UI_GUIDE)


# ---------------------------------------------------------------------------
# 3. Diagnosing Data Skew
# ---------------------------------------------------------------------------
print("=== Part 3: Diagnosing and Fixing Data Skew ===\n")

df = spark.table(TABLE)

# Step 1: Identify skewed keys by checking value distribution
print("Step 1: Identify skewed keys...")
df_skew_analysis = (
    df.groupBy("customer_id")
    .agg(F.count("*").alias("order_count"))
    .orderBy(F.desc("order_count"))
)

print("Top 10 customers by order count (potential skew candidates):")
df_skew_analysis.show(10, truncate=False)

# Calculate skew ratio
stats = df_skew_analysis.agg(
    F.max("order_count").alias("max_count"),
    F.avg("order_count").alias("avg_count"),
    F.percentile_approx("order_count", 0.95).alias("p95_count")
).collect()[0]

skew_ratio = stats["max_count"] / stats["avg_count"] if stats["avg_count"] > 0 else 0
print(f"\nSkew Analysis:")
print(f"  Max orders for a single customer:  {stats['max_count']:,}")
print(f"  Average orders per customer:       {stats['avg_count']:.1f}")
print(f"  P95 orders per customer:           {stats['p95_count']:,}")
print(f"  Skew ratio (max/avg):              {skew_ratio:.1f}x")

if skew_ratio > 10:
    print(f"  WARNING: Skew ratio of {skew_ratio:.1f}x detected! Consider salting.")
else:
    print(f"  Skew ratio is acceptable.")


# ---------------------------------------------------------------------------
# 4. Fixing Data Skew with Salting
# ---------------------------------------------------------------------------
print("\n=== Part 4: Fixing Data Skew with Salting ===\n")

# Salting distributes a skewed key across multiple partitions by appending
# a random "salt" value to the key. The dimension table must be exploded
# to match all possible salt values.

SALT_FACTOR = 10  # Number of salt buckets (increase for more severe skew)

# Create a small dimension table to join against
df_customer_dim = (
    df.select("customer_id")
    .distinct()
    .withColumn("customer_tier", F.when(F.rand() < 0.1, F.lit("Premium")).otherwise(F.lit("Standard")))
    .withColumn("customer_region", F.element_at(F.array([F.lit("US"), F.lit("EU"), F.lit("APAC")]), (F.rand() * 3 + 1).cast(IntegerType())))
)

print(f"Applying salting with SALT_FACTOR={SALT_FACTOR}...")

# Step 1: Add a random salt to the large (skewed) fact table
df_salted_facts = (
    df.withColumn("salt", (F.rand() * SALT_FACTOR).cast(IntegerType()))
      .withColumn("salted_customer_id", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt").cast("string")))
)

# Step 2: Explode the small dimension table to match all salt values
# This creates SALT_FACTOR copies of each row in the dimension table
df_exploded_dim = (
    df_customer_dim
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_FACTOR)])))
    .withColumn("salted_customer_id", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt").cast("string")))
)

print(f"  Dimension table rows before explosion: {df_customer_dim.count():,}")
print(f"  Dimension table rows after explosion:  {df_exploded_dim.count():,} ({SALT_FACTOR}x)")

# Step 3: Join on the salted key
start_time = time.time()
df_result = (
    df_salted_facts
    .join(F.broadcast(df_exploded_dim), "salted_customer_id", "left")
    .groupBy("region", "customer_tier")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("revenue_usd").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.desc("total_revenue"))
)
df_result.show(10, truncate=False)
salted_join_time = time.time() - start_time

print(f"\nSalted join completed in {salted_join_time:.2f}s")
print("The skewed keys are now distributed evenly across partitions.")
print("Check the Spark UI Stages tab — straggler tasks should be eliminated.")


# ---------------------------------------------------------------------------
# 5. Monitoring with System Tables
# ---------------------------------------------------------------------------
print("\n=== Part 5: Monitoring with System Tables ===\n")

# System tables provide a SQL interface to Databricks operational data.
# They are available in all Unity Catalog-enabled workspaces.

MONITORING_QUERIES = {
    "Recent Job Failures": """
        SELECT
            j.job_name,
            r.run_id,
            r.start_time,
            r.end_time,
            r.state_message
        FROM system.lakeflow.job_runs r
        JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
        WHERE
            r.result_state = 'FAILED'
            AND r.start_time >= DATEADD(DAY, -7, CURRENT_DATE())
        ORDER BY r.start_time DESC
        LIMIT 20
    """,

    "Job Duration Trends (SLA Monitoring)": """
        SELECT
            j.job_name,
            DATE(r.start_time)                                          AS run_date,
            COUNT(*)                                                    AS run_count,
            AVG((unix_timestamp(r.end_time) - unix_timestamp(r.start_time)) / 60) AS avg_duration_min,
            MAX((unix_timestamp(r.end_time) - unix_timestamp(r.start_time)) / 60) AS max_duration_min,
            SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failure_count
        FROM system.lakeflow.job_runs r
        JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
        WHERE r.start_time >= DATEADD(DAY, -30, CURRENT_DATE())
        GROUP BY j.job_name, run_date
        ORDER BY j.job_name, run_date DESC
    """,

    "Cluster DBU Consumption by Job": """
        SELECT
            u.usage_metadata.job_name   AS job_name,
            SUM(u.usage_quantity)       AS total_dbus,
            SUM(u.usage_quantity * u.list_price) AS estimated_cost_usd
        FROM system.billing.usage u
        WHERE
            u.usage_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            AND u.sku_name LIKE '%JOBS%'
            AND u.usage_metadata.job_name IS NOT NULL
        GROUP BY job_name
        ORDER BY estimated_cost_usd DESC
        LIMIT 20
    """
}

print("System table monitoring queries:\n")
for query_name, query_sql in MONITORING_QUERIES.items():
    print(f"--- {query_name} ---")
    try:
        spark.sql(query_sql.strip()).show(5, truncate=False)
    except Exception as e:
        print(f"  (Skipped: {e})")
    print()


# ---------------------------------------------------------------------------
# 6. Checking for Spill to Disk
# ---------------------------------------------------------------------------
print("=== Part 6: Detecting and Fixing Spill to Disk ===\n")

print("""
Spill to disk occurs when a task's data exceeds executor memory capacity.
Spark serializes the overflow data to disk, which is 10-100x slower than
memory operations.

How to detect spill in the Spark UI:
1. Go to Spark UI > Stages
2. Click on a slow stage
3. In the Tasks table, look for non-zero values in:
   - "Spill (Memory)" column: data spilled from memory
   - "Spill (Disk)" column: data written to disk

How to fix spill:
1. Increase executor memory (use E-series instances)
2. Reduce partition size by increasing spark.sql.shuffle.partitions
3. Use AQE to automatically coalesce or split partitions
4. Avoid caching large DataFrames that don't fit in memory
5. Use Liquid Clustering to pre-sort data and reduce shuffle size
""")

# Demonstrate configuring memory to reduce spill risk
spark.conf.set("spark.sql.shuffle.partitions", "400")  # More, smaller partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")    # Let AQE fine-tune further

print("Configured shuffle partitions to 400 to reduce per-partition size.")
print("AQE is enabled to further coalesce small partitions at runtime.")

print("\n=== Monitoring and Debugging demonstration complete. ===")
print("\nKey Takeaways:")
print("  1. Always start debugging in the Spark UI Stages > Tasks view.")
print("  2. Straggler tasks = data skew. Fix with salting or AQE.")
print("  3. High shuffle sizes = too few partitions or missing broadcast hints.")
print("  4. Spill to disk = memory pressure. Upgrade instance type or reduce partition size.")
print("  5. Use system tables for proactive monitoring and SLA alerting.")
