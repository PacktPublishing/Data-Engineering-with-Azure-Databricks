"""
Chapter 10: Optimizing Query Performance and Cost Management
File: 01_delta_lake_performance_tuning.py

This script demonstrates comprehensive Delta Lake performance tuning techniques,
including:
- OPTIMIZE and file compaction
- Z-Ordering for multi-dimensional data skipping
- Liquid Clustering (the modern replacement for partitioning + Z-Order)
- Predictive Optimization for Unity Catalog managed tables
- Measuring the before/after impact of each optimization

Prerequisites:
- Azure Databricks Runtime 15.2 or higher (required for Liquid Clustering)
- Unity Catalog enabled workspace
- Sufficient permissions to run OPTIMIZE on target tables

Run this notebook in Azure Databricks. The `spark` session is pre-configured.
"""

# ---------------------------------------------------------------------------
# 1. Setup: Create a sample Delta table with intentionally fragmented files
# ---------------------------------------------------------------------------
# In production, you would point these at your real tables.
# Here we create a synthetic dataset to demonstrate the optimizations.

CATALOG = "analytics_dev"
SCHEMA   = "silver"
TABLE_UNOPTIMIZED = f"{CATALOG}.{SCHEMA}.sales_events_unoptimized"
TABLE_ZORDER      = f"{CATALOG}.{SCHEMA}.sales_events_zorder"
TABLE_LIQUID      = f"{CATALOG}.{SCHEMA}.sales_events_liquid"
SOURCE_VOLUME     = "/Volumes/analytics_dev/sales_raw/staging/"

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, DateType
)
import time

print("=== Chapter 10: Delta Lake Performance Tuning ===\n")

# ---------------------------------------------------------------------------
# 2. Generate a synthetic sales dataset
# ---------------------------------------------------------------------------
print("Step 1: Generating synthetic sales dataset (5 million rows)...")

NUM_ROWS       = 5_000_000
NUM_CUSTOMERS  = 100_000
NUM_PRODUCTS   = 10_000
REGIONS        = ["US", "EU", "APAC", "LATAM", "MEA"]

df_synthetic = (
    spark.range(NUM_ROWS)
    .withColumn("event_id",        F.concat(F.lit("EVT-"), F.col("id").cast(StringType())))
    .withColumn("customer_id",     F.concat(F.lit("CUST-"), (F.rand() * NUM_CUSTOMERS).cast(IntegerType()).cast(StringType())))
    .withColumn("product_id",      F.concat(F.lit("PROD-"), (F.rand() * NUM_PRODUCTS).cast(IntegerType()).cast(StringType())))
    .withColumn("region",          F.element_at(F.array([F.lit(r) for r in REGIONS]), (F.rand() * len(REGIONS) + 1).cast(IntegerType())))
    .withColumn("quantity",        (F.rand() * 10 + 1).cast(IntegerType()))
    .withColumn("unit_price",      F.round(F.rand() * 500 + 1, 2))
    .withColumn("revenue_usd",     F.round(F.col("quantity") * F.col("unit_price"), 2))
    # Simulate 2 years of data spread across many small files
    .withColumn("event_timestamp", (F.lit(1640995200) + (F.rand() * 63072000)).cast(TimestampType()))
    .withColumn("event_date",      F.to_date(F.col("event_timestamp")))
    .drop("id")
)

print(f"  Generated {NUM_ROWS:,} rows across {len(REGIONS)} regions.")


# ---------------------------------------------------------------------------
# 3. Write the data in many small files (simulating a fragmented table)
# ---------------------------------------------------------------------------
print("\nStep 2: Writing fragmented data (many small files)...")

# Writing with high parallelism creates many small files — a common anti-pattern
df_synthetic.repartition(500).write.format("delta").mode("overwrite").saveAsTable(TABLE_UNOPTIMIZED)
print(f"  Written to: {TABLE_UNOPTIMIZED}")

# Check the number of files before optimization
files_before = spark.sql(f"DESCRIBE DETAIL {TABLE_UNOPTIMIZED}").select("numFiles").collect()[0][0]
print(f"  Files before OPTIMIZE: {files_before:,}")


# ---------------------------------------------------------------------------
# 4. Technique A: OPTIMIZE with Z-Ordering
# ---------------------------------------------------------------------------
# Z-Ordering co-locates related data in the same set of files, enabling
# Delta Lake's data skipping to skip large portions of data during queries.
# Best for: tables with 2-4 high-cardinality filter columns.

print("\nStep 3: Applying OPTIMIZE with Z-ORDER BY...")

# First, copy the table for a fair comparison
spark.sql(f"CREATE OR REPLACE TABLE {TABLE_ZORDER} AS SELECT * FROM {TABLE_UNOPTIMIZED}")

start_time = time.time()
spark.sql(f"""
    OPTIMIZE {TABLE_ZORDER}
    ZORDER BY (customer_id, product_id, region)
""")
zorder_time = time.time() - start_time

files_after_zorder = spark.sql(f"DESCRIBE DETAIL {TABLE_ZORDER}").select("numFiles").collect()[0][0]
print(f"  OPTIMIZE + ZORDER completed in {zorder_time:.1f}s")
print(f"  Files after OPTIMIZE: {files_after_zorder:,} (reduced from {files_before:,})")

# Measure query performance improvement
print("\n  Measuring query performance (filter on customer_id)...")

start_time = time.time()
spark.sql(f"SELECT COUNT(*), SUM(revenue_usd) FROM {TABLE_UNOPTIMIZED} WHERE customer_id = 'CUST-42000'").collect()
unoptimized_query_time = time.time() - start_time

start_time = time.time()
spark.sql(f"SELECT COUNT(*), SUM(revenue_usd) FROM {TABLE_ZORDER} WHERE customer_id = 'CUST-42000'").collect()
zorder_query_time = time.time() - start_time

print(f"  Query time (unoptimized): {unoptimized_query_time:.2f}s")
print(f"  Query time (Z-Ordered):   {zorder_query_time:.2f}s")
if unoptimized_query_time > 0:
    print(f"  Speedup: {unoptimized_query_time / zorder_query_time:.1f}x faster")


# ---------------------------------------------------------------------------
# 5. Technique B: Liquid Clustering (Modern Approach — DBR 15.2+)
# ---------------------------------------------------------------------------
# Liquid Clustering is the recommended replacement for both partitioning
# and Z-Ordering. It provides:
# - Incremental clustering (no full table rewrite needed)
# - Automatic cluster maintenance
# - Better performance for high-cardinality columns
# - Flexibility to change clustering columns without rewriting the table

print("\nStep 4: Creating a table with Liquid Clustering...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {TABLE_LIQUID}
    CLUSTER BY (customer_id, product_id, region)
    AS SELECT * FROM {TABLE_UNOPTIMIZED}
""")

print(f"  Table created with Liquid Clustering: {TABLE_LIQUID}")
print("  Clustering columns: customer_id, product_id, region")

# Run OPTIMIZE to apply the initial clustering
print("  Running OPTIMIZE to apply initial clustering...")
start_time = time.time()
spark.sql(f"OPTIMIZE {TABLE_LIQUID}")
liquid_optimize_time = time.time() - start_time

files_after_liquid = spark.sql(f"DESCRIBE DETAIL {TABLE_LIQUID}").select("numFiles").collect()[0][0]
print(f"  OPTIMIZE completed in {liquid_optimize_time:.1f}s")
print(f"  Files after Liquid Clustering: {files_after_liquid:,}")

# Measure query performance
start_time = time.time()
spark.sql(f"SELECT COUNT(*), SUM(revenue_usd) FROM {TABLE_LIQUID} WHERE customer_id = 'CUST-42000'").collect()
liquid_query_time = time.time() - start_time
print(f"  Query time (Liquid Clustered): {liquid_query_time:.2f}s")


# ---------------------------------------------------------------------------
# 6. Predictive Optimization (Unity Catalog Managed Tables)
# ---------------------------------------------------------------------------
# For Unity Catalog managed tables, Predictive Optimization automatically
# runs OPTIMIZE and VACUUM in the background. You do NOT need to schedule
# manual OPTIMIZE jobs for these tables.

print("\nStep 5: Enabling Predictive Optimization (Unity Catalog)...")

# Enable at the catalog level (applies to all managed tables)
spark.sql(f"ALTER CATALOG {CATALOG} ENABLE PREDICTIVE OPTIMIZATION")

# Or enable at the schema level
spark.sql(f"ALTER SCHEMA {CATALOG}.{SCHEMA} ENABLE PREDICTIVE OPTIMIZATION")

# Or enable for a specific table
spark.sql(f"ALTER TABLE {TABLE_LIQUID} ENABLE PREDICTIVE OPTIMIZATION")

print("  Predictive Optimization enabled.")
print("  Databricks will now automatically run OPTIMIZE and VACUUM as needed.")
print("  Track costs in: system.storage.predictive_optimization_operations_history")


# ---------------------------------------------------------------------------
# 7. When to run OPTIMIZE manually
# ---------------------------------------------------------------------------
# Manual OPTIMIZE is still appropriate for:
# - External tables (not managed by Unity Catalog)
# - Immediately after a large bulk load
# - After a major schema change

print("\nStep 6: Manual OPTIMIZE patterns...")

# Pattern A: Optimize a specific date partition after a bulk load
spark.sql(f"""
    OPTIMIZE {TABLE_ZORDER}
    WHERE event_date = '2024-01-15'
    ZORDER BY (customer_id, product_id)
""")
print("  Pattern A: Optimized a specific date partition after bulk load.")

# Pattern B: Set Auto Optimize properties for incremental writes
spark.sql(f"""
    ALTER TABLE {TABLE_UNOPTIMIZED}
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")
print("  Pattern B: Enabled Auto Optimize for incremental writes.")

# Pattern C: VACUUM to remove old file versions (default retention: 7 days)
spark.sql(f"VACUUM {TABLE_ZORDER} RETAIN 168 HOURS")
print("  Pattern C: Vacuumed old file versions (7-day retention).")


# ---------------------------------------------------------------------------
# 8. Performance Summary
# ---------------------------------------------------------------------------
print("\n=== Performance Summary ===")
print(f"{'Technique':<30} {'Files':<10} {'Query Time':<15}")
print("-" * 55)
print(f"{'Unoptimized':<30} {files_before:<10,} {unoptimized_query_time:<15.2f}s")
print(f"{'OPTIMIZE + Z-ORDER':<30} {files_after_zorder:<10,} {zorder_query_time:<15.2f}s")
print(f"{'Liquid Clustering':<30} {files_after_liquid:<10,} {liquid_query_time:<15.2f}s")
print("\nKey Takeaway: Liquid Clustering provides the best balance of")
print("performance and maintainability for most production workloads.")
