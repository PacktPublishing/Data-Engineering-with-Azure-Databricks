# Chapter 10: Optimizing Query Performance and Cost Management

This folder contains the complete, production-ready code examples for **Chapter 10: Optimizing Query Performance and Cost Management** from *Data Engineering with Azure Databricks*.

The examples go beyond the book snippets to provide fully runnable, well-commented code that demonstrates real-world patterns for optimizing Spark and Delta Lake workloads on Azure Databricks.

## Prerequisites

| Requirement | Version |
|---|---|
| Azure Databricks Runtime | **15.2 or higher** (required for Liquid Clustering) |
| Unity Catalog | Enabled |
| Databricks SDK | `pip install databricks-sdk` |
| Photon-enabled cluster | Recommended for best performance |

## File Structure

```
chapter_10_code/
├── README.md
├── 01_delta_lake_performance_tuning.py   # OPTIMIZE, Z-Order, Liquid Clustering, Predictive Optimization
├── 02_aqe_and_caching.py                 # AQE features, Disk Cache, Spark Cache, Photon
├── 03_cluster_cost_management.py         # Instance families, autoscaling, pools, cost analysis
└── 04_monitoring_and_debugging.py        # Spark UI guide, skew detection, salting, system tables
```

## File Descriptions

### `01_delta_lake_performance_tuning.py`
Demonstrates the full spectrum of Delta Lake performance tuning techniques, from classic OPTIMIZE to modern Liquid Clustering.

Key concepts covered:
- Generating a synthetic 5-million-row sales dataset for benchmarking
- **OPTIMIZE with Z-Ordering**: co-locates related data for multi-column data skipping
- **Liquid Clustering** (DBR 15.2+): the modern replacement for partitioning + Z-Order
- **Predictive Optimization**: automatic background OPTIMIZE for Unity Catalog managed tables
- Before/after performance measurements for each technique
- Auto Optimize properties (`autoOptimize.optimizeWrite`, `autoOptimize.autoCompact`)

**Run order:** Run this file first — it creates the tables used by the other scripts.

---

### `02_aqe_and_caching.py`
Demonstrates Adaptive Query Execution (AQE) and the full range of caching strategies.

Key concepts covered:
- **AQE Feature 1**: Dynamic Partition Coalescing — merges small shuffle partitions
- **AQE Feature 2**: Dynamic Join Strategy Switching — sort-merge to broadcast at runtime
- **AQE Feature 3**: Automatic Skew Join Handling — splits skewed partitions into sub-tasks
- **Disk Cache** (automatic): Databricks-specific NVMe SSD caching for remote storage reads
- **Spark Cache** (manual): in-memory caching for repeated DataFrame access
- Storage level selection: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY
- When NOT to cache: anti-patterns that hurt rather than help performance
- **Photon** vectorized engine: automatic acceleration for SQL and DataFrame operations

---

### `03_cluster_cost_management.py`
Demonstrates comprehensive cluster cost management strategies using the Databricks SDK.

Key concepts covered:
- **Cluster type selection**: Serverless vs. Job Clusters vs. All-Purpose Clusters
- **Instance family guide**: D-series, F-series, E-series, M-series, L-series
- **Cost-optimized cluster configuration**: spot instances, autoscaling, resource tags
- **Cluster Pools**: pre-warmed VMs for fast cluster startup (<30 seconds)
- **Autoscaling for streaming**: conservative scale-down to avoid latency spikes
- **Cost analysis with system tables**: `system.billing.usage`, `system.lakeflow.job_runs`
- Resource tagging for team-level cost attribution and chargeback

---

### `04_monitoring_and_debugging.py`
A comprehensive guide to diagnosing and fixing the four key Spark performance bottlenecks.

Key concepts covered:
- **The four bottleneck indicators**: data skew, large shuffles, spill to disk, inefficient joins
- **Spark UI navigation guide**: Jobs, Stages, Tasks, SQL/DataFrame, Storage, Executors tabs
- **Skew detection**: analyzing key distribution to identify skewed partitions
- **Salting technique**: distributing skewed keys across multiple partitions (complete example)
- **System table monitoring**: job failures, duration trends, DBU consumption
- **Spill to disk**: detection and remediation strategies

## Running the Examples

### In Azure Databricks (Recommended)

1. Upload the files to your Databricks workspace:
   - Go to **Workspace** → **Import** → select the `.py` files
2. Open each file as a notebook
3. Run cells in order, starting with `01_delta_lake_performance_tuning.py`

### Via Databricks CLI

```bash
# Upload files to workspace
databricks workspace import-dir ./chapter_10_code /Workspace/Users/your@email.com/chapter_10

# Run a specific file as a job
databricks jobs create --json '{
  "name": "Chapter 10 - Performance Tuning",
  "tasks": [{
    "task_key": "run_tuning",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/your@email.com/chapter_10/01_delta_lake_performance_tuning"
    }
  }]
}'
```

## Unity Catalog Volume Paths

All examples use the following Unity Catalog path convention, consistent with Chapter 2:

```
/Volumes/analytics_dev/sales_raw/staging/
```

Update the `CATALOG`, `SCHEMA`, and `SOURCE_VOLUME` variables at the top of each file to match your own Unity Catalog setup before running.

## Key Optimization Decision Framework

| Scenario | Recommended Approach |
|---|---|
| Table has many small files | `OPTIMIZE` + Liquid Clustering |
| Unity Catalog managed table | Enable Predictive Optimization |
| Query filters on 2-4 columns | Liquid Clustering on those columns |
| Same DataFrame used 3+ times | `df.cache()` with `MEMORY_AND_DISK` |
| Large table joined with small table | `F.broadcast(small_df)` hint |
| Straggler tasks in Spark UI | Salting or AQE skew join handling |
| Spill to disk in Spark UI | Increase executor memory or use E-series |
| High shuffle sizes | Tune `spark.sql.shuffle.partitions` or use AQE |
| Scheduled production jobs | Job Clusters with spot instances |
| Interactive development | All-Purpose Cluster with 30-min auto-terminate |
