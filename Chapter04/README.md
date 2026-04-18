# Chapter 4: Apache Spark Architecture and Execution Model

This repository contains the code examples for Chapter 4 of *Data Engineering with Apache Spark on Azure Databricks*. 

The examples provided here are expanded, production-ready versions of the code snippets found in the book. They include additional context, comments, and functionality to help you better understand and apply these concepts in real-world scenarios.

## Code Examples

The chapter code is organized into the following executable Python scripts:

### 1. `01_spark_session_config.py`
Demonstrates how to configure a `SparkSession` with key Azure Databricks optimizations enabled, including Adaptive Query Execution (AQE), dynamic partition coalescing, skew join optimization, and Delta Lake-specific optimizations like auto-compaction and optimized writes.

### 2. `02_lazy_evaluation_dag.py`
Illustrates Spark's lazy evaluation execution model. It clearly separates the definition of transformations (which build the logical plan without executing data) from the triggering of actions (which compile the physical plan and execute the Directed Acyclic Graph across the cluster).

### 3. `03_partitioning_strategies.py`
Explores different data partitioning strategies in Apache Spark:
* **Hash Partitioning:** The default strategy, useful for distributing data evenly based on key hashes.
* **Range Partitioning:** Sorts data into partitions based on value ranges, excellent for time-series queries.
* **File-based Partitioning:** Creates directory structures (e.g., `year=2023/month=01`) to enable partition pruning during reads.

### 4. `04_caching_persistence.py`
Demonstrates how to manage cache in Apache Spark to accelerate reads. It covers basic caching (`MEMORY_AND_DISK`), specific storage levels (like `MEMORY_ONLY` and `DISK_ONLY`), and how to properly unpersist DataFrames to free up memory resources.

### 5. `05_handling_data_skew.py`
Provides a comprehensive example of identifying and mitigating data skew—one of the most common performance bottlenecks in distributed processing. It implements a "salting" technique to distribute heavily skewed keys evenly across partitions during join operations, preventing individual executors from being overwhelmed.

## Running the Examples

These scripts are designed to be run in a PySpark environment. While they can be executed in a local Spark installation, they are best experienced within an Azure Databricks workspace where the platform-specific optimizations can be fully utilized.

To run an example locally (assuming PySpark is installed):
```bash
python 01_spark_session_config.py
```

To run in Databricks:
1. Import the scripts into your Databricks workspace as Notebooks or Python files.
2. Note that in Databricks, the `spark` session is automatically provided, so the session creation blocks are primarily for demonstration or standalone application use.

## Note on File Paths
In the book and these examples, you will see file paths referencing Databricks Unity Catalog Volumes (e.g., `/Volumes/analytics_dev/sales_raw/staging/`). When running these examples in your own environment, you will need to update these paths to point to your actual data locations.
