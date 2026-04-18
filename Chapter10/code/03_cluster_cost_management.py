"""
Chapter 10: Optimizing Query Performance and Cost Management
File: 03_cluster_cost_management.py

This script demonstrates cluster cost management strategies, including:
- Cluster type selection (Serverless vs. Job Clusters vs. All-Purpose)
- Right-sizing with instance families (D, F, E, L series)
- Autoscaling configuration via the Databricks SDK
- Spot instance strategies
- Cluster policies for cost governance
- Resource tagging for cost attribution
- Querying system tables for cost analysis

Prerequisites:
- Azure Databricks Runtime 15.2 or higher
- Databricks SDK: pip install databricks-sdk
- Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables,
  or run inside a Databricks notebook.
"""

import os
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

print("=== Chapter 10: Cluster Cost Management ===\n")

# Initialize the Databricks SDK client
w = WorkspaceClient()
print(f"Connected to: {w.config.host}\n")


# ---------------------------------------------------------------------------
# 1. Cluster Type Selection
# ---------------------------------------------------------------------------
# Understanding the three cluster types and when to use each.

print("=== Part 1: Cluster Type Selection ===\n")

CLUSTER_TYPE_GUIDE = """
┌─────────────────────┬──────────────────────────────────────────────────────────┐
│ Cluster Type        │ When to Use                                              │
├─────────────────────┼──────────────────────────────────────────────────────────┤
│ Serverless Compute  │ RECOMMENDED for most workloads. No infrastructure        │
│                     │ management. Instant scaling. Pay only for compute used.  │
│                     │ Best for: notebooks, SQL warehouses, automated jobs.     │
├─────────────────────┼──────────────────────────────────────────────────────────┤
│ Job Clusters        │ Created on-demand for a specific job, terminated when    │
│                     │ complete. No idle cost. Best for: scheduled production   │
│                     │ pipelines where you need custom runtime configuration.   │
├─────────────────────┼──────────────────────────────────────────────────────────┤
│ All-Purpose         │ Persistent clusters for interactive development and      │
│ Clusters            │ exploration. More expensive due to idle time. Set a      │
│                     │ short auto-termination timeout (e.g., 30-60 minutes).   │
└─────────────────────┴──────────────────────────────────────────────────────────┘
"""
print(CLUSTER_TYPE_GUIDE)


# ---------------------------------------------------------------------------
# 2. Instance Family Selection
# ---------------------------------------------------------------------------
print("=== Part 2: Instance Family Selection ===\n")

INSTANCE_FAMILY_GUIDE = """
Azure VM Instance Families for Databricks:

┌──────────────────┬────────────────────────────────────────────────────────────┐
│ Family           │ Best For                                                   │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ D-series         │ General-purpose workloads. Good starting point.            │
│ (Dv3, Dv4, Dv5)  │ Balanced CPU/memory ratio. Example: Standard_D4ds_v5      │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ F-series         │ Compute-intensive workloads: complex transformations,      │
│ (Fsv2)           │ feature engineering, ML training. High CPU/memory ratio.  │
│                  │ Example: Standard_F8s_v2                                   │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ E-series         │ Memory-intensive workloads: large shuffles, wide joins,    │
│ (Ev3, Ev4, Ev5)  │ large caches. High memory/CPU ratio.                      │
│                  │ Example: Standard_E8ds_v5                                  │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ M-series         │ Very large memory workloads: multi-TB datasets in memory.  │
│                  │ Example: Standard_M64s                                     │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ L-series         │ Disk Cache workloads: fast local NVMe SSDs for caching.   │
│ (Lsv2, Lsv3)     │ Best for repeated reads of large datasets.                │
│                  │ Example: Standard_L8s_v3                                   │
└──────────────────┴────────────────────────────────────────────────────────────┘
"""
print(INSTANCE_FAMILY_GUIDE)


# ---------------------------------------------------------------------------
# 3. Create a Cost-Optimized Job Cluster via SDK
# ---------------------------------------------------------------------------
print("=== Part 3: Creating a Cost-Optimized Job Cluster ===\n")

# This cluster configuration uses spot instances for workers (up to 90% savings)
# with an on-demand driver for stability.
cost_optimized_cluster = compute.ClusterSpec(
    spark_version="15.4.x-scala2.12",
    node_type_id="Standard_D4ds_v5",   # General-purpose D-series
    driver_node_type_id="Standard_D4ds_v5",
    autoscale=compute.AutoScale(
        min_workers=2,
        max_workers=20,
    ),
    # Use spot instances for workers — on-demand for driver
    azure_attributes=compute.AzureAttributes(
        availability=compute.AzureAvailability.SPOT_WITH_FALLBACK_AZURE,
        spot_bid_max_price=-1,  # Use current spot market price
        first_on_demand=1,      # Keep 1 on-demand worker as fallback
    ),
    spark_conf={
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.databricks.delta.preview.enabled": "true",
    },
    # Auto-terminate after 30 minutes of inactivity (for all-purpose clusters)
    autotermination_minutes=30,
    # Resource tags for cost attribution
    custom_tags={
        "Project":     "Data-Platform-Migration",
        "Team":        "Data-Engineering",
        "Environment": "Production",
        "CostCenter":  "CC-1234",
        "Chapter":     "10",
    },
)

print("Cost-optimized cluster configuration:")
print(f"  Node type:        {cost_optimized_cluster.node_type_id}")
print(f"  Autoscale:        {cost_optimized_cluster.autoscale.min_workers} - {cost_optimized_cluster.autoscale.max_workers} workers")
print(f"  Spot instances:   Enabled (SPOT_WITH_FALLBACK)")
print(f"  Auto-terminate:   {cost_optimized_cluster.autotermination_minutes} minutes")
print(f"  Tags:             {cost_optimized_cluster.custom_tags}")


# ---------------------------------------------------------------------------
# 4. Autoscaling Configuration
# ---------------------------------------------------------------------------
print("\n=== Part 4: Autoscaling Configuration ===\n")

# Standard autoscaling JSON (for use in cluster UI or Terraform)
standard_autoscaling_config = {
    "autoscale": {
        "min_workers": 2,
        "max_workers": 20
    },
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true"
    },
    "azure_attributes": {
        "availability": "SPOT_WITH_FALLBACK_AZURE",
        "spot_bid_max_price": -1
    }
}

print("Standard autoscaling cluster JSON configuration:")
print(json.dumps(standard_autoscaling_config, indent=2))

# Autoscaling for Structured Streaming (more conservative scale-down)
streaming_autoscaling_config = {
    "autoscale": {
        "min_workers": 4,   # Higher minimum to handle continuous stream load
        "max_workers": 16
    },
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        # More conservative scale-down for streaming to avoid latency spikes
        "spark.databricks.streaming.stateful.operatorStateStore.stateSchemaCheck": "true"
    }
}

print("\nStreaming-optimized autoscaling configuration:")
print(json.dumps(streaming_autoscaling_config, indent=2))


# ---------------------------------------------------------------------------
# 5. Cluster Pools for Faster Startup
# ---------------------------------------------------------------------------
print("\n=== Part 5: Cluster Pools ===\n")

# Cluster pools maintain a set of idle, ready-to-use VMs.
# When a job cluster starts, it draws from the pool instead of waiting
# for the cloud provider to provision new VMs, reducing startup time
# from 5-10 minutes to under 30 seconds.

try:
    pool = w.instance_pools.create(
        instance_pool_name="chapter_10_cost_optimization_pool",
        node_type_id="Standard_D4ds_v5",
        min_idle_instances=2,       # Keep 2 VMs warm at all times
        max_capacity=50,            # Maximum total VMs in the pool
        idle_instance_autotermination_minutes=30,  # Release idle VMs after 30 min
        azure_attributes=compute.AzureAttributes(
            availability=compute.AzureAvailability.SPOT_WITH_FALLBACK_AZURE,
            spot_bid_max_price=-1,
        ),
        custom_tags={
            "Team": "Data-Engineering",
            "Purpose": "Cost-Optimization-Pool",
        },
    )
    print(f"  Cluster pool created: {pool.instance_pool_id}")
    print(f"  Pool keeps {2} VMs warm, reducing cluster start time to <30 seconds.")
    print(f"  Pool auto-terminates idle VMs after 30 minutes.")

    # Clean up the pool after demonstration
    # w.instance_pools.delete(instance_pool_id=pool.instance_pool_id)

except Exception as e:
    print(f"  (Skipped pool creation in this environment: {e})")


# ---------------------------------------------------------------------------
# 6. Cost Analysis with System Tables
# ---------------------------------------------------------------------------
print("\n=== Part 6: Cost Analysis with System Tables ===\n")

# These SQL queries can be run in a Databricks notebook to analyze costs.
# They use the system.billing.usage table which is available in all
# Unity Catalog-enabled workspaces.

COST_ANALYSIS_QUERIES = {
    "Daily Compute Costs by Cluster": """
        SELECT
            DATE(usage_start_time)          AS usage_date,
            cluster_id,
            cluster_name,
            SUM(usage_quantity)             AS total_dbus,
            SUM(usage_quantity * list_price) AS estimated_cost_usd
        FROM system.billing.usage
        WHERE
            usage_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            AND sku_name LIKE '%JOBS%'
        GROUP BY usage_date, cluster_id, cluster_name
        ORDER BY estimated_cost_usd DESC
        LIMIT 20
    """,

    "Cost by Team (via Tags)": """
        SELECT
            usage_metadata.cluster_tags.Team   AS team,
            usage_metadata.cluster_tags.Project AS project,
            SUM(usage_quantity)                 AS total_dbus,
            SUM(usage_quantity * list_price)    AS estimated_cost_usd
        FROM system.billing.usage
        WHERE usage_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
        GROUP BY team, project
        ORDER BY estimated_cost_usd DESC
    """,

    "Predictive Optimization Costs": """
        SELECT
            operation_type,
            COUNT(*)                        AS operation_count,
            SUM(optimized_files_size_bytes) / 1e9 AS total_gb_processed
        FROM system.storage.predictive_optimization_operations_history
        WHERE operation_timestamp >= DATEADD(DAY, -7, CURRENT_DATE())
        GROUP BY operation_type
        ORDER BY total_gb_processed DESC
    """,

    "Job Run Duration and Cost Trends": """
        SELECT
            j.job_name,
            DATE(r.start_time)              AS run_date,
            COUNT(*)                        AS run_count,
            AVG(
                (unix_timestamp(r.end_time) - unix_timestamp(r.start_time)) / 60
            )                               AS avg_duration_minutes,
            MAX(
                (unix_timestamp(r.end_time) - unix_timestamp(r.start_time)) / 60
            )                               AS max_duration_minutes
        FROM system.lakeflow.job_runs r
        JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
        WHERE r.start_time >= DATEADD(DAY, -30, CURRENT_DATE())
        GROUP BY j.job_name, run_date
        ORDER BY j.job_name, run_date DESC
    """
}

print("Cost analysis SQL queries (run in a Databricks notebook):\n")
for query_name, query_sql in COST_ANALYSIS_QUERIES.items():
    print(f"--- {query_name} ---")
    print(query_sql.strip())
    print()

# Run the queries if we have a spark session (inside Databricks)
try:
    print("Running cost analysis queries...")
    for query_name, query_sql in COST_ANALYSIS_QUERIES.items():
        try:
            print(f"\n{query_name}:")
            spark.sql(query_sql.strip()).show(5, truncate=False)
        except Exception as e:
            print(f"  (Skipped: {e})")
except NameError:
    print("  (spark session not available — run inside Databricks to execute queries)")

print("\n=== Cluster Cost Management demonstration complete. ===")
