"""
Chapter 8: Orchestrating Data Workflows with Lakeflow Jobs
File: 01_lakeflow_jobs_config.py

This script demonstrates how to create, configure, and manage Lakeflow Jobs
(formerly Databricks Jobs) using the Databricks SDK for Python.

It covers:
- Creating a multi-task job with task dependencies (DAG)
- Configuring job clusters vs. all-purpose clusters
- Setting up retries, timeouts, and SLA alerts
- Triggering jobs programmatically and monitoring run status
- Using system tables to analyze job run history

Prerequisites:
- pip install databricks-sdk
- Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
  OR run inside a Databricks notebook where authentication is automatic.
"""

import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# ---------------------------------------------------------------------------
# 1. Initialize the Databricks SDK client
# ---------------------------------------------------------------------------
# When running inside a Databricks notebook, the SDK auto-configures itself.
# When running locally, set DATABRICKS_HOST and DATABRICKS_TOKEN env vars.
w = WorkspaceClient()

print(f"Connected to Databricks workspace: {w.config.host}")


# ---------------------------------------------------------------------------
# 2. Define a reusable job cluster specification
# ---------------------------------------------------------------------------
# Job clusters are created on-demand and terminated when the job completes.
# This is the recommended approach for production workflows because you only
# pay for compute while the job is actively running.
JOB_CLUSTER_KEY = "chapter_08_job_cluster"

job_cluster_spec = jobs.JobCluster(
    job_cluster_key=JOB_CLUSTER_KEY,
    new_cluster=jobs.ClusterSpec(
        spark_version="15.4.x-scala2.12",  # Use Databricks Runtime 15.4 LTS or higher
        node_type_id="Standard_D4ds_v5",   # General-purpose D-series instance
        autoscale=jobs.AutoScale(
            min_workers=2,
            max_workers=10,
        ),
        spark_conf={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
        },
        azure_attributes=jobs.AzureAttributes(
            # Use spot instances for workers to reduce cost by up to 90%
            availability=jobs.AzureAvailability.SPOT_WITH_FALLBACK_AZURE,
            spot_bid_max_price=-1,  # -1 means use the current spot price
        ),
        custom_tags={
            "Project": "Data-Platform",
            "Team": "Data-Engineering",
            "Chapter": "08",
        },
    ),
)


# ---------------------------------------------------------------------------
# 3. Define individual task specifications
# ---------------------------------------------------------------------------
# Each task references a notebook path. Parameters are passed at runtime.
# Tasks can depend on each other, forming a Directed Acyclic Graph (DAG).

task_ingest = jobs.Task(
    task_key="data_ingestion",
    description="Ingest raw sales data from the landing zone into the Bronze layer.",
    job_cluster_key=JOB_CLUSTER_KEY,
    notebook_task=jobs.NotebookTask(
        notebook_path="/Workspace/Repos/data-engineering/chapter_08/notebooks/01_ingest_data",
        base_parameters={
            "source_path": "/Volumes/analytics_dev/sales_raw/staging/",
            "target_table": "bronze.raw_sales_events",
            "execution_date": "{{job.start_time.iso_date}}",
        },
    ),
    timeout_seconds=3600,   # 1 hour timeout
    max_retries=2,
    min_retry_interval_millis=60000,  # Wait 1 minute between retries
    retry_on_timeout=False,
)

task_transform = jobs.Task(
    task_key="data_transformation",
    description="Apply business rules and transform Bronze data to Silver.",
    depends_on=[jobs.TaskDependency(task_key="data_ingestion")],  # DAG dependency
    job_cluster_key=JOB_CLUSTER_KEY,
    notebook_task=jobs.NotebookTask(
        notebook_path="/Workspace/Repos/data-engineering/chapter_08/notebooks/02_transform_data",
        base_parameters={
            "source_table": "bronze.raw_sales_events",
            "target_table": "silver.cleaned_sales_events",
        },
    ),
    timeout_seconds=7200,   # 2 hour timeout
    max_retries=1,
    min_retry_interval_millis=120000,  # Wait 2 minutes between retries
)

task_aggregate = jobs.Task(
    task_key="data_aggregation",
    description="Aggregate Silver data into Gold-layer business metrics.",
    depends_on=[jobs.TaskDependency(task_key="data_transformation")],  # DAG dependency
    job_cluster_key=JOB_CLUSTER_KEY,
    notebook_task=jobs.NotebookTask(
        notebook_path="/Workspace/Repos/data-engineering/chapter_08/notebooks/03_aggregate_data",
        base_parameters={
            "source_table": "silver.cleaned_sales_events",
            "target_table": "gold.daily_sales_summary",
        },
    ),
    timeout_seconds=3600,
    max_retries=1,
    min_retry_interval_millis=60000,
)

task_quality_check = jobs.Task(
    task_key="data_quality_check",
    description="Run data quality checks on the Gold layer output.",
    depends_on=[jobs.TaskDependency(task_key="data_aggregation")],  # DAG dependency
    job_cluster_key=JOB_CLUSTER_KEY,
    notebook_task=jobs.NotebookTask(
        notebook_path="/Workspace/Repos/data-engineering/chapter_08/notebooks/04_quality_checks",
        base_parameters={
            "target_table": "gold.daily_sales_summary",
            "alert_on_failure": "true",
        },
    ),
    timeout_seconds=1800,
    max_retries=0,  # Do not retry quality checks — fail fast
)


# ---------------------------------------------------------------------------
# 4. Create the job with all tasks, schedule, and SLA alert
# ---------------------------------------------------------------------------
print("Creating Lakeflow Job with DAG task dependencies...")

created_job = w.jobs.create(
    name="Chapter_08_Daily_ETL_Pipeline",
    description="Production daily ETL pipeline: Ingest → Transform → Aggregate → Quality Check",
    job_clusters=[job_cluster_spec],
    tasks=[task_ingest, task_transform, task_aggregate, task_quality_check],
    # Schedule: Run daily at 2:00 AM UTC
    schedule=jobs.CronSchedule(
        quartz_cron_expression="0 0 2 * * ?",
        timezone_id="UTC",
        pause_status=jobs.PauseStatus.UNPAUSED,
    ),
    # SLA alert: Notify if the job runs longer than 4 hours
    health=jobs.JobsHealthRules(
        rules=[
            jobs.JobsHealthRule(
                metric=jobs.JobsHealthMetric.RUN_DURATION_SECONDS,
                op=jobs.JobsHealthOperator.GREATER_THAN,
                value=14400,  # 4 hours in seconds
            )
        ]
    ),
    email_notifications=jobs.JobEmailNotifications(
        on_failure=["data-engineering-team@company.com"],
        on_success=["data-engineering-team@company.com"],
        no_alert_for_skipped_runs=True,
    ),
    tags={
        "environment": "production",
        "chapter": "08",
        "team": "data-engineering",
    },
)

print(f"Job created successfully! Job ID: {created_job.job_id}")
print(f"View job at: {w.config.host}/#job/{created_job.job_id}")


# ---------------------------------------------------------------------------
# 5. Trigger a manual run and monitor its status
# ---------------------------------------------------------------------------
print("\nTriggering a manual job run...")

run_response = w.jobs.run_now(
    job_id=created_job.job_id,
    job_parameters={
        # Override the execution_date for this specific manual run
        "execution_date": "2024-01-15",
    },
)

run_id = run_response.run_id
print(f"Run triggered! Run ID: {run_id}")
print(f"Monitor run at: {w.config.host}/#job/{created_job.job_id}/run/{run_id}")

# Poll for completion
print("\nPolling for run completion (checking every 30 seconds)...")
while True:
    run_state = w.jobs.get_run(run_id=run_id)
    life_cycle_state = run_state.state.life_cycle_state
    result_state = run_state.state.result_state

    print(f"  Status: {life_cycle_state} | Result: {result_state}")

    if life_cycle_state in (
        jobs.RunLifeCycleState.TERMINATED,
        jobs.RunLifeCycleState.SKIPPED,
        jobs.RunLifeCycleState.INTERNAL_ERROR,
    ):
        break

    time.sleep(30)

if result_state == jobs.RunResultState.SUCCESS:
    print("\nJob completed successfully!")
else:
    print(f"\nJob finished with result: {result_state}")
    # Get the URL of the failed task for debugging
    for task_run in run_state.tasks:
        if task_run.state.result_state != jobs.RunResultState.SUCCESS:
            print(f"  Failed task: {task_run.task_key}")
            print(f"  Error: {task_run.state.state_message}")


# ---------------------------------------------------------------------------
# 6. Analyze job run history using Databricks System Tables (SQL)
# ---------------------------------------------------------------------------
# This section shows how to query system tables for operational insights.
# Run this inside a Databricks notebook with a spark session available.

SYSTEM_TABLE_ANALYSIS_SQL = """
-- Analyze job run performance over the last 30 days using system tables.
-- system.lakeflow.job_run_timeline provides detailed per-task metrics.

SELECT
    j.job_name,
    r.run_id,
    r.result_state,
    r.start_time,
    r.end_time,
    ROUND((unix_timestamp(r.end_time) - unix_timestamp(r.start_time)) / 60, 2) AS duration_minutes,
    r.trigger_type
FROM
    system.lakeflow.job_runs AS r
JOIN
    system.lakeflow.jobs AS j ON r.job_id = j.job_id
WHERE
    j.job_name = 'Chapter_08_Daily_ETL_Pipeline'
    AND r.start_time >= DATEADD(DAY, -30, CURRENT_DATE())
ORDER BY
    r.start_time DESC;
"""

print("\n--- System Table Query for Job Run History ---")
print("Run the following SQL in a Databricks notebook to analyze run history:")
print(SYSTEM_TABLE_ANALYSIS_SQL)


# ---------------------------------------------------------------------------
# 7. Clean up: Delete the job (optional, comment out to keep the job)
# ---------------------------------------------------------------------------
# w.jobs.delete(job_id=created_job.job_id)
# print(f"\nJob {created_job.job_id} deleted.")
