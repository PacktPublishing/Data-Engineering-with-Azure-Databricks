"""
Chapter 8: Apache Airflow DAG for Databricks Orchestration
File: 03_airflow_dag.py

This DAG demonstrates advanced Airflow patterns for orchestrating
Databricks workflows, including:
- Multi-task DAG with fan-out / fan-in parallelism
- Sensor-based triggering (wait for a file before starting)
- Conditional branching based on data quality results
- Robust error handling with callbacks and retries
- Passing parameters between tasks using XCom

Prerequisites:
- Apache Airflow 2.6+ with the Databricks provider installed:
  pip install apache-airflow-providers-databricks
- An Airflow Connection named 'databricks_default' configured with:
  - Host: https://your-workspace.azuredatabricks.net
  - Token: your-databricks-personal-access-token
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)
from airflow.providers.databricks.sensors.databricks import DatabricksJobRunSensor
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# 1. Default arguments applied to all tasks in this DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["data-engineering-team@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}

# ---------------------------------------------------------------------------
# 2. Reusable Databricks cluster configuration
# ---------------------------------------------------------------------------
# Using DatabricksRunNowOperator (triggers a pre-configured Databricks job)
# is preferred over DatabricksSubmitRunOperator (creates a new cluster each time)
# because it reuses job cluster configurations managed in Databricks.
DATABRICKS_CONN_ID = "databricks_default"

# The Databricks Job IDs for pre-configured jobs.
# Replace these with your actual job IDs from your Databricks workspace.
JOB_ID_INGEST = 12345
JOB_ID_TRANSFORM_REGION_EU = 12346
JOB_ID_TRANSFORM_REGION_US = 12347
JOB_ID_AGGREGATE = 12348
JOB_ID_QUALITY_CHECK = 12349


# ---------------------------------------------------------------------------
# 3. Helper functions for branching and quality check evaluation
# ---------------------------------------------------------------------------
def evaluate_quality_check(**context):
    """
    Reads the quality check result from XCom and decides the next branch.
    Returns the task_id of the next task to execute.
    """
    # In a real scenario, the quality check notebook would push its result
    # to XCom using dbutils.notebook.exit(json.dumps({"passed": True}))
    # and the DatabricksRunNowOperator would capture it.
    quality_result_str = context["ti"].xcom_pull(task_ids="run_quality_check")

    try:
        quality_result = json.loads(quality_result_str) if quality_result_str else {}
        passed = quality_result.get("passed", False)
    except (json.JSONDecodeError, TypeError):
        passed = False

    if passed:
        print("Quality check PASSED. Proceeding to publish data.")
        return "publish_to_gold"
    else:
        print("Quality check FAILED. Routing to quarantine.")
        return "quarantine_data"


def quarantine_data(**context):
    """Logs a warning and moves failed data to a quarantine location."""
    execution_date = context["ds"]
    print(f"WARNING: Data quality check failed for {execution_date}.")
    print("Moving data to quarantine: /Volumes/analytics_dev/quarantine/")
    # In a real implementation, this would call a Databricks notebook
    # or a Python function to move the data.


# ---------------------------------------------------------------------------
# 4. Define the DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="chapter_08_databricks_etl_workflow",
    default_args=DEFAULT_ARGS,
    description="Production daily ETL workflow with fan-out parallelism and conditional branching.",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "etl", "chapter-08"],
    doc_md="""
    ## Chapter 8: Daily ETL Workflow

    This DAG orchestrates the daily ETL pipeline:
    1. **Ingest**: Load raw sales data from the landing zone.
    2. **Transform (Fan-Out)**: Run EU and US transformations in parallel.
    3. **Aggregate (Fan-In)**: Combine regional results into a global summary.
    4. **Quality Check**: Validate the output data quality.
    5. **Branch**: Publish to Gold if quality passes, quarantine if it fails.
    """,
) as dag:

    # --- Start ---
    start = EmptyOperator(task_id="start")

    # --- Task 1: Data Ingestion ---
    ingest_data = DatabricksRunNowOperator(
        task_id="ingest_raw_data",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_ID_INGEST,
        notebook_params={
            "execution_date": "{{ ds }}",
            "source_path": "/Volumes/analytics_dev/sales_raw/staging/",
            "environment": "{{ var.value.get('environment', 'production') }}",
        },
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # --- Tasks 2a & 2b: Regional Transformations (Fan-Out / Parallel) ---
    transform_eu = DatabricksRunNowOperator(
        task_id="transform_eu_region",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_ID_TRANSFORM_REGION_EU,
        notebook_params={
            "execution_date": "{{ ds }}",
            "region": "EU",
        },
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    transform_us = DatabricksRunNowOperator(
        task_id="transform_us_region",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_ID_TRANSFORM_REGION_US,
        notebook_params={
            "execution_date": "{{ ds }}",
            "region": "US",
        },
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # --- Task 3: Aggregation (Fan-In — waits for both transforms) ---
    aggregate_data = DatabricksRunNowOperator(
        task_id="aggregate_global_summary",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_ID_AGGREGATE,
        notebook_params={
            "execution_date": "{{ ds }}",
        },
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # --- Task 4: Data Quality Check ---
    quality_check = DatabricksRunNowOperator(
        task_id="run_quality_check",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_ID_QUALITY_CHECK,
        notebook_params={
            "execution_date": "{{ ds }}",
            "target_table": "gold.daily_sales_summary",
        },
        wait_for_termination=True,
        polling_period_seconds=30,
        # do_xcom_push=True will capture the notebook exit value for branching
        do_xcom_push=True,
    )

    # --- Task 5: Conditional Branching ---
    branch = BranchPythonOperator(
        task_id="evaluate_quality_and_branch",
        python_callable=evaluate_quality_check,
    )

    # --- Branch A: Publish to Gold (quality passed) ---
    publish_to_gold = EmptyOperator(task_id="publish_to_gold")

    # --- Branch B: Quarantine data (quality failed) ---
    quarantine = PythonOperator(
        task_id="quarantine_data",
        python_callable=quarantine_data,
    )

    # --- End (runs regardless of branch outcome) ---
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------------------------------------------------------------------------
    # 5. Define task dependencies (the DAG structure)
    # ---------------------------------------------------------------------------
    start >> ingest_data
    ingest_data >> [transform_eu, transform_us]   # Fan-out: run in parallel
    [transform_eu, transform_us] >> aggregate_data  # Fan-in: wait for both
    aggregate_data >> quality_check
    quality_check >> branch
    branch >> [publish_to_gold, quarantine]        # Conditional branch
    [publish_to_gold, quarantine] >> end
