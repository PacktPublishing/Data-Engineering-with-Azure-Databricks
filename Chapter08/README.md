# Chapter 8: Orchestrating Data Workflows

This folder contains the complete, production-ready code examples for **Chapter 8: Orchestrating Data Workflows** from *Data Engineering with Azure Databricks*.

The examples go beyond the book snippets to provide fully runnable, well-commented code that demonstrates real-world patterns for orchestrating data pipelines on Azure Databricks.

## Prerequisites

| Requirement | Version |
|---|---|
| Azure Databricks Runtime | 15.2 or higher |
| Unity Catalog | Enabled |
| Databricks CLI | 0.200+ |
| Databricks SDK for Python | `pip install databricks-sdk` |
| Apache Airflow (optional) | 2.6+ with `apache-airflow-providers-databricks` |

## File Structure

```
chapter_08_code/
├── README.md
├── 01_lakeflow_jobs_config.py          # Create and manage Lakeflow Jobs via SDK
├── 02_adf_pipeline_integration.json    # ADF pipeline ARM template
├── 03_airflow_dag.py                   # Apache Airflow DAG with fan-out/fan-in
├── 04_notebook_modularization.py       # Production-ready modular notebook code
└── 05_databricks_asset_bundle/
    └── databricks.yml                  # DABs configuration for all environments
```

## File Descriptions

### `01_lakeflow_jobs_config.py`
Demonstrates how to use the **Databricks SDK for Python** to programmatically create and manage Lakeflow Jobs. This is the recommended approach for infrastructure-as-code workflows.

Key concepts covered:
- Creating a multi-task job with DAG task dependencies
- Configuring job clusters with autoscaling and spot instances
- Setting SLA alerts and email notifications
- Triggering a manual run and polling for completion
- Querying `system.lakeflow` tables for job run history

**How to run:** Execute in a Databricks notebook or locally with `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables set.

---

### `02_adf_pipeline_integration.json`
An **Azure Data Factory pipeline definition** (ARM template) that integrates with Databricks. Import this into ADF Studio or deploy via the Azure CLI.

Key concepts covered:
- Multi-activity pipeline with conditional branching
- Passing parameters from ADF to Databricks notebooks
- Error handling with retry logic and failure notification activities
- Using Managed Identity for secure authentication

**How to deploy:**
```bash
az datafactory pipeline create \
  --factory-name <your-adf-name> \
  --resource-group <your-rg> \
  --name databricks_etl_pipeline \
  --pipeline @02_adf_pipeline_integration.json
```

---

### `03_airflow_dag.py`
A production-grade **Apache Airflow DAG** that orchestrates Databricks workflows with advanced patterns.

Key concepts covered:
- Fan-out parallelism (EU and US transforms run simultaneously)
- Fan-in synchronization (aggregation waits for both transforms)
- Conditional branching based on data quality results (XCom)
- Robust error handling with callbacks and retries

**How to deploy:** Copy this file to your Airflow `dags/` directory. Update the `JOB_ID_*` constants with your actual Databricks job IDs.

---

### `04_notebook_modularization.py`
A fully modular, production-ready Python module demonstrating how to structure notebook code for maintainability and testability.

Key concepts covered:
- Separating concerns into distinct layers (ingest, transform, validate, output)
- Reusable, testable functions with full type hints and docstrings
- Schema enforcement at the ingestion layer
- Structured exit values for orchestration systems
- Proper logging and error handling

**How to use in Databricks:**
1. Upload this file to your Databricks workspace
2. Import it in a notebook using `%run ./04_notebook_modularization`
3. Call `run_transformation_pipeline(spark, source_path, target_table, execution_date)`

---

### `05_databricks_asset_bundle/databricks.yml`
A **Databricks Asset Bundle (DABs)** configuration file — the recommended way to deploy Databricks resources as code.

Key concepts covered:
- Multi-environment configuration (dev, staging, production)
- Variable substitution for environment-specific settings
- Job cluster configuration with autoscaling and spot instances
- CI/CD-ready deployment workflow

**How to use:**
```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate
databricks auth login --host https://your-workspace.azuredatabricks.net

# Navigate to the bundle directory
cd 05_databricks_asset_bundle

# Validate the configuration
databricks bundle validate

# Deploy to development
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t production

# Run the job in production
databricks bundle run -t production chapter_08_daily_etl_job
```

## Unity Catalog Volume Paths

All examples use the following Unity Catalog path convention, consistent with Chapter 2:

```
/Volumes/analytics_dev/sales_raw/staging/
```

Update these paths to match your own Unity Catalog setup before running.