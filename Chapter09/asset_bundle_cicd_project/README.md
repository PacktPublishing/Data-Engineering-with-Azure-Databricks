# Sales Orders Project

Companion project for **Chapter 9: CI/CD and DevOps for Azure Databricks**. This project implements a complete data pipeline with Asset Bundles, CI/CD automation, and production monitoring.

The dataset was generated for this book and does not reflect real business numbers.

## What This Project Does

Ingests raw CSV files (customers, orders, order items, products) through a medallion architecture:

- **Bronze** - raw ingestion with audit columns
- **Silver** - cleansed and validated records
- **Gold** - daily sales metrics and aggregations
- **ML Features** - per-customer order features for downstream models

## Project Structure

```
sales_orders/
├── databricks.yml              # Bundle configuration and targets
├── resources/
│   ├── schemas.yml             # Unity Catalog schemas
│   ├── volumes.yml             # Storage volumes
│   ├── sales_orders_pipeline.yml
│   ├── sales_orders_job.yml
│   ├── sales_dashboard.yml
│   └── alerts.yml              # Revenue drop and low order alerts
├── src/
│   ├── setup_catalog.sql       # One-time catalog creation
│   ├── upload_raw_files.py     # Upload CSVs to volumes
│   ├── pipelines/
│   │   ├── load_bronze_to_gold.py
│   │   └── utils/
│   │       ├── schemas.py
│   │       ├── bronze.py
│   │       ├── silver.py
│   │       └── features_orders.py
│   ├── dashboards/
│   │   └── sales_analytics_dashboard.lvdash.json
│   └── monitoring/
│       └── monitoring.sql       # Job and cost monitoring queries
├── test/
│   ├── unit_test/
│   │   ├── conftest.py
│   │   ├── test_features_orders.py
│   │   └── test_silver.py
│   └── integration_test/
│       ├── conftest.py
│       └── test_pipeline_output.py
├── .cicd/
│   ├── azure-pipelines-ci.yml  # PR validation pipeline
│   └── azure-pipelines-cd.yml  # Staging and production pipeline
└── pytest.ini
```

## Deployment Targets

| Target    | Catalog             | Schema Prefix     | Use Case               |
| --------- | ------------------- | ----------------- | ---------------------- |
| `dev`     | `analytics_dev`     | `dev_<username>_` | Local development      |
| `dev_ci`  | `analytics_dev`     | _(none)_          | CI pipeline            |
| `staging` | `analytics_staging` | _(none)_          | Pre-production testing |
| `prod`    | `analytics_prod`    | _(none)_          | Production             |

## Prerequisites

- Databricks CLI installed
- A Databricks workspace with Unity Catalog
- Python 3.11+

## Getting Started

1. **Create the target catalog** (one-time per environment):

   Run `src/setup_catalog.sql` in the target workspace, or let the CI/CD pipeline handle it automatically.

2. **Validate the bundle**:

   ```bash
   databricks bundle validate -t dev
   ```

3. **Deploy**:

   ```bash
   databricks bundle deploy -t dev
   ```

4. **Run the job** (uploads sample data and triggers the pipeline):
   ```bash
   databricks bundle run -t dev sales_orders_job
   ```

## Running Tests

Install test dependencies:

```bash
pip install -r requirements-test.txt
```

Run unit tests (uses Databricks Connect with serverless compute):

```bash
pytest test/unit_test/
```

Run integration tests (after deploying and running the pipeline in staging):

```bash
BUNDLE_TARGET=staging pytest test/integration_test/ -m integration
```

## CI/CD

Two Azure DevOps pipelines automate the workflow:

- **CI** - triggers on pull requests. Validates the bundle, runs unit tests, and deploys to `dev_ci`.
- **CD** - triggers on merge to main. Deploys to staging with manual approval, runs the pipeline and integration tests, and rolls back on failure.

Both pipelines create the target catalog automatically before deploying.

## Monitoring

`src/monitoring/monitoring.sql` queries Databricks system tables:

- **Job runs** - history and failures from `system.lakeflow.job_run_timeline`
- **Success rates** - daily job completion rates over 30 days
- **Compute cost** - DBU consumption by SKU from `system.billing.usage`

Data quality alerts in `resources/alerts.yml` fire daily when revenue drops or order counts fall below configured thresholds.
