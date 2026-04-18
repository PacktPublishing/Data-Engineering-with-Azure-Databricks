"""
Chapter 8: Modularizing Notebooks for Production
File: 04_notebook_modularization.py

This module demonstrates how to structure production-ready, modular
PySpark code for Azure Databricks. It covers:
- Separating concerns: ingestion, transformation, validation, output
- Writing reusable, testable functions instead of monolithic notebooks
- Using widget parameters for parameterized execution
- Proper logging and error handling patterns
- Structured exit values for orchestration systems

This file is designed to be run as a Databricks notebook or as a
standalone Python module imported by other notebooks via %run or
the Databricks notebook utilities.

Prerequisites:
- Azure Databricks Runtime 15.2 or higher
- Unity Catalog enabled workspace
- Tables: bronze.raw_sales_events, silver.cleaned_sales_events
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, BooleanType
)
import logging
import json
from datetime import datetime
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# 1. Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("chapter_08.modularization")


# ---------------------------------------------------------------------------
# 2. Schema definitions (single source of truth)
# ---------------------------------------------------------------------------
RAW_SALES_SCHEMA = StructType([
    StructField("event_id",       StringType(),    nullable=False),
    StructField("customer_id",    StringType(),    nullable=False),
    StructField("product_id",     StringType(),    nullable=False),
    StructField("quantity",       IntegerType(),   nullable=True),
    StructField("unit_price",     DoubleType(),    nullable=True),
    StructField("currency",       StringType(),    nullable=True),
    StructField("region",         StringType(),    nullable=True),
    StructField("event_timestamp",TimestampType(), nullable=False),
    StructField("source_system",  StringType(),    nullable=True),
])

CLEANED_SALES_SCHEMA = StructType([
    StructField("event_id",       StringType(),    nullable=False),
    StructField("customer_id",    StringType(),    nullable=False),
    StructField("product_id",     StringType(),    nullable=False),
    StructField("quantity",       IntegerType(),   nullable=False),
    StructField("unit_price",     DoubleType(),    nullable=False),
    StructField("revenue_usd",    DoubleType(),    nullable=False),
    StructField("currency",       StringType(),    nullable=False),
    StructField("region",         StringType(),    nullable=False),
    StructField("event_date",     StringType(),    nullable=False),
    StructField("event_timestamp",TimestampType(), nullable=False),
    StructField("is_valid",       BooleanType(),   nullable=False),
    StructField("processing_date",StringType(),    nullable=False),
])


# ---------------------------------------------------------------------------
# 3. Ingestion layer
# ---------------------------------------------------------------------------
def read_raw_sales_data(
    spark: SparkSession,
    source_path: str,
    execution_date: str,
    file_format: str = "parquet"
) -> DataFrame:
    """
    Reads raw sales data from a Unity Catalog Volume for a specific date.

    Args:
        spark: Active SparkSession.
        source_path: Base path to the source data in Unity Catalog Volumes.
        execution_date: The date partition to read (format: YYYY-MM-DD).
        file_format: Source file format (parquet, json, csv, delta).

    Returns:
        A DataFrame with the raw sales data for the given date.

    Raises:
        ValueError: If the source path or execution date is invalid.
        RuntimeError: If no data is found for the given date.
    """
    if not source_path or not execution_date:
        raise ValueError("source_path and execution_date must be provided.")

    dated_path = f"{source_path.rstrip('/')}/date={execution_date}/"
    logger.info(f"Reading {file_format} data from: {dated_path}")

    try:
        df = (
            spark.read
            .format(file_format)
            .schema(RAW_SALES_SCHEMA)
            .load(dated_path)
        )
        record_count = df.count()
        logger.info(f"Successfully read {record_count:,} records from {dated_path}")

        if record_count == 0:
            raise RuntimeError(f"No data found at {dated_path} for date {execution_date}")

        return df

    except Exception as e:
        logger.error(f"Failed to read data from {dated_path}: {e}")
        raise


# ---------------------------------------------------------------------------
# 4. Transformation layer
# ---------------------------------------------------------------------------
def clean_and_enrich_sales_data(
    df: DataFrame,
    execution_date: str,
    exchange_rates: Optional[dict] = None
) -> Tuple[DataFrame, dict]:
    """
    Applies business rules to clean and enrich raw sales data.

    Business rules applied:
    - Remove records with null event_id, customer_id, or event_timestamp
    - Remove records with non-positive quantity or unit_price
    - Standardize currency codes to uppercase
    - Standardize region codes to uppercase
    - Calculate revenue in USD using exchange rates
    - Add processing metadata columns

    Args:
        df: Raw sales DataFrame.
        execution_date: The logical execution date (for metadata).
        exchange_rates: Optional dict mapping currency codes to USD rates.
                        Defaults to {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}.

    Returns:
        A tuple of (cleaned_df, quality_metrics_dict).
    """
    if exchange_rates is None:
        exchange_rates = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "CAD": 0.74}

    logger.info("Applying data cleaning and enrichment transformations...")
    input_count = df.count()

    # Step 1: Remove records with null primary keys or timestamps
    df_not_null = df.filter(
        F.col("event_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("event_timestamp").isNotNull()
    )
    null_dropped = input_count - df_not_null.count()
    logger.info(f"  Dropped {null_dropped:,} records with null primary keys.")

    # Step 2: Remove records with invalid quantities or prices
    df_valid_amounts = df_not_null.filter(
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0)
    )
    invalid_amounts_dropped = df_not_null.count() - df_valid_amounts.count()
    logger.info(f"  Dropped {invalid_amounts_dropped:,} records with invalid amounts.")

    # Step 3: Standardize string columns and add derived columns
    exchange_rate_map = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in exchange_rates.items()] for item in pair]
    )

    df_enriched = (
        df_valid_amounts
        .withColumn("currency",       F.upper(F.trim(F.col("currency"))))
        .withColumn("region",         F.upper(F.trim(F.col("region"))))
        .withColumn("event_date",     F.to_date(F.col("event_timestamp")).cast(StringType()))
        .withColumn("processing_date",F.lit(execution_date))
        .withColumn("is_valid",       F.lit(True))
        # Calculate revenue in USD using the exchange rate map
        .withColumn(
            "revenue_usd",
            F.col("quantity") * F.col("unit_price") *
            F.coalesce(exchange_rate_map[F.col("currency")], F.lit(1.0))
        )
        # Select only the columns defined in the output schema
        .select(
            "event_id", "customer_id", "product_id",
            "quantity", "unit_price", "revenue_usd",
            "currency", "region", "event_date",
            "event_timestamp", "is_valid", "processing_date"
        )
    )

    output_count = df_enriched.count()
    quality_metrics = {
        "input_records":            input_count,
        "output_records":           output_count,
        "null_records_dropped":     null_dropped,
        "invalid_amount_dropped":   invalid_amounts_dropped,
        "total_records_dropped":    input_count - output_count,
        "pass_rate_pct":            round((output_count / input_count) * 100, 2) if input_count > 0 else 0,
        "execution_date":           execution_date,
    }

    logger.info(f"Transformation complete. Output: {output_count:,} records "
                f"({quality_metrics['pass_rate_pct']}% pass rate).")
    return df_enriched, quality_metrics


# ---------------------------------------------------------------------------
# 5. Validation layer
# ---------------------------------------------------------------------------
def validate_output_data(df: DataFrame, min_pass_rate: float = 95.0) -> bool:
    """
    Runs post-transformation data quality checks on the output DataFrame.

    Checks:
    - No null values in critical columns
    - All revenue_usd values are positive
    - Pass rate is above the minimum threshold

    Args:
        df: The cleaned and enriched DataFrame to validate.
        min_pass_rate: Minimum acceptable percentage of valid records.

    Returns:
        True if all checks pass, False otherwise.
    """
    logger.info("Running output data validation checks...")
    all_passed = True

    # Check 1: No nulls in critical columns
    critical_cols = ["event_id", "customer_id", "revenue_usd", "event_date"]
    for col_name in critical_cols:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            logger.warning(f"  FAIL: Found {null_count:,} null values in '{col_name}'")
            all_passed = False
        else:
            logger.info(f"  PASS: No null values in '{col_name}'")

    # Check 2: All revenue values are positive
    negative_revenue = df.filter(F.col("revenue_usd") <= 0).count()
    if negative_revenue > 0:
        logger.warning(f"  FAIL: Found {negative_revenue:,} records with non-positive revenue.")
        all_passed = False
    else:
        logger.info("  PASS: All revenue_usd values are positive.")

    # Check 3: Minimum record count
    total_records = df.count()
    if total_records < 100:
        logger.warning(f"  WARN: Very low record count: {total_records:,}. Expected at least 100.")

    logger.info(f"Validation complete. All checks passed: {all_passed}")
    return all_passed


# ---------------------------------------------------------------------------
# 6. Output layer
# ---------------------------------------------------------------------------
def write_to_delta_table(
    df: DataFrame,
    target_table: str,
    partition_by: list = None,
    mode: str = "append"
) -> None:
    """
    Writes a DataFrame to a Unity Catalog Delta table.

    Args:
        df: The DataFrame to write.
        target_table: Fully qualified table name (catalog.schema.table).
        partition_by: List of columns to partition the table by.
        mode: Write mode — 'append', 'overwrite', or 'merge'.

    Raises:
        ValueError: If the target table name is not fully qualified.
        RuntimeError: If the write operation fails.
    """
    parts = target_table.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"target_table must be fully qualified (catalog.schema.table). Got: '{target_table}'"
        )

    logger.info(f"Writing {df.count():,} records to Delta table: {target_table} (mode={mode})")

    try:
        writer = (
            df.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")  # Allow schema evolution
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)
            logger.info(f"  Partitioning by: {partition_by}")

        writer.saveAsTable(target_table)
        logger.info(f"Successfully wrote data to {target_table}")

    except Exception as e:
        logger.error(f"Failed to write to {target_table}: {e}")
        raise RuntimeError(f"Write operation failed: {e}") from e


# ---------------------------------------------------------------------------
# 7. Main orchestration function (entry point for the notebook)
# ---------------------------------------------------------------------------
def run_transformation_pipeline(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    execution_date: str,
) -> dict:
    """
    Orchestrates the full ingestion → transformation → validation → output pipeline.

    This function is the single entry point called by the Databricks notebook.
    It returns a structured result dict suitable for use with dbutils.notebook.exit().

    Args:
        spark: Active SparkSession.
        source_path: Path to the source data in Unity Catalog Volumes.
        target_table: Target Delta table (catalog.schema.table).
        execution_date: The logical execution date (YYYY-MM-DD).

    Returns:
        A dict with keys: success, records_written, quality_metrics, error.
    """
    result = {
        "success": False,
        "records_written": 0,
        "quality_metrics": {},
        "error": None,
        "execution_date": execution_date,
    }

    try:
        # Step 1: Ingest
        raw_df = read_raw_sales_data(spark, source_path, execution_date)

        # Step 2: Transform
        cleaned_df, quality_metrics = clean_and_enrich_sales_data(raw_df, execution_date)
        result["quality_metrics"] = quality_metrics

        # Step 3: Validate
        validation_passed = validate_output_data(cleaned_df)
        if not validation_passed:
            raise RuntimeError("Output data validation failed. Aborting write.")

        # Step 4: Write output
        write_to_delta_table(
            df=cleaned_df,
            target_table=target_table,
            partition_by=["event_date"],
            mode="append",
        )

        result["success"] = True
        result["records_written"] = quality_metrics["output_records"]
        logger.info(f"Pipeline completed successfully for {execution_date}.")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Pipeline failed for {execution_date}: {e}")
        raise

    return result


# ---------------------------------------------------------------------------
# 8. Databricks notebook entry point
# ---------------------------------------------------------------------------
# When this file is run as a Databricks notebook, the following block
# reads widget parameters, executes the pipeline, and exits with a
# structured JSON result that can be captured by the orchestrator.
#
# To test locally (outside Databricks), comment out the dbutils block
# and use the direct call below.

if __name__ == "__main__":
    # --- Local testing (outside Databricks) ---
    # Uncomment and configure for local unit testing with a local SparkSession.
    #
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # result = run_transformation_pipeline(
    #     spark=spark,
    #     source_path="/tmp/test_data/",
    #     target_table="analytics_dev.silver.cleaned_sales_events",
    #     execution_date="2024-01-15",
    # )
    # print(json.dumps(result, indent=2))
    pass

# --- Databricks notebook execution block ---
# Uncomment the following block when running inside a Databricks notebook:
#
# dbutils.widgets.text("source_path",   "/Volumes/analytics_dev/sales_raw/staging/")
# dbutils.widgets.text("target_table",  "analytics_dev.silver.cleaned_sales_events")
# dbutils.widgets.text("execution_date", datetime.now().strftime("%Y-%m-%d"))
#
# source_path    = dbutils.widgets.get("source_path")
# target_table   = dbutils.widgets.get("target_table")
# execution_date = dbutils.widgets.get("execution_date")
#
# result = run_transformation_pipeline(spark, source_path, target_table, execution_date)
#
# # Exit with a JSON result so the orchestrator (Lakeflow Jobs / ADF / Airflow)
# # can capture the outcome and make decisions.
# dbutils.notebook.exit(json.dumps(result))
