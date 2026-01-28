# ============================================
# Chapter 7: Lakeflow Pipeline - Complete Example
# ============================================
# This pipeline demonstrates Lakeflow Spark Declarative Pipelines with:
# - Bronze layer: Raw data ingestion
# - Silver layer: Data cleaning and quality checks
# - Gold layer: Business analytics
# ============================================

import dlt
from pyspark.sql.functions import *

# Configuration
VOLUME_PATH = "/Volumes/cat_dev/vol_chapter7/sdp/"

# ============================================
# SECTION 1: Bronze Layer - Raw Ingestion
# ============================================

@dlt.table(
    comment="Raw orders data from CSV files - Bronze layer",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def sdp_orders_bronze():
    """
    Bronze layer: Ingest raw orders data with minimal transformation.
    Uses Auto Loader (cloudFiles) for incremental processing.
    Schema is automatically inferred from the CSV file.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}_schemas/orders")
        .load(f"{VOLUME_PATH}orders.csv")
    )

@dlt.table(
    comment="Raw customers data from CSV files - Bronze layer",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def sdp_customers_bronze():
    """
    Bronze layer: Ingest raw customer data.
    Schema is automatically inferred from the CSV file.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}_schemas/customers")
        .load(f"{VOLUME_PATH}customers.csv")
    )

# ============================================
# SECTION 2: Silver Layer - Data Quality
# ============================================

@dlt.table(
    comment="Cleaned orders with data quality checks - Silver layer",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_status", "order_status IN ('Delivered', 'Shipped', 'Pending')")
@dlt.expect("valid_order_total", "order_total > 0")
def sdp_orders_silver():
    """
    Silver layer: Apply business rules and data quality checks.
    
    Quality rules:
    - Drop rows with invalid/missing critical fields (order_id, customer_id)
    - Drop rows with unexpected order status (only Delivered, Shipped, Pending are valid)
    - Log violations for order_total but keep the rows for investigation
    - Standardize date formats
    - Calculate derived fields
    
    Expected violations:
    - 52 orders with status 'Cancelled' will be dropped (not in allowed statuses)
    """
    return (
        dlt.read_stream("sdp_orders_bronze")
        # Remove duplicates based on order_id
        .dropDuplicates(["order_id"])
        # Convert to TIMESTAMP (not DATE) for watermarking support in downstream tables
        .withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd"))
        # Extract date components for partitioning/analytics
        .withColumn("order_year", year("order_date"))
        .withColumn("order_month", month("order_date"))
        .withColumn("order_quarter", quarter("order_date"))
        .withColumn("order_day_of_week", dayofweek("order_date"))
        # Categorize order value
        .withColumn("order_value_category",
                   when(col("order_total") < 500, "Low")
                   .when(col("order_total") < 1500, "Medium")
                   .when(col("order_total") < 2500, "High")
                   .otherwise("Premium"))
        # Clean order status (standardize case)
        .withColumn("order_status", initcap(trim(col("order_status"))))
    )

@dlt.table(
    comment="Cleaned customers with validation - Silver layer",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL AND length(name) > 0")
@dlt.expect("valid_country", "country IS NOT NULL")
@dlt.expect("recent_signup", "signup_date >= '2023-01-01'")
def sdp_customers_silver():
    """
    Silver layer: Clean and validate customer data.
    
    Quality rules:
    - Drop rows with missing customer_id or name
    - Log violations for missing country (but keep the rows for manual review)
    - Log violations for customers who signed up before 2023 (historical customers)
    - Standardize country and state names
    - Parse signup dates and calculate tenure
    
    Expected violations:
    - 11 customers signed up before 2023-01-01 (logged as warnings)
    """
    return (
        dlt.read_stream("sdp_customers_bronze")
        .dropDuplicates(["customer_id"])
        # Standardize date format
        .withColumn("signup_date", to_date("signup_date", "yyyy-MM-dd"))
        # Calculate customer tenure
        .withColumn("customer_tenure_days", 
                   datediff(current_date(), col("signup_date")))
        .withColumn("customer_tenure_years",
                   round(col("customer_tenure_days") / 365, 1))
        # Standardize text fields (trim whitespace, title case)
        .withColumn("name", trim(col("name")))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("country", initcap(trim(col("country"))))
        .withColumn("email", lower(trim(col("email"))))
        # Categorize customer by tenure
        .withColumn("customer_tenure_segment",
                   when(col("customer_tenure_days") < 180, "New")
                   .when(col("customer_tenure_days") < 365, "Growing")
                   .when(col("customer_tenure_days") < 730, "Established")
                   .otherwise("Loyal"))
    )

# ============================================
# SECTION 3: Gold Layer - Business Analytics
# ============================================

@dlt.table(
    comment="Geographic sales distribution - Gold layer",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def sdp_geographic_sales():
    """
    Gold layer: Sales metrics by geographic location.
    
    This table uses streaming aggregation with watermarks and time windows.
    The window function is required for streaming aggregations with watermarks -
    it groups data into time-based buckets (30-day periods) and allows the
    aggregation to complete once the watermark passes, ensuring correct results
    even with late-arriving data.
    
    Note: We use approx_count_distinct instead of countDistinct because
    exact distinct counts are not supported in streaming aggregations.
    
    Provides:
    - Revenue and customer distribution by state/country over 30-day windows
    - Regional performance metrics with time periods
    """
    from pyspark.sql.functions import approx_count_distinct
    
    # Read orders as stream with watermark
    # Watermark allows data up to 1 day late before finalizing aggregations
    orders_stream = (
        dlt.read_stream("sdp_orders_silver")
        .withWatermark("order_date", "1 day")
    )
    
    # Read customers as batch (not streaming) for join
    # This creates a stream-to-static join which is more efficient
    customers_batch = dlt.read("sdp_customers_silver")
    
    # Join orders with customer location data
    orders_with_location = orders_stream.join(
        customers_batch.select("customer_id", "city", "state", "country"),
        "customer_id",
        "inner"
    )
    
    # Aggregate by geography with 30-day windows
    # Window aggregation is REQUIRED for streaming aggregations with watermarks
    return (
        orders_with_location
        .groupBy(
            window("order_date", "30 days"),  # 30-day time windows
            "country", 
            "state"
        )
        .agg(
            count("order_id").alias("total_orders"),
            sum("order_total").alias("total_revenue"),
            avg("order_total").alias("avg_order_value"),
            approx_count_distinct("customer_id").alias("unique_customers"),
            max("order_total").alias("max_order_value"),
            # Status metrics
            sum(when(col("order_status") == "Delivered", col("order_total")).otherwise(0)).alias("delivered_revenue"),
            sum(when(col("order_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_orders")
        )
        .withColumn("revenue_per_customer", col("total_revenue") / col("unique_customers"))
        .withColumn("cancellation_rate", col("cancelled_orders") / col("total_orders"))
        # Extract window start and end times as separate columns
        .select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            "country",
            "state",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "unique_customers",
            "max_order_value",
            "delivered_revenue",
            "cancelled_orders",
            "revenue_per_customer",
            "cancellation_rate"
        )
    )
