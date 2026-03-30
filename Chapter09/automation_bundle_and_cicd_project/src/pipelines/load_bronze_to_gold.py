# Databricks notebook source
# DBTITLE 1,Untitled
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp

# Import pipeline helper functions
from utils.bronze import add_bronze_audit_cols
from utils.silver import add_silver_audit_cols
from utils.schemas import TABLE_SCHEMAS
from utils.features_orders import (
    add_label_next_order_amount,
    add_last_order_amount,
    add_days_since_prev_order,
    add_purchase_count_to_date,
    add_hist_avg_purchase,
    add_total_spend_to_date,
)

# COMMAND ----------


def ingest_bronze_table(table_name):
    @dp.table(
        name=f"{catalog}.{bronze_schema}.{table_name}",
        comment=f"Bronze {table_name} ingested from raw CSVs",
    )
    def bronze():
        table_schema = TABLE_SCHEMAS.get(table_name)
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .option(
                "checkpointLocation",
                f"{raw_volume_path}/_checkpoint/{table_name}/",
            )
            .schema(table_schema)
            .load(f"{raw_volume_path}/{table_name}/")
        )
        return add_bronze_audit_cols(df)


# COMMAND ----------


def create_cdc_flow(table_name: str, key_column: str):
    bronze_source = f"{catalog}.{bronze_schema}.{table_name}"
    silver_target = f"{catalog}.{silver_schema}.{table_name}"
    silver_view = f"{table_name}_silver_vw"

    rules = silver_expectations.get(table_name)
    if not rules or not isinstance(rules, dict):
        raise ValueError(f"No expectations for table: {table_name}")

    @dp.view(name=silver_view)
    @dp.expect_all_or_fail(rules)
    def cdc_view():
        return add_silver_audit_cols(spark.readStream.table(bronze_source))

    dp.create_streaming_table(
        name=f"{silver_target}",
    )

    dp.create_auto_cdc_flow(
        target=silver_target,
        source=silver_view,
        keys=[key_column],
        sequence_by=col("bronze_loaded_ts_utc"),
        except_column_list=[
            "_rescued_data",
            "bronze_loaded_ts_utc",
        ],
        stored_as_scd_type=1,
    )


# COMMAND ----------

# DBTITLE 1,Cell 4

def create_feature_store():
    @dp.materialized_view(
        name=f"{catalog}.{ml_schema}.fs_order_features",
        schema="""
            customer_id STRING NOT NULL,
            order_id STRING NOT NULL,
            order_date DATE NOT NULL,
            order_amount DOUBLE,
            label_next_order_amount DOUBLE,
            last_order_amount DOUBLE,
            days_since_prev_order INT,
            purchase_count_to_date INT,
            hist_avg_purchase DOUBLE,
            total_spend_to_date DOUBLE,
            CONSTRAINT customer_features_pk PRIMARY KEY (customer_id, order_date TIMESERIES)
        """,
        comment="Order-level features",
    )
    def fs_order_features():
        df = spark.read.table(f"{catalog}.{silver_schema}.orders").select(
            col("customer_id").cast("STRING").alias("customer_id"),
            col("order_id").cast("STRING").alias("order_id"),
            col("order_date").cast("date").alias("order_date"),
            col("total_amount").cast("double").alias("order_amount"),
        )
        # Sequence
        df = add_label_next_order_amount(df)
        df = add_last_order_amount(df)
        df = add_days_since_prev_order(df)
        # Aggregates
        df = add_purchase_count_to_date(df)
        df = add_hist_avg_purchase(df)
        df = add_total_spend_to_date(df)
        return df


# COMMAND ----------

# DBTITLE 1,Gold Metrics - Daily Sales

def create_gold_daily_metrics():
    @dp.materialized_view(
        name=f"{catalog}.{gold_schema}.sales_daily_metrics",
        comment="Daily sales performance metrics for trend analysis",
    )
    def sales_daily_metrics():
        orders = spark.read.table(f"{catalog}.{silver_schema}.orders")
        order_items = spark.read.table(f"{catalog}.{silver_schema}.order_items")

        daily_metrics = (
            orders.groupBy(F.col("order_date").cast("date").alias("date"))
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.count("order_id").alias("total_orders"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.avg("total_amount").alias("average_order_value"),
                F.sum(F.when(F.col("repeat_customer") == 1, 1).otherwise(0)).alias(
                    "repeat_orders"
                ),
                F.sum(F.when(F.col("repeat_customer") == 0, 1).otherwise(0)).alias(
                    "new_customer_orders"
                ),
            )
            .withColumn(
                "repeat_order_rate",
                F.round(F.col("repeat_orders") / F.col("total_orders") * 100, 2),
            )
        )

        items_daily = (
            order_items.join(orders.select("order_id", "order_date"), "order_id")
            .groupBy(F.col("order_date").cast("date").alias("date"))
            .agg(
                F.sum("quantity").alias("total_items_sold"),
                F.countDistinct("product_id").alias("unique_products_sold"),
            )
        )

        return daily_metrics.join(items_daily, "date", "left").orderBy("date")


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Gold Metrics - Category Performance

def create_gold_category_performance():
    @dp.materialized_view(
        name=f"{catalog}.{gold_schema}.category_performance",
        comment="Category-level aggregated metrics",
    )
    def category_performance():
        order_items = spark.read.table(f"{catalog}.{silver_schema}.order_items")
        products = spark.read.table(f"{catalog}.{silver_schema}.products")

        return (
            order_items.join(products.select("product_id", "category"), "product_id")
            .groupBy("category")
            .agg(
                F.sum("quantity").alias("total_units_sold"),
                F.sum("line_total").alias("total_revenue"),
                F.countDistinct("product_id").alias("products_sold"),
                F.countDistinct("order_id").alias("orders_with_category"),
                F.avg("line_total").alias("avg_line_total"),
            )
            .orderBy(F.desc("total_revenue"))
        )


# COMMAND ----------



# COMMAND ----------

# PIPELINE LEVEL DECLARATIONS
environment = spark.conf.get("environment")
catalog = spark.conf.get("catalog")
department = spark.conf.get("department")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")
ml_schema = spark.conf.get("ml_schema")
raw_volume_path = spark.conf.get("raw_volume_path")

table_list = ["order_items", "orders", "customers", "products"]

key_columns = {
    "order_items": "order_item_id",
    "orders": "order_id",
    "customers": "customer_id",
    "products": "product_id",
}

silver_expectations = {
    "order_items": {
        "valid_order_item_id": "order_item_id IS NOT NULL",
        "valid_order_id": "order_id IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
        "valid_quantity": "quantity > 0",
        "valid_unit_price": "unit_price > 0",
    },
    "orders": {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_order_total": "total_amount > 0",
    },
    "customers": {
        "valid_customer_id": "customer_id IS NOT NULL",
    },
    "products": {
        "valid_product_id": "product_id IS NOT NULL",
        "valid_unit_price": "unit_price > 0",
        "valid_category": "category IS NOT NULL",
    },
}

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG analytics_dev
# MAGIC --TODO: star schema before metrics
# MAGIC --TODO: developer names schemas with source fraction of data

# COMMAND ----------

# DBTITLE 1,Cell 3
# PIPELINE EXECUTION

# bronze
for table in table_list:
    ingest_bronze_table(table)

# silver
for table_name in table_list:
    create_cdc_flow(table_name, key_columns[table_name])

# feature store
create_feature_store()

# gold metrics
create_gold_daily_metrics()
create_gold_category_performance()
