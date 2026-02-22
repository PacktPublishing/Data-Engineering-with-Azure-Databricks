"""
Integration tests that verify the deployed pipeline produced expected tables
with data in the target catalog.
"""

import pytest


pytestmark = pytest.mark.integration

SILVER_TABLES = [
    "sales_silver.orders",
    "sales_silver.customers",
    "sales_silver.products",
    "sales_silver.order_items",
]

GOLD_TABLES = [
    "sales_gold.sales_daily_metrics",
    "sales_gold.category_performance",
]

FEATURE_TABLES = [
    "ml_features.fs_order_features",
]


class TestTablesExist:
    """Verify that pipeline output tables exist in the catalog."""

    @pytest.mark.parametrize("table", SILVER_TABLES + GOLD_TABLES + FEATURE_TABLES)
    def test_table_exists(self, spark, catalog, table):
        fqn = f"{catalog}.{table}"
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{table.split('.')[0]}").collect()
        table_names = [row.tableName for row in tables]
        assert table.split(".")[1] in table_names, f"Table {fqn} does not exist"


class TestTablesHaveData:
    """Verify that key tables are non-empty after a pipeline run."""

    @pytest.mark.parametrize("table", SILVER_TABLES)
    def test_silver_table_has_rows(self, spark, catalog, table):
        fqn = f"{catalog}.{table}"
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}").first().cnt
        assert count > 0, f"{fqn} is empty"

    @pytest.mark.parametrize("table", GOLD_TABLES)
    def test_gold_table_has_rows(self, spark, catalog, table):
        fqn = f"{catalog}.{table}"
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}").first().cnt
        assert count > 0, f"{fqn} is empty"


class TestDataQuality:
    """Basic data quality checks on pipeline output."""

    def test_daily_metrics_no_null_dates(self, spark, catalog):
        fqn = f"{catalog}.sales_gold.sales_daily_metrics"
        nulls = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE date IS NULL"
        ).first().cnt
        assert nulls == 0, f"Found {nulls} null date rows in {fqn}"

    def test_orders_have_positive_amounts(self, spark, catalog):
        fqn = f"{catalog}.sales_silver.orders"
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE total_amount <= 0"
        ).first().cnt
        assert bad == 0, f"Found {bad} orders with non-positive total_amount"

    def test_feature_store_has_all_customers(self, spark, catalog):
        """Feature store should cover all customers present in orders."""
        orders_customers = spark.sql(
            f"SELECT COUNT(DISTINCT customer_id) AS cnt FROM {catalog}.sales_silver.orders"
        ).first().cnt
        fs_customers = spark.sql(
            f"SELECT COUNT(DISTINCT customer_id) AS cnt FROM {catalog}.ml_features.fs_order_features"
        ).first().cnt
        assert fs_customers == orders_customers, (
            f"Feature store has {fs_customers} customers but orders has {orders_customers}"
        )
