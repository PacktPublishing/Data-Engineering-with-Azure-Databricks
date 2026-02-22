import pytest
from pyspark.sql.functions import col

import sys
sys.path.insert(0, "src/pipelines")

from utils.features_orders import (
    add_label_next_order_amount,
    add_last_order_amount,
    add_days_since_prev_order,
    add_purchase_count_to_date,
    add_hist_avg_purchase,
    add_total_spend_to_date,
)


class TestAddLabelNextOrderAmount:
    """Tests for add_label_next_order_amount function."""

    def test_adds_next_order_amount_column(self, sample_orders_with_dates):
        """Test that the function adds the label_next_order_amount column."""
        result = add_label_next_order_amount(sample_orders_with_dates)
        assert "label_next_order_amount" in result.columns

    def test_next_order_amount_values_for_customer(self, sample_orders_with_dates):
        """Test that next order amount is correctly calculated per customer."""
        result = add_label_next_order_amount(sample_orders_with_dates)
        c001_orders = (
            result.filter(col("customer_id") == "C001")
            .orderBy("order_date")
            .collect()
        )
        # First order should have next = 150.0
        assert c001_orders[0]["label_next_order_amount"] == 150.0
        # Second order should have next = 200.0
        assert c001_orders[1]["label_next_order_amount"] == 200.0
        # Last order should have next = None
        assert c001_orders[2]["label_next_order_amount"] is None

    def test_last_order_has_null_next_amount(self, sample_orders_with_dates):
        """Test that the last order for each customer has null next amount."""
        result = add_label_next_order_amount(sample_orders_with_dates)
        c002_orders = (
            result.filter(col("customer_id") == "C002")
            .orderBy("order_date")
            .collect()
        )
        assert c002_orders[-1]["label_next_order_amount"] is None


class TestAddLastOrderAmount:
    """Tests for add_last_order_amount function."""

    def test_adds_last_order_amount_column(self, sample_orders_with_dates):
        """Test that the function adds the last_order_amount column."""
        result = add_last_order_amount(sample_orders_with_dates)
        assert "last_order_amount" in result.columns

    def test_last_order_amount_values_for_customer(self, sample_orders_with_dates):
        """Test that last order amount is correctly calculated per customer."""
        result = add_last_order_amount(sample_orders_with_dates)
        c001_orders = (
            result.filter(col("customer_id") == "C001")
            .orderBy("order_date")
            .collect()
        )
        # First order should have last = None
        assert c001_orders[0]["last_order_amount"] is None
        # Second order should have last = 100.0
        assert c001_orders[1]["last_order_amount"] == 100.0
        # Third order should have last = 150.0
        assert c001_orders[2]["last_order_amount"] == 150.0

    def test_first_order_has_null_last_amount(self, sample_orders_with_dates):
        """Test that the first order for each customer has null last amount."""
        result = add_last_order_amount(sample_orders_with_dates)
        c002_orders = (
            result.filter(col("customer_id") == "C002")
            .orderBy("order_date")
            .collect()
        )
        assert c002_orders[0]["last_order_amount"] is None


class TestAddDaysSincePrevOrder:
    """Tests for add_days_since_prev_order function."""

    def test_adds_days_since_prev_order_column(self, sample_orders_with_dates):
        """Test that the function adds the days_since_prev_order column."""
        result = add_days_since_prev_order(sample_orders_with_dates)
        assert "days_since_prev_order" in result.columns

    def test_days_since_prev_order_values(self, sample_orders_with_dates):
        """Test that days since previous order is correctly calculated."""
        result = add_days_since_prev_order(sample_orders_with_dates)
        c001_orders = (
            result.filter(col("customer_id") == "C001")
            .orderBy("order_date")
            .collect()
        )
        # First order should have days_since = None
        assert c001_orders[0]["days_since_prev_order"] is None
        # Second order: 2023-01-05 - 2023-01-01 = 4 days
        assert c001_orders[1]["days_since_prev_order"] == 4
        # Third order: 2023-01-10 - 2023-01-05 = 5 days
        assert c001_orders[2]["days_since_prev_order"] == 5

    def test_first_order_has_null_days(self, sample_orders_with_dates):
        """Test that the first order for each customer has null days."""
        result = add_days_since_prev_order(sample_orders_with_dates)
        c002_orders = (
            result.filter(col("customer_id") == "C002")
            .orderBy("order_date")
            .collect()
        )
        assert c002_orders[0]["days_since_prev_order"] is None


class TestAddPurchaseCountToDate:
    """Tests for add_purchase_count_to_date function."""

    def test_adds_purchase_count_column(self, sample_orders_with_dates):
        """Test that the function adds the purchase_count_to_date column."""
        result = add_purchase_count_to_date(sample_orders_with_dates)
        assert "purchase_count_to_date" in result.columns

    def test_purchase_count_values(self, sample_orders_with_dates):
        """Test that purchase count is correctly calculated."""
        result = add_purchase_count_to_date(sample_orders_with_dates)
        c001_orders = (
            result.filter(col("customer_id") == "C001")
            .orderBy("order_date")
            .collect()
        )
        # First order: 0 previous purchases
        assert c001_orders[0]["purchase_count_to_date"] == 0
        # Second order: 1 previous purchase
        assert c001_orders[1]["purchase_count_to_date"] == 1
        # Third order: 2 previous purchases
        assert c001_orders[2]["purchase_count_to_date"] == 2

    def test_count_is_per_customer(self, sample_orders_with_dates):
        """Test that purchase count is calculated per customer."""
        result = add_purchase_count_to_date(sample_orders_with_dates)
        c002_orders = (
            result.filter(col("customer_id") == "C002")
            .orderBy("order_date")
            .collect()
        )
        # C002 first order: 0 previous
        assert c002_orders[0]["purchase_count_to_date"] == 0
        # C002 second order: 1 previous
        assert c002_orders[1]["purchase_count_to_date"] == 1


class TestAddHistAvgPurchase:
    """Tests for add_hist_avg_purchase function."""

    def test_adds_hist_avg_purchase_column(self, sample_orders_with_dates):
        """Test that the function adds the hist_avg_purchase column."""
        result = add_hist_avg_purchase(sample_orders_with_dates)
        assert "hist_avg_purchase" in result.columns

    def test_hist_avg_purchase_values(self, sample_orders_with_dates):
        """Test that historical average is correctly calculated."""
        result = add_hist_avg_purchase(sample_orders_with_dates)
        c001_orders = (
            result.filter(col("customer_id") == "C001")
            .orderBy("order_date")
            .collect()
        )
        # First order: no previous, avg = None
        assert c001_orders[0]["hist_avg_purchase"] is None
        # Second order: avg of [100.0] = 100.0
        assert c001_orders[1]["hist_avg_purchase"] == 100.0
        # Third order: avg of [100.0, 150.0] = 125.0
        assert c001_orders[2]["hist_avg_purchase"] == 125.0

    def test_first_order_has_null_avg(self, sample_orders_with_dates):
        """Test that the first order has null historical average."""
        result = add_hist_avg_purchase(sample_orders_with_dates)
        c002_orders = (
            result.filter(col("customer_id") == "C002")
            .orderBy("order_date")
            .collect()
        )
        assert c002_orders[0]["hist_avg_purchase"] is None


class TestAddTotalSpendToDate:
    """Tests for add_total_spend_to_date function."""

    def test_adds_total_spend_column(self, sample_orders_with_dates):
        """Test that the function adds the total_spend_to_date column."""
        result = add_total_spend_to_date(sample_orders_with_dates)
        assert "total_spend_to_date" in result.columns

    def test_total_spend_values(self, sample_orders_with_dates):
        """Test that total spend is correctly calculated."""
        result = add_total_spend_to_date(sample_orders_with_dates)
        c001_orders = (
            result.filter(col("customer_id") == "C001")
            .orderBy("order_date")
            .collect()
        )
        # First order: no previous, total = None
        assert c001_orders[0]["total_spend_to_date"] is None
        # Second order: sum of [100.0] = 100.0
        assert c001_orders[1]["total_spend_to_date"] == 100.0
        # Third order: sum of [100.0, 150.0] = 250.0
        assert c001_orders[2]["total_spend_to_date"] == 250.0

    def test_first_order_has_null_total(self, sample_orders_with_dates):
        """Test that the first order has null total spend."""
        result = add_total_spend_to_date(sample_orders_with_dates)
        c002_orders = (
            result.filter(col("customer_id") == "C002")
            .orderBy("order_date")
            .collect()
        )
        assert c002_orders[0]["total_spend_to_date"] is None


class TestFeaturePipelineIntegration:
    """Integration tests for chaining multiple feature functions."""

    def test_chain_all_features(self, sample_orders_with_dates):
        """Test that all feature functions can be chained together."""
        df = sample_orders_with_dates
        df = add_label_next_order_amount(df)
        df = add_last_order_amount(df)
        df = add_days_since_prev_order(df)
        df = add_purchase_count_to_date(df)
        df = add_hist_avg_purchase(df)
        df = add_total_spend_to_date(df)

        expected_columns = [
            "customer_id",
            "order_id",
            "order_date",
            "order_amount",
            "label_next_order_amount",
            "last_order_amount",
            "days_since_prev_order",
            "purchase_count_to_date",
            "hist_avg_purchase",
            "total_spend_to_date",
        ]
        assert all(c in df.columns for c in expected_columns)

    def test_row_count_preserved(self, sample_orders_with_dates):
        """Test that row count is preserved after applying all features."""
        original_count = sample_orders_with_dates.count()
        df = sample_orders_with_dates
        df = add_label_next_order_amount(df)
        df = add_last_order_amount(df)
        df = add_days_since_prev_order(df)
        df = add_purchase_count_to_date(df)
        df = add_hist_avg_purchase(df)
        df = add_total_spend_to_date(df)

        assert df.count() == original_count
