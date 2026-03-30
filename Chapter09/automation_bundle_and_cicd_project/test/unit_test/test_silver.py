import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys
sys.path.insert(0, "src/pipelines")

from utils.silver import add_silver_audit_cols


class TestAddSilverAuditCols:
    """Tests for add_silver_audit_cols function."""

    def test_adds_silver_loaded_ts_column(self, spark):
        """Test that the function adds the silver_loaded_ts_utc column."""
        data = [("ORD001", "C001", 100.0)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = add_silver_audit_cols(df)
        assert "silver_loaded_ts_utc" in result.columns

    def test_preserves_original_columns(self, spark):
        """Test that original columns are preserved."""
        data = [("ORD001", "C001", 100.0)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = add_silver_audit_cols(df)

        original_cols = ["order_id", "customer_id", "amount"]
        for col in original_cols:
            assert col in result.columns

    def test_row_count_preserved(self, spark):
        """Test that row count is preserved after adding audit columns."""
        data = [
            ("ORD001", "C001", 100.0),
            ("ORD002", "C002", 150.0),
            ("ORD003", "C003", 200.0),
        ]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = add_silver_audit_cols(df)
        assert result.count() == 3

    def test_timestamp_is_not_null(self, spark):
        """Test that silver_loaded_ts_utc is populated."""
        data = [("ORD001", "C001", 100.0)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = add_silver_audit_cols(df)
        row = result.collect()[0]
        assert row["silver_loaded_ts_utc"] is not None

    def test_column_count_increases_by_one(self, spark):
        """Test that exactly one column is added."""
        data = [("ORD001", "C001", 100.0)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        original_col_count = len(df.columns)

        result = add_silver_audit_cols(df)
        assert len(result.columns) == original_col_count + 1

    def test_works_with_empty_dataframe(self, spark):
        """Test that function works with empty DataFrame."""
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame([], schema)

        result = add_silver_audit_cols(df)
        assert "silver_loaded_ts_utc" in result.columns
        assert result.count() == 0

    def test_works_with_complex_schema(self, spark):
        """Test that function works with more complex schemas."""
        data = [
            ("ORD001", "C001", 100.0, "2023-01-01", "Credit Card", True),
        ]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("order_date", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("is_repeat", StringType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = add_silver_audit_cols(df)
        assert "silver_loaded_ts_utc" in result.columns
        assert len(result.columns) == 7
