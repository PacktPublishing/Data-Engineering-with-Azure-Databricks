import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    """Create a DatabricksSession for testing with serverless compute."""
    spark = DatabricksSession.builder.serverless().getOrCreate()
    yield spark


@pytest.fixture
def sample_orders_with_dates(spark):
    """Sample orders DataFrame with proper date types."""
    from pyspark.sql.functions import to_date, col

    data = [
        ("C001", "ORD001", "2023-01-01", 100.0),
        ("C001", "ORD002", "2023-01-05", 150.0),
        ("C001", "ORD003", "2023-01-10", 200.0),
        ("C002", "ORD004", "2023-01-02", 50.0),
        ("C002", "ORD005", "2023-01-08", 75.0),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "order_id", "order_date", "order_amount"]
    )
    return df.withColumn("order_date", to_date(col("order_date")))


@pytest.fixture
def sample_bronze_df(spark):
    """Sample DataFrame for bronze audit column testing."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    data = [
        ("ORD001", "C001", 100.0),
        ("ORD002", "C002", 150.0),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    return spark.createDataFrame(data, schema)


# Edge case fixtures

@pytest.fixture
def empty_orders_df(spark):
    """Empty orders DataFrame for edge case testing."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_amount", DoubleType(), True),
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture
def single_order_customer(spark):
    """Single order customer - boundary testing."""
    from pyspark.sql.functions import to_date, col

    data = [
        ("C001", "ORD001", "2023-01-01", 100.0),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "order_id", "order_date", "order_amount"]
    )
    return df.withColumn("order_date", to_date(col("order_date")))


@pytest.fixture
def same_day_orders(spark):
    """Multiple orders on same day - edge case."""
    from pyspark.sql.functions import to_date, col

    data = [
        ("C001", "ORD001", "2023-01-10", 80.0),
        ("C001", "ORD002", "2023-01-10", 120.0),
        ("C001", "ORD003", "2023-01-10", 50.0),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "order_id", "order_date", "order_amount"]
    )
    return df.withColumn("order_date", to_date(col("order_date")))


@pytest.fixture
def orders_with_nulls(spark):
    """Orders with null values for null handling tests."""
    from pyspark.sql.functions import to_date, col
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    data = [
        ("C001", "ORD001", "2023-01-01", 100.0),
        ("C001", "ORD002", "2023-01-05", None),
        ("C002", "ORD003", "2023-01-02", 50.0),
        ("C002", "ORD004", None, 75.0),
    ]

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_amount", DoubleType(), True),
    ])

    df = spark.createDataFrame(data, schema)
    return df.withColumn("order_date", to_date(col("order_date")))
