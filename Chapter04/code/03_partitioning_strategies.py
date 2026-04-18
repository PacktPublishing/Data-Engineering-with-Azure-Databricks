"""
Chapter 4: Optimizing Spark Jobs with Partitioning and Caching
Example 3: Data Partitioning Strategies

This script demonstrates different partitioning strategies in Apache Spark:
hash partitioning, range partitioning, and file-based partitioning.
It shows how to analyze partition distribution and measure performance differences.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date, lit
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField
import random
import datetime

def create_sample_data(spark, num_records=100000):
    """
    Creates a sample dataset for demonstrating partitioning strategies.
    """
    print(f"Creating sample dataset with {num_records} records...")
    
    # Define a schema for our dataset
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", DateType(), True),
        StructField("event_type", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    
    # Generate random data
    start_date = datetime.date(2023, 1, 1)
    end_date = datetime.date(2023, 12, 31)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    
    data = []
    for _ in range(num_records):
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)
        
        user_id = random.randint(1, 1000)
        event_type = random.choice(["click", "view", "purchase", "login"])
        value = random.randint(10, 500)
        
        data.append((user_id, random_date, event_type, value))
        
    df = spark.createDataFrame(data, schema=schema)
    
    # Add year and month columns for file-based partitioning
    df = df.withColumn("year", year(col("timestamp"))) \
           .withColumn("month", month(col("timestamp")))
           
    return df

def analyze_partition_distribution(df, strategy_name):
    """
    Analyzes how data is distributed across partitions for a given DataFrame.
    """
    print(f"\nAnalyzing partition distribution for {strategy_name}:")
    num_partitions = df.rdd.getNumPartitions()
    print(f"Total number of partitions: {num_partitions}")
    
    # Calculate the number of records per partition
    # Note: This triggers an action (glom) which can be expensive on large datasets
    partition_counts = df.rdd.glom().map(len).collect()
    
    print(f"Records per partition: {partition_counts[:10]}...") # Show first 10
    print(f"Max records in a partition: {max(partition_counts)}")
    print(f"Min records in a partition: {min(partition_counts)}")
    
    # Check for data skew (difference between max and min)
    skew = max(partition_counts) - min(partition_counts)
    print(f"Data skew (Max - Min): {skew}")

def run_partitioning_demo():
    # Create a Spark session
    spark = SparkSession.builder.appName("PartitioningStrategiesDemo").getOrCreate()
    
    try:
        # Load a dataset (or create sample data for demonstration)
        df = create_sample_data(spark)
        
        print("\n--- Strategy 1: Hash Partitioning ---")
        # Hash partitioning (default for most operations)
        # Distributes data based on the hash of the 'user_id' column
        # Useful for join operations where you want all records with the same key on the same node
        hash_partitioned_df = df.repartition(10, "user_id")
        analyze_partition_distribution(hash_partitioned_df, "Hash Partitioning (user_id)")
        
        print("\n--- Strategy 2: Range Partitioning ---")
        # Range partitioning
        # Sorts data and splits it into partitions based on value ranges of 'timestamp'
        # Useful for queries that filter on date or time ranges
        range_partitioned_df = df.repartitionByRange(10, "timestamp")
        analyze_partition_distribution(range_partitioned_df, "Range Partitioning (timestamp)")
        
        print("\n--- Strategy 3: File-based Partitioning ---")
        # Write with file-based partitioning
        # Creates a directory structure like: year=2023/month=1/part-00000.parquet
        # Useful for partition pruning (skipping irrelevant directories during reads)
        output_path = "/tmp/partitioning_demo_output"
        print(f"Writing file-based partitioned data to {output_path}...")
        
        # In a real scenario, you would write to a persistent location like ADLS Gen2:
        # df.write.mode("overwrite").partitionBy("year", "month").parquet("/Volumes/analytics_dev/sales_raw/staging/output")
        
        df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        print("Write complete. Directory structure created based on year and month.")
        
        print("\n--- Partition Pruning Example ---")
        # Demonstrate partition pruning by reading back only a specific month
        # Spark will only read the directory year=2023/month=6, skipping the rest
        print("Reading data for June 2023...")
        pruned_df = spark.read.parquet(output_path).filter((col("year") == 2023) & (col("month") == 6))
        print(f"Records read: {pruned_df.count()}")
        
    finally:
        # Stop the Spark session
        print("\nStopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    run_partitioning_demo()
