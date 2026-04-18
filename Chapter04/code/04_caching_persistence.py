"""
Chapter 4: Optimizing Spark Jobs with Partitioning and Caching
Example 4: Caching and Persistence Mechanisms

This script demonstrates how to manage cache in Apache Spark, including
different storage levels and when to use them.
"""

from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time

def create_sample_dataframe(spark, num_records=1000000):
    """
    Creates a sample DataFrame for caching demonstration.
    """
    print(f"Creating a sample DataFrame with {num_records} records...")
    # Create a DataFrame with random numbers
    df = spark.range(0, num_records).withColumn("value", (col("id") * 2).cast("double"))
    return df

def run_caching_demo():
    # Create a Spark session
    spark = SparkSession.builder.appName("CachingPersistenceDemo").getOrCreate()
    
    try:
        df = create_sample_dataframe(spark)
        
        print("\n--- Uncached Performance ---")
        # Measure time without caching
        start_time = time.time()
        count1 = df.filter(df["value"] > 500000).count()
        end_time = time.time()
        print(f"First action (uncached): {count1} records found in {end_time - start_time:.2f} seconds")
        
        start_time = time.time()
        count2 = df.filter(df["value"] < 100000).count()
        end_time = time.time()
        print(f"Second action (uncached): {count2} records found in {end_time - start_time:.2f} seconds")
        
        print("\n--- Basic Caching (MEMORY_AND_DISK) ---")
        # Cache the DataFrame using the default storage level (MEMORY_AND_DISK for DataFrames)
        print("Caching the DataFrame...")
        df.cache()
        
        # The first action after caching will materialize the cache
        start_time = time.time()
        count3 = df.filter(df["value"] > 500000).count()
        end_time = time.time()
        print(f"First action (populating cache): {count3} records found in {end_time - start_time:.2f} seconds")
        
        # Subsequent actions will read from the cache, which should be much faster
        start_time = time.time()
        count4 = df.filter(df["value"] < 100000).count()
        end_time = time.time()
        print(f"Second action (reading from cache): {count4} records found in {end_time - start_time:.2f} seconds")
        
        print("\n--- Unpersisting ---")
        # Always unpersist when you're done with the cached data to free up memory
        print("Unpersisting the DataFrame...")
        df.unpersist()
        
        print("\n--- Specific Storage Levels ---")
        # Demonstrate persisting with a specific storage level
        print("Persisting with MEMORY_ONLY...")
        # MEMORY_ONLY is the fastest but doesn't spill to disk if memory is full
        df.persist(StorageLevel.MEMORY_ONLY)
        
        # Materialize the cache
        df.count()
        
        # Check if the DataFrame is cached
        is_cached = df.is_cached
        print(f"Is the DataFrame cached? {is_cached}")
        
        # Clean up
        df.unpersist()
        
        print("\n--- Other Storage Levels ---")
        # Other useful storage levels:
        # StorageLevel.MEMORY_AND_DISK_SER (Stores serialized data, uses less memory but more CPU)
        # StorageLevel.DISK_ONLY (Stores data only on disk, useful for very large datasets)
        # StorageLevel.MEMORY_ONLY_2 (Replicates data to two cluster nodes for fault tolerance)
        print("Persisting with DISK_ONLY...")
        df.persist(StorageLevel.DISK_ONLY)
        df.count()
        df.unpersist()
        
    finally:
        # Stop the Spark session
        print("\nStopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    from pyspark.sql.functions import col
    run_caching_demo()
