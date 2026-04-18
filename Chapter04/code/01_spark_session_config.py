"""
Chapter 4: Apache Spark Architecture and Execution Model
Example 1: Spark Session Configuration

This script demonstrates how to configure a SparkSession with Azure Databricks optimizations.
Note: In a Databricks notebook environment, the `spark` session is automatically created.
This code is for demonstration purposes, showing how you would configure these settings 
in a standalone Spark application or when creating a custom session.
"""

from pyspark.sql import SparkSession

def create_optimized_session(app_name="DatabricksOptimizedSession"):
    """
    Creates and returns a SparkSession with key Databricks optimizations enabled.
    """
    print(f"Initializing SparkSession: {app_name}")
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Enable Adaptive Query Execution (AQE)
        .config("spark.sql.adaptive.enabled", "true")
        # Enable dynamic partition coalescing
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Enable dynamic skew join optimization
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Databricks specific: Enable optimized writes for Delta Lake
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        # Databricks specific: Enable auto compaction for Delta Lake
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )
    
    # Print the active configuration to verify
    print("\nActive Spark Configurations:")
    configs_to_check = [
        "spark.sql.adaptive.enabled",
        "spark.databricks.delta.optimizeWrite.enabled",
        "spark.databricks.delta.autoCompact.enabled"
    ]
    
    for conf in configs_to_check:
        # Note: getConf might throw an error if the config isn't explicitly set in the environment
        try:
            val = spark.conf.get(conf)
            print(f"  {conf} = {val}")
        except Exception:
            print(f"  {conf} = Not explicitly set (using default)")
            
    return spark

if __name__ == "__main__":
    # Create the session
    spark = create_optimized_session()
    
    # Your Spark application code would go here
    print("\nSpark session created successfully. Ready for data processing.")
    
    # Stop the session when done
    print("Stopping Spark session...")
    spark.stop()
