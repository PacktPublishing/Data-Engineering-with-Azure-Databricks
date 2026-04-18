"""
Chapter 4: Handling Large-Scale Data Processing with Spark
Example 5: Data Skew and Resource Optimization

This script demonstrates how to identify and mitigate data skew in Apache Spark.
It uses a technique called "salting" to distribute skewed keys more evenly
across partitions during join operations.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

def create_skewed_data(spark, num_records=1000000):
    """
    Creates a sample dataset with a deliberately skewed key distribution.
    """
    print(f"Creating a skewed dataset with {num_records} records...")
    
    # Generate skewed data: 90% of records have customer_id=1, 10% are distributed
    data = []
    for _ in range(num_records):
        if random.random() < 0.9:
            customer_id = 1 # Highly skewed key
        else:
            customer_id = random.randint(2, 100) # Normal distribution
            
        amount = random.randint(10, 1000)
        data.append((customer_id, amount))
        
    schema = ["customer_id", "amount"]
    df = spark.createDataFrame(data, schema=schema)
    
    return df

def run_data_skew_demo():
    # Stop any existing Spark session
    try:
        SparkSession.builder.getOrCreate().stop()
    except Exception:
        pass
        
    # Create optimized Spark session for scaling and skew handling
    print("\n--- Creating Optimized Spark Session ---")
    spark = (
        SparkSession.builder
        .appName("ScalingAndSkewDemo")
        # Enable Adaptive Query Execution (AQE)
        .config("spark.sql.adaptive.enabled", "true")
        # Enable AQE's automatic skew join optimization
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Configure executor memory
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    
    try:
        # Load dataset (or create skewed sample data)
        df = create_skewed_data(spark, num_records=100000) # Using a smaller number for faster execution
        
        print("\n--- Identifying Data Skew ---")
        # In a real scenario, you'd identify skew using the Spark UI (Task Execution Times)
        # Programmatically, we can check the distribution of keys
        skewed_distribution = df.groupBy("customer_id").count().orderBy(F.desc("count"))
        print("Top 5 customer_ids by frequency (showing severe skew on ID 1):")
        skewed_distribution.show(5)
        
        print("\n--- Mitigating Data Skew with Salting ---")
        # 1. Identify the skewed keys
        skew_threshold = 1000
        skewed_customers = df.groupBy("customer_id").agg(F.count("*").alias("cnt")).filter(f"cnt > {skew_threshold}")
        print(f"Identified {skewed_customers.count()} skewed customer_ids (count > {skew_threshold})")
        
        # 2. Add a random "salt" to the skewed keys in the large DataFrame
        # This distributes the skewed records across multiple partitions
        salt_range = 10 # Number of partitions to distribute the skewed data across
        
        # We need another dataframe to join with to demonstrate salting properly
        # Let's create a dimension table of customers
        dim_data = [(i, f"Customer {i}") for i in range(1, 101)]
        dim_df = spark.createDataFrame(dim_data, ["customer_id", "customer_name"])
        
        print("\nExecuting join with salting technique...")
        # Step A: Add salt to the fact table (only for skewed keys)
        fact_salted = (
            df.join(F.broadcast(skewed_customers.select("customer_id")), "customer_id", "left")
            .withColumn("is_skewed", F.col("customer_id").isNotNull())
            .withColumn("salt", F.when(F.col("is_skewed"), (F.rand() * salt_range).cast("int")).otherwise(0))
            .withColumn("salted_key", F.when(F.col("is_skewed"), F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))).otherwise(F.col("customer_id").cast("string")))
        )
        
        # Step B: Explode the dimension table for the skewed keys
        # We need a copy of the dimension record for every possible salt value
        dim_skewed = dim_df.join(F.broadcast(skewed_customers.select("customer_id")), "customer_id", "inner")
        
        # Create a dataframe of all possible salt values
        salts = spark.range(0, salt_range).withColumnRenamed("id", "salt_val")
        
        # Cross join skewed dimensions with all salt values and create salted key
        dim_skewed_exploded = dim_skewed.crossJoin(salts).withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt_val")))
        
        # Step C: Get the non-skewed dimensions (salt is always 0, key is just customer_id)
        dim_non_skewed = dim_df.join(F.broadcast(skewed_customers.select("customer_id")), "customer_id", "left_anti").withColumn("salted_key", F.col("customer_id").cast("string"))
        
        # Step D: Union the exploded skewed dimensions with the non-skewed dimensions
        dim_salted = dim_skewed_exploded.select("customer_id", "customer_name", "salted_key").unionByName(dim_non_skewed.select("customer_id", "customer_name", "salted_key"))
        
        # Step E: Perform the final join on the salted keys
        # This join will now be evenly distributed, avoiding the bottleneck caused by customer_id=1
        result = fact_salted.join(dim_salted, "salted_key", "inner")
        
        # Aggregate the results (grouping by the original customer_id)
        final_agg = result.groupBy(fact_salted["customer_id"]).agg(F.sum("amount").alias("total_amount"))
        
        print("Final aggregation results (Top 5):")
        final_agg.orderBy(F.desc("total_amount")).show(5)
        
        print("\nSalting complete. The skewed data was distributed across 10 partitions during the join, preventing any single executor from being overwhelmed.")
        
    finally:
        # Stop the Spark session
        print("\nStopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    run_data_skew_demo()
