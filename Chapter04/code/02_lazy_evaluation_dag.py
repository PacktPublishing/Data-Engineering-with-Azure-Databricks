"""
Chapter 4: Execution Model and Job Scheduling
Example 2: Lazy Evaluation and DAG Construction

This script demonstrates how Spark's lazy evaluation works. It shows the difference
between defining transformations (which build the logical plan) and triggering 
actions (which execute the physical plan).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# Create a Spark session
spark = SparkSession.builder.appName("LazyEvaluationDemo").getOrCreate()

def run_lazy_evaluation_demo():
    print("--- Step 1: Defining the Data Source ---")
    # Load a dataset (Transformation - lazy)
    # In a real scenario, this would point to a valid path like:
    # df = spark.read.format("csv").option("header", "true").load("/Volumes/analytics_dev/sales_raw/staging/dataset.csv")
    
    # For demonstration, we'll create a simple DataFrame in memory
    data = [
        ("Alice", 28, "Engineering"),
        ("Bob", 35, "Sales"),
        ("Charlie", 42, "Engineering"),
        ("David", 25, "Marketing"),
        ("Eve", 31, "Sales"),
        ("Frank", 45, "Engineering")
    ]
    columns = ["name", "age", "department"]
    df = spark.createDataFrame(data, schema=columns)
    print("DataFrame defined. No computation has occurred yet.")
    
    print("\n--- Step 2: Defining Transformations ---")
    # Define a series of transformations (lazy)
    # 1. Filter for employees over 30
    # 2. Group by department
    # 3. Count the number of employees in each department
    transformed_df = (
        df.filter(col("age") > 30)
        .groupBy("department")
        .agg(count("*").alias("employee_count"))
    )
    print("Transformations defined. The DAG has been built, but still no execution.")
    
    print("\n--- Step 3: Viewing the Execution Plan ---")
    # At this point, no computation has occurred. We can view the plan.
    print("Logical and Physical Plans:")
    transformed_df.explain(extended=True)
    
    print("\n--- Step 4: Triggering an Action ---")
    # Now, trigger an action to execute the plan
    # collect() is an action that brings all results back to the driver
    print("Executing collect() action...")
    results = transformed_df.collect()
    
    print("\n--- Step 5: Displaying Results ---")
    # The results are now materialized and can be displayed
    print("Results materialized:")
    for row in results:
        print(f"Department: {row['department']}, Count: {row['employee_count']}")

if __name__ == "__main__":
    try:
        run_lazy_evaluation_demo()
    finally:
        print("\nStopping Spark session...")
        spark.stop()
