# Complete REST API to Databricks Pipeline
# Using JSONPlaceholder - a free fake REST API (no API key needed!)
import requests
import json
from datetime import datetime
from pyspark.sql.functions import col, current_timestamp
import pandas as pd

# JSONPlaceholder API - No authentication required
BASE_URL = "https://jsonplaceholder.typicode.com"

def fetch_users():
    """Fetch all users from the API"""
    url = f"{BASE_URL}/users"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            users = response.json()
            # Add query timestamp to each record
            for user in users:
                user["query_timestamp"] = datetime.now().isoformat()
            return users
        else:
            print(f"Error fetching users: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception fetching users: {e}")
        return []

def fetch_posts():
    """Fetch all posts from the API"""
    url = f"{BASE_URL}/posts"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            posts = response.json()
            for post in posts:
                post["query_timestamp"] = datetime.now().isoformat()
            return posts
        else:
            print(f"Error fetching posts: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception fetching posts: {e}")
        return []

# Fetch users data
print("Fetching users from API...")
users_data = fetch_users()
print(f"Retrieved {len(users_data)} users")

# Fetch posts data
print("Fetching posts from API...")
posts_data = fetch_posts()
print(f"Retrieved {len(posts_data)} posts")

# Convert users to Spark DataFrame
# Use pandas as intermediate format (works on shared clusters)
if users_data:
    # Convert to pandas DataFrame first
    users_pdf = pd.DataFrame(users_data)
    
    # Convert pandas to Spark DataFrame
    users_df_raw = spark.createDataFrame(users_pdf)
    
    # Flatten nested address and company structures
    users_df = users_df_raw.select(
        col("id").alias("user_id"),
        col("name"),
        col("username"),
        col("email"),
        col("phone"),
        col("address.city").alias("city"),
        col("address.geo.lat").alias("latitude"),
        col("address.geo.lng").alias("longitude"),
        col("company.name").alias("company_name")
    ).withColumn("ingestion_timestamp", current_timestamp())
    
    # Create schema in Unity Catalog
    spark.sql("CREATE SCHEMA IF NOT EXISTS sandbox")
    
    # Write to Delta Lake
    users_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("sandbox.users")
    
    print(f"Loaded {users_df.count()} users to Delta Lake")
    display(users_df.limit(5))

# Convert posts to Spark DataFrame
if posts_data:
    # Convert to pandas DataFrame first
    posts_pdf = pd.DataFrame(posts_data)
    
    # Convert pandas to Spark DataFrame
    posts_df_raw = spark.createDataFrame(posts_pdf)
    
    posts_df = posts_df_raw.select(
        col("id").alias("post_id"),
        col("userId").alias("user_id"),
        col("title"),
        col("body").alias("content")
    ).withColumn("ingestion_timestamp", current_timestamp())
    
    # Write to Delta Lake
    posts_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("sandbox.posts")
    
    print(f"Loaded {posts_df.count()} posts to Delta Lake")

# Create an enriched view joining users and posts
print("\nCreating enriched view...")
spark.sql("""
    CREATE OR REPLACE VIEW sandbox.posts_with_authors AS
    SELECT 
        p.post_id,
        p.title,
        u.name as author_name,
        u.company_name,
        u.city
    FROM sandbox.posts p
    JOIN sandbox.users u ON p.user_id = u.user_id
""")

print("\nAPI data ingestion completed successfully!")
display(spark.sql("SELECT * FROM sandbox.posts_with_authors LIMIT 10"))
