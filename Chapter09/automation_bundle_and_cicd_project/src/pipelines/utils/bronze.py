from pyspark.sql.functions import col, current_timestamp

def add_bronze_audit_cols(df):
    return (df
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("bronze_loaded_ts_utc", current_timestamp())
    )