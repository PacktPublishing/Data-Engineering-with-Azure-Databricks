from pyspark.sql.functions import current_timestamp

def add_silver_audit_cols(df):
    return (df
        .withColumn("silver_loaded_ts_utc", current_timestamp())
    )