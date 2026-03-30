from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def add_label_next_order_amount(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("customer_id").orderBy(F.col("order_date"), F.col("order_id"))
    return df.withColumn("label_next_order_amount", F.lead("order_amount").over(w))

def add_last_order_amount(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("customer_id").orderBy(F.col("order_date"), F.col("order_id"))
    return df.withColumn("last_order_amount", F.lag("order_amount").over(w))

def add_days_since_prev_order(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("customer_id").orderBy(F.col("order_date"), F.col("order_id"))
    return df.withColumn(
        "days_since_prev_order",
        F.datediff(F.col("order_date"), F.lag("order_date").over(w)).cast("int"),
    )

def add_purchase_count_to_date(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_date"), F.col("order_id"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return df.withColumn("purchase_count_to_date", F.count(F.lit(1)).over(w).cast("int"))

def add_hist_avg_purchase(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_date"), F.col("order_id"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return df.withColumn("hist_avg_purchase", F.avg("order_amount").over(w))

def add_total_spend_to_date(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_date"), F.col("order_id"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return df.withColumn("total_spend_to_date", F.sum("order_amount").over(w))
