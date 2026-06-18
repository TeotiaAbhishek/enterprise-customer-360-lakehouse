from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    to_date,
    current_timestamp,
    row_number,
    lit,
    when
)
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ORDERS_PATH = PROJECT_ROOT / "data" / "bronze" / "orders"
SILVER_ORDERS_PATH = PROJECT_ROOT / "data" / "silver" / "orders"
REJECTED_ORDERS_PATH = PROJECT_ROOT / "data" / "silver" / "rejected_orders"
SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"

VALID_STATUSES = [
    "PLACED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED"
]

VALID_CHANNELS = ["WEB", "MOBILE", "STORE"]

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverOrders")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    bronze_df = spark.read.parquet(str(BRONZE_ORDERS_PATH))

    standardised_df = (
        bronze_df
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("sales_channel", upper(trim(col("sales_channel"))))
        .withColumn("created_at", to_date(col("created_at")))
        .withColumn("updated_at", to_date(col("updated_at")))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc_nulls_last())

    deduped_df = (
        standardised_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    silver_customers = spark.read.parquet(str(SILVER_CUSTOMERS_PATH))

    customer_lookup = (
        silver_customers
        .select("customer_id")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_exists", lit(True))
    )

    orders_with_customer_check = (
        deduped_df
        .join(
            customer_lookup,
            on="customer_id",
            how="left"
        )
    )

    quality_df = (
        orders_with_customer_check
        .withColumn(
            "_has_future_updated_at",
            col("updated_at") > current_timestamp()
        )
        .withColumn(
            "_is_valid",
            when(col("order_id").isNull(), lit(False))
            .when(col("customer_id").isNull(), lit(False))
            .when(col("customer_exists").isNull(), lit(False))
            .when(~col("order_status").isin(VALID_STATUSES), lit(False))
            .when(col("sales_channel").isNull(), lit(False))
            .when(~col("sales_channel").isin(VALID_CHANNELS), lit(False))
            .when(col("order_amount")<=0, lit(False))
            .otherwise(lit(True))
        )
        .withColumn(
            "_rejection_reason",
            when(col("order_id").isNull(), lit("order_id is null"))
            .when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(col("customer_exists").isNull(), lit("customer_id does not exist in silver customers"))
            .when(~col("order_status").isin(VALID_STATUSES), lit("invalid status"))
            .when(col("sales_channel").isNull(), lit("sales_channel is null"))
            .when(~col("sales_channel").isin(VALID_CHANNELS), lit("invalid sales_channel"))
            .when(col("order_amount")<=0, lit("order_amount must be greater than zero"))
            .otherwise(lit(None))
        )
    )

    valid_orders = quality_df.filter(col("_is_valid") == True)
    rejected_orders = quality_df.filter(col("_is_valid") == False)

    (
        valid_orders
        .write
        .mode("overwrite")
        .parquet(str(SILVER_ORDERS_PATH))
    )

    (
        rejected_orders
        .write
        .mode("overwrite")
        .parquet(str(REJECTED_ORDERS_PATH))
    )

    print("Silver Orders completed")
    print(f"Bronze rows: {bronze_df.count():,}")
    print(f"After deduplication: {deduped_df.count():,}")
    print(f"Future updated_at warnings: {valid_orders.filter(col('_has_future_updated_at') == True).count():,}")
    print(f"Valid orders: {valid_orders.count():,}")
    print(f"Rejected orders: {rejected_orders.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()