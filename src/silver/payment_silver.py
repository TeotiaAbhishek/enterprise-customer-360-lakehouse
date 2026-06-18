from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import(
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

BRONZE_PAYMENTS_PATH = PROJECT_ROOT / "data" / "bronze" / "payments"
SILVER_PAYMENTS_PATH = PROJECT_ROOT / "data" / "silver" / "payments"
REJECTED_PAYMENTS_PATH = PROJECT_ROOT / "data" / "silver" / "rejected_payments"
SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"
SILVER_ORDERS_PATH = PROJECT_ROOT / "data" / "silver" / "orders"

PAYMENT_STATUSES = [
    "SUCCESS",
    "FAILED",
    "PENDING",
    "REFUNDED"
]
PAYMENT_METHODS = [
    "CARD",
    "PAYPAL",
    "APPLE_PAY",
    "GOOGLE_PAY",
    "BANK_TRANSFER"
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverPayments")
        .master("local[*]")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    bronze_df = spark.read.parquet(str(BRONZE_PAYMENTS_PATH))


    standardised_df = (
        bronze_df
        .withColumn("payment_date", to_date(col("payment_date")))
        .withColumn("payment_status", upper(trim(col("payment_status"))))
        .withColumn("payment_method", upper(trim(col("payment_method"))))
        .withColumn("payment_amount", col("payment_amount"))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("created_at", to_date(col("created_at")))
        .withColumn("updated_at", to_date(col("updated_at")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    window_spec = Window.partitionBy("payment_id").orderBy(col("updated_at").desc_nulls_last())

    deduped_df = (
        standardised_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") ==1)
        .drop("row_num")
    )
    silver_customers = spark.read.parquet(str(SILVER_CUSTOMERS_PATH))

    customer_lookup = (
        silver_customers
        .select("customer_id")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_exists", lit(True))
    )

    silver_orders = spark.read.parquet(str(SILVER_ORDERS_PATH))

    order_lookup = (
        silver_orders
        .select("order_id")
        .dropDuplicates(["order_id"])
        .withColumn("order_exists", lit(True))
    )

    payments_with_customer_order_check = (
        deduped_df
        .join(
            customer_lookup,
            on="customer_id",
            how= "left"
        )
        .join(
            order_lookup,
            on="order_id",
            how = "left"
        )
    )
    quality_df = (
        payments_with_customer_order_check
        .withColumn(
            "_has_future_updated_at",
            col("updated_at") > current_timestamp()
        )
        .withColumn(
            "_is_valid",
            when(col("payment_id").isNull(), lit(False))
            .when(col("order_id").isNull(), lit(False))
            .when(col("order_exists").isNull(), lit(False))
            .when(col("customer_id").isNull(), lit(False))
            .when(col("customer_exists").isNull(), lit(False))
            .when(col("payment_date").isNull(), lit(False))
            .when(col("payment_status").isNull(), lit(False))
            .when(~col("payment_status").isin(PAYMENT_STATUSES), lit(False))
            .when(col("payment_method").isNull(), lit(False))
            .when(~col("payment_method").isin(PAYMENT_METHODS), lit(False))
            .when(col("payment_amount") <= 0, lit(False))
            .otherwise(lit(True))
        )
        .withColumn(
            "_rejection_reason",
            when(col("payment_id").isNull(), lit("payment_id is null"))
            .when(col("order_id").isNull(), lit("order_id is null"))
            .when(col("order_exists").isNull(), lit("order_id does not exist in silver orders"))
            .when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(col("customer_exists").isNull(), lit("customer_id does not exist in silver customers"))
            .when(col("payment_date").isNull(), lit("payment_date is null"))
            .when(col("payment_status").isNull(), lit("payment_status is null"))
            .when(~col("payment_status").isin(PAYMENT_STATUSES), lit("invalid payment_status"))
            .when(col("payment_method").isNull(), lit("payment_method is null"))
            .when(~col("payment_method").isin(PAYMENT_METHODS), lit("invalid payment_method"))
            .when(col("payment_amount") <= 0, lit("payment_amount must be greater than zero"))
            .otherwise(lit(None))
        )
    )

    valid_payments = quality_df.filter(col("_is_valid")== True)
    rejected_payments = quality_df.filter(col("_is_valid") == False)

    (
        valid_payments
        .write
        .mode("overwrite")
        .parquet(str(SILVER_PAYMENTS_PATH))
    )
    (
        rejected_payments
        .write
        .mode("overwrite")
        .parquet(str(REJECTED_PAYMENTS_PATH))
    )

    print("Silver Payments completed")
    print(f"Bronze rows: {bronze_df.count():,}")
    print(f"After deduplication: {deduped_df.count():,}")
    print(f"Future updated_at warnings: {valid_payments.filter(col('_has_future_updated_at') == True).count():,}")
    print(f"Valid payments: {valid_payments.count():,}")
    print(f"Rejected payments: {rejected_payments.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()