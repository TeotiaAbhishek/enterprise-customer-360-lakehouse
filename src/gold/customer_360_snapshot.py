from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    max,
    when,
    lit,
    coalesce,
    current_timestamp
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"
SILVER_ADDRESSES_PATH = PROJECT_ROOT / "data" / "silver" / "addresses"
SILVER_ORDERS_PATH = PROJECT_ROOT / "data" / "silver" / "orders"
SILVER_PAYMENTS_PATH = PROJECT_ROOT / "data" / "silver" / "payments"
SILVER_SUPPORT_PATH = PROJECT_ROOT / "data" / "silver" / "support_tickets"
SILVER_LOYALTY_PATH = PROJECT_ROOT / "data" / "silver" / "loyalty_accounts"

GOLD_CUSTOMER_360_PATH = PROJECT_ROOT / "data" / "gold" / "customer_360_snapshot"


REVENUE_STATUSES = ["PAID", "SHIPPED", "DELIVERED"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldCustomer360Snapshot")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    customers = spark.read.parquet(str(SILVER_CUSTOMERS_PATH))
    addresses = spark.read.parquet(str(SILVER_ADDRESSES_PATH))
    orders = spark.read.parquet(str(SILVER_ORDERS_PATH))
    payments = spark.read.parquet(str(SILVER_PAYMENTS_PATH))
    support = spark.read.parquet(str(SILVER_SUPPORT_PATH))
    loyalty = spark.read.parquet(str(SILVER_LOYALTY_PATH))

    customer_base = customers.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "gender",
        "status",
        "signup_date"
    )

    address_summary = (
        addresses
        .filter(col("is_primary") == "true")
        .select(
            "customer_id",
            "state",
            "postcode",
            "country"
        )
        .dropDuplicates(["customer_id"])
    )

    order_summary = (
        orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum(
                when(
                    col("order_status").isin(REVENUE_STATUSES),
                    col("order_amount")
                ).otherwise(lit(0))
            ).alias("total_revenue"),
            max("order_date").alias("last_order_date")
        )
    )

    payment_summary = (
        payments
        .groupBy("customer_id")
        .agg(
            count(
                when(col("payment_status") == "SUCCESS", col("payment_id"))
            ).alias("successful_payment_count"),
            sum(
                when(
                    col("payment_status") == "SUCCESS",
                    col("payment_amount")
                ).otherwise(lit(0))
            ).alias("successful_payment_amount"),
            max("payment_date").alias("last_payment_date")
        )
    )

    support_summary = (
        support
        .groupBy("customer_id")
        .agg(
            count("ticket_id").alias("support_ticket_count"),
            count(
                when(col("ticket_status").isin(["OPEN", "IN_PROGRESS"]), col("ticket_id"))
            ).alias("open_ticket_count"),
            max("ticket_created_at").alias("last_support_ticket_date")
        )
    )

    loyalty_summary = (
        loyalty
        .select(
            "customer_id",
            col("tier").alias("loyalty_tier"),
            "points_balance",
            "loyalty_status"
        )
        .dropDuplicates(["customer_id"])
    )

    customer_360 = (
        customer_base
        .join(address_summary, on="customer_id", how="left")
        .join(order_summary, on="customer_id", how="left")
        .join(payment_summary, on="customer_id", how="left")
        .join(support_summary, on="customer_id", how="left")
        .join(loyalty_summary, on="customer_id", how="left")
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0.0)))
        .withColumn("successful_payment_count", coalesce(col("successful_payment_count"), lit(0)))
        .withColumn("successful_payment_amount", coalesce(col("successful_payment_amount"), lit(0.0)))
        .withColumn("support_ticket_count", coalesce(col("support_ticket_count"), lit(0)))
        .withColumn("open_ticket_count", coalesce(col("open_ticket_count"), lit(0)))
        .withColumn("points_balance", coalesce(col("points_balance"), lit(0)))
        .withColumn("_gold_processed_at", current_timestamp())
    )

    (
        customer_360
        .write
        .mode("overwrite")
        .parquet(str(GOLD_CUSTOMER_360_PATH))
    )

    print("Gold Customer 360 Snapshot completed")
    print(f"Customer base rows: {customer_base.count():,}")
    print(f"Customer 360 rows: {customer_360.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()