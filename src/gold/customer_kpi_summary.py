from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    current_timestamp
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CUSTOMER_360_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "customer_360_snapshot"
)

KPI_OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "customer_kpi_summary"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CustomerKPISummary")
        .master("local[*]")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    customer_360 = spark.read.parquet(str(CUSTOMER_360_PATH))

    kpi_df = (
        customer_360
        .agg(
            count("*").alias("total_customers"),

            sum(
                (col("status") == "active").cast("int")
            ).alias("active_customers"),

            sum(
                (col("status") == "blocked").cast("int")
            ).alias("blocked_customers"),

            sum(
                (col("total_orders") > 0).cast("int")
            ).alias("customers_with_orders"),

            sum(
                col("total_revenue")
            ).alias("total_revenue"),

            avg(
                col("total_revenue")
            ).alias("avg_customer_revenue"),

            sum(
                (col("loyalty_tier").isNotNull()).cast("int")
            ).alias("customers_with_loyalty"),

            sum(
                col("points_balance")
            ).alias("total_points_balance"),

            sum(
                (col("support_ticket_count") > 0).cast("int")
            ).alias("customers_with_support_tickets"),

            sum(
                col("open_ticket_count")
            ).alias("open_support_tickets")
        )
        .withColumn(
            "_gold_processed_at",
            current_timestamp()
        )
    )

    (
        kpi_df
        .write
        .mode("overwrite")
        .parquet(str(KPI_OUTPUT_PATH))
    )

    kpi_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()