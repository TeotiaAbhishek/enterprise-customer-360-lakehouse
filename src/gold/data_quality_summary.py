from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, round, current_timestamp


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_BASE = PROJECT_ROOT / "data" / "bronze"
SILVER_BASE = PROJECT_ROOT / "data" / "silver"
GOLD_DQ_SUMMARY_PATH = PROJECT_ROOT / "data" / "gold" / "data_quality_summary"


SOURCE_MAPPING = [
    ("customers", "crm_customers", "customers", "rejected_customers"),
    ("addresses", "crm_addresses", "addresses", "rejected_addresses"),
    ("orders", "orders", "orders", "rejected_orders"),
    ("payments", "payments", "payments", "rejected_payments"),
    ("support_tickets", "support_tickets", "support_tickets", "rejected_support_tickets"),
    ("loyalty_accounts", "loyalty_accounts", "loyalty_accounts", "rejected_loyalty_accounts"),
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldDataQualitySummary")
        .master("local[*]")
        .getOrCreate()
    )


def count_rows(spark, path):
    path = Path(path)

    if not path.exists():
        return 0

    return spark.read.parquet(str(path)).count()


def main():
    spark = create_spark_session()

    rows = []

    for source_name, bronze_name, silver_name, rejected_name in SOURCE_MAPPING:
        bronze_rows = count_rows(spark, BRONZE_BASE / bronze_name)
        silver_rows = count_rows(spark, SILVER_BASE / silver_name)
        rejected_rows = count_rows(spark, SILVER_BASE / rejected_name)

        total_processed = silver_rows + rejected_rows

        rows.append(
            Row(
                source_name=source_name,
                bronze_rows=bronze_rows,
                silver_valid_rows=silver_rows,
                silver_rejected_rows=rejected_rows,
                total_processed_rows=total_processed
            )
        )

    dq_df = spark.createDataFrame(rows)

    dq_df = (
        dq_df
        .withColumn(
            "rejection_rate_percent",
            round(
                (col("silver_rejected_rows") / col("total_processed_rows")) * 100,
                2
            )
        )
        .withColumn("_gold_processed_at", current_timestamp())
    )

    (
        dq_df
        .write
        .mode("overwrite")
        .parquet(str(GOLD_DQ_SUMMARY_PATH))
    )

    dq_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()