from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    to_date,
    current_timestamp,
    current_date,
    row_number,
    lit,
    when
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_LOYALTY_PATH = PROJECT_ROOT / "data" / "bronze" / "loyalty_accounts"
SILVER_LOYALTY_PATH = PROJECT_ROOT / "data" / "silver" / "loyalty_accounts"
REJECTED_LOYALTY_PATH = PROJECT_ROOT / "data" / "silver" / "rejected_loyalty_accounts"
SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"


VALID_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
VALID_STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverLoyaltyAccounts")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    bronze_df = spark.read.parquet(str(BRONZE_LOYALTY_PATH))

    standardised_df = (
        bronze_df
        .withColumn("join_date", to_date(col("join_date")))
        .withColumn("tier", upper(trim(col("tier"))))
        .withColumn("loyalty_status", upper(trim(col("loyalty_status"))))
        .withColumn("created_at", to_date(col("created_at")))
        .withColumn("updated_at", to_date(col("updated_at")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    window_spec = Window.partitionBy("loyalty_account_id").orderBy(
        col("updated_at").desc_nulls_last()
    )

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

    loyalty_with_customer_check = (
        deduped_df
        .join(
            customer_lookup,
            on="customer_id",
            how="left"
        )
    )

    quality_df = (
        loyalty_with_customer_check
        .withColumn(
            "_has_future_date",
            (
                (col("join_date") > current_date()) |
                (col("updated_at") > current_date())
            )
        )
        .withColumn(
            "_is_valid",
            when(col("loyalty_account_id").isNull(), lit(False))
            .when(col("customer_id").isNull(), lit(False))
            .when(col("customer_exists").isNull(), lit(False))
            .when(col("join_date").isNull(), lit(False))
            .when(col("tier").isNull(), lit(False))
            .when(~col("tier").isin(VALID_TIERS), lit(False))
            .when(col("loyalty_status").isNull(), lit(False))
            .when(~col("loyalty_status").isin(VALID_STATUSES), lit(False))
            .when(col("points_balance").isNull(), lit(False))
            .when(col("points_balance") < 0, lit(False))
            .otherwise(lit(True))
        )
        .withColumn(
            "_rejection_reason",
            when(col("loyalty_account_id").isNull(), lit("loyalty_account_id is null"))
            .when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(col("customer_exists").isNull(), lit("customer_id does not exist in silver customers"))
            .when(col("join_date").isNull(), lit("join_date is null"))
            .when(col("tier").isNull(), lit("tier is null"))
            .when(~col("tier").isin(VALID_TIERS), lit("invalid tier"))
            .when(col("loyalty_status").isNull(), lit("loyalty_status is null"))
            .when(~col("loyalty_status").isin(VALID_STATUSES), lit("invalid loyalty_status"))
            .when(col("points_balance").isNull(), lit("points_balance is null"))
            .when(col("points_balance") < 0, lit("points_balance must be greater than or equal to zero"))
            .otherwise(lit(None))
        )
    )

    valid_loyalty = quality_df.filter(col("_is_valid") == True)
    rejected_loyalty = quality_df.filter(col("_is_valid") == False)

    (
        valid_loyalty
        .write
        .mode("overwrite")
        .parquet(str(SILVER_LOYALTY_PATH))
    )

    (
        rejected_loyalty
        .write
        .mode("overwrite")
        .parquet(str(REJECTED_LOYALTY_PATH))
    )

    print("Silver Loyalty Accounts completed")
    print(f"Bronze rows: {bronze_df.count():,}")
    print(f"After deduplication: {deduped_df.count():,}")
    print(f"Future date warnings: {valid_loyalty.filter(col('_has_future_date') == True).count():,}")
    print(f"Valid loyalty accounts: {valid_loyalty.count():,}")
    print(f"Rejected loyalty accounts: {rejected_loyalty.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()