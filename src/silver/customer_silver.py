from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_date,
    current_timestamp,
    row_number,
    lit,
    when
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "bronze" / "crm_customers"
SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"
REJECTED_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "rejected_customers"


VALID_STATUSES = ["active", "inactive", "blocked", "pending"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverCustomers")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    bronze_df = spark.read.parquet(str(BRONZE_CUSTOMERS_PATH))

    standardised_df = (
        bronze_df
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("gender", lower(trim(col("gender"))))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("date_of_birth", to_date(col("date_of_birth")))
        .withColumn("signup_date", to_date(col("signup_date")))
        .withColumn("created_at", to_date(col("created_at")))
        .withColumn("updated_at", to_date(col("updated_at")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    window_spec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc_nulls_last())

    deduped_df = (
        standardised_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    quality_df = (
        deduped_df
        .withColumn(
            "_has_future_updated_at",
            col("updated_at") > current_timestamp()
        )
        .withColumn(
            "_is_valid",
            when(col("customer_id").isNull(), lit(False))
            .when(~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), lit(False))
            .when(~col("status").isin(VALID_STATUSES), lit(False))
            .otherwise(lit(True))
        )
        .withColumn(
            "_rejection_reason",
            when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), lit("invalid email"))
            .when(~col("status").isin(VALID_STATUSES), lit("invalid status"))
            .otherwise(lit(None))
        )
    )

    valid_customers = quality_df.filter(col("_is_valid") == True)
    rejected_customers = quality_df.filter(col("_is_valid") == False)

    (
        valid_customers
        .write
        .mode("overwrite")
        .parquet(str(SILVER_CUSTOMERS_PATH))
    )

    (
        rejected_customers
        .write
        .mode("overwrite")
        .parquet(str(REJECTED_CUSTOMERS_PATH))
    )

    print("Silver Customers completed")
    print(f"Bronze rows: {bronze_df.count():,}")
    print(f"After deduplication: {deduped_df.count():,}")
    print(f"Valid customers: {valid_customers.count():,}")
    print(f"Rejected customers: {rejected_customers.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()