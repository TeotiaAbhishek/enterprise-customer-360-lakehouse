from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
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

BRONZE_ADDRESSES_PATH = PROJECT_ROOT / "data" / "bronze" / "crm_addresses"
SILVER_ADDRESSES_PATH = PROJECT_ROOT / "data" / "silver" / "addresses"
REJECTED_ADDRESSES_PATH = PROJECT_ROOT / "data" / "silver" / "rejected_addresses"
SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"

VALID_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverAddresses")
        .master("local[*]")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    bronze_df = spark.read.parquet(str(BRONZE_ADDRESSES_PATH))

    standardised_df = (
        bronze_df
        .withColumn("address_line_1", lower(trim(col("address_line_1"))))
        .withColumn("suburb", lower(trim(col("suburb"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("postcode", trim(col("postcode")))
        .withColumn("country", lower(trim(col("country"))))
        .withColumn("is_primary", lower(trim(col("is_primary"))))
        .withColumn("created_at", to_date(col("created_at")))
        .withColumn("updated_at", to_date(col("updated_at")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    window_spec = Window.partitionBy("address_id").orderBy(col("updated_at").desc_nulls_last())

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

    addresses_with_customer_check = (
        deduped_df
        .join(
            customer_lookup,
            on="customer_id",
            how="left"
        )
    )

    quality_df = (
        addresses_with_customer_check
        .withColumn(
            "_has_future_updated_at",
            col("updated_at") > current_timestamp()
        )
        .withColumn(
            "_is_valid",
            when(col("address_id").isNull(), lit(False))
            .when(col("customer_id").isNull(), lit(False))
            .when(col("customer_exists").isNull(), lit(False))
            .when(~col("state").isin(VALID_STATES), lit(False))
            .when(col("postcode").isNull(), lit(False))
            .when(~col("postcode").rlike(r"^[0-9]{4}$"), lit(False))
            .otherwise(lit(True))
        )
        .withColumn(
            "_rejection_reason",
            when(col("address_id").isNull(), lit("address_id is null"))
            .when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(col("customer_exists").isNull(), lit("customer_id does not exist in silver customers"))
            .when(~col("state").isin(VALID_STATES), lit("invalid state"))
            .when(col("postcode").isNull(), lit("postcode is null"))
            .when(~col("postcode").rlike(r"^[0-9]{4}$"), lit("invalid postcode"))
            .otherwise(lit(None))
        )
    )

    valid_addresses = quality_df.filter(col("_is_valid") == True)
    rejected_addresses = quality_df.filter(col("_is_valid") == False)

    (
        valid_addresses
        .write
        .mode("overwrite")
        .parquet(str(SILVER_ADDRESSES_PATH))
    )

    (
        rejected_addresses
        .write
        .mode("overwrite")
        .parquet(str(REJECTED_ADDRESSES_PATH))
    )

    print("Silver Addresses completed")
    print(f"Bronze rows: {bronze_df.count():,}")
    print(f"After deduplication: {deduped_df.count():,}")
    print(f"Future updated_at warnings: {valid_addresses.filter(col('_has_future_updated_at') == True).count():,}")
    print(f"Valid addresses: {valid_addresses.count():,}")
    print(f"Rejected addresses: {rejected_addresses.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()