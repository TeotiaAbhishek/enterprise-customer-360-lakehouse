from pathlib import Path
from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


PROJECT_ROOT = Path(__file__).resolve().parents[2]

LANDING_BASE = PROJECT_ROOT / "data" / "landing"
BRONZE_BASE = PROJECT_ROOT / "data" / "bronze"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EnterpriseCustomer360BronzeIngestion")
        .master("local[*]")
        .getOrCreate()
    )


def get_customer_schema():
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("source_system", StringType(), True),
    ])


def get_orders_schema():
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("sales_channel", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("source_system", StringType(), True),
    ])

def get_address_schema():
    return StructType([
        StructField("address_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("suburb", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_primary", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("source_system", StringType(), True),
    ])

def get_payments_schema():
    return StructType([
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("payment_date", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("source_system", StringType(), True),
    ])
def get_support_tickets_schema():
    return StructType([
        StructField("ticket_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("ticket_created_at", StringType(), True),
        StructField("ticket_resolved_at", StringType(), True),
        StructField("ticket_status", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("category", StringType(), True),
        StructField("agent_id", StringType(), True),
        StructField("source_system", StringType(), True),
    ])

def get_loyalty_accounts_schema():
    return StructType([
        StructField("loyalty_account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("join_date", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("points_balance", IntegerType(), True),
        StructField("loyalty_status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("source_system", StringType(), True),
    ])


def ingest_csv_to_bronze(spark, source_name, schema):
    batch_id = str(uuid.uuid4())

    input_path = str(LANDING_BASE / source_name / "*.csv")
    output_path = str(BRONZE_BASE / source_name)

    print(f"Ingesting source: {source_name}")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Batch ID: {batch_id}")

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(input_path)
    )

    df_with_metadata = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_record_hash", sha2(concat_ws("||", *df.columns), 256))
    )

    (
        df_with_metadata
            .write
            .mode("append")
            .parquet(output_path)
    )

    print(f"Completed Bronze ingestion for {source_name}")
    print(f"Rows ingested: {df_with_metadata.count():,}")


def main():
    spark = create_spark_session()

    ingest_csv_to_bronze(
        spark=spark,
        source_name="crm_customers",
        schema=get_customer_schema()
    )

    ingest_csv_to_bronze(
        spark=spark,
        source_name="orders",
        schema=get_orders_schema()
    )

    ingest_csv_to_bronze(
        spark=spark,
        source_name="crm_addresses",
        schema=get_address_schema()
    )

    ingest_csv_to_bronze(
        spark=spark,
        source_name="payments",
        schema=get_payments_schema()
    )

    ingest_csv_to_bronze(
        spark=spark,
        source_name="support_tickets",
        schema=get_support_tickets_schema()
    )

    ingest_csv_to_bronze(
        spark=spark,
        source_name="loyalty_accounts",
        schema=get_loyalty_accounts_schema()
    )

    spark.stop()


if __name__ == "__main__":
    main()