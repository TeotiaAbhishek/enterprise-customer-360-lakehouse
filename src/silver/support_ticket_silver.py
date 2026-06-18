from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import(
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

BRONZE_SUPPORT_TICKET_PATH = PROJECT_ROOT / "data" / "bronze" / "support_tickets"
SILVER_SUPPORT_TICKET_PATH = PROJECT_ROOT / "data" / "silver" / "support_tickets"
REJECTED_SUPPORT_TICKET_PATH = PROJECT_ROOT / "data" / "silver" / "rejected_support_tickets"
SILVER_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "silver" / "customers"


TICKET_STATUSES = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]

PRIORITIES = ["LOW", "MEDIUM", "HIGH", "URGENT"]
CHANNELS = ["EMAIL", "PHONE", "CHAT", "WEB"]

CATEGORIES = [
    "PAYMENT_ISSUE",
    "DELIVERY_DELAY",
    "ACCOUNT_ACCESS",
    "REFUND_REQUEST",
    "PRODUCT_QUERY",
    "LOYALTY_QUERY",
    "COMPLAINT"
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverSupportTickets")
        .master("local[*]")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    bronze_df = spark.read.parquet(str(BRONZE_SUPPORT_TICKET_PATH))


    standardised_df = (
        bronze_df
        .withColumn("ticket_created_at", to_date(col("ticket_created_at")))
        .withColumn("ticket_resolved_at", to_date(col("ticket_resolved_at")))
        .withColumn("ticket_status", upper(trim(col("ticket_status"))))
        .withColumn("priority", upper(trim(col("priority"))))
        .withColumn("channel", upper(trim(col("channel"))))
        .withColumn("category", upper(trim(col("category"))))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    window_spec = Window.partitionBy("ticket_id").orderBy(col("ticket_created_at").desc_nulls_last())

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


    support_tickets_with_customer_check = (
        deduped_df
        .join(
            customer_lookup,
            on="customer_id",
            how= "left"
        )
    )
    quality_df = (
        support_tickets_with_customer_check
        .withColumn(
            "_has_future_ticket_date",
            (
                (col("ticket_created_at") > current_date()) | 
                ((col("ticket_resolved_at").isNotNull()) & (col("ticket_resolved_at") > current_date()))
            )
        )
        .withColumn(
            "_is_valid",
            when(col("ticket_id").isNull(), lit(False))
            .when(col("customer_id").isNull(), lit(False))
            .when(col("customer_exists").isNull(), lit(False))
            .when(col("ticket_created_at").isNull(), lit(False))
            .when((col("ticket_resolved_at").isNotNull()) & (col("ticket_resolved_at") < col("ticket_created_at")),lit(False))
            .when(col("ticket_status").isNull(), lit(False))            
            .when(~col("ticket_status").isin(TICKET_STATUSES), lit(False))
            .when(col("priority").isNull(), lit(False))
            .when(~col("priority").isin(PRIORITIES), lit(False))
            .when(col("channel").isNull(), lit(False))
            .when(~col("channel").isin(CHANNELS), lit(False))
            .when(col("category").isNull(), lit(False))
            .when(~col("category").isin(CATEGORIES), lit(False))
            .otherwise(lit(True))
        )
        .withColumn(
            "_rejection_reason",
            when(col("ticket_id").isNull(), lit("ticket_id is null"))
            .when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(col("customer_exists").isNull(), lit("customer_id does not exist in silver customers"))
            .when(col("ticket_created_at").isNull(), lit("ticket_created_at is null"))
            .when((col("ticket_resolved_at").isNotNull()) & (col("ticket_resolved_at") < col("ticket_created_at")), lit("negative resolution time"))
            .when(col("ticket_status").isNull(), lit("ticket_status is null"))            
            .when(~col("ticket_status").isin(TICKET_STATUSES), lit("invalid ticket_status"))
            .when(col("priority").isNull(), lit("priority is null"))
            .when(~col("priority").isin(PRIORITIES), lit("invalid priority"))
            .when(col("channel").isNull(), lit("channel is null"))
            .when(~col("channel").isin(CHANNELS), lit("invalid channel"))
            .when(col("category").isNull(), lit("category is null"))
            .when(~col("category").isin(CATEGORIES), lit("invalid category"))
            .otherwise(lit(None))
        )
    )

    valid_support_tickets = quality_df.filter(col("_is_valid")== True)
    rejected_support_tickets = quality_df.filter(col("_is_valid") == False)

    (
        valid_support_tickets
        .write
        .mode("overwrite")
        .parquet(str(SILVER_SUPPORT_TICKET_PATH))
    )
    (
        rejected_support_tickets
        .write
        .mode("overwrite")
        .parquet(str(REJECTED_SUPPORT_TICKET_PATH))
    )

    print("Silver Support Tickets completed")
    print(f"Bronze rows: {bronze_df.count():,}")
    print(f"After deduplication: {deduped_df.count():,}")
    print(f"Future ticket date warnings: {valid_support_tickets.filter(col('_has_future_ticket_date') == True).count():,}")
    print(f"Valid support tickets: {valid_support_tickets.count():,}")
    print(f"Rejected support tickets: {rejected_support_tickets.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()