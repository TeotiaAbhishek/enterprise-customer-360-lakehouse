"""Register the local landing CSVs as Spark tables in the `bronze` schema.

This mirrors the Unity Catalog tables (workspace.bronze.*) so the dbt project
can run locally with the dbt-spark `session` target. Run from dbt/customer_360
so the Spark metastore is created next to the dbt project.
"""
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LANDING_BASE = PROJECT_ROOT / "data" / "landing"

# dbt source table name -> landing folder
TABLES = {
    "customers": "crm_customers",
    "addresses": "crm_addresses",
    "orders": "orders",
    "payments": "payments",
    "support_tickets": "support_tickets",
    "loyalty_accounts": "loyalty_accounts",
}


def main():
    spark = (
        SparkSession.builder
        .appName("RegisterLocalBronze")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    for table_name, folder in TABLES.items():
        df = spark.read.option("header", True).csv(str(LANDING_BASE / folder / "*.csv"))
        df.write.mode("overwrite").saveAsTable(f"bronze.{table_name}")
        print(f"bronze.{table_name}: {spark.table(f'bronze.{table_name}').count():,} rows")

    spark.stop()


if __name__ == "__main__":
    main()
