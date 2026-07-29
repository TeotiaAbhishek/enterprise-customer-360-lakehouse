from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]

EXPORT_PATH = PROJECT_ROOT / "dashboards" / "powerbi_data"

CUSTOMER_360_PATH = PROJECT_ROOT / "data" / "gold" / "customer_360_snapshot"
KPI_PATH = PROJECT_ROOT / "data" / "gold" / "customer_kpi_summary"
DQ_PATH = PROJECT_ROOT / "data" / "gold" / "data_quality_summary"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ExportPowerBIData")
        .master("local[*]")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    EXPORT_PATH.mkdir(parents=True, exist_ok=True)

    customer_360 = spark.read.parquet(str(CUSTOMER_360_PATH))
    kpi_summary = spark.read.parquet(str(KPI_PATH))
    dq_summary = spark.read.parquet(str(DQ_PATH))

    (
        customer_360
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(EXPORT_PATH / "customer_360"))
    )

    (
        kpi_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(EXPORT_PATH / "customer_kpi_summary"))
    )

    (
        dq_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(EXPORT_PATH / "data_quality_summary"))
    )

    print("Power BI exports completed")

    spark.stop()


if __name__ == "__main__":
    main()