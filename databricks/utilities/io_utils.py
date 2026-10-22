from glob import glob
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
)


def discover_source_files(config: dict) -> list[str]:
    return sorted(glob(config["landing_path"]))


def get_loaded_source_files(
    spark,
    table_name: str,
) -> set[str]:

    if not spark.catalog.tableExists(table_name):
        return set()

    bronze_df = spark.table(table_name)

    if "_source_file" not in bronze_df.columns:
        return set()

    return {
        Path(row["_source_file"]).name
        for row in (
            bronze_df
            .select("_source_file")
            .where(col("_source_file").isNotNull())
            .distinct()
            .collect()
        )
    }


def get_unprocessed_source_files(
    spark,
    config: dict,
) -> list[str]:

    available_files = discover_source_files(config)

    loaded_files = get_loaded_source_files(
        spark,
        config["bronze_table"],
    )

    return [
        file
        for file in available_files
        if Path(file).name not in loaded_files
    ]


def read_dataset(
    spark,
    config: dict,
    source_files: list[str],
) -> DataFrame:

    if not source_files:
        raise ValueError("No source files supplied for ingestion.")

    dataframe = (
        spark.read
        .format(config["source_format"])
        .options(**config["options"])
        .load(source_files)
    )

    return (
        dataframe
        .withColumn(
            "_source_file",
            regexp_extract(
                col("_metadata.file_path"),
                r"([^/]+)$",
                1,
            ),
        )
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp(),
        )
    )


def write_delta_table(
    spark,
    dataframe: DataFrame,
    table_name: str,
    mode: str = "append",
) -> None:

    parts = table_name.split(".")

    if len(parts) == 3:
        catalog_name, schema_name, _ = parts

        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"`{catalog_name}`.`{schema_name}`"
        )

    elif len(parts) == 2:
        schema_name, _ = parts

        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`"
        )

    else:
        raise ValueError(
            "table_name must use either 'schema.table' "
            "or 'catalog.schema.table' format."
        )

    (
        dataframe.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )