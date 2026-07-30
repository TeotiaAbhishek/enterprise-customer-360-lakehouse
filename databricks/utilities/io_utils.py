from pyspark.sql import DataFrame

def read_dataset(spark, config: dict) -> DataFrame:
    return (
        spark.read
        .format(config["source_format"])
        .options(**config["options"])
        .load(config["landing_path"])
    )

def write_delta_table(
    spark,
    dataframe: DataFrame,
    table_name: str,
    mode: str = "overwrite",
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
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )