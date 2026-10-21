# Databricks

Databricks implementation of the lakehouse on Unity Catalog (`workspace` catalog).

| Folder | Contents |
|---|---|
| `config/datasets.py` | Registry of the six datasets: landing path in the Volume, target Bronze table and primary key |
| `utilities/io_utils.py` | `read_dataset` and `write_delta_table` (creates the schema if needed and writes Delta with `saveAsTable`) |
| `bronze/bronze_ingestion.ipynb` | Generic Bronze notebook. Set the `dataset_name` widget (or Job parameter) to ingest one dataset into `workspace.bronze.<dataset>` |
| `bronze/Archive/` | First-iteration, one-notebook-per-source Bronze ingestion, kept for reference |
| `silver/`, `gold/` | PySpark notebook versions of the Silver and Gold logic. These were migrated to dbt (`dbt/customer_360`), which is now the source of truth |

To orchestrate it as a Databricks Job: one `bronze_ingestion` notebook task per dataset (or a for-each task over the six dataset names), followed by a dbt task running `dbt build` against a SQL warehouse.
