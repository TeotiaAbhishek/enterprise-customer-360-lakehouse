# Architecture

The end-to-end architecture, medallion layers, dbt lineage and data model are documented in the [main README](../README.md#architecture).

Key design decisions:

- **Unity Catalog namespaces.** Raw files live in a Volume (`/Volumes/workspace/default/data`). Each layer has its own schema in the `workspace` catalog: `bronze`, `staging`, `silver` and `gold` (see `dbt/customer_360/macros/generate_schema_name.sql`).
- **Config-driven Bronze.** One parameterised notebook (`databricks/bronze/bronze_ingestion.ipynb`) ingests any dataset registered in `databricks/config/datasets.py`, so a new source is a config entry rather than a new notebook.
- **Reject, don't drop.** Every Silver model has a matching `rejected_*` table with a `rejection_reason`, and `gold.data_quality_summary` reconciles Bronze, valid and rejected counts per source.
- **Referential integrity in Silver.** Child entities are validated against already-validated parents (`silver_customers`, `silver_orders`), so orphans are rejected instead of disappearing in Gold joins.
- **Deterministic deduplication.** Duplicates are resolved per primary key with `row_number()` ordered by `updated_at desc`.
- **Two runtimes, one logic.** The same transformations exist as local PySpark jobs (`src/`) and as dbt models. dbt runs on Databricks SQL in the cloud and on a local Spark session in CI, and both produce identical Gold numbers.
