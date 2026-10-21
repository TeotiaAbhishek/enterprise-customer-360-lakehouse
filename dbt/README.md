# dbt

The `customer_360` dbt project builds the Silver and Gold layers from the Bronze tables in Unity Catalog.

```
models/
├── staging/       stg_*                 views: casting, trimming, case standardisation
├── intermediate/  int_*_validation      ephemeral: dedup, integrity checks, rejection_reason
├── silver/        silver_*, rejected_*  tables: valid and rejected records
└── gold/          customer_360, customer_kpi_summary, data_quality_summary
tests/             singular data tests on the Gold layer
```

## Targets

See `customer_360/profiles.example.yml`:

- `databricks`: a Databricks SQL warehouse. Credentials come from `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH` and `DATABRICKS_TOKEN`.
- `local`: dbt-spark in `session` mode, so no cluster is needed. Register Bronze tables first with `python ../../scripts/register_local_bronze.py` (run from `customer_360/`). `scripts/run_local_pipeline.sh` does all of this.

```bash
cd dbt/customer_360
cp profiles.example.yml profiles.yml
dbt build --profiles-dir . --target local        # or --target databricks
dbt docs generate --profiles-dir . --target local
```
