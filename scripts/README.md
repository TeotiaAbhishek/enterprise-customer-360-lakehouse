# Scripts

- `run_local_pipeline.sh` runs the whole project locally: Faker generation, PySpark Bronze, Silver and Gold, the Power BI CSV export, then `dbt build` on a local Spark session. Set `NUM_CUSTOMERS=2000` for a run of a couple of minutes, and pass `--skip-dbt` to stop after the PySpark jobs.
- `register_local_bronze.py` registers the landing CSVs as `bronze.*` Spark tables so the dbt `local` target can read them.
