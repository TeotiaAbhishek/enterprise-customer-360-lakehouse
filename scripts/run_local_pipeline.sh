#!/usr/bin/env bash
# Run the full local pipeline: Faker data -> Bronze -> Silver -> Gold -> Power BI export,
# then build and test the dbt project against a local Spark session.
#
# Usage: NUM_CUSTOMERS=2000 ./scripts/run_local_pipeline.sh [--skip-dbt]
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHON="${PYTHON:-python}"

# Start from a clean slate: Bronze reads every CSV in data/landing
rm -rf data/landing data/bronze data/silver data/gold data/metadata

echo "==> Generating synthetic source data"
for source in customers addresses orders payments support_tickets loyalty_accounts; do
    "$PYTHON" "src/data_generator/generate_${source}.py"
done

echo "==> Bronze"
"$PYTHON" src/bronze/ingest_bronze.py

echo "==> Silver"
for job in customer_silver address_silver order_silver payment_silver support_ticket_silver loyalty_silver; do
    "$PYTHON" "src/silver/${job}.py"
done

echo "==> Gold"
for job in customer_360_snapshot customer_kpi_summary data_quality_summary export_powerbi_data; do
    "$PYTHON" "src/gold/${job}.py"
done

if [[ "${1:-}" == "--skip-dbt" ]]; then
    exit 0
fi

echo "==> dbt (local Spark)"
cd dbt/customer_360
rm -rf metastore_db spark-warehouse
[[ -f profiles.yml ]] || cp profiles.example.yml profiles.yml
"$PYTHON" ../../scripts/register_local_bronze.py
dbt build --profiles-dir . --target local
