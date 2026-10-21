"""Contract tests: every Faker generator must emit exactly the columns that
the Bronze ingestion schema expects, in the same order. A mismatch would make
Spark silently shift or null columns when reading the CSVs with a fixed schema."""
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "bronze"))
import ingest_bronze  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GENERATORS = PROJECT_ROOT / "src" / "data_generator"

# generator script -> (landing folder, Bronze schema)
CONTRACTS = {
    "generate_customers.py": ("crm_customers", ingest_bronze.get_customer_schema()),
    "generate_addresses.py": ("crm_addresses", ingest_bronze.get_address_schema()),
    "generate_orders.py": ("orders", ingest_bronze.get_orders_schema()),
    "generate_payments.py": ("payments", ingest_bronze.get_payments_schema()),
    "generate_support_tickets.py": ("support_tickets", ingest_bronze.get_support_tickets_schema()),
    "generate_loyalty_accounts.py": ("loyalty_accounts", ingest_bronze.get_loyalty_accounts_schema()),
}


@pytest.fixture(scope="module")
def landing_dir(tmp_path_factory):
    workdir = tmp_path_factory.mktemp("pipeline")
    env = {**os.environ, "NUM_CUSTOMERS": "200"}

    # Customers first: the other generators read the latest customer file
    for script in CONTRACTS:
        subprocess.run(
            [sys.executable, str(GENERATORS / script)],
            cwd=workdir,
            env=env,
            check=True,
            capture_output=True,
        )

    return workdir / "data" / "landing"


@pytest.mark.parametrize("script", CONTRACTS)
def test_generator_columns_match_bronze_schema(landing_dir, script):
    folder, schema = CONTRACTS[script]
    csv_file = next((landing_dir / folder).glob("*.csv"))

    generated_columns = list(pd.read_csv(csv_file, nrows=0).columns)

    assert generated_columns == schema.fieldNames()


def test_generators_inject_duplicates(landing_dir):
    customers = pd.read_csv(next((landing_dir / "crm_customers").glob("*.csv")))

    assert customers["customer_id"].duplicated().any()
    assert customers["customer_id"].nunique() == 200
