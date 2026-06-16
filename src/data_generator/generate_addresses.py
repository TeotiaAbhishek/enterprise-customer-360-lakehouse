from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker


fake = Faker("en_AU")
Faker.seed(43)
random.seed(43)


CUSTOMER_INPUT_PATH = Path("data/landing/crm_customers")
OUTPUT_PATH = Path("data/landing/crm_addresses")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

NULL_POSTCODE_RATE = 0.01
INVALID_STATE_RATE = 0.005
DUPLICATE_RATE = 0.01

VALID_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
INVALID_STATES = ["XX", "UNKNOWN", "N/A"]


def get_latest_customer_file():
    files = sorted(CUSTOMER_INPUT_PATH.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No customer files found. Run generate_customers.py first.")
    return files[-1]


def generate_addresses():
    customer_file = get_latest_customer_file()
    customers = pd.read_csv(customer_file)

    customers = customers.drop_duplicates("customer_id")
    rows = []

    for _, row in customers.iterrows():
        created_at = pd.to_datetime(row["created_at"])
        updated_at = created_at + timedelta(days=random.randint(0, 1200))

        state = random.choice(VALID_STATES)
        postcode = fake.postcode()

        if random.random() < INVALID_STATE_RATE:
            state = random.choice(INVALID_STATES)

        if random.random() < NULL_POSTCODE_RATE:
            postcode = None

        rows.append({
            "address_id": f"ADDR{len(rows) + 1:07d}",
            "customer_id": row["customer_id"],
            "address_line_1": fake.street_address(),
            "suburb": fake.city(),
            "state": state,
            "postcode": postcode,
            "country": "Australia",
            "is_primary": True,
            "created_at": created_at.date(),
            "updated_at": updated_at.date(),
            "source_system": "crm"
        })

    df = pd.DataFrame(rows)

    duplicates = df.sample(frac=DUPLICATE_RATE, random_state=43)
    df = pd.concat([df, duplicates], ignore_index=True)

    file_name = f"crm_addresses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_file = OUTPUT_PATH / file_name

    df.to_csv(output_file, index=False)

    print(f"Generated {len(df):,} CRM address records")
    print(f"Base address records: {len(rows):,}")
    print(f"Duplicate records added: {len(duplicates):,}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    generate_addresses()