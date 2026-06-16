from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd


CUSTOMER_INPUT_PATH = Path("data/landing/crm_customers")
OUTPUT_PATH = Path("data/landing/loyalty_accounts")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

random.seed(47)

TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
INVALID_TIERS = ["UNKNOWN", "INVALID"]

STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]
INVALID_STATUSES = ["ERROR", "UNKNOWN"]

DUPLICATE_ACCOUNT_RATE = 0.01
INVALID_TIER_RATE = 0.004
INVALID_STATUS_RATE = 0.003
NEGATIVE_POINTS_RATE = 0.002
NULL_CUSTOMER_ID_RATE = 0.004
FUTURE_JOIN_DATE_RATE = 0.003


def get_latest_customer_file():
    files = sorted(CUSTOMER_INPUT_PATH.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No customer file found. Run generate_customers.py first.")
    return files[-1]


def generate_loyalty_accounts():
    customers = pd.read_csv(get_latest_customer_file())
    customers = customers["customer_id"].drop_duplicates()

    loyalty_customers = customers.sample(frac=0.55, random_state=47)

    rows = []
    account_counter = 1

    for customer_id in loyalty_customers:
        join_date = datetime.now() - timedelta(days=random.randint(0, 1825))

        if random.random() < FUTURE_JOIN_DATE_RATE:
            join_date = datetime.now() + timedelta(days=random.randint(1, 365))

        tier = random.choices(
            TIERS,
            weights=[0.45, 0.30, 0.18, 0.07],
            k=1
        )[0]

        if random.random() < INVALID_TIER_RATE:
            tier = random.choice(INVALID_TIERS)

        status = random.choices(
            STATUSES,
            weights=[0.85, 0.12, 0.03],
            k=1
        )[0]

        if random.random() < INVALID_STATUS_RATE:
            status = random.choice(INVALID_STATUSES)

        points_balance = random.randint(0, 250_000)

        if random.random() < NEGATIVE_POINTS_RATE:
            points_balance = -points_balance

        customer_ref = customer_id
        if random.random() < NULL_CUSTOMER_ID_RATE:
            customer_ref = None

        rows.append({
            "loyalty_account_id": f"LOY{account_counter:09d}",
            "customer_id": customer_ref,
            "join_date": join_date,
            "tier": tier,
            "points_balance": points_balance,
            "loyalty_status": status,
            "created_at": join_date,
            "updated_at": join_date + timedelta(days=random.randint(0, 365)),
            "source_system": "loyalty_platform"
        })

        account_counter += 1

    df = pd.DataFrame(rows)

    duplicates = df.sample(frac=DUPLICATE_ACCOUNT_RATE, random_state=47)
    df = pd.concat([df, duplicates], ignore_index=True)

    file_name = f"loyalty_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_file = OUTPUT_PATH / file_name

    df.to_csv(output_file, index=False)

    print(f"Generated {len(df):,} loyalty accounts")
    print(f"Duplicate loyalty accounts added: {len(duplicates):,}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    generate_loyalty_accounts()