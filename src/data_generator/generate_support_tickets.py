from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd


CUSTOMER_INPUT_PATH = Path("data/landing/crm_customers")
OUTPUT_PATH = Path("data/landing/support_tickets")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

random.seed(46)

TICKET_STATUSES = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]
INVALID_STATUSES = ["UNKNOWN", "INVALID"]

PRIORITIES = ["LOW", "MEDIUM", "HIGH", "URGENT"]
CHANNELS = ["EMAIL", "PHONE", "CHAT", "WEB"]

CATEGORIES = [
    "PAYMENT_ISSUE",
    "DELIVERY_DELAY",
    "ACCOUNT_ACCESS",
    "REFUND_REQUEST",
    "PRODUCT_QUERY",
    "LOYALTY_QUERY",
    "COMPLAINT"
]

DUPLICATE_TICKET_RATE = 0.01
INVALID_STATUS_RATE = 0.005
NULL_CUSTOMER_ID_RATE = 0.005
FUTURE_CREATED_RATE = 0.003
NEGATIVE_RESOLUTION_RATE = 0.002


def get_latest_customer_file():
    files = sorted(CUSTOMER_INPUT_PATH.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No customer file found. Run generate_customers.py first.")
    return files[-1]


def generate_support_tickets():
    customers = pd.read_csv(get_latest_customer_file())
    customers = customers["customer_id"].drop_duplicates()

    ticket_customers = customers.sample(frac=0.25, random_state=46)

    rows = []
    ticket_counter = 1

    for customer_id in ticket_customers:
        num_tickets = random.randint(1, 5)

        for _ in range(num_tickets):
            created_at = datetime.now() - timedelta(days=random.randint(0, 1095))

            if random.random() < FUTURE_CREATED_RATE:
                created_at = datetime.now() + timedelta(days=random.randint(1, 180))

            status = random.choice(TICKET_STATUSES)

            if random.random() < INVALID_STATUS_RATE:
                status = random.choice(INVALID_STATUSES)

            resolved_at = None
            if status in ["RESOLVED", "CLOSED"]:
                resolved_at = created_at + timedelta(hours=random.randint(1, 240))

                if random.random() < NEGATIVE_RESOLUTION_RATE:
                    resolved_at = created_at - timedelta(hours=random.randint(1, 48))

            customer_ref = customer_id
            if random.random() < NULL_CUSTOMER_ID_RATE:
                customer_ref = None

            rows.append({
                "ticket_id": f"TCK{ticket_counter:09d}",
                "customer_id": customer_ref,
                "ticket_created_at": created_at,
                "ticket_resolved_at": resolved_at,
                "ticket_status": status,
                "priority": random.choice(PRIORITIES),
                "channel": random.choice(CHANNELS),
                "category": random.choice(CATEGORIES),
                "agent_id": f"AGT{random.randint(1, 500):05d}",
                "source_system": "zendesk"
            })

            ticket_counter += 1

    df = pd.DataFrame(rows)

    duplicates = df.sample(frac=DUPLICATE_TICKET_RATE, random_state=46)
    df = pd.concat([df, duplicates], ignore_index=True)

    file_name = f"support_tickets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_file = OUTPUT_PATH / file_name

    df.to_csv(output_file, index=False)

    print(f"Generated {len(df):,} support tickets")
    print(f"Duplicate tickets added: {len(duplicates):,}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    generate_support_tickets()