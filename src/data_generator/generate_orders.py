from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd

from config import (
    DUPLICATE_ORDER_RATE,
    INVALID_STATUS_RATE,
    NULL_CUSTOMER_ID_RATE,
    NEGATIVE_AMOUNT_RATE,
    FUTURE_DATE_RATE
)


CUSTOMER_INPUT_PATH = Path("data/landing/crm_customers")
OUTPUT_PATH = Path("data/landing/orders")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

random.seed(44)

VALID_STATUSES = [
    "PLACED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED"
]

INVALID_STATUSES = [
    "UNKNOWN",
    "INVALID"
]

CHANNELS = [
    "WEB",
    "MOBILE",
    "STORE"
]


def get_latest_customer_file():
    files = sorted(CUSTOMER_INPUT_PATH.glob("*.csv"))
    return files[-1]


def generate_orders():

    customers = pd.read_csv(get_latest_customer_file())

    customers = customers["customer_id"].drop_duplicates()

    purchasing_customers = customers.sample(
        frac=0.60,
        random_state=44
    )

    rows = []
    order_counter = 1

    for customer_id in purchasing_customers:

        num_orders = random.randint(1, 15)

        for _ in range(num_orders):

            order_date = datetime.now() - timedelta(
                days=random.randint(0, 1825)
            )

            if random.random() < FUTURE_DATE_RATE:
                order_date = datetime.now() + timedelta(
                    days=random.randint(1, 365)
                )

            status = random.choice(VALID_STATUSES)

            if random.random() < INVALID_STATUS_RATE:
                status = random.choice(INVALID_STATUSES)

            amount = round(random.uniform(10, 5000), 2)

            if random.random() < NEGATIVE_AMOUNT_RATE:
                amount = -amount

            customer_ref = customer_id

            if random.random() < NULL_CUSTOMER_ID_RATE:
                customer_ref = None

            rows.append({
                "order_id": f"ORD{order_counter:09d}",
                "customer_id": customer_ref,
                "order_date": order_date,
                "order_status": status,
                "order_amount": amount,
                "currency": "AUD",
                "sales_channel": random.choice(CHANNELS),
                "created_at": order_date,
                "updated_at": order_date + timedelta(
                    days=random.randint(0, 30)
                ),
                "source_system": "ecommerce"
            })

            order_counter += 1

    df = pd.DataFrame(rows)

    duplicates = df.sample(
        frac=DUPLICATE_ORDER_RATE,
        random_state=44
    )

    df = pd.concat(
        [df, duplicates],
        ignore_index=True
    )

    file_name = (
        f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    output_file = OUTPUT_PATH / file_name

    df.to_csv(output_file, index=False)

    print(f"Generated {len(df):,} orders")
    print(f"Duplicate orders added: {len(duplicates):,}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    generate_orders()