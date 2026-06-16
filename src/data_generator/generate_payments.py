from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd


ORDERS_INPUT_PATH = Path("data/landing/orders")
OUTPUT_PATH = Path("data/landing/payments")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

random.seed(45)

PAYMENT_STATUSES = [
    "SUCCESS",
    "FAILED",
    "PENDING",
    "REFUNDED"
]

INVALID_PAYMENT_STATUSES = [
    "UNKNOWN",
    "ERROR"
]

PAYMENT_METHODS = [
    "CARD",
    "PAYPAL",
    "APPLE_PAY",
    "GOOGLE_PAY",
    "BANK_TRANSFER"
]

DUPLICATE_PAYMENT_RATE = 0.008
INVALID_STATUS_RATE = 0.004
NEGATIVE_PAYMENT_RATE = 0.001
ORPHAN_PAYMENT_RATE = 0.003


def get_latest_orders_file():
    files = sorted(ORDERS_INPUT_PATH.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No orders file found. Run generate_orders.py first.")
    return files[-1]


def generate_payments():
    orders = pd.read_csv(get_latest_orders_file())

    valid_orders = orders.dropna(subset=["order_id"])
    valid_orders = valid_orders.drop_duplicates("order_id")

    rows = []
    payment_counter = 1

    for _, order in valid_orders.iterrows():
        # Not every order has a successful payment
        if random.random() > 0.92:
            continue

        payment_status = random.choices(
            PAYMENT_STATUSES,
            weights=[0.82, 0.08, 0.05, 0.05],
            k=1
        )[0]

        if random.random() < INVALID_STATUS_RATE:
            payment_status = random.choice(INVALID_PAYMENT_STATUSES)

        amount = float(order["order_amount"])

        if random.random() < NEGATIVE_PAYMENT_RATE:
            amount = -abs(amount)

        payment_date = pd.to_datetime(order["order_date"]) + timedelta(
            minutes=random.randint(1, 180)
        )

        rows.append({
            "payment_id": f"PAY{payment_counter:09d}",
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "payment_date": payment_date,
            "payment_status": payment_status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_amount": round(amount, 2),
            "currency": "AUD",
            "created_at": payment_date,
            "updated_at": payment_date + timedelta(days=random.randint(0, 10)),
            "source_system": "payment_gateway"
        })

        payment_counter += 1

    df = pd.DataFrame(rows)

    # Add orphan payments that reference non-existent orders
    num_orphans = int(len(df) * ORPHAN_PAYMENT_RATE)

    orphan_rows = []
    for i in range(num_orphans):
        payment_date = datetime.now() - timedelta(days=random.randint(0, 365))

        orphan_rows.append({
            "payment_id": f"PAY{payment_counter:09d}",
            "order_id": f"ORD_MISSING_{i:06d}",
            "customer_id": None,
            "payment_date": payment_date,
            "payment_status": "SUCCESS",
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_amount": round(random.uniform(10, 5000), 2),
            "currency": "AUD",
            "created_at": payment_date,
            "updated_at": payment_date,
            "source_system": "payment_gateway"
        })

        payment_counter += 1

    df = pd.concat([df, pd.DataFrame(orphan_rows)], ignore_index=True)

    duplicates = df.sample(frac=DUPLICATE_PAYMENT_RATE, random_state=45)
    df = pd.concat([df, duplicates], ignore_index=True)

    file_name = f"payments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_file = OUTPUT_PATH / file_name

    df.to_csv(output_file, index=False)

    print(f"Generated {len(df):,} payments")
    print(f"Orphan payments added: {len(orphan_rows):,}")
    print(f"Duplicate payments added: {len(duplicates):,}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    generate_payments()