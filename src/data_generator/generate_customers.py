from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker


fake = Faker("en_AU")
Faker.seed(42)
random.seed(42)


OUTPUT_PATH = Path("data/landing/crm_customers")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

NUM_CUSTOMERS = 100_000

BAD_EMAIL_RATE = 0.01
NULL_PHONE_RATE = 0.01
DUPLICATE_RATE = 0.02
NULL_DOB_RATE = 0.005

STATUSES = ["active", "inactive", "blocked", "pending"]
GENDERS = ["male", "female", "other", "unknown"]


def random_timestamp(start_date, max_days=1200):
    return start_date + timedelta(days=random.randint(0, max_days))


def generate_customers():
    rows = []

    for i in range(1, NUM_CUSTOMERS + 1):
        signup_date = fake.date_between(start_date="-5y", end_date="today")
        created_at = signup_date
        updated_at = random_timestamp(signup_date)

        first_name = fake.first_name()
        last_name = fake.last_name()

        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
        phone = fake.phone_number()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)

        if random.random() < BAD_EMAIL_RATE:
            email = random.choice(["invalid-email", "missing_at_symbol.com", "abc@"])

        if random.random() < NULL_PHONE_RATE:
            phone = None

        if random.random() < NULL_DOB_RATE:
            dob = None

        rows.append({
            "customer_id": f"CRM{i:07d}",
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "date_of_birth": dob,
            "gender": random.choice(GENDERS),
            "signup_date": signup_date,
            "status": random.choice(STATUSES),
            "created_at": created_at,
            "updated_at": updated_at,
            "source_system": "crm"
        })

    df = pd.DataFrame(rows)

    duplicates = df.sample(frac=DUPLICATE_RATE, random_state=42)
    df = pd.concat([df, duplicates], ignore_index=True)

    file_name = f"crm_customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_file = OUTPUT_PATH / file_name

    df.to_csv(output_file, index=False)

    print(f"Generated {len(df):,} CRM customer records")
    print(f"Base customers: {NUM_CUSTOMERS:,}")
    print(f"Duplicate records added: {len(duplicates):,}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    generate_customers()