DEFAULT_CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
}

DATASETS = {
    "customers": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/crm_customers_*.csv",
        "bronze_table": "workspace.bronze.customers",
        "primary_key": "customer_id",
        "options": DEFAULT_CSV_OPTIONS,
    },

    "orders": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/orders_*.csv",
        "bronze_table": "workspace.bronze.orders",
        "primary_key": "order_id",
        "options": DEFAULT_CSV_OPTIONS,
    },

    "addresses": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/crm_addresses_*.csv",
        "bronze_table": "workspace.bronze.addresses",
        "primary_key": "address_id",
        "options": DEFAULT_CSV_OPTIONS,
    },

    "loyalty_accounts": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/loyalty_accounts_*.csv",
        "bronze_table": "workspace.bronze.loyalty_accounts",
        "primary_key": "loyalty_account_id",
        "options": DEFAULT_CSV_OPTIONS,
    },

    "payments": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/payments_*.csv",
        "bronze_table": "workspace.bronze.payments",
        "primary_key": "payment_id",
        "options": DEFAULT_CSV_OPTIONS,
    },

    "support_tickets": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/support_tickets_*.csv",
        "bronze_table": "workspace.bronze.support_tickets",
        "primary_key": "ticket_id",
        "options": DEFAULT_CSV_OPTIONS,
    },
}


def get_dataset_config(dataset_name: str) -> dict:
    if dataset_name not in DATASETS:
        available = ", ".join(sorted(DATASETS))
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. "
            f"Available datasets: {available}"
        )

    return DATASETS[dataset_name]