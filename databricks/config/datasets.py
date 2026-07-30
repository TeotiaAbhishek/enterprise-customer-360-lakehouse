DEFAULT_CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
}
DATASETS = {
    "customers": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/crm_customers_20260616_154420.csv",
        "bronze_table": "workspace.bronze.customers",
        "primary_key": "customer_id",
        "options": DEFAULT_CSV_OPTIONS
    },

    "orders": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/orders_20260616_161607.csv",
        "bronze_table": "workspace.bronze.orders",
        "primary_key": "order_id",
        "options": DEFAULT_CSV_OPTIONS
    },

    "addresses": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/crm_addresses_20260616_155826.csv",
        "bronze_table": "workspace.bronze.addresses",
        "primary_key": "address_id",
        "options": DEFAULT_CSV_OPTIONS
    },

    "loyalty_accounts": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/loyalty_accounts_20260616_162316.csv",
        "bronze_table": "workspace.bronze.loyalty_accounts",
        "primary_key": "loyalty_account_id",
        "options": DEFAULT_CSV_OPTIONS
    },

    "payments": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/payments_20260616_162017.csv",
        "bronze_table": "workspace.bronze.payments",
        "primary_key": "payment_id",
        "options": DEFAULT_CSV_OPTIONS
    },

    "support_tickets": {
        "source_format": "csv",
        "landing_path": "/Volumes/workspace/default/data/support_tickets_20260616_162156.csv",
        "bronze_table": "workspace.bronze.support_tickets",
        "primary_key": "ticket_id",
        "options": DEFAULT_CSV_OPTIONS
    },
}


def get_dataset_config(dataset_name: str) -> dict:
    """Return configuration for a configured dataset."""

    if dataset_name not in DATASETS:
        available = ", ".join(sorted(DATASETS))
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. "
            f"Available datasets: {available}"
        )

    return DATASETS[dataset_name]