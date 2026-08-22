{{ config(
    materialized = 'table'
) }}

select
    loyalty_account_id,
    customer_id,
    join_date,
    tier,
    points_balance,
    loyalty_status,
    created_at,
    updated_at,
    source_system,
    customer_exists,
    has_future_date,
    silver_processed_at,
    rejection_reason

from {{ ref('int_loyalty_validation') }}

where is_valid = false