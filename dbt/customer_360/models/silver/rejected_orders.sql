{{ config(
    materialized = 'table'
) }}

select
    order_id,
    customer_id,
    order_date,
    order_status,
    order_amount,
    currency,
    sales_channel,
    created_at,
    updated_at,
    source_system,
    customer_exists,
    has_future_updated_at,
    silver_processed_at,
    rejection_reason

from {{ ref('int_order_validation') }}

where is_valid = false