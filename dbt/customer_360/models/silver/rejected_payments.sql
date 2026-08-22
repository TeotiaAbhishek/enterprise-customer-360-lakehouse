{{ config(
    materialized = 'table'
) }}

select
    payment_id,
    order_id,
    customer_id,
    payment_date,
    payment_status,
    payment_method,
    payment_amount,
    currency,
    created_at,
    updated_at,
    source_system,
    customer_exists,
    order_exists,
    has_future_updated_at,
    silver_processed_at,
    rejection_reason

from {{ ref('int_payment_validation') }}

where is_valid = false