{{ config(
    materialized = 'view'
) }}

select
    trim(payment_id) as payment_id,
    trim(order_id) as order_id,
    trim(customer_id) as customer_id,
    cast(payment_date as date) as payment_date,
    lower(trim(payment_status)) as payment_status,
    lower(trim(payment_method)) as payment_method,
    cast(payment_amount as decimal(18, 2)) as payment_amount,
    lower(trim(currency)) as currency,
    cast(created_at as date) as created_at,
    cast(updated_at as date) as updated_at,
    lower(trim(source_system)) as source_system,
    _ingestion_timestamp

from {{ source('bronze', 'payments') }}