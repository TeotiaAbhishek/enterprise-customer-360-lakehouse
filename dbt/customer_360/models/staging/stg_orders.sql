{{ config(
    materialized = 'view'
) }}

select
    trim(order_id) as order_id,
    trim(customer_id) as customer_id,
    cast(order_date as date) as order_date,
    lower(trim(order_status)) as order_status,
    cast(order_amount as decimal(18, 2)) as order_amount,
    lower(trim(currency)) as currency,
    lower(trim(sales_channel)) as sales_channel,
    cast(created_at as date) as created_at,
    cast(updated_at as date) as updated_at,
    lower(trim(source_system)) as source_system

from {{ source('bronze', 'orders') }}