{{ config(
    materialized = 'view'
) }}

select
    trim(loyalty_account_id) as loyalty_account_id,
    trim(customer_id) as customer_id,
    cast(join_date as date) as join_date,
    lower(trim(tier)) as tier,
    cast(points_balance as int) as points_balance,
    lower(trim(loyalty_status)) as loyalty_status,
    cast(created_at as date) as created_at,
    cast(updated_at as date) as updated_at,
    lower(trim(source_system)) as source_system

from {{ source('bronze', 'loyalty_accounts') }}