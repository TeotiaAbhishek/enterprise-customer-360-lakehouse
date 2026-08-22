{{ config(
    materialized = 'view'
) }}

select
    trim(address_id) as address_id,
    trim(customer_id) as customer_id,
    lower(trim(address_line_1)) as address_line_1,
    lower(trim(suburb)) as suburb,
    upper(trim(state)) as state,
    trim(postcode) as postcode,
    lower(trim(country)) as country,
    lower(trim(is_primary)) as is_primary,
    cast(created_at as date) as created_at,
    cast(updated_at as date) as updated_at,
    lower(trim(source_system)) as source_system

from {{ source('bronze', 'addresses') }}