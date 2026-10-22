{{ config(
    materialized='view'
) }}

select
    trim(customer_id) as customer_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    lower(trim(email)) as email,
    lower(trim(gender)) as gender,
    lower(trim(status)) as status,
    cast(date_of_birth as date) as date_of_birth,
    cast(signup_date as date) as signup_date,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    lower(trim(source_system)) as source_system,
    _ingestion_timestamp

from {{ source('bronze', 'customers') }}