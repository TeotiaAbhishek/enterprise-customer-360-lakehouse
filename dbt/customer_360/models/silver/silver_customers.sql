{{ config(
    materialized = 'table'
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    gender,
    status,
    date_of_birth,
    signup_date,
    created_at,
    updated_at,
    source_system,
    has_future_updated_at,
    silver_processed_at

from {{ ref('int_customer_validation') }}

where is_valid = true