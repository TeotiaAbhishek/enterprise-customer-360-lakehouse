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
    silver_processed_at,
    rejection_reason
    
from {{ ref('int_customer_validation') }}
where not is_valid