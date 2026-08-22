{{ config(
    materialized = 'table'
) }}

select
    address_id,
    customer_id,
    address_line_1,
    suburb,
    state,
    postcode,
    country,
    is_primary,
    created_at,
    updated_at,
    source_system,
    customer_exists,
    has_future_updated_at,
    silver_processed_at,
    rejection_reason

from {{ ref('int_address_validation') }}

where is_valid = false