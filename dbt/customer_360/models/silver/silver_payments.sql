{{ config(
    materialized = 'incremental',
    unique_key = 'payment_id',
    incremental_strategy = 'merge' if target.type == 'databricks' else 'append',
    liquid_clustered_by = ['customer_id', 'payment_date']
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
    _ingestion_timestamp

from {{ ref('int_payment_validation') }}

where is_valid = true
{% if is_incremental() %}
  and _ingestion_timestamp > (select max(_ingestion_timestamp) from {{ this }})
{% endif %}