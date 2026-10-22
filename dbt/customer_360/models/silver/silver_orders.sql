{{ config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'merge' if target.type == 'databricks' else 'append'
) }}

select
    order_id,
    customer_id,
    order_date,
    order_status,
    order_amount,
    currency,
    sales_channel,
    created_at,
    updated_at,
    source_system,
    customer_exists,
    has_future_updated_at,
    silver_processed_at,
    _ingestion_timestamp

from {{ ref('int_order_validation') }}

where is_valid = true
{% if is_incremental() %}
  and _ingestion_timestamp > (select max(_ingestion_timestamp) from {{ this }})
{% endif %}
