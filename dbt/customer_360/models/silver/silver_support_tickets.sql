{{ config(
    materialized = 'table'
) }}

select
    ticket_id,
    customer_id,
    ticket_created_at,
    ticket_resolved_at,
    ticket_status,
    priority,
    channel,
    category,
    agent_id,
    source_system,
    customer_exists,
    has_future_ticket_date,
    silver_processed_at

from {{ ref('int_support_ticket_validation') }}

where is_valid = true