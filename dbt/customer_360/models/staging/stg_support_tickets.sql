{{ config(
    materialized = 'view'
) }}

select
    trim(ticket_id) as ticket_id,
    trim(customer_id) as customer_id,
    cast(ticket_created_at as date) as ticket_created_at,
    cast(ticket_resolved_at as date) as ticket_resolved_at,
    lower(trim(ticket_status)) as ticket_status,
    lower(trim(priority)) as priority,
    lower(trim(channel)) as channel,
    lower(trim(category)) as category,
    trim(agent_id) as agent_id,
    lower(trim(source_system)) as source_system

from {{ source('bronze', 'support_tickets') }}