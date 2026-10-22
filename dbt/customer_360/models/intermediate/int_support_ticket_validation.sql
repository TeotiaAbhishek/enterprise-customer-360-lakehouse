{{ config(
    materialized = 'ephemeral'
) }}

with ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by
                case when ticket_created_at > current_timestamp() then 1 else 0 end, 
                ticket_created_at desc nulls last,
                _ingestion_timestamp desc
        ) as row_num

    from {{ ref('stg_support_tickets') }}

),

deduplicated as (

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
        source_system

    from ranked
    where row_num = 1

),

customer_lookup as (

    select distinct
        customer_id,
        true as customer_exists

    from {{ ref('silver_customers') }}

),

tickets_with_customer_check as (

    select
        t.*,
        c.customer_exists

    from deduplicated as t

    left join customer_lookup as c
        on t.customer_id = c.customer_id

),

validated as (

    select
        *,

        case
            when ticket_created_at > current_date() then true
            when ticket_resolved_at is not null
                 and ticket_resolved_at > current_date() then true
            else false
        end as has_future_ticket_date,

        case
            when ticket_id is null then false
            when customer_id is null then false
            when customer_exists is null then false
            when ticket_created_at is null then false
            when ticket_resolved_at is not null
                 and ticket_resolved_at < ticket_created_at then false
            when ticket_status is null then false
            when ticket_status not in (
                'open',
                'in_progress',
                'resolved',
                'closed'
            ) then false
            when priority is null then false
            when priority not in (
                'low',
                'medium',
                'high',
                'urgent'
            ) then false
            when channel is null then false
            when channel not in (
                'email',
                'phone',
                'chat',
                'web'
            ) then false
            when category is null then false
            when category not in (
                'payment_issue',
                'delivery_delay',
                'account_access',
                'refund_request',
                'product_query',
                'loyalty_query',
                'complaint'
            ) then false
            else true
        end as is_valid,

        case
            when ticket_id is null then 'ticket_id is null'
            when customer_id is null then 'customer_id is null'
            when customer_exists is null
                then 'customer_id does not exist in silver customers'
            when ticket_created_at is null
                then 'ticket_created_at is null'
            when ticket_resolved_at is not null
                 and ticket_resolved_at < ticket_created_at
                then 'negative resolution time'
            when ticket_status is null
                then 'ticket_status is null'
            when ticket_status not in (
                'open',
                'in_progress',
                'resolved',
                'closed'
            ) then 'invalid ticket_status'
            when priority is null
                then 'priority is null'
            when priority not in (
                'low',
                'medium',
                'high',
                'urgent'
            ) then 'invalid priority'
            when channel is null
                then 'channel is null'
            when channel not in (
                'email',
                'phone',
                'chat',
                'web'
            ) then 'invalid channel'
            when category is null
                then 'category is null'
            when category not in (
                'payment_issue',
                'delivery_delay',
                'account_access',
                'refund_request',
                'product_query',
                'loyalty_query',
                'complaint'
            ) then 'invalid category'
            else null
        end as rejection_reason,

        current_timestamp() as silver_processed_at

    from tickets_with_customer_check

)

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
    is_valid,
    rejection_reason,
    silver_processed_at

from validated