{{ config(
    materialized = 'ephemeral'
) }}

with ranked as (

    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc nulls last
        ) as row_num

    from {{ ref('stg_orders') }}

),

deduplicated as (

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

orders_with_customer_check as (

    select
        orders.*,
        customers.customer_exists

    from deduplicated as orders

    left join customer_lookup as customers
        on orders.customer_id = customers.customer_id

),

validated as (

    select
        *,

        case
            when updated_at > current_timestamp() then true
            else false
        end as has_future_updated_at,

        case
            when order_id is null then false
            when customer_id is null then false
            when customer_exists is null then false
            when order_status not in (
                'placed',
                'paid',
                'shipped',
                'delivered',
                'cancelled',
                'returned'
            ) then false
            when sales_channel is null then false
            when sales_channel not in (
                'web',
                'mobile',
                'store'
            ) then false
            when order_amount <= 0 then false
            else true
        end as is_valid,

        case
            when order_id is null then 'order_id is null'
            when customer_id is null then 'customer_id is null'
            when customer_exists is null
                then 'customer_id does not exist in silver customers'
            when order_status not in (
                'placed',
                'paid',
                'shipped',
                'delivered',
                'cancelled',
                'returned'
            ) then 'invalid status'
            when sales_channel is null then 'sales_channel is null'
            when sales_channel not in (
                'web',
                'mobile',
                'store'
            ) then 'invalid sales_channel'
            when order_amount <= 0
                then 'order_amount must be greater than zero'
            else null
        end as rejection_reason,

        current_timestamp() as silver_processed_at

    from orders_with_customer_check

)

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
    is_valid,
    rejection_reason,
    silver_processed_at

from validated