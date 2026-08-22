{{ config(
    materialized = 'ephemeral'
) }}

with ranked as (

    select
        *,
        row_number() over (
            partition by payment_id
            order by updated_at desc nulls last
        ) as row_num

    from {{ ref('stg_payments') }}

),

deduplicated as (

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

order_lookup as (

    select distinct
        order_id,
        true as order_exists

    from {{ ref('silver_orders') }}

),

payments_with_checks as (

    select
        p.*,
        c.customer_exists,
        o.order_exists

    from deduplicated as p

    left join customer_lookup as c
        on p.customer_id = c.customer_id

    left join order_lookup as o
        on p.order_id = o.order_id

),

validated as (

    select
        *,

        case
            when updated_at > current_timestamp() then true
            else false
        end as has_future_updated_at,

        case
            when payment_id is null then false
            when order_id is null then false
            when order_exists is null then false
            when customer_id is null then false
            when customer_exists is null then false
            when payment_date is null then false
            when payment_status is null then false
            when payment_status not in (
                'success',
                'failed',
                'pending',
                'refunded'
            ) then false
            when payment_method is null then false
            when payment_method not in (
                'card',
                'paypal',
                'apple_pay',
                'google_pay',
                'bank_transfer'
            ) then false
            when payment_amount <= 0 then false
            else true
        end as is_valid,

        case
            when payment_id is null then 'payment_id is null'
            when order_id is null then 'order_id is null'
            when order_exists is null
                then 'order_id does not exist in silver orders'
            when customer_id is null then 'customer_id is null'
            when customer_exists is null
                then 'customer_id does not exist in silver customers'
            when payment_date is null then 'payment_date is null'
            when payment_status is null then 'payment_status is null'
            when payment_status not in (
                'success',
                'failed',
                'pending',
                'refunded'
            ) then 'invalid payment_status'
            when payment_method is null then 'payment_method is null'
            when payment_method not in (
                'card',
                'paypal',
                'apple_pay',
                'google_pay',
                'bank_transfer'
            ) then 'invalid payment_method'
            when payment_amount <= 0
                then 'payment_amount must be greater than zero'
            else null
        end as rejection_reason,

        current_timestamp() as silver_processed_at

    from payments_with_checks

)

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
    is_valid,
    rejection_reason,
    silver_processed_at

from validated