{{ config(
    materialized = 'ephemeral'
) }}

with ranked as (

    select
        *,
        row_number() over (
            partition by loyalty_account_id
            order by updated_at desc nulls last
        ) as row_num

    from {{ ref('stg_loyalty_accounts') }}

),

deduplicated as (

    select
        loyalty_account_id,
        customer_id,
        join_date,
        tier,
        points_balance,
        loyalty_status,
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

loyalty_with_customer_check as (

    select
        loyalty.*,
        customers.customer_exists

    from deduplicated as loyalty

    left join customer_lookup as customers
        on loyalty.customer_id = customers.customer_id

),

validated as (

    select
        *,

        case
            when join_date > current_date()
                or updated_at > current_date()
            then true
            else false
        end as has_future_date,

        case
            when loyalty_account_id is null then false
            when customer_id is null then false
            when customer_exists is null then false
            when join_date is null then false
            when tier is null then false
            when tier not in (
                'bronze',
                'silver',
                'gold',
                'platinum'
            ) then false
            when loyalty_status is null then false
            when loyalty_status not in (
                'active',
                'inactive',
                'suspended'
            ) then false
            when points_balance is null then false
            when points_balance < 0 then false
            else true
        end as is_valid,

        case
            when loyalty_account_id is null
                then 'loyalty_account_id is null'
            when customer_id is null
                then 'customer_id is null'
            when customer_exists is null
                then 'customer_id does not exist in silver customers'
            when join_date is null
                then 'join_date is null'
            when tier is null
                then 'tier is null'
            when tier not in (
                'bronze',
                'silver',
                'gold',
                'platinum'
            ) then 'invalid tier'
            when loyalty_status is null
                then 'loyalty_status is null'
            when loyalty_status not in (
                'active',
                'inactive',
                'suspended'
            ) then 'invalid loyalty_status'
            when points_balance is null
                then 'points_balance is null'
            when points_balance < 0
                then 'points_balance must be greater than or equal to zero'
            else null
        end as rejection_reason,

        current_timestamp() as silver_processed_at

    from loyalty_with_customer_check

)

select
    loyalty_account_id,
    customer_id,
    join_date,
    tier,
    points_balance,
    loyalty_status,
    created_at,
    updated_at,
    source_system,
    customer_exists,
    has_future_date,
    is_valid,
    rejection_reason,
    silver_processed_at

from validated