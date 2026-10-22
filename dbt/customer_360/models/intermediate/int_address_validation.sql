{{ config(
    materialized = 'ephemeral'
) }}

with ranked as (

    select
        *,
        row_number() over (
            partition by address_id
            order by 
                case when updated_at > current_timestamp() then 1 else 0 end,
                updated_at desc nulls last,
                _ingestion_timestamp desc
        ) as row_num

    from {{ ref('stg_addresses') }}

),

deduplicated as (

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

addresses_with_customer_check as (

    select
        a.*,
        c.customer_exists

    from deduplicated as a

    left join customer_lookup as c
        on a.customer_id = c.customer_id

),

validated as (

    select
        *,

        case
            when updated_at > current_timestamp() then true
            else false
        end as has_future_updated_at,

        case
            when address_id is null then false
            when customer_id is null then false
            when customer_exists is null then false
            when state not in (
                'NSW',
                'VIC',
                'QLD',
                'WA',
                'SA',
                'TAS',
                'ACT',
                'NT'
            ) then false
            when postcode is null then false
            when postcode not rlike '^[0-9]{4}$' then false
            else true
        end as is_valid,

        case
            when address_id is null then 'address_id is null'
            when customer_id is null then 'customer_id is null'
            when customer_exists is null
                then 'customer_id does not exist in silver customers'
            when state not in (
                'NSW',
                'VIC',
                'QLD',
                'WA',
                'SA',
                'TAS',
                'ACT',
                'NT'
            ) then 'invalid state'
            when postcode is null then 'postcode is null'
            when postcode not rlike '^[0-9]{4}$'
                then 'invalid postcode'
            else null
        end as rejection_reason,

        current_timestamp() as silver_processed_at

    from addresses_with_customer_check

)

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
    is_valid,
    rejection_reason,
    silver_processed_at
    
from validated