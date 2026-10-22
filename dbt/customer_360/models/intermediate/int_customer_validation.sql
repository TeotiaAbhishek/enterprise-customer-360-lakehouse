{{ config(
    materialized = 'ephemeral'
) }}

with ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by
                case when updated_at > current_timestamp() then 1 else 0 end,  -- future-dated rows lose
                updated_at desc nulls last,
                _ingestion_timestamp desc                                         -- tie-break: latest load wins
        ) as row_num

    from {{ ref('stg_customers') }}

),

deduplicated as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        gender,
        status,
        date_of_birth,
        signup_date,
        created_at,
        updated_at,
        source_system

    from ranked
    where row_num = 1

),

validated as (

    select
        *,

        case
            when updated_at > current_timestamp() then true
            else false
        end as has_future_updated_at,

        case
            when customer_id is null then false
            when email is null then false
            when email not rlike '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' then false
            when status is null then false
            when status not in (
                'active',
                'inactive',
                'blocked',
                'pending'
            ) then false
            else true
        end as is_valid,

        case
            when customer_id is null then 'customer_id is null'
            when email is null then 'email is null'
            when email not rlike '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                then 'invalid email'
            when status is null then 'status is null'
            when status not in (
                'active',
                'inactive',
                'blocked',
                'pending'
            ) then 'invalid status'
            else null
        end as rejection_reason,

        current_timestamp() as silver_processed_at

    from deduplicated

)

select
    customer_id,
    first_name,
    last_name,
    email,
    gender,
    status,
    date_of_birth,
    signup_date,
    created_at,
    updated_at,
    source_system,
    has_future_updated_at,
    is_valid,
    rejection_reason,
    silver_processed_at

from validated