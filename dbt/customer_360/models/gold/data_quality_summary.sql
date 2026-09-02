{{ config(
    materialized = 'table'
) }}

with customers as (

    select
        'customers' as source_name,
        (select count(*) from {{ source('bronze', 'customers') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_customers') }}) as silver_valid_rows,
        (select count(*) from {{ ref('rejected_customers') }}) as silver_rejected_rows

),

addresses as (

    select
        'addresses' as source_name,
        (select count(*) from {{ source('bronze', 'addresses') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_addresses') }}) as silver_valid_rows,
        (select count(*) from {{ ref('rejected_addresses') }}) as silver_rejected_rows

),

orders as (

    select
        'orders' as source_name,
        (select count(*) from {{ source('bronze', 'orders') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_orders') }}) as silver_valid_rows,
        (select count(*) from {{ ref('rejected_orders') }}) as silver_rejected_rows

),

payments as (

    select
        'payments' as source_name,
        (select count(*) from {{ source('bronze', 'payments') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_payments') }}) as silver_valid_rows,
        (select count(*) from {{ ref('rejected_payments') }}) as silver_rejected_rows

),

support_tickets as (

    select
        'support_tickets' as source_name,
        (select count(*) from {{ source('bronze', 'support_tickets') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_support_tickets') }}) as silver_valid_rows,
        (select count(*) from {{ ref('rejected_support_tickets') }}) as silver_rejected_rows

),

loyalty_accounts as (

    select
        'loyalty_accounts' as source_name,
        (select count(*) from {{ source('bronze', 'loyalty_accounts') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_loyalty_accounts') }}) as silver_valid_rows,
        (select count(*) from {{ ref('rejected_loyalty_accounts') }}) as silver_rejected_rows

),

combined as (

    select * from customers
    union all
    select * from addresses
    union all
    select * from orders
    union all
    select * from payments
    union all
    select * from support_tickets
    union all
    select * from loyalty_accounts

),

final as (

    select
        source_name,
        bronze_rows,
        silver_valid_rows,
        silver_rejected_rows,

        silver_valid_rows + silver_rejected_rows
            as total_processed_rows,

        bronze_rows - (silver_valid_rows + silver_rejected_rows)
            as duplicate_rows_removed,

        round(
            silver_rejected_rows * 100.0
            / nullif(
                silver_valid_rows + silver_rejected_rows,
                0
            ),
            2
        ) as rejection_rate_percent,

        current_timestamp() as gold_processed_at

    from combined

)

select *
from final