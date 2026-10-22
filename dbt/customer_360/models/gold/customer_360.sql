{{ config(
    materialized = 'table',
    liquid_clustered_by = ['state']
) }}

with customer_base as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        gender,
        status,
        signup_date
    from {{ ref('silver_customers') }}

),

address_summary as (

    select
        customer_id,
        state,
        postcode,
        country
    from {{ ref('silver_addresses') }}
    where is_primary = 'true'

),

order_summary as (

    select
        customer_id,
        count(order_id) as total_orders,
        sum(
            case
                when order_status in ('paid', 'shipped', 'delivered')
                then order_amount
                else 0
            end
        ) as total_revenue,
        max(order_date) as last_order_date

    from {{ ref('silver_orders') }}
    group by customer_id

),

payment_summary as (

    select
        customer_id,
        count(
            case
                when payment_status = 'success'
                then payment_id
            end
        ) as successful_payment_count,

        sum(
            case
                when payment_status = 'success'
                then payment_amount
                else 0
            end
        ) as successful_payment_amount,

        max(payment_date) as last_payment_date

    from {{ ref('silver_payments') }}
    group by customer_id

),

support_summary as (

    select
        customer_id,
        count(ticket_id) as support_ticket_count,

        count(
            case
                when ticket_status in ('open', 'in_progress')
                then ticket_id
            end
        ) as open_ticket_count,

        max(ticket_created_at) as last_support_ticket_date

    from {{ ref('silver_support_tickets') }}
    group by customer_id

),

loyalty_summary as (

    select
        customer_id,
        tier as loyalty_tier,
        points_balance,
        loyalty_status

    from {{ ref('silver_loyalty_accounts') }}

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.gender,
        c.status,
        c.signup_date,

        a.state,
        a.postcode,
        a.country,

        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.total_revenue, 0) as total_revenue,
        o.last_order_date,

        coalesce(p.successful_payment_count, 0) as successful_payment_count,
        coalesce(p.successful_payment_amount, 0) as successful_payment_amount,
        p.last_payment_date,

        coalesce(s.support_ticket_count, 0) as support_ticket_count,
        coalesce(s.open_ticket_count, 0) as open_ticket_count,
        s.last_support_ticket_date,

        l.loyalty_tier,
        coalesce(l.points_balance, 0) as points_balance,
        l.loyalty_status,

        current_timestamp() as gold_processed_at

    from customer_base c

    left join address_summary a
        on c.customer_id = a.customer_id

    left join order_summary o
        on c.customer_id = o.customer_id

    left join payment_summary p
        on c.customer_id = p.customer_id

    left join support_summary s
        on c.customer_id = s.customer_id

    left join loyalty_summary l
        on c.customer_id = l.customer_id

)

select *
from final