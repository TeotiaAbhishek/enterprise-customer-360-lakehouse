{{ config(
    materialized = 'table'
) }}

select
    count(*) as total_customers,

    sum(
        case
            when status = 'active' then 1
            else 0
        end
    ) as active_customers,

    sum(
        case
            when status = 'blocked' then 1
            else 0
        end
    ) as blocked_customers,

    sum(
        case
            when total_orders > 0 then 1
            else 0
        end
    ) as customers_with_orders,

    sum(total_revenue) as total_revenue,

    avg(total_revenue) as avg_customer_revenue,

    sum(
        case
            when loyalty_tier is not null then 1
            else 0
        end
    ) as customers_with_loyalty,

    sum(points_balance) as total_points_balance,

    sum(
        case
            when support_ticket_count > 0 then 1
            else 0
        end
    ) as customers_with_support_tickets,

    sum(open_ticket_count) as open_support_tickets,

    current_timestamp() as gold_processed_at

from {{ ref('customer_360') }}