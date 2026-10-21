-- Every valid Silver customer must appear in customer_360 exactly once.
select
    (select count(*) from {{ ref('silver_customers') }}) as silver_customers,
    (select count(*) from {{ ref('customer_360') }}) as customer_360_rows
where (select count(*) from {{ ref('silver_customers') }})
   != (select count(*) from {{ ref('customer_360') }})
