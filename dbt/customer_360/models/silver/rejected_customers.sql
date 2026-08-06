{{ config(
    materialized = 'table'
) }}

select
    *
from {{ ref('int_customer_validation') }}
where not is_valid