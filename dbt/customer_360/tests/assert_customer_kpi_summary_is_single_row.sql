-- The KPI summary is a single aggregate row; anything else means the model broke.
select count(*) as row_count
from {{ ref('customer_kpi_summary') }}
having count(*) != 1
