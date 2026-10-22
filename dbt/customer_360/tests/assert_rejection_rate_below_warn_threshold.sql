-- Warns (build still passes) when any source rejects more than 3% of its rows.
{{ config(severity = 'warn') }}

select source_name, rejection_rate_percent
from {{ ref('data_quality_summary') }}
where rejection_rate_percent > 3