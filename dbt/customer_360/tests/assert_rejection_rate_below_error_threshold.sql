-- Fails the build when any source rejects more than 5% of its rows.
{{ config(severity = 'error') }}

select source_name, rejection_rate_percent
from {{ ref('data_quality_summary') }}
where rejection_rate_percent > 5