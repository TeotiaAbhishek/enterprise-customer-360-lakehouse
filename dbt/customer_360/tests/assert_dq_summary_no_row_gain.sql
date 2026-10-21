-- Silver can only drop rows (dedup); it must never produce more rows than Bronze.
select *
from {{ ref('data_quality_summary') }}
where silver_valid_rows + silver_rejected_rows > bronze_rows
