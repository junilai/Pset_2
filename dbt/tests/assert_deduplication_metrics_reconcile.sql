with raw_count as (
    select count(*) as row_count
    from {{ source('bronze', 'emergencias_raw') }}
),
silver_metrics as (
    select
        count(*) as compact_rows,
        sum(duplicate_rows_collapsed) as collapsed_rows
    from {{ ref('stg_emergencias') }}
)

select r.row_count, s.compact_rows, s.collapsed_rows
from raw_count r
cross join silver_metrics s
where r.row_count - s.compact_rows <> s.collapsed_rows
