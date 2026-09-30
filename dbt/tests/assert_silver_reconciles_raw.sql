with raw_count as (
    select count(*) as row_count
    from {{ source('bronze', 'emergencias_raw') }}
),
silver_count as (
    select sum(incident_count) as incident_count
    from {{ ref('stg_emergencias') }}
)

select r.row_count, s.incident_count
from raw_count r
cross join silver_count s
where r.row_count <> s.incident_count
