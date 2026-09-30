select
    emergency_group_id,
    event_date,
    source_period
from {{ ref('stg_emergencias') }}
where event_date is null
   or not regexp_like(source_period, '^[0-9]{6}$')
   or to_char(event_date, 'YYYYMM') <> source_period
