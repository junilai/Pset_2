select event_date, canton_key, count(*) as duplicate_count
from {{ ref('fct_canton_daily') }}
group by event_date, canton_key
having count(*) > 1
