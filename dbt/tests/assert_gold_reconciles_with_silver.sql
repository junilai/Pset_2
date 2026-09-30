with silver_total as (
    select sum(incident_count) as incident_count
    from {{ ref('stg_emergencias') }}
    where event_date is not null
),
gold_total as (
    select sum(incident_count) as incident_count
    from {{ ref('fct_emergencias_daily') }}
)

select s.incident_count as silver_incidents, g.incident_count as gold_incidents
from silver_total s
cross join gold_total g
where s.incident_count <> g.incident_count
