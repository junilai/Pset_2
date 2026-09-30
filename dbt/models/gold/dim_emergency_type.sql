{{ config(materialized='table') }}

with type_names as (
    select
        emergency_type_key,
        service_category,
        coalesce(servicio_name, 'SIN_SERVICIO') as servicio_name,
        subtipo_name,
        sum(incident_count) as support_incidents
    from {{ ref('stg_emergencias') }}
    group by
        emergency_type_key,
        service_category,
        coalesce(servicio_name, 'SIN_SERVICIO'),
        subtipo_name
),

canonical as (
    select *
    from type_names
    qualify row_number() over (
        partition by emergency_type_key
        order by support_incidents desc, servicio_name
    ) = 1
),

stats as (
    select
        emergency_type_key,
        sum(incident_count) as total_incidents,
        min(event_date) as first_event_date,
        max(event_date) as last_event_date
    from {{ ref('stg_emergencias') }}
    group by emergency_type_key
)

select
    c.emergency_type_key,
    c.service_category,
    c.servicio_name,
    c.subtipo_name,
    s.total_incidents,
    s.first_event_date,
    s.last_event_date
from canonical c
join stats s using (emergency_type_key)
