{{ config(materialized='table') }}

with name_support as (
    select
        canton_code,
        province_code,
        provincia_name,
        canton_name,
        sum(incident_count) as name_support_incidents
    from {{ ref('stg_emergencias') }}
    where canton_code is not null
    group by canton_code, province_code, provincia_name, canton_name
),

canonical_name as (
    select *
    from name_support
    qualify row_number() over (
        partition by canton_code
        order by
            name_support_incidents desc,
            provincia_name nulls last,
            canton_name nulls last
    ) = 1
),

stats as (
    select
        canton_code,
        min(event_date) as first_event_date,
        max(event_date) as last_event_date,
        count(distinct event_date) as active_days,
        sum(incident_count) as total_incidents,
        count(distinct concat_ws('|', coalesce(provincia_name, ''), coalesce(canton_name, ''))) as naming_variants,
        count(distinct case
            when event_date <= '{{ var("training_cutoff") }}'::date then event_date
        end) as observed_training_days,
        sum(case
            when event_date <= '{{ var("training_cutoff") }}'::date then incident_count
            else 0
        end) as training_incidents
    from {{ ref('stg_emergencias') }}
    where canton_code is not null
      and event_date is not null
    group by canton_code
)

select
    n.canton_code as canton_key,
    n.canton_code,
    n.province_code,
    coalesce(n.provincia_name, 'DESCONOCIDA') as provincia_name,
    coalesce(n.canton_name, 'DESCONOCIDO') as canton_name,
    s.first_event_date,
    s.last_event_date,
    s.active_days,
    s.total_incidents,
    s.naming_variants,
    s.observed_training_days,
    s.training_incidents,
    s.observed_training_days >= 730
        and s.training_incidents >= 1000 as is_model_eligible,
    true as is_known_canton
from canonical_name n
join stats s using (canton_code)

union all

select
    'UNKNOWN',
    null,
    null,
    'DESCONOCIDA',
    'DESCONOCIDO',
    null,
    null,
    0,
    0,
    0,
    0,
    0,
    false,
    false
