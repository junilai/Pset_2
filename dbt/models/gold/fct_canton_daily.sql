{{ config(
    materialized='table',
    cluster_by=['event_date', 'canton_key']
) }}

select
    f.date_key,
    f.event_date,
    f.canton_key,
    sum(f.incident_count) as total_incidents,
    sum(iff(t.service_category = 'SEGURIDAD', f.incident_count, 0)) as security_incidents,
    sum(iff(t.service_category = 'SALUD', f.incident_count, 0)) as health_incidents,
    sum(iff(t.service_category = 'TRANSITO', f.incident_count, 0)) as transit_incidents,
    sum(iff(t.service_category = 'MUNICIPAL', f.incident_count, 0)) as municipal_incidents,
    sum(iff(t.service_category = 'SINIESTROS', f.incident_count, 0)) as incident_service_incidents,
    sum(iff(t.service_category = 'MILITAR', f.incident_count, 0)) as military_incidents,
    sum(iff(t.service_category = 'RIESGOS', f.incident_count, 0)) as risk_incidents,
    sum(iff(t.service_category = 'OTRO', f.incident_count, 0)) as other_incidents,
    count_if(f.incident_count > 0) as active_emergency_types,
    sum(f.compact_group_count) as compact_group_count,
    sum(f.duplicate_rows_collapsed) as duplicate_rows_collapsed,
    sum(f.repaired_date_incidents) as repaired_date_incidents,
    sum(f.invalid_incidents) as invalid_incidents,
    sum(f.warning_incidents) as warning_incidents
from {{ ref('fct_emergencias_daily') }} f
join {{ ref('dim_emergency_type') }} t using (emergency_type_key)
group by f.date_key, f.event_date, f.canton_key
