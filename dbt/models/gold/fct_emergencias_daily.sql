{{ config(
    materialized='table',
    cluster_by=['event_date', 'canton_key']
) }}

select
    sha2_hex(
        concat(
            to_char(event_date, 'YYYY-MM-DD'),
            '|', coalesce(canton_code, 'UNKNOWN'),
            '|', emergency_type_key
        ),
        256
    ) as daily_emergency_key,
    to_number(to_char(event_date, 'YYYYMMDD')) as date_key,
    event_date,
    coalesce(canton_code, 'UNKNOWN') as canton_key,
    emergency_type_key,
    sum(incident_count) as incident_count,
    count(*) as compact_group_count,
    sum(duplicate_rows_collapsed) as duplicate_rows_collapsed,
    sum(iff(is_date_repaired, incident_count, 0)) as repaired_date_incidents,
    sum(iff(quality_status = 'INVALID', incident_count, 0)) as invalid_incidents,
    sum(iff(quality_status = 'VALID_WITH_WARNINGS', incident_count, 0)) as warning_incidents
from {{ ref('stg_emergencias') }}
where event_date is not null
group by
    to_number(to_char(event_date, 'YYYYMMDD')),
    event_date,
    coalesce(canton_code, 'UNKNOWN'),
    emergency_type_key
