{{ config(
    materialized='table',
    cluster_by=['event_date', 'canton_code']
) }}

-- La fuente no contiene un identificador ni la hora del incidente. Por eso las
-- filas idénticas se compactan, pero su frecuencia se conserva en incident_count.
with exact_groups as (
    select
        source_period::varchar as raw_source_period,
        fecha::varchar as raw_fecha,
        provincia::varchar as raw_provincia,
        canton::varchar as raw_canton,
        cod_parroquia::varchar as raw_cod_parroquia,
        parroquia::varchar as raw_parroquia,
        servicio::varchar as raw_servicio,
        subtipo::varchar as raw_subtipo,
        count(*)::number as incident_count
    from {{ source('bronze', 'emergencias_raw') }}
    group by
        source_period,
        fecha,
        provincia,
        canton,
        cod_parroquia,
        parroquia,
        servicio,
        subtipo
),

cleaned as (
    select
        *,
        trim(raw_source_period) as source_period,
        nullif(trim(raw_fecha), '') as fecha_clean,
        nullif(regexp_replace(upper(replace(trim(raw_provincia), 'Ð', 'Ñ')), '[[:space:]]+', ' '), '') as provincia_name,
        nullif(regexp_replace(upper(replace(trim(raw_canton), 'Ð', 'Ñ')), '[[:space:]]+', ' '), '') as canton_name,
        nullif(regexp_replace(upper(replace(trim(raw_parroquia), 'Ð', 'Ñ')), '[[:space:]]+', ' '), '') as parroquia_name,
        nullif(regexp_replace(upper(replace(trim(raw_servicio), 'Ð', 'Ñ')), '[[:space:]]+', ' '), '') as servicio_name,
        nullif(regexp_replace(upper(replace(trim(raw_subtipo), 'Ð', 'Ñ')), '[[:space:]]+', ' '), '') as subtipo_name,
        regexp_replace(trim(coalesce(raw_cod_parroquia, '')), '[.]0$', '') as parish_code_clean,
        sha2_hex(
            to_json(object_construct_keep_null(
                'SOURCE_PERIOD', raw_source_period,
                'FECHA', raw_fecha,
                'PROVINCIA', raw_provincia,
                'CANTON', raw_canton,
                'COD_PARROQUIA', raw_cod_parroquia,
                'PARROQUIA', raw_parroquia,
                'SERVICIO', raw_servicio,
                'SUBTIPO', raw_subtipo
            )),
            256
        ) as emergency_group_id
    from exact_groups
),

normalized as (
    select
        *,
        coalesce(
            try_to_date(fecha_clean, 'DD/MM/YYYY'),
            try_to_date(left(fecha_clean, 10), 'YYYY-MM-DD')
        ) as parsed_date,
        case
            when regexp_like(parish_code_clean, '^[0-9]{5,6}$')
                then lpad(parish_code_clean, 6, '0')
        end as parish_code,
        translate(coalesce(servicio_name, ''), 'ÁÉÍÓÚÜÑ', 'AEIOUUN') as servicio_ascii
    from cleaned
),

classified as (
    select
        *,
        case
            -- Los Excel de abril y mayo de 2026 intercambiaron día y mes
            -- durante su conversión. El mes real está en SOURCE_PERIOD y el
            -- mes de la fecha mal convertida contiene el día real (1 a 12).
            when source_period in ('202604', '202605')
              and parsed_date is not null
              and to_char(parsed_date, 'YYYYMM') <> source_period
                then date_from_parts(
                    substr(source_period, 1, 4)::integer,
                    substr(source_period, 5, 2)::integer,
                    month(parsed_date)
                )
            else parsed_date
        end as event_date,
        case
            when servicio_ascii like '%SEGURIDAD%' then 'SEGURIDAD'
            when servicio_ascii like '%SANITAR%' or servicio_ascii like '%SALUD%' then 'SALUD'
            when servicio_ascii like '%TRANSITO%' or servicio_ascii like '%MOVILIDAD%' then 'TRANSITO'
            when servicio_ascii like '%MUNICIPAL%' then 'MUNICIPAL'
            when servicio_ascii like '%SINIESTRO%' then 'SINIESTROS'
            when servicio_ascii like '%MILITAR%' then 'MILITAR'
            when servicio_ascii like '%RIESGO%' then 'RIESGOS'
            else 'OTRO'
        end as service_category
    from normalized
),

typed as (
    select
        *,
        left(parish_code, 2) as province_code,
        left(parish_code, 4) as canton_code,
        iff(
            source_period in ('202604', '202605')
            and parsed_date is not null
            and event_date <> parsed_date,
            true,
            false
        ) as is_date_repaired,
        iff(
            event_date is null
            or not regexp_like(source_period, '^[0-9]{6}$')
            or to_char(event_date, 'YYYYMM') <> source_period,
            true,
            false
        ) as is_source_period_mismatch,
        sha2_hex(
            concat(service_category, '|', coalesce(subtipo_name, 'SIN_SUBTIPO')),
            256
        ) as emergency_type_key
    from classified
)

select
    emergency_group_id,
    event_date,
    source_period,
    province_code,
    provincia_name,
    canton_code,
    canton_name,
    parish_code,
    parroquia_name,
    emergency_type_key,
    service_category,
    servicio_name,
    coalesce(subtipo_name, 'SIN_SUBTIPO') as subtipo_name,
    incident_count,
    incident_count - 1 as duplicate_rows_collapsed,
    incident_count > 1 as is_repeated_group,
    is_date_repaired,
    is_source_period_mismatch,
    event_date is not null
        and canton_code is not null
        and not is_source_period_mismatch as is_model_usable,
    case
        when event_date is null or canton_code is null or is_source_period_mismatch
            then 'INVALID'
        when provincia_name is null
          or canton_name is null
          or parroquia_name is null
          or servicio_name is null
          or subtipo_name is null
          or service_category = 'OTRO'
          or is_date_repaired
            then 'VALID_WITH_WARNINGS'
        else 'VALID'
    end as quality_status,
    raw_fecha,
    raw_provincia,
    raw_canton,
    raw_cod_parroquia,
    raw_parroquia,
    raw_servicio,
    raw_subtipo
from typed
