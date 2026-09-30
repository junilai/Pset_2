{{ config(
    incremental_strategy='delete+insert',
    unique_key='source_period',
    cluster_by=['source_period']
) }}

-- SILVER: emergencias limpias y estandarizadas. Grano: una emergencia coordinada.
with classified as (
    {{ classify_emergencias() }}
)

select
    emergencia_key,
    fecha,
    provincia_codigo,
    provincia_nombre,
    canton_codigo,
    canton_nombre,
    parroquia_codigo,
    parroquia_nombre,
    servicio_id,
    servicio_original,
    subtipo,
    source_file,
    source_period,
    source_row_number,
    source_encoding,
    bronze_loaded_at,
    current_timestamp()::timestamp_ltz as silver_processed_at
from classified
where rejection_reason is null
