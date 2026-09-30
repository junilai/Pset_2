{{ config(
    incremental_strategy='delete+insert',
    unique_key='source_period'
) }}

-- SILVER (cuarentena): filas de bronze descartadas y su motivo.
-- Permite auditar la limpieza y conciliar: bronze = silver + rechazadas.
with classified as (
    {{ classify_emergencias() }}
)

select
    emergencia_key,
    rejection_reason,
    fecha,
    parroquia_codigo,
    provincia_nombre,
    canton_nombre,
    servicio_original,
    subtipo,
    source_file,
    source_period,
    source_row_number,
    bronze_loaded_at,
    current_timestamp()::timestamp_ltz as silver_processed_at
from classified
where rejection_reason is not null
