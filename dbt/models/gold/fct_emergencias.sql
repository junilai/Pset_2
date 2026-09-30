{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_period',
    cluster_by=['fecha_key']
) }}

-- HECHOS: una fila por emergencia coordinada por el ECU 911 (grano = emergencia).
-- La fuente no trae métricas numéricas (hecho "factless"): la medida es cantidad = 1,
-- que se suma para obtener volúmenes por cantón, día, servicio, etc.
select
    -- PK
    e.emergencia_key,

    -- FKs
    to_number(to_char(e.fecha, 'YYYYMMDD'))           as fecha_key,
    e.parroquia_codigo,
    e.canton_codigo,
    e.servicio_id,
    md5(e.servicio_id || '|' || e.subtipo)            as tipo_emergencia_key,

    -- atributos degenerados / linaje
    e.fecha,
    e.source_period,
    e.source_row_number,

    -- medida
    1                                                 as cantidad,

    e.silver_processed_at,
    current_timestamp()::timestamp_ltz                as gold_processed_at
from {{ ref('slv_emergencias') }} e
where {{ periods_to_process(ref('slv_emergencias'), 'source_period', 'silver_processed_at', 'silver_processed_at') }}
