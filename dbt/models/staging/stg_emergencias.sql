{#- Limpieza fila por fila de BRONZE.EMERGENCIAS; no agrega ni quita filas. Hallazgos y reglas en docs/perfilado.md. -#}
with bronze as (
    select * from {{ source('bronze', 'emergencias') }}
),

limpio as (
    select
        _periodo                                  as periodo,
        _file_row_number                          as fila_archivo,
        trim(fecha)                               as fecha_texto,
        {{ ubicacion_o_nulo('provincia') }}       as provincia,
        {{ ubicacion_o_nulo('canton') }}          as canton,
        {{ ubicacion_o_nulo('parroquia') }}       as parroquia,
        {{ ubicacion_o_nulo('cod_parroquia') }}   as cod_parroquia_texto,
        nullif(trim(servicio), '')                as servicio,
        nullif(trim(subtipo), '')                 as subtipo,
        _resource_id                              as resource_id,
        _source_file                              as archivo_origen,
        _loaded_at                                as cargado_en
    from bronze
)

select
    periodo || '-' || lpad(fila_archivo::varchar, 7, '0') as id_emergencia,
    periodo,
    -- d/m/aaaa y dd/mm/aaaa conviven en varios meses
    try_to_date(
        lpad(split_part(fecha_texto, '/', 1), 2, '0') || '/' ||
        lpad(split_part(fecha_texto, '/', 2), 2, '0') || '/' ||
        split_part(fecha_texto, '/', 3),
        'DD/MM/YYYY'
    )                                             as fecha,
    provincia,
    canton,
    parroquia,
    -- H3 / regla 2: 10 periodos perdieron el cero inicial de las provincias 01-09
    iff(cod_parroquia_texto rlike '[0-9]{5,6}', lpad(cod_parroquia_texto, 6, '0'), null) as cod_parroquia,
    servicio,
    -- H5 / regla 7: en 202302 la columna trae las categorias de SERVICIO, no subtipos
    iff(periodo = '202302', null, subtipo)        as subtipo,
    periodo = '202302'                            as subtipo_no_disponible,
    provincia is null                             as sin_ubicacion,
    fila_archivo,
    resource_id,
    archivo_origen,
    cargado_en
from limpio
