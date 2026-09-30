{#- Una fila por emergencia, con claves hacia dim_fecha, dim_ubicacion y dim_tipo_emergencia. -#}
select
    id_emergencia,
    to_number(to_char(fecha, 'YYYYMMDD'))                     as fecha_key,
    ubicacion_key,
    md5(servicio_etiqueta || '|' || subtipo_etiqueta)         as tipo_emergencia_key,
    periodo,
    sin_ubicacion,
    subtipo_no_disponible,
    ajuste_codigo
from {{ ref('int_emergencias_etiquetadas') }}
