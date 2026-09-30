{#- Una fila por combinacion de servicio y subtipo: 103 subtipos aparecen en mas de un servicio. -#}
select
    md5(servicio_etiqueta || '|' || subtipo_etiqueta) as tipo_emergencia_key,
    servicio_etiqueta as servicio,
    subtipo_etiqueta  as subtipo
from {{ ref('int_emergencias_etiquetadas') }}
group by all
