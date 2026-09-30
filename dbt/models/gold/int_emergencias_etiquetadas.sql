{{ config(materialized='ephemeral') }}
{#- Etiquetas de Gold sobre Silver: reemplaza los NULL por valores explicitos para que toda fila tenga clave. -#}
select
    e.*,
    coalesce(e.servicio, 'NO INFORMADO')             as servicio_etiqueta,
    case
        when e.subtipo_no_disponible then 'NO DISPONIBLE'
        else coalesce(e.subtipo, 'NO INFORMADO')
    end                                              as subtipo_etiqueta,
    case
        when e.sin_ubicacion then '000000'
        when e.cod_parroquia is not null then e.cod_parroquia
        else c.cod_canton || '00'
    end                                              as ubicacion_key
from {{ ref('emergencias') }} e
left join (
    select distinct provincia, canton, cod_canton from {{ ref('dim_parroquia') }}
) c
    on e.cod_parroquia is null
   and c.provincia = e.provincia
   and c.canton = e.canton
