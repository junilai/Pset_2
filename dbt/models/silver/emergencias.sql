{#- Una fila por emergencia de BRONZE.EMERGENCIAS (regla 1: no se deduplica). Reglas en docs/perfilado.md. -#}
with stg as (
    select * from {{ ref('stg_emergencias') }}
),

dim as (
    select * from {{ ref('dim_parroquia') }}
),

codigo_por_nombre as (
    -- Regla 6: codigo mas frecuente para cada combinacion de nombres; corrige o completa el codigo de la fila
    select provincia, canton, parroquia, cod_parroquia
    from stg
    where cod_parroquia is not null
      and provincia is not null
      and canton is not null
      and parroquia is not null
    group by all
    qualify row_number() over (partition by provincia, canton, parroquia order by count(*) desc, cod_parroquia) = 1
),

subtipo_canonico as (
    -- Regla 8: una sola grafia por subtipo, la mas frecuente
    select upper(subtipo) as clave, subtipo
    from stg
    where subtipo is not null
    group by all
    qualify row_number() over (partition by upper(subtipo) order by count(*) desc, subtipo) = 1
),

con_referencias as (
    select
        s.*,
        d.provincia      as provincia_del_codigo,
        d.canton         as canton_del_codigo,
        n.cod_parroquia  as cod_por_nombre
    from stg s
    left join dim d
        on d.cod_parroquia = s.cod_parroquia
    left join codigo_por_nombre n
        on n.provincia = s.provincia
       and n.canton = s.canton
       and n.parroquia = s.parroquia
),

ajustado as (
    select
        *,
        case
            when cod_parroquia is null and cod_por_nombre is not null
                then 'INFERIDO'
            when (provincia_del_codigo <> provincia or canton_del_codigo <> canton)
                 and cod_por_nombre <> cod_parroquia
                then 'CORREGIDO'
            -- El codigo contradice la provincia y los nombres no permiten corregirlo (p. ej. 202201: filas de
            -- Morona Santiago con parroquia y codigo de Guayaquil). Se conservan provincia y canton, se anula la parroquia.
            when provincia_del_codigo <> provincia or canton_del_codigo <> canton
                then 'ANULADO'
        end as ajuste_codigo
    from con_referencias
),

final as (
    select
        *,
        case ajuste_codigo
            when 'ANULADO' then null
            when 'INFERIDO' then cod_por_nombre
            when 'CORREGIDO' then cod_por_nombre
            else cod_parroquia
        end as cod_parroquia_final
    from ajustado
)

select
    f.id_emergencia,
    f.periodo,
    f.fecha,
    f.cod_parroquia_final                    as cod_parroquia,
    coalesce(d.provincia, f.provincia)       as provincia,
    coalesce(d.canton, f.canton)             as canton,
    iff(f.ajuste_codigo = 'ANULADO', null, coalesce(d.parroquia, f.parroquia)) as parroquia,
    f.servicio,
    sc.subtipo,
    f.sin_ubicacion,
    f.subtipo_no_disponible,
    f.ajuste_codigo,
    f.cod_parroquia                          as cod_parroquia_origen,
    f.provincia                              as provincia_origen,
    f.canton                                 as canton_origen,
    f.parroquia                              as parroquia_origen,
    f.fila_archivo,
    f.resource_id,
    f.archivo_origen,
    f.cargado_en
from final f
left join dim d
    on d.cod_parroquia = f.cod_parroquia_final
left join subtipo_canonico sc
    on sc.clave = upper(f.subtipo)
