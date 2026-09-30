{{ config(materialized='table') }}

-- SILVER: control de completitud por mes.
-- La fuente no avisa si un mes se publicó incompleto. Se compara el volumen de cada
-- mes (total y por servicio) contra la mediana de todos los meses. Un mes queda como
-- "cobertura_sospechosa" si su total cae bajo el 80 % de la mediana o si algún servicio
-- relevante (mediana >= 1000/mes) cae bajo el 30 % de su mediana.
-- Importa para el modelo: un mes incompleto distorsiona el percentil 90 del target.
with conteos as (
    select source_period, servicio_id, count(*) as n
    from {{ ref('slv_emergencias') }}
    group by 1, 2
),

periodos as (select distinct source_period from conteos),

grilla as (
    -- Todos los meses x todos los servicios: un servicio ausente cuenta como 0
    select p.source_period, s.servicio_id, s.servicio_nombre, coalesce(c.n, 0) as n
    from periodos p
    cross join {{ ref('servicios') }} s
    left join conteos c
        on c.source_period = p.source_period and c.servicio_id = s.servicio_id
),

por_servicio as (
    select
        g.*,
        median(n) over (partition by servicio_id) as mediana_servicio
    from grilla g
),

totales as (
    select source_period, sum(n) as total
    from grilla
    group by 1
),

mediana_total as (
    select median(total) as mediana from totales
),

resumen as (
    select
        s.source_period,
        min(case when s.mediana_servicio >= {{ var('cobertura_servicio_mediana_min') }}
                 then s.n / s.mediana_servicio end)                          as ratio_min_servicio,
        listagg(case when s.mediana_servicio >= {{ var('cobertura_servicio_mediana_min') }}
                      and s.n / s.mediana_servicio < {{ var('cobertura_ratio_servicio_min') }}
                     then s.servicio_nombre end, ', ')
            within group (order by s.servicio_nombre)                       as servicios_bajo_umbral
    from por_servicio s
    group by 1
)

select
    t.source_period,
    t.total                                                                  as total_emergencias,
    round(t.total / m.mediana, 3)                                            as ratio_total,
    round(r.ratio_min_servicio, 3)                                           as ratio_min_servicio,
    nullif(r.servicios_bajo_umbral, '')                                      as servicios_bajo_umbral,
    (t.total / m.mediana < {{ var('cobertura_ratio_total_min') }}
     or r.ratio_min_servicio < {{ var('cobertura_ratio_servicio_min') }})    as cobertura_sospechosa
from totales t
cross join mediana_total m
join resumen r on r.source_period = t.source_period
