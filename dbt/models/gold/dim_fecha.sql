-- Dimensión calendario. Grano: un día. PK: fecha_key (YYYYMMDD).
-- Incluye feriados nacionales (seed) y la marca de cobertura del mes (silver).
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'" ~ var('dim_fecha_inicio') ~ "'::date",
        end_date="dateadd(day, 1, '" ~ var('dim_fecha_fin') ~ "'::date)"
    ) }}
),

feriados as (
    select fecha, listagg(nombre_feriado, ' / ') within group (order by nombre_feriado) as nombre_feriado
    from {{ ref('feriados_ecuador') }}
    group by 1
),

dias as (
    select
        s.date_day::date                                  as fecha,
        f.nombre_feriado
    from spine s
    left join feriados f on f.fecha = s.date_day::date
)

select
    to_number(to_char(d.fecha, 'YYYYMMDD'))               as fecha_key,
    d.fecha,
    year(d.fecha)                                         as anio,
    quarter(d.fecha)                                      as trimestre,
    month(d.fecha)                                        as mes,
    decode(month(d.fecha),
        1, 'Enero', 2, 'Febrero', 3, 'Marzo', 4, 'Abril', 5, 'Mayo', 6, 'Junio',
        7, 'Julio', 8, 'Agosto', 9, 'Septiembre', 10, 'Octubre', 11, 'Noviembre', 12, 'Diciembre'
    )                                                     as nombre_mes,
    date_trunc('month', d.fecha)::date                    as inicio_mes,
    day(d.fecha)                                          as dia_mes,
    dayofweekiso(d.fecha)                                 as dia_semana,
    decode(dayofweekiso(d.fecha),
        1, 'Lunes', 2, 'Martes', 3, 'Miércoles', 4, 'Jueves', 5, 'Viernes', 6, 'Sábado', 7, 'Domingo'
    )                                                     as nombre_dia,
    weekiso(d.fecha)                                      as semana_iso,
    dayofweekiso(d.fecha) in (6, 7)                       as es_fin_de_semana,
    d.nombre_feriado is not null                          as es_feriado,
    d.nombre_feriado,
    coalesce(lead(d.nombre_feriado is not null) over (order by d.fecha), false)  as es_vispera_feriado,
    coalesce(lag(d.nombre_feriado is not null) over (order by d.fecha), false)   as es_post_feriado,
    coalesce(c.cobertura_sospechosa, false)               as mes_cobertura_sospechosa
from dias d
left join {{ ref('slv_cobertura_mensual') }} c
    on c.source_period = date_trunc('month', d.fecha)::date
