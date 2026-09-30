{#- Un dia por fila entre el primer y el ultimo mes con datos; incluye dias sin emergencias. -#}
with limites as (
    select
        date_trunc('month', min(fecha)) as desde,
        last_day(max(fecha))            as hasta
    from {{ ref('emergencias') }}
),

dias as (
    select dateadd(day, row_number() over (order by seq4()) - 1, l.desde) as fecha
    from table(generator(rowcount => 10000))
    cross join limites l
    qualify fecha <= max(l.hasta) over ()
)

select
    to_number(to_char(fecha, 'YYYYMMDD'))          as fecha_key,
    fecha,
    year(fecha)                                    as anio,
    quarter(fecha)                                 as trimestre,
    month(fecha)                                   as mes,
    decode(month(fecha),
        1, 'Enero', 2, 'Febrero', 3, 'Marzo', 4, 'Abril', 5, 'Mayo', 6, 'Junio',
        7, 'Julio', 8, 'Agosto', 9, 'Septiembre', 10, 'Octubre', 11, 'Noviembre', 12, 'Diciembre'
    )                                              as nombre_mes,
    to_char(fecha, 'YYYYMM')                       as periodo,
    day(fecha)                                     as dia,
    -- ISO: 1 = lunes ... 7 = domingo, sin depender del parametro WEEK_START de la sesion
    dayofweekiso(fecha)                            as dia_semana,
    decode(dayofweekiso(fecha),
        1, 'Lunes', 2, 'Martes', 3, 'Miércoles', 4, 'Jueves', 5, 'Viernes', 6, 'Sábado', 7, 'Domingo'
    )                                              as nombre_dia,
    dayofweekiso(fecha) in (6, 7)                  as es_fin_de_semana,
    weekiso(fecha)                                 as semana_iso
from dias
