-- Continuidad temporal: entre el primer y el último mes cargado no puede faltar ningún mes.
-- Un hueco rompería los lags de 1/7/14/28 días de la OBT.
with meses as (
    select distinct source_period from {{ ref('fct_emergencias') }}
),
rango as (
    select min(source_period) as desde, max(source_period) as hasta from meses
),
secuencia as (
    select row_number() over (order by seq4()) - 1 as n
    from table(generator(rowcount => 240))
),
esperados as (
    select dateadd(month, s.n, r.desde)::date as source_period
    from secuencia s
    cross join rango r
    where dateadd(month, s.n, r.desde)::date <= r.hasta
)
select e.source_period as mes_faltante
from esperados e
left join meses m on m.source_period = e.source_period
where m.source_period is null
