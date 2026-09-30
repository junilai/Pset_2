-- Conciliación: cada fila de bronze termina en silver o en cuarentena.
-- Nada se pierde ni se duplica en la limpieza (no se deduplica a propósito).
with b as (
    select _source_period as p, count(*) as n from {{ source('bronze', 'emergencias') }} group by 1
),
s as (
    select source_period as p, count(*) as n from {{ ref('slv_emergencias') }} group by 1
),
r as (
    select source_period as p, count(*) as n from {{ ref('slv_emergencias_rechazadas') }} group by 1
)
select b.p, b.n as bronze_rows, coalesce(s.n, 0) as silver_rows, coalesce(r.n, 0) as rechazadas
from b
left join s on s.p = b.p
left join r on r.p = b.p
where b.n <> coalesce(s.n, 0) + coalesce(r.n, 0)
