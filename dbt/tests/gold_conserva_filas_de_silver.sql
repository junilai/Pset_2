-- La tabla de hechos tiene exactamente las filas de SILVER.EMERGENCIAS, periodo por periodo.
with silver as (
    select periodo, count(*) as filas from {{ ref('emergencias') }} group by all
),

gold as (
    select periodo, count(*) as filas from {{ ref('fct_emergencias') }} group by all
)

select coalesce(s.periodo, g.periodo) as periodo, s.filas as filas_silver, g.filas as filas_gold
from silver s
full outer join gold g on g.periodo = s.periodo
where s.filas is distinct from g.filas
