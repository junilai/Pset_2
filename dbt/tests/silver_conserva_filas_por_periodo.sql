-- Regla 1: Silver no pierde ni duplica emergencias. Devuelve los periodos cuyo conteo difiere de Bronze.
with bronze as (
    select _periodo as periodo, count(*) as filas
    from {{ source('bronze', 'emergencias') }}
    group by all
),

silver as (
    select periodo, count(*) as filas
    from {{ ref('emergencias') }}
    group by all
)

select
    coalesce(b.periodo, s.periodo) as periodo,
    b.filas as filas_bronze,
    s.filas as filas_silver
from bronze b
full outer join silver s on s.periodo = b.periodo
where b.filas is distinct from s.filas
