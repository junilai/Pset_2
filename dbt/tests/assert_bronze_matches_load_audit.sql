-- Completitud de la ingesta: las filas de cada mes en bronze coinciden con las filas
-- contadas en el archivo original durante la carga (última carga de cada período).
with ultima_carga as (
    select source_period, rows_in_file
    from {{ source('bronze', 'load_audit') }}
    where status = 'LOADED'
    qualify row_number() over (partition by source_period order by loaded_at desc) = 1
),
bronze as (
    select _source_period as source_period, count(*) as filas
    from {{ source('bronze', 'emergencias') }}
    group by 1
)
select u.source_period, u.rows_in_file, coalesce(b.filas, 0) as filas_bronze
from ultima_carga u
left join bronze b on b.source_period = u.source_period
where u.rows_in_file <> coalesce(b.filas, 0)
