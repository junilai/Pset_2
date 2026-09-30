{#- Regla 6: catalogo codigo -> provincia, canton y nombre, con la asociacion mas frecuente de todos los periodos. -#}
with conteos as (
    select cod_parroquia, provincia, canton, parroquia, count(*) as filas
    from {{ ref('stg_emergencias') }}
    where cod_parroquia is not null
      and provincia is not null
      and canton is not null
      and parroquia is not null
    group by all
),

ubicacion as (
    select cod_parroquia, provincia, canton, sum(filas) as filas_ubicacion
    from conteos
    group by all
    qualify row_number() over (partition by cod_parroquia order by sum(filas) desc, provincia, canton) = 1
),

nombre as (
    -- H6: el mismo lugar aparece truncado o con puntuacion distinta; ante empate gana el nombre mas largo
    select c.cod_parroquia, c.parroquia
    from conteos c
    inner join ubicacion u
        on u.cod_parroquia = c.cod_parroquia
       and u.provincia = c.provincia
       and u.canton = c.canton
    group by all
    qualify row_number() over (
        partition by c.cod_parroquia
        order by sum(c.filas) desc, length(c.parroquia) desc, c.parroquia
    ) = 1
),

totales as (
    select cod_parroquia, sum(filas) as filas_total
    from conteos
    group by all
)

select
    u.cod_parroquia,
    left(u.cod_parroquia, 2)                     as cod_provincia,
    left(u.cod_parroquia, 4)                     as cod_canton,
    u.provincia,
    u.canton,
    n.parroquia,
    t.filas_total,
    -- Que tan dominante es la asociacion elegida; < 1 indica filas con otra ubicacion para el mismo codigo (H4)
    round(u.filas_ubicacion / t.filas_total, 6)  as proporcion_asociacion
from ubicacion u
inner join nombre n on n.cod_parroquia = u.cod_parroquia
inner join totales t on t.cod_parroquia = u.cod_parroquia
