-- Dimensión cantón (unidad de predicción del proyecto). PK: canton_codigo (DPA INEC).
-- Incluye los 221 cantones del INEC aunque no tengan emergencias registradas
-- (la OBT necesita el panel completo) y las zonas no delimitadas (código 90xx) que
-- aparecen en los datos. Población: proyecciones INEC (Censo 2022, revisión 2024).
with observados as (
    select canton_codigo, canton_nombre, count(*) as n
    from {{ ref('slv_emergencias') }}
    group by 1, 2
),

nombre_canton as (
    -- La fuente escribe el mismo cantón de varias formas: se usa la más frecuente
    select canton_codigo, canton_nombre
    from observados
    qualify row_number() over (partition by canton_codigo order by n desc, canton_nombre) = 1
),

nombre_provincia as (
    select provincia_codigo, provincia_nombre
    from (
        select provincia_codigo, provincia_nombre, count(*) as n
        from {{ ref('slv_emergencias') }}
        group by 1, 2
    )
    qualify row_number() over (partition by provincia_codigo order by n desc, provincia_nombre) = 1
),

poblacion as (
    select
        canton_codigo,
        max(canton_nombre_inec)                                as canton_nombre_inec,
        max(case when anio = 2022 then poblacion end)          as poblacion_2022,
        max(case when anio = 2023 then poblacion end)          as poblacion_2023,
        max(case when anio = 2024 then poblacion end)          as poblacion_2024,
        max(case when anio = 2025 then poblacion end)          as poblacion_2025,
        max(case when anio = 2026 then poblacion end)          as poblacion_2026
    from {{ ref('poblacion_cantonal') }}
    group by 1
),

codigos as (
    select canton_codigo from nombre_canton
    union
    select canton_codigo from poblacion
)

select
    c.canton_codigo,
    coalesce(n.canton_nombre, {{ norm_text('p.canton_nombre_inec') }})  as canton_nombre,
    p.canton_nombre_inec,
    left(c.canton_codigo, 2)                                         as provincia_codigo,
    pr.provincia_nombre,
    left(c.canton_codigo, 2) = '90'                                  as es_zona_no_delimitada,
    n.canton_codigo is not null                                      as tiene_emergencias,
    p.poblacion_2022,
    p.poblacion_2023,
    p.poblacion_2024,
    p.poblacion_2025,
    p.poblacion_2026
from codigos c
left join nombre_canton n on n.canton_codigo = c.canton_codigo
left join poblacion p on p.canton_codigo = c.canton_codigo
left join nombre_provincia pr on pr.provincia_codigo = left(c.canton_codigo, 2)
