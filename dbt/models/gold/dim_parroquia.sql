-- Dimensión parroquia (nivel geográfico más fino de la fuente). PK: parroquia_codigo.
-- Jerarquía DPA: parroquia (6 dígitos) -> cantón (4) -> provincia (2).
with observadas as (
    select parroquia_codigo, parroquia_nombre, count(*) as n
    from {{ ref('slv_emergencias') }}
    group by 1, 2
)

select
    o.parroquia_codigo,
    o.parroquia_nombre,
    left(o.parroquia_codigo, 4)                    as canton_codigo,
    c.canton_nombre,
    left(o.parroquia_codigo, 2)                    as provincia_codigo,
    c.provincia_nombre,
    o.parroquia_nombre like '%CABECERA CANTONAL%'  as es_cabecera_cantonal
from observadas o
left join {{ ref('dim_canton') }} c on c.canton_codigo = left(o.parroquia_codigo, 4)
qualify row_number() over (partition by o.parroquia_codigo order by o.n desc, o.parroquia_nombre) = 1
