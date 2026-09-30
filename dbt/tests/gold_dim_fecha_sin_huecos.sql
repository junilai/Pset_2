-- dim_fecha cubre todos los dias entre su primera y su ultima fecha, sin huecos.
select min(fecha) as desde, max(fecha) as hasta, count(*) as dias
from {{ ref('dim_fecha') }}
having count(*) <> datediff(day, min(fecha), max(fecha)) + 1
