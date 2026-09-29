select
    fecha,
    provincia_normalizada as provincia,
    canton_normalizado as canton,
    count(*) as total_emergencias

from {{ ref('silver_ecu911_emergencias') }}

where provincia_normalizada <> 'ZONA NO DELIMITADA'
  and canton_normalizado <> 'SEVILLA DON BOSCO'

group by
    fecha,
    provincia_normalizada,
    canton_normalizado