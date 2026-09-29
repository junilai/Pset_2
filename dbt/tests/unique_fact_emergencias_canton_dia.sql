select
    fecha,
    provincia,
    canton,
    count(*) as cantidad

from {{ ref('fact_emergencias_canton_dia') }}

group by
    fecha,
    provincia,
    canton

having count(*) > 1