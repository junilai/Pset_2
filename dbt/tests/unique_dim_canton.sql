select
    provincia,
    canton,
    count(*) as cantidad
from {{ ref('dim_canton') }}
group by
    provincia,
    canton
having count(*) > 1