select
    canton_ecu911 as canton,
    provincia_normalizada as provincia,
    poblacion

from {{ ref('silver_INEC') }}