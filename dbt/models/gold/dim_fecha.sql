select distinct
    fecha,
    year(fecha) as anio,
    month(fecha) as mes,
    day(fecha) as dia,
    dayofweekiso(fecha) as dia_semana,
    case
        when dayofweekiso(fecha) in (6, 7) then true
        else false
    end as es_fin_semana

from {{ ref('silver_ecu911_emergencias') }}