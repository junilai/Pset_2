-- Regla 6: despues de la correccion, la provincia y el canton de origen coinciden con los del codigo.
-- Devuelve las filas que la correccion no pudo resolver.
select e.id_emergencia, e.cod_parroquia, e.provincia_origen, e.canton_origen, d.provincia, d.canton
from {{ ref('emergencias') }} e
inner join {{ ref('dim_parroquia') }} d on d.cod_parroquia = e.cod_parroquia
where e.provincia_origen <> d.provincia
   or e.canton_origen <> d.canton
