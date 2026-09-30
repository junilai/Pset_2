-- Validez temporal: cada emergencia del hecho cae dentro del mes de su archivo.
select emergencia_key, fecha, source_period
from {{ ref('fct_emergencias') }}
where date_trunc('month', fecha)::date <> source_period
