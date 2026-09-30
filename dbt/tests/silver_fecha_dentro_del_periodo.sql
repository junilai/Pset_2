-- Cada fecha debe caer en el mes de su periodo.
select id_emergencia, periodo, fecha
from {{ ref('emergencias') }}
where to_char(fecha, 'YYYYMM') <> periodo
