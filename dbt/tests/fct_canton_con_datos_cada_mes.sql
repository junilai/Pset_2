-- Regla: todo cantón registra al menos 1 emergencia al mes (el más pequeño tiene ~9/mes).
-- Un cantón-mes en 0 casi siempre es un cambio de código o de límites en la fuente,
-- no ausencia real de emergencias (así apareció SEVILLA DON BOSCO, hallazgo #11).
select cod_canton, to_char(fecha, 'YYYY-MM') as periodo, sum(n_emergencias) as emergencias
from {{ ref('fct_emergencias_canton_dia') }}
group by 1, 2
having sum(n_emergencias) = 0
