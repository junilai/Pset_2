-- Dimensión tipo de emergencia = servicio + subtipo asignado por el operador.
-- El mismo subtipo puede aparecer en varios servicios, por eso la PK combina ambos.
select distinct
    md5(e.servicio_id || '|' || e.subtipo)   as tipo_emergencia_key,
    e.servicio_id,
    s.servicio_nombre,
    e.subtipo
from {{ ref('slv_emergencias') }} e
join {{ ref('servicios') }} s on s.servicio_id = e.servicio_id
