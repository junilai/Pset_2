-- Dimensión servicio (institución que atiende). PK: servicio_id (0 = no especificado).
select servicio_id, servicio_nombre, servicio_slug
from {{ ref('servicios') }}
