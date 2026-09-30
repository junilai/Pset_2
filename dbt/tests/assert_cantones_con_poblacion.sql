{{ config(severity='warn') }}
-- Aviso (no bloquea): cantones con emergencias que no tienen población INEC.
-- Esperado solo para zonas no delimitadas (90xx); cualquier otro indica un código DPA
-- desconocido y la tasa por 100 mil habitantes de la OBT quedaría vacía.
select canton_codigo, canton_nombre
from {{ ref('dim_canton') }}
where tiene_emergencias
  and poblacion_2025 is null
  and not es_zona_no_delimitada
