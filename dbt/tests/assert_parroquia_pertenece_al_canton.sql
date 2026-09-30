-- Consistencia geográfica (DPA INEC): el cantón de cada emergencia debe ser el
-- prefijo de 4 dígitos de su parroquia. Garantiza que agregar por cantón sea correcto.
select emergencia_key, parroquia_codigo, canton_codigo
from {{ ref('fct_emergencias') }}
where left(parroquia_codigo, 4) <> canton_codigo
