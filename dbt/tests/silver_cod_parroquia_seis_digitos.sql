-- Regla 2: todo codigo presente tiene exactamente 6 digitos.
select id_emergencia, cod_parroquia
from {{ ref('emergencias') }}
where cod_parroquia is not null
  and not cod_parroquia rlike '[0-9]{6}'
