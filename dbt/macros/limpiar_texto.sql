-- Limpieza común de columnas de texto (Fase 6):
--   #10 TRIM de espacios sobrantes
--   #5  'Ð' -> 'Ñ' (error de codificación de la fuente: LOGROÐO -> LOGROÑO)
--   #1  '', 'NULL' (texto) y '0' se convierten en NULL real
{% macro limpiar_texto(columna) -%}
    nullif(nullif(nullif(replace(trim({{ columna }}), 'Ð', 'Ñ'), ''), 'NULL'), '0')
{%- endmacro %}
