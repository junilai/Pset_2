{#- H2 / reglas 3 y 5: la falta de ubicacion llega como vacio, '0' (202107) o el texto 'NULL' (202108).
    Ademas corrige la Ñ mal codificada como Ð (canton LOGROÐO en 57 periodos; la Ð no existe en espanol). -#}
{% macro ubicacion_o_nulo(columna) -%}
    iff(upper(trim({{ columna }})) in ('', '0', 'NULL'), null, replace(trim({{ columna }}), 'Ð', 'Ñ'))
{%- endmacro %}
