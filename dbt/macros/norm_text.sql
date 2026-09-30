{#- Estandariza texto libre: sin tildes (conserva la Ñ), mayúsculas,
    sin espacios al inicio/fin y sin espacios repetidos. -#}
{% macro norm_text(expr) -%}
    nullif(upper(trim(regexp_replace(
        translate({{ expr }}, 'áéíóúüÁÉÍÓÚÜ', 'aeiouuAEIOUU'),
        '\s+', ' '))), '')
{%- endmacro %}
