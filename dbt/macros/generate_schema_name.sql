-- Por defecto dbt crea "<schema_del_perfil>_<schema_custom>" (ej. SILVER_GOLD).
-- Queremos exactamente los schemas creados en la Fase 3: SILVER, GOLD.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim | upper }}
{%- endmacro %}
