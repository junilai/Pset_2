{#- Usa el esquema configurado tal cual (SILVER, GOLD) en lugar del prefijo por defecto de dbt (SILVER_GOLD). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
