{#- Con target 'prod' (Kestra, servicio local y el job de despliegue de dbt Cloud) usa el esquema configurado
    tal cual: SILVER, GOLD. En cualquier otro target (el IDE de desarrollo de dbt Cloud) lo antepone con el
    esquema personal, p. ej. DBT_JAROD_SILVER, para que desarrollar nunca sobrescriba las tablas publicadas. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
