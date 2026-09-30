{#- Opcion A (Kestra y servicio local: target 'prod' con esquema SILVER, ver profiles.yml) escribe en SILVER y GOLD.
    Cualquier otro entorno antepone su propio esquema: el job de dbt Cloud (esquema CLOUD) escribe en CLOUD_SILVER
    y CLOUD_GOLD, y el IDE en <esquema personal>_SILVER. Exigir ademas el esquema SILVER evita que un job de dbt
    Cloud con target 'prod' sobrescriba las tablas de la opcion A. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' and target.schema | upper == 'SILVER' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
