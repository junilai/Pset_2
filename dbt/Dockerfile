# Imagen de dbt con versiones fijas. Kestra la usa en transformar_silver y el servicio dbt del compose
# para desarrollo local. Sin ENTRYPOINT: Kestra ejecuta los comandos con /bin/sh -c.
FROM python:3.12-slim

# git: lo exige dbt debug y dbt deps con paquetes alojados en git
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==1.12.5 dbt-snowflake==1.12.1

ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /dbt
