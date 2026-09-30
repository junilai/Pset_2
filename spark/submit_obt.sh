#!/usr/bin/env bash
# Envía build_obt.py al cluster Spark (spark-master + spark-worker del docker-compose).
# El driver corre en este contenedor (Kestra o cli) y los executors en spark-worker.
set -euo pipefail

export JAVA_HOME="${SPARK_JAVA_HOME}"                 # Java 17 (Spark 3.5)
export PYSPARK_DRIVER_PYTHON=/opt/spark-venv/bin/python
export PYSPARK_PYTHON=python3                          # executors (el job no usa UDFs)
DRIVER_HOST=$(hostname -i | awk '{print $1}')          # IP alcanzable desde el worker

"${SPARK_SUBMIT}" \
  --master "${SPARK_MASTER_URL}" \
  --name ecu911_obt \
  --jars "${SPARK_JARS}" \
  --conf spark.driver.host="${DRIVER_HOST}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  --conf spark.sql.session.timeZone=America/Guayaquil \
  /pset2/spark/build_obt.py
