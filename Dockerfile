# Imagen de Kestra + herramientas de la tubería ECU 911:
#   /opt/pipeline   venv (Python 3.12) con dbt-snowflake y el conector de Snowflake
#   /opt/spark-venv venv (Python 3.11) con PySpark 3.5.3 (misma versión que el cluster)
#   Java 17 para el driver de Spark (Kestra usa su propio Java 21)
#   Jars del conector Spark <-> Snowflake en /opt/spark-jars
# Kestra ejecuta todo con el task runner "Process": no necesita el socket de Docker.
FROM kestra/kestra:v1.3.37

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-venv openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN /usr/bin/python3 -m venv /opt/pipeline \
 && /opt/pipeline/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements.txt

ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python
RUN /opt/pipeline/bin/uv python install 3.11 \
 && /opt/pipeline/bin/uv venv --python 3.11 /opt/spark-venv \
 && /opt/pipeline/bin/uv pip install --python /opt/spark-venv/bin/python pyspark==3.5.3

ARG MAVEN=https://repo1.maven.org/maven2/net/snowflake
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/spark-snowflake.jar \
      $MAVEN/spark-snowflake_2.12/3.2.2-spark_3.5/spark-snowflake_2.12-3.2.2-spark_3.5.jar \
 && curl -fsSL -o /opt/spark-jars/snowflake-jdbc.jar \
      $MAVEN/snowflake-jdbc/4.0.2/snowflake-jdbc-4.0.2.jar

ENV PIPELINE_PY=/opt/pipeline/bin/python \
    DBT_BIN=/opt/pipeline/bin/dbt \
    SPARK_SUBMIT=/opt/spark-venv/bin/spark-submit \
    SPARK_JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_JARS=/opt/spark-jars/spark-snowflake.jar,/opt/spark-jars/snowflake-jdbc.jar
