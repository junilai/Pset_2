# syntax=docker/dockerfile:1.7
# Spark 4.1.3 con el conector de Snowflake incluido en la imagen, para no descargarlo en cada spark-submit.
# spark-snowflake 3.2.2-spark_4.1 esta compilado contra Spark 4.1 y depende de snowflake-jdbc 4.0.2.
# Los checksums SHA-256 se calcularon sobre los JAR de Maven Central tras verificar su SHA-1 publicado.
FROM apache/spark:4.1.3-python3

ADD --chmod=644 \
    --checksum=sha256:7a4ba6eaa66a63aa23b486feefce0f1e6281467f7e6e71b4972136a67aac02ef \
    https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.13/3.2.2-spark_4.1/spark-snowflake_2.13-3.2.2-spark_4.1.jar \
    /opt/spark/jars/

ADD --chmod=644 \
    --checksum=sha256:8447eff0cf04cd327ab6b6f642a3a21ac2c1c2f2ce442953a11f5cf2b0fcd4bb \
    https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/4.0.2/snowflake-jdbc-4.0.2.jar \
    /opt/spark/jars/
