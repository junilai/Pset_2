"""Construye la One Big Table de emergencias desnormalizando el modelo estrella de GOLD.

Lee GOLD.FCT_EMERGENCIAS y sus tres dimensiones con el conector de Snowflake, hace los joins en el
cluster de Spark y escribe el resultado en GOLD.OBT_EMERGENCIAS_NUEVA. Publicar la tabla definitiva
(GOLD.OBT_EMERGENCIAS) le corresponde al flow construir_obt, despues de validar el conteo.

Las credenciales llegan por variables de entorno SNOWFLAKE_*, igual que en dbt.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FORMATO_SNOWFLAKE = "net.snowflake.spark.snowflake"
TABLA_DESTINO = "OBT_EMERGENCIAS_NUEVA"


def opciones_snowflake():
    faltantes = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                             "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_ROLE")
                 if not os.environ.get(v)]
    if faltantes:
        sys.exit(f"Faltan variables de entorno: {faltantes}")
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfRole": os.environ["SNOWFLAKE_ROLE"],
        "sfSchema": "GOLD",
        # Sin pushdown el conector solo lee tablas: los joins los ejecuta Spark, no Snowflake.
        "autopushdown": "off",
    }


def leer(spark, opciones, tabla):
    return spark.read.format(FORMATO_SNOWFLAKE).options(**opciones).option("dbtable", tabla).load()


def construir_obt(spark, opciones):
    hechos = leer(spark, opciones, "FCT_EMERGENCIAS")
    fechas = leer(spark, opciones, "DIM_FECHA").drop("PERIODO")
    ubicaciones = leer(spark, opciones, "DIM_UBICACION").withColumnRenamed("NIVEL", "NIVEL_UBICACION")
    tipos = leer(spark, opciones, "DIM_TIPO_EMERGENCIA")

    # Las dimensiones tienen menos de 2,000 filas: broadcast evita redistribuir los 17 millones de hechos.
    return (
        hechos
        .join(F.broadcast(fechas), "FECHA_KEY", "left")
        .join(F.broadcast(ubicaciones), "UBICACION_KEY", "left")
        .join(F.broadcast(tipos), "TIPO_EMERGENCIA_KEY", "left")
        .select(
            "ID_EMERGENCIA",
            "PERIODO",
            "FECHA", "ANIO", "TRIMESTRE", "MES", "NOMBRE_MES", "DIA",
            "DIA_SEMANA", "NOMBRE_DIA", "ES_FIN_DE_SEMANA", "SEMANA_ISO",
            "COD_PROVINCIA", "PROVINCIA", "COD_CANTON", "CANTON", "COD_PARROQUIA", "PARROQUIA",
            "NIVEL_UBICACION",
            "SERVICIO", "SUBTIPO",
            "SIN_UBICACION", "SUBTIPO_NO_DISPONIBLE", "AJUSTE_CODIGO",
        )
    )


def main():
    spark = SparkSession.builder.appName("obt_emergencias").getOrCreate()
    opciones = opciones_snowflake()

    (
        construir_obt(spark, opciones)
        .write.format(FORMATO_SNOWFLAKE)
        .options(**opciones)
        .option("dbtable", TABLA_DESTINO)
        .mode("overwrite")
        .save()
    )
    print(f"OBT escrita en GOLD.{TABLA_DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
