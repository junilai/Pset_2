"""
OBT cantón-día para el modelo de alta demanda del ECU 911 (PSet #1).

Grano: un cantón x un día (panel completo: los días sin emergencias quedan en 0).
Fuente: tablas Gold en Snowflake (fct_emergencias + dim_fecha + dim_canton + dim_servicio).
Destino: PSET2_DB.OBT.OBT_CANTON_DIA (overwrite) y OBT.OBT_VALIDACIONES (append).

Columnas para el modelo:
  - volumen del día total y por servicio (N_*), tasa por 100 mil habitantes;
  - lags 1/7/14/28 días y medias móviles 7/28 días (solo con días anteriores);
  - calendario, feriados y marca de mes con cobertura sospechosa;
  - volúmenes futuros N_T_MAS_3 / N_T_MAS_7 para construir el target.
El target (volumen > P90 del cantón y día de la semana) NO se calcula aquí: el PSet #1
exige calcular el percentil solo con el período de entrenamiento, así que se arma en la
etapa de modelado para evitar fuga de información.

Validaciones (si alguna falla, la OBT anterior no se reemplaza):
  1. filas = n_cantones x n_dias           (el panel no perdió ni duplicó combinaciones)
  2. llave (cantón, día) única             (los joins no duplicaron filas)
  3. suma de N_EMERGENCIAS = filas del hecho en el rango (la agregación no perdió emergencias)
  4. llaves sin nulos
"""
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SF = "net.snowflake.spark.snowflake"
LAGS = [1, 7, 14, 28]
ROLLING = [7, 28]
LEADS = [3, 7]
YEARS = [2022, 2023, 2024, 2025, 2026]


def sf_options(schema):
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]) as fh:
        pem = "".join(line for line in fh.read().splitlines() if "-----" not in line)
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "pem_private_key": pem,
        "sfRole": os.environ["SNOWFLAKE_ROLE"],
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfSchema": schema,
    }


def main():
    spark = SparkSession.builder.appName("ecu911_obt").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def read(table):
        return spark.read.format(SF).options(**sf_options("GOLD")).option("dbtable", table).load()

    fct = read("FCT_EMERGENCIAS").select("CANTON_CODIGO", "FECHA_KEY", "SERVICIO_ID", "CANTIDAD")
    dim_fecha = read("DIM_FECHA")
    dim_canton = read("DIM_CANTON")
    servicios = {r.SERVICIO_ID: r.SERVICIO_SLUG.upper() for r in read("DIM_SERVICIO").collect()}

    # Rango del panel: del primer al último día con datos
    b = fct.agg(F.min("FECHA_KEY").alias("desde"), F.max("FECHA_KEY").alias("hasta"),
                F.sum("CANTIDAD").alias("total")).first()
    fechas = dim_fecha.where(F.col("FECHA_KEY").between(b.desde, b.hasta))
    cantones = dim_canton.select("CANTON_CODIGO")
    n_cantones, n_dias = cantones.count(), fechas.count()
    print(f"Panel: {n_cantones} cantones x {n_dias} días ({b.desde}..{b.hasta}); {b.total:,} emergencias")

    # 1. Volumen diario por cantón y servicio (una columna por servicio)
    diario = (fct.groupBy("CANTON_CODIGO", "FECHA_KEY")
                 .pivot("SERVICIO_ID", sorted(servicios))
                 .agg(F.sum("CANTIDAD")))
    for sid, slug in servicios.items():
        diario = diario.withColumnRenamed(str(sid), f"N_{slug}")
    cols_servicio = [f"N_{slug}" for _, slug in sorted(servicios.items())]

    # 2. Panel completo cantón x día: sin esto, los días sin emergencias no existirían
    #    y los lags / el percentil del target saldrían sesgados.
    obt = (cantones.crossJoin(fechas.select("FECHA_KEY"))
                   .join(diario, ["CANTON_CODIGO", "FECHA_KEY"], "left")
                   .fillna(0, subset=cols_servicio)
                   .withColumn("N_EMERGENCIAS", sum(F.col(c) for c in cols_servicio)))

    # 3. Atributos de calendario y cantón (joins 1:1 por PK de cada dimensión)
    obt = obt.join(fechas.select(
        "FECHA_KEY", "FECHA", "ANIO", "MES", "DIA_SEMANA", "NOMBRE_DIA", "SEMANA_ISO",
        "ES_FIN_DE_SEMANA", "ES_FERIADO", "NOMBRE_FERIADO", "ES_VISPERA_FERIADO",
        "ES_POST_FERIADO", "MES_COBERTURA_SOSPECHOSA"), "FECHA_KEY", "inner")
    obt = obt.join(dim_canton.select(
        "CANTON_CODIGO", "CANTON_NOMBRE", "PROVINCIA_CODIGO", "PROVINCIA_NOMBRE",
        "ES_ZONA_NO_DELIMITADA", *[f"POBLACION_{y}" for y in YEARS]), "CANTON_CODIGO", "inner")

    poblacion = F.coalesce(*[F.when(F.col("ANIO") == y, F.col(f"POBLACION_{y}")) for y in YEARS])
    obt = (obt.withColumn("POBLACION", poblacion)
              .drop(*[f"POBLACION_{y}" for y in YEARS])
              .withColumn("TASA_100K", F.when(F.col("POBLACION") > 0,
                                             F.round(F.col("N_EMERGENCIAS") * 100000 / F.col("POBLACION"), 3))))

    # 4. Variables temporales por cantón (solo información disponible al día t)
    w = Window.partitionBy("CANTON_CODIGO").orderBy("FECHA")
    for k in LAGS:
        obt = obt.withColumn(f"N_LAG_{k}", F.lag("N_EMERGENCIAS", k).over(w))
    for k in ROLLING:
        ventana = w.rowsBetween(-k, -1)
        obt = obt.withColumn(
            f"N_MEDIA_{k}D",
            F.when(F.count("N_EMERGENCIAS").over(ventana) == k,
                   F.round(F.avg("N_EMERGENCIAS").over(ventana), 3)))
    # Volúmenes futuros (insumo del target t+3 / t+7; NULL al final del panel)
    for h in LEADS:
        obt = obt.withColumn(f"N_T_MAS_{h}", F.lead("N_EMERGENCIAS", h).over(w))

    obt = obt.withColumn("OBT_CONSTRUIDA_EN", F.current_timestamp()).cache()

    # 5. Validaciones de grano y de joins
    filas = obt.count()
    llaves = obt.select("CANTON_CODIGO", "FECHA_KEY").distinct().count()
    suma = obt.agg(F.sum("N_EMERGENCIAS")).first()[0]
    nulos = obt.where(F.col("CANTON_CODIGO").isNull() | F.col("FECHA_KEY").isNull()).count()
    checks = [
        ("filas = cantones x dias", n_cantones * n_dias, filas),
        ("llave canton-dia unica", filas, llaves),
        ("suma n_emergencias = filas de fct_emergencias", int(b.total), int(suma)),
        ("llaves sin nulos", 0, nulos),
    ]
    for nombre, esperado, obtenido in checks:
        print(f"[{'OK' if esperado == obtenido else 'FALLA'}] {nombre}: esperado={esperado:,} obtenido={obtenido:,}")

    resultado = spark.createDataFrame(
        [(n, int(e), int(o), e == o) for n, e, o in checks],
        "CHECK_NAME string, ESPERADO long, OBTENIDO long, OK boolean",
    ).withColumn("RUN_AT", F.current_timestamp())
    resultado.write.format(SF).options(**sf_options("OBT")).option("dbtable", "OBT_VALIDACIONES") \
        .mode("append").save()

    if any(e != o for _, e, o in checks):
        sys.exit("Validaciones de la OBT fallaron: no se reemplaza OBT_CANTON_DIA.")

    orden = ["CANTON_CODIGO", "FECHA", "FECHA_KEY", "CANTON_NOMBRE", "PROVINCIA_CODIGO", "PROVINCIA_NOMBRE"]
    obt = obt.select(*orden, *[c for c in obt.columns if c not in orden])
    obt.write.format(SF).options(**sf_options("OBT")).option("dbtable", "OBT_CANTON_DIA") \
        .mode("overwrite").save()
    print(f"OBT.OBT_CANTON_DIA escrita: {filas:,} filas, {len(obt.columns)} columnas")
    spark.stop()


if __name__ == "__main__":
    main()
