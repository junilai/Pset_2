"""Construye la OBT cantón-día a partir de Gold y la guarda en Snowflake.

Ejecutar dentro del contenedor Spark con el conector y JDBC compatibles con
Spark 3.5 / Scala 2.12:

    /opt/spark/bin/spark-submit \
      --conf spark.jars.ivy=/tmp \
      --packages net.snowflake:spark-snowflake_2.12:3.2.2-spark_3.5,net.snowflake:snowflake-jdbc:4.1.0 \
      /app/spark/obt.py

FECHA representa el día t, una vez cerrado y disponible su conteo. La OBT
conserva incluso las fechas recientes para poder generar predicciones. Los
targets t+3/t+7 y el P90 por cantón y día de semana se calcularán después,
usando un corte de entrenamiento definido durante el modelado. Calcularlos
aquí con toda la historia filtraría información del período de evaluación.
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"
KEYS = ["FECHA", "PROVINCIA", "CANTON"]
OUTPUT_SCHEMA = "OBT"
OUTPUT_TABLE = "OBT_EMERGENCIAS_CANTON_DIA"


def read_table(spark, options, schema, table):
    return (
        spark.read.format(SNOWFLAKE_FORMAT)
        .options(**options)
        .option("sfSchema", schema)
        .option("dbtable", table)
        .load()
    )


def read_query(spark, options, schema, query):
    return (
        spark.read.format(SNOWFLAKE_FORMAT)
        .options(**options)
        .option("sfSchema", schema)
        .option("query", query)
        .load()
    )


def ensure_output_schema(spark, options):
    # Conectar sin schema por defecto: OBT puede no existir todavía.
    jvm = spark._jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = loader.loadClass("net.snowflake.client.jdbc.SnowflakeDriver").newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", options["sfUser"])
    properties.setProperty("password", options["sfPassword"])
    properties.setProperty("warehouse", options["sfWarehouse"])
    properties.setProperty("db", options["sfDatabase"])
    properties.setProperty("role", options["sfRole"])
    connection = driver.connect(
        f"jdbc:snowflake://{options['sfURL']}/", properties
    )
    try:
        statement = connection.createStatement()
        try:
            statement.executeUpdate(
                f"CREATE SCHEMA IF NOT EXISTS {options['sfDatabase']}.{OUTPUT_SCHEMA}"
            )
        finally:
            statement.close()
    finally:
        connection.close()


def assert_unique(df, keys, name):
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1)
    if duplicates.count():
        raise ValueError(f"{name} tiene claves duplicadas: {keys}")


def assert_no_nulls(df, columns, name):
    condition = None
    for column in columns:
        test = F.col(column).isNull()
        condition = test if condition is None else condition | test
    if df.filter(condition).limit(1).count():
        raise ValueError(f"{name} tiene valores nulos en {columns}")


def iso_weekday(date_column):
    # Spark: domingo=1; ISO: lunes=1 y domingo=7.
    return F.pmod(F.dayofweek(date_column) + F.lit(5), F.lit(7)) + F.lit(1)


def add_lag(df, window, days):
    previous_date = F.lag("FECHA", days).over(window)
    previous_count = F.lag("TOTAL_EMERGENCIAS", days).over(window)
    return df.withColumn(
        f"LAG_{days}",
        F.when(F.datediff(F.col("FECHA"), previous_date) == days, previous_count),
    )


def add_rolling_average(df, days):
    # DAY_INDEX permite usar días reales en vez de posiciones de filas. Si falta
    # algún día en el período, la media queda nula y no se inventa historia.
    window = (
        Window.partitionBy("PROVINCIA", "CANTON")
        .orderBy("DAY_INDEX")
        .rangeBetween(-days, -1)
    )
    return df.withColumn(
        f"PROMEDIO_{days}D",
        F.when(
            F.count("TOTAL_EMERGENCIAS").over(window) == days,
            F.avg("TOTAL_EMERGENCIAS").over(window),
        ),
    )


def main():
    spark = SparkSession.builder.appName("ecu911_obt").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        account = os.environ["SNOWFLAKE_ACCOUNT"]
        sf_url = (
            account
            if account.endswith(".snowflakecomputing.com")
            else f"{account}.snowflakecomputing.com"
        )
        options = {
            "sfURL": sf_url,
            "sfUser": os.environ["SNOWFLAKE_USER"],
            "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
            "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
            "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
            "sfRole": os.environ["SNOWFLAKE_ROLE"],
        }

        fact = read_table(
            spark, options, "GOLD", "FACT_EMERGENCIAS_CANTON_DIA"
        ).select(*KEYS, "TOTAL_EMERGENCIAS")
        dim_canton = read_table(
            spark, options, "GOLD", "DIM_CANTON"
        ).select("PROVINCIA", "CANTON", "POBLACION")
        dim_fecha = read_table(
            spark, options, "GOLD", "DIM_FECHA"
        ).select("FECHA", "ANIO", "MES", "DIA", "DIA_SEMANA", "ES_FIN_SEMANA")

        # Un mes se considera cubierto únicamente si Bronze contiene filas
        # identificadas con ese mes. Esto evita convertir meses no cargados en
        # una secuencia artificial de ceros.
        database = os.environ["SNOWFLAKE_DATABASE"]
        covered_months = read_query(
            spark,
            options,
            "BRONZE",
            f"SELECT DISTINCT _SOURCE_MONTH AS SOURCE_MONTH "
            f"FROM {database}.BRONZE.ECU911_EMERGENCIAS_RAW "
            "WHERE _SOURCE_MONTH IS NOT NULL",
        ).select("SOURCE_MONTH")

        if not fact.limit(1).count():
            raise ValueError("La tabla Gold de hechos está vacía")
        if not covered_months.limit(1).count():
            raise ValueError("No hay meses cargados en Bronze")

        assert_no_nulls(fact, KEYS + ["TOTAL_EMERGENCIAS"], "fact")
        assert_unique(fact, KEYS, "fact")
        assert_no_nulls(dim_canton, ["PROVINCIA", "CANTON"], "dim_canton")
        assert_unique(dim_canton, ["PROVINCIA", "CANTON"], "dim_canton")
        assert_no_nulls(dim_fecha, ["FECHA"], "dim_fecha")
        assert_unique(dim_fecha, ["FECHA"], "dim_fecha")
        assert_unique(covered_months, ["SOURCE_MONTH"], "meses Bronze")

        month_starts = covered_months.withColumn(
            "MONTH_START", F.to_date(F.concat(F.col("SOURCE_MONTH"), F.lit("-01")))
        )
        if month_starts.filter(
            F.col("MONTH_START").isNull()
            | (F.date_format("MONTH_START", "yyyy-MM") != F.col("SOURCE_MONTH"))
        ).limit(1).count():
            raise ValueError("Bronze contiene meses de origen inválidos")

        # Cada cantón tiene una fila por cada día de los meses cargados,
        # incluso antes de su primer incidente y después del último.
        covered_dates = month_starts.select(
            F.explode(
                F.sequence(F.col("MONTH_START"), F.last_day("MONTH_START"))
            ).alias("FECHA")
        )
        calendar = (
            dim_canton.select("PROVINCIA", "CANTON")
            .crossJoin(covered_dates)
        )

        uncovered_fact = fact.join(calendar, on=KEYS, how="left_anti").count()
        if uncovered_fact:
            raise ValueError(
                f"Hay {uncovered_fact} filas Gold fuera de los meses cubiertos en Bronze"
            )
        assert_unique(calendar, KEYS, "calendario cantonal")

        series = (
            calendar
            .join(fact, on=KEYS, how="left")
            .withColumn(
                "ES_CERO_COMPLETADO", F.col("TOTAL_EMERGENCIAS").isNull()
            )
            .withColumn(
                "TOTAL_EMERGENCIAS",
                F.coalesce(F.col("TOTAL_EMERGENCIAS"), F.lit(0)),
            )
            .join(dim_canton, on=["PROVINCIA", "CANTON"], how="left")
            .join(dim_fecha, on="FECHA", how="left")
        )

        if series.filter(F.col("POBLACION").isNull()).limit(1).count():
            raise ValueError("Hay cantones de la serie sin población en dim_canton")

        missing_dim_dates = series.filter(F.col("ANIO").isNull()).count()
        if missing_dim_dates:
            print(
                f"Aviso: {missing_dim_dates} fechas no están en dim_fecha; "
                "sus atributos se derivan de FECHA."
            )

        series = (
            series
            .withColumn("ANIO", F.coalesce(F.col("ANIO"), F.year("FECHA")))
            .withColumn("MES", F.coalesce(F.col("MES"), F.month("FECHA")))
            .withColumn("DIA", F.coalesce(F.col("DIA"), F.dayofmonth("FECHA")))
            .withColumn(
                "DIA_SEMANA",
                F.coalesce(F.col("DIA_SEMANA"), iso_weekday(F.col("FECHA"))),
            )
            .withColumn(
                "ES_FIN_SEMANA",
                F.coalesce(
                    F.col("ES_FIN_SEMANA"),
                    iso_weekday(F.col("FECHA")).isin(6, 7),
                ),
            )
            .withColumn(
                "DAY_INDEX", F.datediff(F.col("FECHA"), F.lit("1970-01-01"))
            )
        )

        window = Window.partitionBy("PROVINCIA", "CANTON").orderBy("FECHA")
        obt = series
        for days in (1, 7, 14, 21, 28):
            obt = add_lag(obt, window, days)
        for days in (7, 14, 28):
            obt = add_rolling_average(obt, days)
        same_weekday_lags = [F.col(f"LAG_{days}") for days in (7, 14, 21, 28)]
        complete_same_weekday = (
            same_weekday_lags[0].isNotNull()
            & same_weekday_lags[1].isNotNull()
            & same_weekday_lags[2].isNotNull()
            & same_weekday_lags[3].isNotNull()
        )
        obt = obt.withColumn(
            "PROMEDIO_4_MISMO_DIA",
            F.when(
                complete_same_weekday,
                sum(same_weekday_lags) / F.lit(4),
            ),
        )

        # Las fechas y los días de semana futuros son conocidos en t. Los
        # conteos futuros no se incorporan a las variables predictoras.
        for horizon in (3, 7):
            target_date = F.date_add(F.col("FECHA"), horizon)
            obt = (
                obt
                .withColumn(f"FECHA_OBJETIVO_{horizon}D", target_date)
                .withColumn(
                    f"DIA_SEMANA_OBJETIVO_{horizon}D",
                    iso_weekday(target_date),
                )
            )

        obt = obt.drop("DAY_INDEX").persist(StorageLevel.MEMORY_AND_DISK)
        fact_count = fact.count()
        calendar_count = calendar.count()
        obt_count = obt.count()

        if obt_count != calendar_count:
            raise ValueError(
                f"Los joins cambiaron el grano: calendario={calendar_count}, "
                f"OBT={obt_count}"
            )
        assert_unique(obt, KEYS, "OBT")
        missing_fact = fact.join(obt.select(*KEYS), on=KEYS, how="left_anti").count()
        if missing_fact:
            raise ValueError(f"La OBT perdió {missing_fact} filas de hechos")
        if obt.filter(F.col("TOTAL_EMERGENCIAS") < 0).limit(1).count():
            raise ValueError("La OBT contiene conteos negativos")

        print(f"Filas fact Gold: {fact_count}")
        print(f"Filas calendario cubierto: {calendar_count}")
        print(f"Filas OBT: {obt_count}")
        print(f"Días sin incidentes añadidos: {obt_count - fact_count}")

        ensure_output_schema(spark, options)
        (
            obt.write.format(SNOWFLAKE_FORMAT)
            .options(**options)
            .option("sfSchema", OUTPUT_SCHEMA)
            .option("dbtable", OUTPUT_TABLE)
            .mode("overwrite")
            .save()
        )

        saved = read_table(spark, options, OUTPUT_SCHEMA, OUTPUT_TABLE)
        if saved.count() != obt_count:
            raise ValueError("La cantidad de filas escritas en Snowflake no coincide")
        assert_unique(saved, KEYS, "OBT guardada en Snowflake")
        print(f"OBT validada en {database}.{OUTPUT_SCHEMA}.{OUTPUT_TABLE}")
        obt.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
