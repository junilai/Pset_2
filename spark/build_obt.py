"""Construye con PySpark la malla diaria y la OBT de alta demanda del ECU 911."""

from __future__ import annotations

import os
from functools import reduce
from operator import add

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
TRAINING_CUTOFF = os.getenv("TRAINING_CUTOFF", "2024-12-31")
VALIDATION_CUTOFF = os.getenv("VALIDATION_CUTOFF", "2025-12-31")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno obligatoria {name}")
    return value


def snowflake_url(account: str) -> str:
    clean = account.removeprefix("https://").removeprefix("http://").rstrip("/")
    if not clean.endswith(".snowflakecomputing.com"):
        clean = f"{clean}.snowflakecomputing.com"
    return clean


def lowercase_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[column.lower() for column in df.columns])


def read_snowflake_table(
    spark: SparkSession, options: dict[str, str], table: str
) -> DataFrame:
    return lowercase_columns(
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**options)
        .option("dbtable", table)
        .load()
    )


def write_snowflake_table(
    df: DataFrame, options: dict[str, str], table: str
) -> None:
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**options)
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )


def assert_zero_rows(df: DataFrame, message: str) -> None:
    if df.limit(1).count() != 0:
        raise RuntimeError(message)


def split_for_target(target_column: str) -> F.Column:
    target = F.col(target_column)
    return (
        F.when(target > F.col("last_event_date"), F.lit("SCORING"))
        .when(target <= F.lit(TRAINING_CUTOFF).cast("date"), F.lit("TRAIN"))
        .when(
            target <= F.lit(VALIDATION_CUTOFF).cast("date"),
            F.lit("VALIDATION"),
        )
        .otherwise(F.lit("TEST"))
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("ecu911-build-obt")
        .config("spark.sql.session.timeZone", "America/Guayaquil")
        .config("spark.sql.crossJoin.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    account = required_env("SNOWFLAKE_ACCOUNT")
    sf_options = {
        "sfURL": snowflake_url(account),
        "sfUser": required_env("SNOWFLAKE_USER"),
        "sfPassword": required_env("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "PSET2_DB"),
        "sfSchema": "GOLD",
        "sfWarehouse": required_env("SNOWFLAKE_WAREHOUSE"),
        "sfRole": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }

    print("[1/7] Leyendo hechos diarios y dimensión de cantón desde Snowflake...")
    facts = (
        read_snowflake_table(spark, sf_options, "FCT_CANTON_DAILY")
        .withColumn("event_date", F.to_date("event_date"))
        .filter(F.col("canton_key") != "UNKNOWN")
    )
    cantons = (
        read_snowflake_table(spark, sf_options, "DIM_CANTON")
        .filter(F.col("is_known_canton") == F.lit(True))
        .select(
            "canton_key",
            "province_code",
            "provincia_name",
            "canton_name",
            F.to_date("first_event_date").alias("first_event_date"),
            F.to_date("last_event_date").alias("last_event_date"),
            "is_model_eligible",
        )
    )

    service_columns = [
        "security_incidents",
        "health_incidents",
        "transit_incidents",
        "municipal_incidents",
        "incident_service_incidents",
        "military_incidents",
        "risk_incidents",
        "other_incidents",
    ]
    service_total = reduce(add, [F.col(column) for column in service_columns])
    assert_zero_rows(
        facts.filter(
            (F.col("total_incidents") < 0)
            | (F.col("total_incidents") != service_total)
        ),
        "FCT_CANTON_DAILY contiene totales negativos o no reconciliados.",
    )

    bounds = facts.agg(
        F.min("event_date").alias("min_date"),
        F.max("event_date").alias("max_date"),
    ).first()
    if bounds.min_date is None or bounds.max_date is None:
        raise RuntimeError("FCT_CANTON_DAILY está vacía.")

    print("[2/7] Creando la malla completa fecha × cantón...")
    dates = spark.range(1).select(
        F.explode(
            F.sequence(
                F.lit(bounds.min_date),
                F.lit(bounds.max_date),
                F.expr("interval 1 day"),
            )
        ).alias("calendar_date")
    )
    grid = F.broadcast(dates).crossJoin(F.broadcast(cantons))

    metric_columns = [
        "total_incidents",
        *service_columns,
        "active_emergency_types",
        "compact_group_count",
        "duplicate_rows_collapsed",
        "repaired_date_incidents",
        "invalid_incidents",
        "warning_incidents",
    ]
    observed = facts.select(
        F.col("event_date").alias("calendar_date"),
        "canton_key",
        *metric_columns,
    )
    complete = (
        grid.join(observed, ["calendar_date", "canton_key"], "left")
        .withColumn("is_observed_day", F.col("total_incidents").isNotNull())
        .fillna(0, subset=metric_columns)
        .withColumn(
            "is_within_canton_observed_span",
            F.col("calendar_date").between(
                F.col("first_event_date"), F.col("last_event_date")
            ),
        )
        .withColumn(
            "date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int")
        )
        .withColumn(
            "day_of_week_iso",
            ((F.dayofweek("calendar_date") + F.lit(5)) % F.lit(7)) + F.lit(1),
        )
        .withColumn("month_number", F.month("calendar_date"))
        .withColumn("quarter_number", F.quarter("calendar_date"))
        .withColumn("is_weekend", F.col("day_of_week_iso").isin(6, 7))
        .cache()
    )

    date_count = (bounds.max_date - bounds.min_date).days + 1
    canton_count = cantons.count()
    expected_grid_rows = date_count * canton_count
    complete_count = complete.count()
    if complete_count != expected_grid_rows:
        raise RuntimeError(
            f"Malla incompleta: {complete_count:,} filas; "
            f"se esperaban {expected_grid_rows:,}."
        )
    if complete.select("calendar_date", "canton_key").distinct().count() != complete_count:
        raise RuntimeError("La malla contiene más de una fila por fecha y cantón.")
    assert_zero_rows(
        complete.filter(F.col("total_incidents").isNull()),
        "La malla conserva valores nulos en TOTAL_INCIDENTS.",
    )

    print("[3/7] Calculando P90 exclusivamente con el periodo de entrenamiento...")
    thresholds = (
        complete.filter(
            (F.col("calendar_date") <= F.lit(TRAINING_CUTOFF).cast("date"))
            & F.col("is_within_canton_observed_span")
        )
        .groupBy("canton_key", "day_of_week_iso")
        .agg(
            F.percentile_approx("total_incidents", 0.90, 10000).alias(
                "p90_incidents"
            ),
            F.count(F.lit(1)).alias("training_calendar_days"),
            F.min("calendar_date").alias("threshold_start_date"),
            F.max("calendar_date").alias("threshold_end_date"),
        )
        .cache()
    )

    print("[4/7] Calculando lags, promedios móviles y valores objetivo...")
    canton_window = Window.partitionBy("canton_key").orderBy("calendar_date")
    features = complete
    for lag_days in [1, 4, 7, 11, 14, 18, 21, 25, 28]:
        features = features.withColumn(
            f"incidents_lag_{lag_days}",
            F.lag("total_incidents", lag_days).over(canton_window),
        )

    features = (
        features.withColumn(
            "incidents_avg_prior_7d",
            F.avg("total_incidents").over(canton_window.rowsBetween(-7, -1)),
        )
        .withColumn(
            "incidents_avg_prior_28d",
            F.avg("total_incidents").over(canton_window.rowsBetween(-28, -1)),
        )
        .withColumn(
            "incidents_stddev_prior_28d",
            F.stddev_samp("total_incidents").over(
                canton_window.rowsBetween(-28, -1)
            ),
        )
    )
    for service in ["security", "health", "transit", "municipal"]:
        source_column = f"{service}_incidents"
        features = features.withColumn(
            f"{service}_lag_1", F.lag(source_column, 1).over(canton_window)
        ).withColumn(
            f"{service}_lag_7", F.lag(source_column, 7).over(canton_window)
        )

    features = (
        features.withColumn(
            "raw_actual_total_t3", F.lead("total_incidents", 3).over(canton_window)
        )
        .withColumn(
            "raw_actual_total_t7", F.lead("total_incidents", 7).over(canton_window)
        )
        .withColumn(
            "target_t3_within_observed_span",
            F.lead("is_within_canton_observed_span", 3).over(canton_window),
        )
        .withColumn(
            "target_t7_within_observed_span",
            F.lead("is_within_canton_observed_span", 7).over(canton_window),
        )
        .withColumn("as_of_date", F.col("calendar_date"))
        .withColumn("as_of_date_key", F.col("date_key"))
        .withColumn("target_date_t3", F.date_add("calendar_date", 3))
        .withColumn("target_date_t7", F.date_add("calendar_date", 7))
        .withColumn(
            "target_date_key_t3",
            F.date_format("target_date_t3", "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "target_date_key_t7",
            F.date_format("target_date_t7", "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "target_day_of_week_t3",
            ((F.dayofweek("target_date_t3") + F.lit(5)) % F.lit(7)) + F.lit(1),
        )
        .withColumn(
            "target_day_of_week_t7",
            ((F.dayofweek("target_date_t7") + F.lit(5)) % F.lit(7)) + F.lit(1),
        )
        .withColumn(
            "actual_total_t3",
            F.when(
                F.col("target_t3_within_observed_span"),
                F.col("raw_actual_total_t3"),
            ),
        )
        .withColumn(
            "actual_total_t7",
            F.when(
                F.col("target_t7_within_observed_span"),
                F.col("raw_actual_total_t7"),
            ),
        )
        .withColumn(
            "baseline_same_weekday_t3",
            F.when(
                F.col("incidents_lag_25").isNotNull(),
                (
                    F.col("incidents_lag_4")
                    + F.col("incidents_lag_11")
                    + F.col("incidents_lag_18")
                    + F.col("incidents_lag_25")
                )
                / F.lit(4.0),
            ),
        )
        .withColumn(
            "baseline_same_weekday_t7",
            F.when(
                F.col("incidents_lag_21").isNotNull(),
                (
                    F.col("total_incidents")
                    + F.col("incidents_lag_7")
                    + F.col("incidents_lag_14")
                    + F.col("incidents_lag_21")
                )
                / F.lit(4.0),
            ),
        )
        .withColumn(
            "is_feature_ready",
            F.col("is_within_canton_observed_span")
            & (F.date_add("calendar_date", -28) >= F.col("first_event_date")),
        )
    )

    t3_thresholds = thresholds.select(
        F.col("canton_key").alias("t3_canton_key"),
        F.col("day_of_week_iso").alias("t3_day_of_week"),
        F.col("p90_incidents").alias("p90_threshold_t3"),
    )
    t7_thresholds = thresholds.select(
        F.col("canton_key").alias("t7_canton_key"),
        F.col("day_of_week_iso").alias("t7_day_of_week"),
        F.col("p90_incidents").alias("p90_threshold_t7"),
    )
    features = (
        features.join(
            F.broadcast(t3_thresholds),
            (F.col("canton_key") == F.col("t3_canton_key"))
            & (F.col("target_day_of_week_t3") == F.col("t3_day_of_week")),
            "left",
        )
        .drop("t3_canton_key", "t3_day_of_week")
        .join(
            F.broadcast(t7_thresholds),
            (F.col("canton_key") == F.col("t7_canton_key"))
            & (F.col("target_day_of_week_t7") == F.col("t7_day_of_week")),
            "left",
        )
        .drop("t7_canton_key", "t7_day_of_week")
    )

    print("[5/7] Creando etiquetas, baseline y particiones temporales...")
    obt = (
        features.withColumn(
            "is_high_demand_t3",
            F.when(
                F.col("actual_total_t3").isNotNull()
                & F.col("p90_threshold_t3").isNotNull(),
                F.col("actual_total_t3") > F.col("p90_threshold_t3"),
            ),
        )
        .withColumn(
            "is_high_demand_t7",
            F.when(
                F.col("actual_total_t7").isNotNull()
                & F.col("p90_threshold_t7").isNotNull(),
                F.col("actual_total_t7") > F.col("p90_threshold_t7"),
            ),
        )
        .withColumn(
            "baseline_alert_t3",
            F.when(
                F.col("baseline_same_weekday_t3").isNotNull()
                & F.col("p90_threshold_t3").isNotNull(),
                F.col("baseline_same_weekday_t3") > F.col("p90_threshold_t3"),
            ),
        )
        .withColumn(
            "baseline_alert_t7",
            F.when(
                F.col("baseline_same_weekday_t7").isNotNull()
                & F.col("p90_threshold_t7").isNotNull(),
                F.col("baseline_same_weekday_t7") > F.col("p90_threshold_t7"),
            ),
        )
        .withColumn("split_t3", split_for_target("target_date_t3"))
        .withColumn("split_t7", split_for_target("target_date_t7"))
        .select(
            "as_of_date",
            "as_of_date_key",
            "canton_key",
            "province_code",
            "provincia_name",
            "canton_name",
            "is_model_eligible",
            "is_feature_ready",
            F.col("day_of_week_iso").alias("as_of_day_of_week"),
            F.col("month_number").alias("as_of_month"),
            F.col("quarter_number").alias("as_of_quarter"),
            F.col("is_weekend").alias("as_of_is_weekend"),
            F.col("total_incidents").alias("as_of_total_incidents"),
            "incidents_lag_1",
            "incidents_lag_7",
            "incidents_lag_14",
            "incidents_lag_28",
            "incidents_avg_prior_7d",
            "incidents_avg_prior_28d",
            "incidents_stddev_prior_28d",
            (
                F.col("incidents_avg_prior_7d")
                - F.col("incidents_avg_prior_28d")
            ).alias("incidents_trend_7d_vs_28d"),
            "security_lag_1",
            "security_lag_7",
            "health_lag_1",
            "health_lag_7",
            "transit_lag_1",
            "transit_lag_7",
            "municipal_lag_1",
            "municipal_lag_7",
            "target_date_t3",
            "target_date_key_t3",
            "target_day_of_week_t3",
            "actual_total_t3",
            "p90_threshold_t3",
            "is_high_demand_t3",
            "baseline_same_weekday_t3",
            "baseline_alert_t3",
            "split_t3",
            "target_date_t7",
            "target_date_key_t7",
            "target_day_of_week_t7",
            "actual_total_t7",
            "p90_threshold_t7",
            "is_high_demand_t7",
            "baseline_same_weekday_t7",
            "baseline_alert_t7",
            "split_t7",
        )
        .cache()
    )

    obt_count = obt.count()
    if obt_count != complete_count:
        raise RuntimeError(
            f"La OBT alteró el grano: {obt_count:,} filas frente a "
            f"{complete_count:,} de la malla."
        )
    if obt.select("as_of_date", "canton_key").distinct().count() != obt_count:
        raise RuntimeError("La OBT no es única por AS_OF_DATE y CANTON_KEY.")

    print("[6/7] Escribiendo resultados Spark en el esquema GOLD...")
    write_snowflake_table(complete, sf_options, "CANTON_DAILY_COMPLETE_SPARK")
    write_snowflake_table(thresholds, sf_options, "HIGH_DEMAND_THRESHOLDS_SPARK")
    write_snowflake_table(obt, sf_options, "OBT_CANTON_HIGH_DEMAND_SPARK")

    print("[7/7] Proceso completado correctamente.")
    print(
        f"Cantones: {canton_count:,} | Fechas: {date_count:,} | "
        f"Filas OBT: {obt_count:,}"
    )
    print(
        "Grano validado: una fila por AS_OF_DATE × CANTON_KEY; "
        "los días ausentes tienen TOTAL_INCIDENTS = 0."
    )

    obt.unpersist()
    thresholds.unpersist()
    complete.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
