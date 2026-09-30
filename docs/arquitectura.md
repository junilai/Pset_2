# Arquitectura

## Diagrama de infraestructura

```mermaid
flowchart LR
    subgraph Fuente["Datos Abiertos Ecuador"]
        API["API CKAN<br/>package_show"]
        CSV["CSV mensuales<br/>ECU 911"]
    end

    subgraph Docker["docker-compose (local)"]
        subgraph K["Kestra + Postgres"]
            F1["ecu911_ingest<br/>(mensual / backfill)"]
            F2["ecu911_transform"]
            F3["ecu911_backfill"]
            DBT["dbt Core<br/>(en la imagen de Kestra)"]
            DRV["Spark driver<br/>(spark-submit)"]
        end
        SM["spark-master"]
        SW["spark-worker"]
    end

    subgraph SF["Snowflake · PSET2_DB"]
        B["BRONZE<br/>EMERGENCIAS · LOAD_AUDIT"]
        R["REFERENCE<br/>seeds"]
        S["SILVER<br/>emergencias · rechazadas · cobertura"]
        G["GOLD<br/>fct_emergencias + 5 dims"]
        O["OBT<br/>OBT_CANTON_DIA"]
    end

    API -- "URL del mes" --> F1
    CSV -- "descarga (retries)" --> F1
    F1 -- "PUT + COPY<br/>transaccional" --> B
    F3 -- "subflow x mes" --> F1
    F1 -- "subflow" --> F2
    F3 -- "subflow" --> F2
    F2 --> DBT
    DBT -- "source()" --> B
    DBT --> R
    DBT --> S
    DBT -- "ref()" --> G
    F2 --> DRV
    DRV <--> SM
    SM <--> SW
    DRV -- "lee Gold" --> G
    DRV -- "escribe OBT" --> O
```

## Flujo de datos

`Fuente → Kestra → Bronze → dbt → Silver → dbt → Gold → Spark → OBT`

| Paso | Componente | Qué hace |
|---|---|---|
| 1 | Kestra `ecu911_ingest` · `resolve_resource` | Consulta la API CKAN del portal y obtiene la URL del CSV del mes (las URLs tienen IDs aleatorios, no se pueden construir). |
| 2 | Kestra · `download` | Descarga el CSV (~35 MB) con reintentos exponenciales. |
| 3 | Kestra · `load_bronze` | Detecta la codificación (UTF-8 o CP1252), transcodifica a UTF-8, hace `PUT` al stage y en una transacción `DELETE` del período + `COPY INTO BRONZE.EMERGENCIAS`. Valida filas cargadas = filas del archivo y registra en `LOAD_AUDIT`. |
| 4 | dbt · Silver | Tipos, estandarización de textos, catálogo de servicios, códigos DPA, cuarentena de inválidos, control de cobertura mensual. |
| 5 | dbt · Gold | Esquema estrella: `fct_emergencias` + `dim_fecha`, `dim_canton`, `dim_parroquia`, `dim_servicio`, `dim_tipo_emergencia`. Tests. |
| 6 | Spark · `build_obt.py` | Lee Gold, construye el panel cantón × día con lags y calendario, valida grano y joins, escribe `OBT.OBT_CANTON_DIA`. |

## Orquestación

| Flow | Disparo | Qué ejecuta |
|---|---|---|
| `ecu911_ingest` | Trigger `Schedule` mensual (día 20, 07:00 America/Guayaquil) o manual con `period` | Un mes → Bronze. Al final llama a `ecu911_transform` (si `run_transform = true`). |
| `ecu911_backfill` | Manual | `ForEach` sobre los meses del rango (4 en paralelo) → subflow `ecu911_ingest` con `run_transform = false`; luego una sola vez `ecu911_transform`. |
| `ecu911_transform` | Subflow | `dbt deps` → `dbt build` → `spark-submit`. `concurrency: 1` en cola. |

**Retries y errores:** `resolve_resource` (3 intentos, 1 min), `download` (4 intentos con backoff exponencial 30 s → 5 min), `load_bronze` (3 intentos, 1 min), `dbt_build` y `spark_obt` (2 intentos). Cada flow tiene un bloque `errors` que registra el período y la ejecución fallidos. Como la carga de un mes es transaccional, un fallo nunca deja Bronze a medias.

**Backfill:** dos mecanismos. (1) El flow `ecu911_backfill` (recomendado: controla el paralelismo y transforma una sola vez). (2) El backfill nativo del trigger `monthly` de `ecu911_ingest` en la UI de Kestra: el período se calcula como el mes anterior a la fecha programada, así que un backfill del 2023-02-20 al 2026-04-20 genera una ejecución por mes de 2023-01 a 2026-03.

**Idempotencia:** cada mes se reemplaza completo en Bronze (`DELETE` + `COPY` en transacción); Silver y el hecho son incrementales `delete+insert` por `source_period`; las dimensiones y la OBT se reconstruyen (`table` / `overwrite`).
