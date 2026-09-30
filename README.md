# PSet #2 — Pipeline ELT ECU 911 (alta demanda por cantón)

Tubería ELT reproducible que alimenta el proyecto del PSet #1: **anticipar días de demanda
inusualmente alta de emergencias por cantón a t+3 y t+7**.

```
Datos Abiertos (ECU 911) → Kestra → Bronze → dbt → Silver → dbt → Gold → Spark → OBT
```

- **Fuente:** [ECU911 Base de Emergencias](https://www.datosabiertos.gob.ec/dataset/base-de-emergencias),
  CSV mensuales, **enero 2023 – marzo 2026** (39 meses, ~10 M de emergencias).
- **Complementos:** proyecciones de población cantonal INEC (Censo 2022, rev. 2024) y feriados
  nacionales de Ecuador (seeds de dbt).
- **Destino:** Snowflake `PSET2_DB` (schemas `BRONZE`, `REFERENCE`, `SILVER`, `GOLD`, `OBT`).

## Estructura

```
pset2/
├── docker-compose.yml        Kestra + Postgres + Spark master/worker (+ servicio cli)
├── Dockerfile                Imagen de Kestra con dbt, PySpark 3.5.3, Java 17 y conector Spark-Snowflake
├── .env.example              Variables (copiar a .env)
├── snowflake/00_setup.sql    Rol, warehouse, base y usuario de servicio (una vez)
├── kestra/
│   ├── flows/                ecu911_ingest · ecu911_backfill · ecu911_transform
│   └── scripts/              resolve_resource.py · load_bronze.py · month_range.py
├── dbt/                      Silver, Gold (star schema), seeds y tests
├── spark/                    build_obt.py (OBT cantón-día) · submit_obt.sh
├── scripts/                  run_pipeline.sh (CLI) · generadores de seeds
└── docs/                     Arquitectura, modelo dimensional, calidad de datos, decisiones
```

## Documentación

| Documento | Contenido |
|---|---|
| [docs/arquitectura.md](docs/arquitectura.md) | Diagrama de infraestructura, flujo de datos, trigger, retries y backfill |
| [docs/modelo_dimensional.md](docs/modelo_dimensional.md) | Star schema (diagrama), grano, PK/FK, OBT y su validación |
| [docs/calidad_datos.md](docs/calidad_datos.md) | Problema / Evidencia / Acción / Justificación |
| [docs/decisiones.md](docs/decisiones.md) | Batch vs. streaming, decisiones y limitaciones |
| [docs/consultas_calidad.sql](docs/consultas_calidad.sql) | Consultas para las métricas de calidad sobre el histórico completo |

## Cómo ejecutar

Requisitos: Docker Desktop y una cuenta de Snowflake con acceso a ACCOUNTADMIN (sirve una trial).

### 1. Credenciales (una vez)

```bash
# Llave RSA del usuario de servicio (Git Bash / Linux / macOS)
mkdir -p keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
```

1. Reemplazar `rsa_public_key` en `snowflake/00_setup.sql` por el contenido de `keys/rsa_key.pub`
   (sin las líneas `-----BEGIN/END-----`, en una sola línea).
2. En Snowsight, rol ACCOUNTADMIN, ejecutar `snowflake/00_setup.sql` (Run All).
3. `cp .env.example .env` y poner en `SNOWFLAKE_ACCOUNT` el valor que devuelve la última consulta del setup.

`keys/` y `.env` están en `.gitignore`: **no se suben credenciales al repositorio**.

### 2. Levantar la infraestructura

```bash
docker compose up -d --build
```

- Kestra: http://localhost:8080 (la primera vez pide crear un usuario administrador local).
- Spark UI: http://localhost:8081

Los flows se cargan solos desde `kestra/flows/` (namespace `usfq.pset2`).

### 3. Ejecutar la tubería con Kestra

| Qué | Cómo |
|---|---|
| **Backfill histórico** (primera vez) | Flows → `ecu911_backfill` → *Execute* (por defecto 2023-01 a 2026-03). Carga los 39 meses (4 en paralelo) y al final ejecuta dbt + Spark una vez. |
| **Un mes puntual** | `ecu911_ingest` → *Execute* con `period = YYYY-MM`. |
| **Carga mensual automática** | Trigger `monthly` de `ecu911_ingest` (día 20 de cada mes, carga el mes anterior). |
| **Backfill nativo de Kestra** | `ecu911_ingest` → Triggers → `monthly` → *Backfill executions* (p. ej. 2023-02-20 → 2026-04-20 = un mes por ejecución). |
| **Solo transformar** | `ecu911_transform` → *Execute* (dbt build + Spark). |

### 4. Ejecutar por CLI (sin la UI de Kestra)

```bash
docker compose run --rm cli                      # todo el rango del .env
docker compose run --rm cli 2024-01 2024-03      # un rango
```

Por partes, dentro del contenedor:

```bash
docker compose exec kestra bash -c 'cd /pset2/dbt && $DBT_BIN deps && $DBT_BIN build'   # dbt
docker compose exec kestra bash /pset2/spark/submit_obt.sh                              # Spark
```

## Qué queda en Snowflake

| Schema | Tablas | Cargado por |
|---|---|---|
| `BRONZE` | `EMERGENCIAS` (dato original + metadata), `LOAD_AUDIT` | Kestra |
| `REFERENCE` | `SERVICIOS`, `POBLACION_CANTONAL`, `FERIADOS_ECUADOR` | dbt seeds |
| `SILVER` | `SLV_EMERGENCIAS`, `SLV_EMERGENCIAS_RECHAZADAS`, `SLV_COBERTURA_MENSUAL` | dbt |
| `GOLD` | `FCT_EMERGENCIAS`, `DIM_FECHA`, `DIM_CANTON`, `DIM_PARROQUIA`, `DIM_SERVICIO`, `DIM_TIPO_EMERGENCIA` | dbt |
| `OBT` | `OBT_CANTON_DIA`, `OBT_VALIDACIONES` | Spark |

## Reproducir los seeds

Los seeds ya están versionados; se regeneran desde la fuente oficial con:

```bash
pip install openpyxl holidays
python scripts/build_population_seed.py     # descarga Cantonal.zip del INEC
python scripts/build_holidays_seed.py
```
