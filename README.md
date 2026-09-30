# PSet 2 — Pipeline ELT de emergencias del ECU 911

Pipeline ELT con la Base de Emergencias del ECU 911 publicada en [Datos Abiertos Ecuador](https://www.datosabiertos.gob.ec/dataset/base-de-emergencias). Los datos se cargan en Snowflake, se orquestan con Kestra, se transforman con dbt y terminan en una One Big Table construida en Spark.

```
CKAN (CSV mensuales)
   │  Kestra: ingesta_ecu911 / backfill_ecu911   descarga → validación → stage → COPY INTO
   ▼
BRONZE.EMERGENCIAS            datos tal como se publican, todo VARCHAR + linaje
   │  Kestra: perfilar_bronze                     100 métricas de calidad por periodo
   │  dbt:    transformar_silver                  reglas de limpieza (docs/perfilado.md)
   ▼
SILVER.EMERGENCIAS, SILVER.DIM_PARROQUIA
   │  dbt:    transformar_gold                    modelo estrella
   ▼
GOLD.FCT_EMERGENCIAS + DIM_FECHA, DIM_UBICACION, DIM_TIPO_EMERGENCIA
   │  Spark:  construir_obt                       joins en el cluster, conciliación, publicación
   ▼
GOLD.OBT_EMERGENCIAS
```

## Estructura

| Ruta | Contenido |
|---|---|
| `docker-compose.yml` | Kestra 2.0.4, Postgres 18.6 (base interna de Kestra), cluster Spark 4.1.3 (master + 1 worker) y servicio `dbt` para desarrollo local |
| `kestra/flows/pset2/` | Flows de Kestra (uno por archivo) |
| `dbt/` | Proyecto dbt (`staging`, `silver`, `gold`), imagen `pset2-dbt` y `profiles.yml` sin credenciales |
| `spark/` | Job de la OBT e imagen `pset2-spark` con el conector de Snowflake |
| `docs/perfilado.md` | Hallazgos de calidad de los 60 periodos y reglas aplicadas en Silver |

## Requisitos

- Docker Desktop con al menos **5 GB de memoria** (Settings → Resources).
- Una cuenta de Snowflake con la base de datos y los esquemas creados:

  ```sql
  CREATE DATABASE IF NOT EXISTS ECU911;
  CREATE SCHEMA IF NOT EXISTS ECU911.BRONZE;
  CREATE SCHEMA IF NOT EXISTS ECU911.SILVER;
  CREATE SCHEMA IF NOT EXISTS ECU911.GOLD;
  ```

## Puesta en marcha

Todos los comandos se ejecutan **desde la raíz del repo**: el compose calcula con `${PWD}` las rutas de `./dbt` y `./spark` que montan los contenedores de las tareas.

1. **Credenciales.** Copia `.env.example` a `.env` y complétalo. `.env` está en `.gitignore`. Compose pasa estas variables a Kestra con el prefijo `ENV_`, el único que Kestra expone a los flows como `{{ envs.* }}`.

2. **Imágenes propias.** Kestra las usa con `pullPolicy: NEVER`, así que tienen que existir antes de ejecutar los flows:

   ```bash
   docker compose build spark-master          # pset2-spark:4.1.3
   docker compose --profile tools build dbt   # pset2-dbt:1.12.1
   ```

3. **Levantar el stack.**

   ```bash
   docker compose up -d
   ```

   | Servicio | URL |
   |---|---|
   | Kestra | http://localhost:8080 |
   | Spark master | http://localhost:8090 |
   | Spark worker | http://localhost:8091 |

4. **Crear los flows.** En Kestra, Flows → Create, pega el contenido de cada archivo de `kestra/flows/pset2/`. Kestra no los lee del repo, así que después de modificar un archivo hay que volver a pegarlo.

## Orden de ejecución

| # | Flow | Qué hace | Resultado esperado |
|---|---|---|---|
| 1 | `test_snowflake` | Prueba la conexión | SUCCESS |
| 2 | `setup_snowflake` | Crea el file format, el stage, `BRONZE.EMERGENCIAS` y `BRONZE.PERFIL_CALIDAD`. Se puede repetir sin perder datos | 1 file format, 1 stage, 13 y 7 columnas |
| 3 | `backfill_ecu911` | Ejecuta `ingesta_ecu911` y `perfilar_bronze` para cada periodo CSV del rango (por defecto, todos) | 60 periodos, **17,120,516 filas** (unos 25 min) |
| 4 | `transformar_silver` | `dbt build` de `staging` y `silver` | PASS=22 (3 modelos + 19 tests) |
| 5 | `transformar_gold` | `dbt build` de `gold` | PASS=23 (4 modelos + 19 tests) |
| 6 | `construir_obt` | Job de Spark, conciliación con `FCT_EMERGENCIAS` y publicación | `GOLD.OBT_EMERGENCIAS` con 17,120,516 filas y 24 columnas |

`ingesta_ecu911` también se puede ejecutar sola para un periodo (`periodo = AAAAMM`). Reemplaza el periodo completo, así que repetirla no duplica filas.

## Garantías de cada etapa

- **Ingesta:** antes de cargar se valida el tamaño contra CKAN, el encabezado, la codificación UTF-8 y el número de columnas. Después de cargar, las filas en Bronze tienen que coincidir con las del archivo; si no, la ejecución falla.
- **Idempotencia:** cada periodo se carga con `DELETE` + `COPY INTO … FORCE = TRUE` dentro de una transacción. Solo puede correr una ingesta a la vez.
- **Silver y Gold:** los tests de dbt comprueban que no se pierden ni se duplican filas frente a la capa anterior, la integridad referencial y los dominios válidos.
- **OBT:** Spark escribe una tabla candidata, `OBT_EMERGENCIAS_NUEVA`. Solo si concilia con `FCT_EMERGENCIAS` se publica con `CLONE`; si no, la OBT anterior no se modifica.

## Desarrollo local de dbt

```bash
docker compose run --rm dbt debug
docker compose run --rm dbt build --select silver
```

El servicio usa la misma imagen que Kestra y toma las credenciales de `.env`.

## Limitaciones conocidas

- **202604 y 202605 no se cargan:** CKAN solo los publica en xlsx, y `COPY INTO` no carga Excel.
- **El pipeline usa el rol `ACCOUNTADMIN`.** En producción correspondería un rol propio con permisos solo sobre `ECU911` y el warehouse.
- **Valores entre comillas en `.env`:** compose las quita, pero `docker run --env-file` las deja como parte del valor.
