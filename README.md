# PSet 2 — Pipeline ECU911

Kestra ejecuta la ingesta de ECU911 e INEC, dbt construye Silver y Gold y
Spark genera `ECU911.OBT.OBT_EMERGENCIAS_CANTON_DIA`.

## Preparación

Requisitos: Docker Desktop en ejecución, Python 3 para el importador,
acceso a internet y una cuenta de
Snowflake con permisos para usar el warehouse y crear schemas, tablas y vistas.

1. Crea `.env` a partir de `.env.example` y completa las credenciales. Si ya
   tienes `.env`, conserva ese archivo. `SNOWFLAKE_ACCOUNT` debe ser el
   identificador de la cuenta, sin `https://` ni `.snowflakecomputing.com`.
   Los modelos actuales usan la base `ECU911` y Bronze en `BRONZE`.
2. Desde la raíz del repositorio, construye y levanta los servicios:

   ```bash
   docker compose up -d --build
   ```

3. Abre [Kestra](http://localhost:8080) e inicia sesión. En una instalación
   nueva, completa primero el registro del usuario. Importa los tres flujos:

   ```bash
   python3 scripts/import_flows.py
   ```

   El script pide el usuario y la contraseña de Kestra; la contraseña no se
   muestra ni se guarda. Repite la importación cuando modifiques los YAML.
   También puedes importar `load_raw.yml`, `load_raw_inec.yml` y `pipeline.yml`
   desde la interfaz. La importación inicial es necesaria una sola vez; los
   flujos se conservan en el volumen de PostgreSQL.

## Ejecutar todo desde Kestra

Abre el flujo **`pset2.pipeline`**, pulsa **Execute**, selecciona `source_month`
y confirma la ejecución. El valor inicial es `2026-08`, último mes del catálogo
actual. El mes elegido debe existir en `variables.files` de `load_raw.yml`.

Las etapas se ejecutan en este orden:

1. Comprobar que los contenedores de dbt y Spark estén disponibles.
2. Cargar el mes de ECU911 en Bronze mediante `load_raw`.
3. Cargar el archivo INEC incluido en `data/inec/` mediante `load_raw_inec`.
4. Ejecutar `dbt build --fail-fast`: carga los seeds y construye Silver y Gold
   con sus tests, respetando las dependencias de `ref()` y `source()`.
5. Ejecutar Spark para reconstruir la OBT y validar claves y número de filas.

Cada etapa espera a la anterior. Si falla una ingesta, un test de dbt o Spark,
el flujo termina con error y las etapas posteriores no se ejecutan.
Los logs se consultan en la ejecución de cada tarea y sus subflujos.

La ingesta reemplaza el mes seleccionado; conserva los otros meses existentes
en Bronze. dbt y Spark trabajan con **todo el histórico presente en Bronze**.
La OBT se escribe con `overwrite`, por lo que volver a ejecutar el flujo
reemplaza su resultado anterior.

Para una instalación sin histórico, carga primero los meses necesarios desde
`load_raw` mediante ejecuciones por mes o un backfill de su trigger. El trigger
mensual queda desactivado en esta configuración porque el punto de entrada es
el pipeline manual; si utilizas backfill, habilítalo temporalmente y vuelve a
desactivarlo al terminar. Después ejecuta `pipeline`. Para añadir meses nuevos,
incorpora sus URLs al catálogo de `load_raw` y vuelve a importar los flujos.

No ejecutes manualmente cargas o builds en paralelo con `pipeline`. El flujo
principal admite una ejecución a la vez; las siguientes quedan en cola.
La ingesta mantiene el diseño original de DELETE seguido de COPY: si falla
entre ambos pasos, vuelve a ejecutar la carga del mes antes de transformar.

## Contenedores y configuración

- `kestra` coordina las tareas y usa el cliente Docker y el socket del daemon
  para ejecutar comandos en los contenedores `pset2-dbt` y `pset2-spark`.
- `dbt` usa dbt Core 1.10.15 y el adaptador Snowflake 1.10.2. El perfil
  `dbt/profiles.yml.example` toma sus valores de variables de entorno; no
  contiene credenciales. Los seeds se cargan en `SILVER`.
- Los modelos Silver y Gold conservan su materialización como vistas.
- `spark` utiliza el script `spark/obt.py` y escribe la OBT como tabla.

dbt Cloud puede seguir utilizándose para desarrollo. Este pipeline ejecuta
los archivos locales del repositorio mediante dbt Core, sin requerir un job
ni un token de la API de dbt Cloud.

## Ejecutar etapas por separado

```bash
docker compose exec dbt dbt build --fail-fast

docker compose exec spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp \
  --packages net.snowflake:spark-snowflake_2.12:3.2.2-spark_3.5,net.snowflake:snowflake-jdbc:4.1.0 \
  /app/spark/obt.py
```

Referencias: [Subflows de Kestra](https://kestra.io/plugins/core/flow/io.kestra.plugin.core.flow.subflow),
[comandos Shell en Kestra](https://kestra.io/blueprints/shell-scripts) y
[conexión dbt–Snowflake](https://docs.getdbt.com/docs/local/connect-data-platform/snowflake-setup).
