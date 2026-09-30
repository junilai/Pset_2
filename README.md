# PSet 2 — Pipeline de emergencias ECU 911

Pipeline batch reproducible que descarga las bases mensuales del ECU 911,
conserva los registros en Snowflake, construye capas Silver y Gold con dbt y
genera con Spark una One Big Table (OBT) para clasificar días de alta demanda
por cantón con horizontes de 3 y 7 días.

## Arquitectura

```text
datosabiertos.gob.ec
        │
        ▼
Kestra ── orquesta ingesta, dbt, Spark y validaciones
        │
        ▼
Snowflake / BRONZE.EMERGENCIAS_RAW
        │
        ▼
dbt / SILVER.STG_EMERGENCIAS
        │
        ▼
dbt / GOLD (dimensiones y hechos diarios)
        │
        ▼
Spark ── malla fecha × cantón, ventanas y etiquetas
        │
        ▼
Snowflake / GOLD.OBT_CANTON_HIGH_DEMAND_SPARK
```

El proyecto usa procesamiento batch. La fuente publica archivos mensuales y el
caso de uso no necesita decisiones en segundos, por lo que una arquitectura de
streaming añadiría complejidad sin reducir una latencia relevante.

## Estructura

```text
pset_2/
├── .env.example
├── docker-compose.yaml
├── README.md
├── kestra/
│   └── load_emergencias.yml
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   └── tests/
└── spark/
    └── build_obt.py
```

## Requisitos

- Docker Desktop con al menos 4 GB de memoria disponible.
- Acceso a una cuenta de Snowflake con permisos para crear esquemas y tablas.
- Puerto local `8080` disponible para Kestra.
- PowerShell para ejecutar los comandos mostrados a continuación.

## 1. Configurar el entorno

Desde la carpeta del pset:

```powershell
cd C:\Universidad\Data_Science\Pipelines\psets\pset_2
Copy-Item .env.example .env
```

Editar `.env` con las credenciales reales de Snowflake. El archivo `.env` está
ignorado por Git y no debe subirse al repositorio.

Comprobar la configuración de Docker:

```powershell
docker compose config --quiet
```

## 2. Levantar la infraestructura

```powershell
docker compose up -d
docker compose ps
```

Kestra estará disponible en <http://localhost:8080>.

## 3. Importar y ejecutar el pipeline completo

Importar en Kestra el archivo:

```text
kestra/load_emergencias.yml
```

El flow contiene 62 periodos entre julio de 2021 y agosto de 2026. Después de
importarlo, basta con presionar **Execute**. Esa única ejecución realiza, en
orden, todo el pipeline:

1. valida y completa la capa Bronze;
2. clona la rama `juandi` de este repositorio en un directorio temporal;
3. ejecuta `dbt build`, que construye Silver y Gold y corre todas las pruebas;
4. ejecuta Spark para generar la malla completa y la OBT;
5. consulta Snowflake y detiene el flow si encuentra grano duplicado, nulos o
   claves de fecha inconsistentes.

Las tareas son secuenciales: si una capa falla, ninguna capa posterior se
ejecuta. El trigger revisa la fuente el día 15 de cada mes a las 08:00, hora de
Ecuador, y sigue la misma secuencia end-to-end.

La ingesta:

- crea `BRONZE.EMERGENCIAS_RAW` y `BRONZE.EMERGENCIAS_CONTROL`;
- procesa un mes a la vez;
- reintenta descarga y carga hasta tres veces;
- compara el conteo actual con el esperado;
- reemplaza solamente meses ausentes o incompletos;
- limita el flow a una ejecución global para evitar cargas concurrentes.

Para incorporar meses posteriores a `202608`, agregar el nuevo periodo, formato
y URL en `variables.sources` del flow y guardar la nueva revisión en Kestra y
en el repositorio.

## 4. Silver y Gold con dbt

Kestra ejecuta esta etapa automáticamente. Los siguientes comandos quedan como
herramientas de diagnóstico local y no son necesarios para la operación normal.

Validar la conexión:

```powershell
docker compose exec dbt dbt debug --profiles-dir .
docker compose exec dbt dbt parse --profiles-dir .
```

Primera construcción completa:

```powershell
docker compose exec dbt dbt build --full-refresh --profiles-dir .
```

Ejecuciones posteriores, cuando Bronze cambie:

```powershell
docker compose exec dbt dbt build --profiles-dir .
```

### Tratamiento de calidad

Silver realiza las siguientes operaciones:

- tipado y normalización de textos y códigos DPA;
- reparación auditable de fechas mal convertidas en abril y mayo de 2026;
- validación de correspondencia entre fecha y `SOURCE_PERIOD`;
- clasificación de servicios en categorías canónicas;
- indicadores de calidad y elegibilidad para modelado;
- compactación de filas exactamente iguales.

La fuente no incluye ID ni hora del incidente. Por eso las repeticiones no se
eliminan silenciosamente: cada combinación exacta ocupa una fila Silver y su
frecuencia original se conserva en `INCIDENT_COUNT`. Las tablas Gold agregan
con `SUM(INCIDENT_COUNT)`.

### Modelo Gold y grain

- `DIM_DATE`: una fila por fecha y clave `YYYYMMDD`.
- `DIM_CANTON`: una fila por código DPA de cantón.
- `DIM_EMERGENCY_TYPE`: una fila por servicio y subtipo.
- `FCT_EMERGENCIAS_DAILY`: fecha × cantón × tipo de emergencia.
- `FCT_CANTON_DAILY`: fecha × cantón.

Las tablas de hechos incluyen `DATE_KEY`, `CANTON_KEY` y, cuando corresponde,
`EMERGENCY_TYPE_KEY`, con pruebas `not_null`, `unique` y `relationships`.

## 5. OBT con Spark

Kestra ejecuta Spark automáticamente después de que `dbt build` termina sin
errores. Para diagnóstico también se puede lanzar manualmente:

```powershell
docker compose run --rm spark /opt/spark/bin/spark-submit `
  --master "local[*]" `
  --driver-memory 4g `
  --packages "net.snowflake:spark-snowflake_2.13:3.2.2-spark_4.0,net.snowflake:snowflake-jdbc:4.0.2" `
  /app/build_obt.py
```

La primera ejecución descarga los conectores Maven. El volumen
`spark-ivy-cache` los conserva para ejecuciones posteriores.

Spark escribe estas tablas en `GOLD`:

- `CANTON_DAILY_COMPLETE_SPARK`;
- `HIGH_DEMAND_THRESHOLDS_SPARK`;
- `OBT_CANTON_HIGH_DEMAND_SPARK`.

La malla completa contiene una fila por `AS_OF_DATE × CANTON_KEY`. Primero se
generan todas las fechas y cantones, después se rellenan con cero los días sin
observaciones y finalmente se calculan lags de 1, 7, 14 y 28 días, promedios
móviles, desviación, tendencia, baseline y objetivos a 3 y 7 días. Esto evita
que `lag(7)` signifique siete registros en lugar de siete días calendario.

Los P90 se calculan únicamente con datos de entrenamiento hasta `2024-12-31`.
Las particiones son:

- entrenamiento: hasta `2024-12-31`;
- validación: durante 2025;
- prueba: desde 2026;
- scoring: fechas cuyo objetivo aún no está disponible.

Para entrenar se debe filtrar `IS_MODEL_ELIGIBLE = TRUE`,
`IS_FEATURE_READY = TRUE` y la etiqueta del horizonte distinta de `NULL`.
Las columnas `ACTUAL_*`, `IS_HIGH_DEMAND_*`, `P90_*`, `BASELINE_*` y
`SPLIT_*` son de evaluación y no deben entregarse al modelo como predictores.

## 6. Validaciones

`dbt build` ya ejecuta todas las pruebas definidas. Para repetir solamente las
pruebas durante un diagnóstico:

```powershell
docker compose exec dbt dbt test --profiles-dir .
```

El script Spark detiene la ejecución si detecta:

- duplicados en `fecha × cantón`;
- modificación inesperada del número de filas al hacer joins;
- valores nulos después de rellenar la malla;
- diferencias entre el total diario y la suma por servicio.

Al final, Kestra ejecuta además esta validación sobre la tabla persistida:

```sql
select
    count(*) as rows,
    count(distinct concat(to_varchar(as_of_date), '|', canton_key)) as grain_rows,
    count_if(as_of_total_incidents is null) as null_totals
from PSET2_DB.GOLD.OBT_CANTON_HIGH_DEMAND_SPARK;
```

`ROWS` y `GRAIN_ROWS` deben coincidir y `NULL_TOTALS` debe ser cero.

## 7. Operación habitual

La operación normal consiste en abrir `pset2.load_emergencias` en Kestra y
presionar **Execute**. No se deben lanzar dbt ni Spark por separado para una
ejecución de producción.

La ingesta no borra ni duplica meses correctos: compara cada periodo con
`EMERGENCIAS_CONTROL` y solo reemplaza, dentro de una transacción, los periodos
ausentes o incompletos. Las ejecuciones concurrentes quedan en cola.

Spark puede mostrar dependencias con nombres Parquet, Avro o Zstandard. Son
formatos internos del conector para transferir datos a Snowflake; el proyecto
no crea archivos Parquet permanentes.

## 8. Detener la infraestructura

```powershell
docker compose down
```

Este comando conserva los volúmenes. No usar `docker compose down -v` si se
desea mantener el historial local de Kestra y la caché de conectores.

## Seguridad

- No versionar `.env`, contraseñas, tokens o claves privadas.
- El repositorio contiene únicamente `.env.example` con valores ficticios.
- Kestra, dbt y Spark reciben credenciales mediante variables de entorno.
