# Bitácora del PSet #2 — ECU 911

Registro de lo construido, decidido y encontrado, fase por fase. Sirve para retomar el trabajo
y como base del documento técnico (máx. 6 páginas) y de la defensa oral.

**Última sesión:** 02-oct-2026 · **Fases completas:** 0 a 13 · **Siguiente:** FASE 14 — demo de retries y backfill

---

## Cómo trabajamos (acuerdo con el tutor)

- Avanzar **una fase a la vez**; no pasar a la siguiente sin confirmación.
- En cada fase: explicar concepto → escribir código → explicar partes clave → ejecutar/verificar → explicar resultado.
- Cierre de cada fase: resumen, diagrama ASCII, dónde están los datos, comandos de verificación,
  qué observar, 3–5 preguntas del profesor con respuestas ideales.
- Errores se muestran y se diagnostican antes de corregir (nunca se esconden).
- Estilo **simple y fácil de entender, pocos archivos**; no sobre-dimensionar la seguridad (es un proyecto universitario).
- Repo de referencia de estilo: https://github.com/2004Santo/Laboratorio-Integrador (flows Kestra con `{{ envs.* }}`).

## Retomar mañana

1. Abrir Docker Desktop y ejecutar `docker compose up -d` en la carpeta del proyecto.
2. Verificar Kestra en http://localhost:8080 (credenciales en `.env`).
3. Los datos siguen en Snowflake (`ECU911.BRONZE.EMERGENCIAS_RAW`, 17,680,253 filas); no hay que recargar nada.
4. SILVER y GOLD ya están construidas (Kestra → `ecu911.dbt_build`, PASS=39) y la OBT (Kestra → `ecu911.spark_obt`); todo encadenado en `ingest_scheduled → transform`. Continuar con **FASE 14** (abajo, "Pendiente").

Estado de Git: repositorio inicializado (`main`), **sin commits todavía** (decisión del usuario).

---

## FASE 0 — Entender el problema
- PSet 2 = pipeline ELT que deja una tabla **cantón × día** lista para ML. No entrena modelos.
- Target (PSet 1): `y_h = 1` si el volumen del cantón en t+h supera el **P90 histórico de ese cantón y día de la semana**,
  calculado **solo con datos de entrenamiento** → el pipeline entrega conteos; el P90 se calcula en la etapa de ML (evita leakage).
- El grain cambia en **Gold**: de 1 fila = 1 incidente → 1 fila = 1 cantón en 1 día.
- Días sin emergencias no existen en la fuente → hace falta **date spine** con ceros.

## FASE 1 — Arquitectura (hallazgos de la fuente real, API CKAN)
| # | Hallazgo | Evidencia |
|---|---|---|
| H1 | Un archivo por mes | 62 recursos, 2021-07 → 2026-08, sin huecos |
| H2 | Mismas 7 columnas | verificado en 6 archivos y luego en los 62 (Fase 5) |
| H3 | **No hay ID de incidente** | no se puede MERGE ni deduplicar por ID |
| H4 | Formatos mezclados; etiqueta del catálogo no confiable | 2026-04/05 son XLSX; 2022-09 dice XLSX y es CSV |
| H5 | Dos formatos de fecha | CSV `1/7/2021`; XLSX serial de Excel `46113` |
| H6 | `Cod_Parroquia` pierde cero inicial | `90150` vs `070150` |
| H7 | Provincias "de más" | 26–29 valores distintos (explicado en Fase 6) |
| H8 | Volumen | ~270–300 mil filas/mes, ~17.7 M total |
| H9 | Publicación con 1–8 semanas de retraso, sin día fijo | ej. 2026-08 publicado el 7-sep |
| H10 | Archivos viejos re-publicados | archivos 2021 modificados en 2025 |
| H11 | Servidor expone ETag / Last-Modified | encabezados HTTP |
| H12 | Fallos transitorios reales del portal | descarga falló y al reintentar funcionó |
| H13 | `Cod_Parroquia` sigue DPA INEC (2 prov + 2 cantón + 2 parroquia) | confirmado en Fase 6 |

Decisiones: unidad de ingesta = **mes**; carga = **reemplazo de partición** (DELETE mes + COPY, en transacción);
Bronze todo **VARCHAR** + metadata; dbt Core en contenedor lanzado por Kestra; Spark standalone 3.5 (conector Snowflake
no soporta 4.2); batch (la fuente publica mensual con retraso: streaming no aplica).

## FASE 2 — Infraestructura Docker
- `docker-compose.yml`: postgres:16 (metadata de Kestra, NO datos ECU), kestra v1.3.40 (LTS), kestra-init (curl),
  spark-master + spark-worker `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` (UI en :8090).
- Postgres = "memoria de la cocina" de Kestra: flows, ejecuciones, logs, estado de triggers, cola de tareas.
  Si se pierde: se pierde historial; flows se recuperan de Git (kestra-init); datos siguen en Snowflake.
- `docker.sock` montado: Kestra lanza contenedores efímeros para tareas (probado con un contenedor Python).
- **Incidentes reales:**
  1. `--flow-path` cargaba flows ANTES de registrar plugins → "Invalid type: python.Script" (race condition,
     visto por timestamps de logs). Solución: servicio `kestra-init` que sube flows por API cuando Kestra está listo.
  2. La API `/flows/import` responde **HTTP 200 aunque rechace un flow** (lista rechazados en el cuerpo) →
     `deploy_flows.sh` revisa el cuerpo, no el código HTTP.
  3. `.gitattributes` fuerza LF en `.sh` (evita `sh\r: not found` al clonar en Windows).
  4. Kestra crea flows de tutorial → desactivados con `tutorial-flows.enabled: false`.
- Spark verificado: 1 worker (2 cores, 2 GB), smoke test distribuido OK.

## FASE 3 — Snowflake
- Tras varias iteraciones se eligió el enfoque simple del repo de referencia: **un flow `snowflake_setup`** que, con el
  usuario admin (usuario/contraseña en `.env`), crea todo: warehouse `ECU911_WH` (XS, auto-suspend 60, auto-resume),
  database `ECU911`, schemas `BRONZE/SILVER/GOLD/OBT`, rol `ECU911_ROLE`, usuario `ECU911_USER`. Idempotente (`IF NOT EXISTS`).
- Credenciales: `.env` → `docker-compose` (`ENV_SNOWFLAKE_*`) → flows `{{ envs.snowflake_* }}`.
- Incidente: "Connection string is invalid" → `.env` vacío dentro del contenedor (no se había guardado / no se hizo `up -d`).
- Ejecución exitosa: `test_pipeline_user` devolvió `ECU911_USER / ECU911_ROLE / ECU911_WH`.
- Idempotencia DDL vs datos: sin `IF NOT EXISTS` el setup **falla** (no duplica); `CREATE OR REPLACE` sería destructivo.
  Los duplicados reales aparecen en la **carga de datos** → se evitan con DELETE+COPY por mes.

## FASE 4 — Ingesta con Kestra
- `ingest_month` (input `month` YYYY-MM, con validator): prepare (stage + tabla) → catalog (API CKAN) → resolve_url (jq)
  → fail_if_not_published → download → [XLSX → CSV con ExcelToIon/IonToCsv, fecha como serial] → upload al stage
  (carpeta única `mes/execution_id`) → load_bronze (`BEGIN; DELETE mes; COPY INTO … FORCE=TRUE; COMMIT; REMOVE`)
  → count_bronze → fail_if_empty → log → finally purge_files. Concurrencia máx. 4 (QUEUE).
- Retries: exponential en HTTP (30 s → 5 min, 4 intentos); constant en Snowflake (30 s, 3 intentos).
- `ingest_scheduled`: Schedule `0 6 10 * *` America/Guayaquil, `recoverMissedSchedules: ALL`; carga N meses (default 2)
  calculados desde `trigger.date ?? execution.startDate`; subflow con `allowFailure` e `inheritLabels`.
- Pruebas: 2026-08 CSV = 280,867 filas (= conteo manual); 2026-04 XLSX = 268,763 (= conteo manual); re-ejecución
  de 2026-08 sigue en 280,867 (idempotencia); **retry real** (portal rechazó conexión, reintento OK);
  2026-10 → FAILED con mensaje claro; scheduled manual cargó 2026-08 y 2026-07.
- Incidentes: `Loop` no existe en Kestra 1.3 (se usa `ForEach`); `Queries` multi-sentencia no devuelve el SELECT
  (se agregó task `Query`); condición con variable inexistente daba error críptico (se usó `?? ''`).

## FASE 5 — Bronze (backfill histórico)
- Backfill nativo de Kestra sobre el trigger `monthly`: 2021-08-01 → 2026-09-10, `meses=1`, label `tipo=backfill`.
  62/62 SUCCESS en ~25 min; el trigger volvió solo a su calendario (próxima 2026-10-10).
- `BRONZE.EMERGENCIAS_RAW`: **17,680,253 filas, 62 meses, 1 ejecución por mes, 0 filas de encabezado**.
- Columnas: FECHA, PROVINCIA, CANTON, COD_PARROQUIA, PARROQUIA, SERVICIO, SUBTIPO (VARCHAR) +
  `_PERIODO, _URL_ORIGEN, _ARCHIVO_STAGE, _FILA_ARCHIVO, _CARGADO_EN, _EJECUCION_KESTRA`.

## FASE 6 — Data Quality (consultas en `docs/data_quality.sql`)
| # | Problema | Evidencia | Acción (Silver/Gold) | Justificación |
|---|---|---|---|---|
| 1 | Filas sin ubicación | 2,127 vacías + 17 `'NULL'` + 16 `'0'` = 2,160 (0.012%) | Conservar con `es_ubicacion_valida=false`; excluir del conteo cantón-día | No asignables; marcar en vez de borrar = auditable |
| 2 | Código sin cero inicial | 1,265,543 filas (7.2%), 10 meses | `LPAD(cod,6,'0')` | DPA = 6 dígitos; tras el relleno el prefijo coincide con la provincia |
| 3 | Código por defecto 090150 en Morona Santiago | 44 filas (0.02% de Morona) | Clave cantón = código más frecuente por (provincia, cantón) | El nombre es confiable; el código es un default |
| 4 | Cantones homónimos | BOLIVAR (0402 Carchi / 1302 Manabí), OLMEDO (1116 Loja / 1318 Manabí); 223 nombres vs 225 prov+nombre | Clave = código DPA 4 dígitos, nunca solo el nombre | Agrupar por nombre sumaría cantones distintos |
| 5 | LOGROÐO (error de codificación en la fuente) | 4,637 filas en 58 meses; LOGROÑO 234 en 3 meses | `Ð` → `Ñ` | Mismo código 1410 |
| 6 | Fechas: 2 formatos + variantes de texto | serial 559,737 filas; cero inicial 1,308,320; 2,066 textos = 1,888 días | Parsear ambos a DATE | 100% interpretable, 0 fuera de su mes |
| 7 | Duplicados exactos | 11,747,431 filas (66.4%) | **NO eliminar** | Sin ID; son incidentes distintos con mismos atributos; borrar destruiría el target |
| 8 | 2024-01 ~30% menos volumen | 208,194 filas; 20/31 días bajos; todas las provincias (49–65%) | Conservar y marcar `es_periodo_anomalo` | Causa no confirmada (¿archivo incompleto? ¿toque de queda ene-2024?); decisión de exclusión en ML |
| 9 | Cantón-día sin filas | 7.0% (394,903 de 424,800); 13 cantones < 1/día (57% días sin filas) | Date spine con ceros en Gold | Sin ceros, lags y P90 se sesgan |
| 10 | Espacios sobrantes | 1 fila en SUBTIPO | `TRIM` en textos | Evita categorías duplicadas |

Otros datos útiles: 24 provincias + ZONA NO DELIMITADA (código 90, 3,757 filas, categoría INEC válida);
7 servicios (Seguridad Ciudadana 69.4%); 694 subtipos; 224 códigos de cantón; volumen baja de ~310k/mes (2021) a ~270k (2025-26);
nombre de archivo cambia de `emergencias_*` a `incidentes_*` en 2024.

## FASE 7 — Silver con dbt
- dbt Core 1.9 (`ghcr.io/dbt-labs/dbt-snowflake:1.9.0`), **misma imagen** en dos lugares:
  servicio `dbt` de compose (perfil `manual`, para desarrollo: `docker compose run --rm dbt build`) y
  flow `ecu911.dbt_build` (DbtCLI + Docker task runner, contenedor efímero).
- El proyecto dbt vive en Git (`dbt/`); `kestra-init` lo sube a los **namespace files** de `ecu911`
  (API `/namespaces/ecu911/files`) y el flow lo monta con `namespaceFiles: include dbt/**`.
- `profiles.yml` sin secretos: `env_var('SNOWFLAKE_*')`; `connect_retries: 3`.
- Macro `generate_schema_name`: tablas en `SILVER` (no `SILVER_SILVER`, el default de dbt).
- `source('bronze','emergencias_raw')` con **freshness** (warn 45 d / error 75 d sobre `_CARGADO_EN`).
- Modelo `SILVER.EMERGENCIAS` (table), **grain = 1 incidente = 1 fila de Bronze**. Implementa las decisiones de la Fase 6:
  | # | Implementación |
  |---|---|
  | 1 | macro `limpiar_texto`: `''`/`'NULL'`/`'0'` → NULL; bandera `es_ubicacion_valida` |
  | 2 | `LPAD(cod,6,'0')` si es numérico de 5–6 dígitos |
  | 3/4 | CTE `codigo_canton`: código de 4 dígitos más frecuente por (provincia, cantón) → `cod_canton`; bandera `es_codigo_corregido` |
  | 5/10 | `REPLACE('Ð','Ñ')` y `TRIM` en todos los textos |
  | 6 | `COALESCE(TRY_TO_DATE(d/m/yyyy), serial Excel + 1899-12-30)` |
  | 7 | duplicados exactos se conservan; `incidente_id = periodo-fila_archivo` |
  | 8 | `es_periodo_anomalo` desde `var('periodos_anomalos')` en `dbt_project.yml` |
- Tests (8/8 PASS): `unique`+`not_null` en `incidente_id` (detecta un mes cargado 2 veces), `not_null` fecha/periodo/bandera,
  singular `silver_conserva_filas_de_bronze`, singular `silver_fecha_dentro_de_su_mes` (detecta confusión día/mes).
- **Verificación (29-sep-2026):** 17,680,253 filas (= Bronze); 2,160 sin ubicación; 44 códigos corregidos (los 44 de Morona con 090150,
  repartidos en 9 cantones); 208,194 filas anómalas (2024-01); 0 `Ð`; LOGROÑO 4,871 (= 4,637 + 234); 224 cantones = 224 pares
  provincia-cantón (relación 1:1; antes 225 por LOGROÐO); homónimos BOLIVAR/OLMEDO siguen separados; 1,888 días (2021-07-01 → 2026-08-31).
- Tiempos: `dbt build` ~25 s en XS. Flow `dbt_build` en Kestra: SUCCESS (freshness PASS, PASS=8).
- Decisión: materialización **table** (reconstrucción completa). Incremental por `periodo` sería posible, pero con ~25 s no compensa
  la complejidad; queda como mejora si el volumen crece.
- Aún no se encadena ingesta → dbt (se hará en la Fase 12, cuidando que el backfill no dispare 62 builds).

## FASE 8 — Diseño del star schema (detalle en `docs/modelo_dimensional.md`)
- Hecho `FCT_EMERGENCIAS_CANTON_DIA`, **grain = 1 cantón × 1 día** (con ceros vía date spine); medidas: `n_emergencias`
  + 7 columnas por servicio + `n_sin_servicio`. Dims: `DIM_CANTON` (PK `cod_canton` DPA), `DIM_FECHA` (PK `fecha`, feriados por seed,
  `es_periodo_anomalo`). Servicios como columnas (conjunto fijo de 7) en vez de `dim_servicio` (multiplicaría el hecho ×8).
- Perfilado de Silver: 7 servicios (Seguridad Ciudadana 69.4%, Gestión Sanitaria 11.7%, Tránsito 10.6%, Municipales 4.8%,
  Siniestros 1.7%, Militar 1.2%, Riesgos 0.5%; 2,214 sin servicio); ZONA NO DELIMITADA = 2 "cantones" (9001 Las Golondrinas, 9004 El Piedrero).
- **Hallazgo #11:** SEVILLA DON BOSCO pasó de parroquia de MORONA (1401, cód. 140157, 2021-07-01 → 2025-02-09, 11,018 filas)
  a cantón 1413 (cód. 141350, desde 2025-01-23, 5,487 filas); volumen similar (~256 vs ~274/mes). Único cantón que no existe desde el inicio.
  Decisión: reasignar las filas antiguas al 1413 en Silver (geografía vigente) para evitar un quiebre artificial en MORONA.
- Esperado en Gold: 224 cantones, dim_fecha 2,191 días (2021–2026), hecho **422,912** filas (224 × 1,888), suma **17,678,093**.
- Fuera de Gold: target/P90 (ML), lags y medias móviles (OBT en Spark), población INEC (limitación).

## FASE 9 — Gold con dbt
- **Seeds** (schema SILVER): `feriados_ecuador.csv` (83 días de descanso 2021-2026, columnas fecha/nombre_feriado/tipo=ley|traslado|decreto)
  y `cantones_reasignados.csv` (SEVILLA DON BOSCO).
  - Feriados generados con la librería python `holidays` (EC, aplica la Ley de feriados: mar→lun, mié/jue→vie, sáb→vie, dom→lun),
    se elimina la fecha original si cayó en día laborable y se trasladó. Contrastados con el calendario del Ministerio de Turismo
    publicado en prensa (2024 completo; traslados 2021-2026). Agregados a mano 2 puentes por decreto: 2021-11-02 y 2026-01-02.
  - Error propio detectado y corregido: al emparejar traslados solo por nombre se perdía 2021-01-01 (el Año Nuevo 2022 se trasladó
    al 2021-12-31) → se empareja por nombre + fecha cercana (< 7 días) sobre un calendario multi-año.
- **Silver** ahora aplica `cantones_reasignados` → `es_canton_reasignado` (11,019 filas: 11,018 con código + 1 sin código).
- **Gold** (schema GOLD, tables): `dim_canton` (CASE de región por cod_provincia), `dim_fecha` (generator 2021-2026, nombres en español,
  feriados, `es_periodo_anomalo`), `fct_emergencias_canton_dia` (conteos con `COUNT_IF` por servicio + spine `dim_canton × dim_fecha`
  limitado al rango con datos → no inventa ceros en meses no publicados).
- Tests nuevos: PK unique/not_null en dims y seed; `accepted_values` región y día de semana; `relationships` hecho→dims;
  singulares `fct_grain_canton_dia_unico`, `fct_spine_completo`, `fct_conserva_emergencias_de_silver`, `fct_servicios_suman_total`.
- `deploy_flows.sh` ahora sube también `*.csv` (seeds) → 20 archivos en namespace files.
- **Verificación (29-sep-2026):** `dbt build` PASS=32 (manual y desde Kestra, freshness PASS). dim_canton **224**, dim_fecha **2,191**,
  feriados **83**, hecho **422,912** (= 224 × 1,888), suma **17,678,093** (= Silver válidas), cantón-días en 0: **26,716 (6.32%)**.
- Evidencia de valor de los feriados (promedio emergencias por cantón-día): laborable 38.1 · feriado entre semana 42.8 (+12%)
  · fin de semana 50.3 · feriado en fin de semana 56.4.
- Incidente: el clasificador de permisos de Claude Code no respondió varias veces; el usuario ejecutó `dbt build` en su terminal y se
  leyeron los resultados desde `dbt/logs/dbt.log`.

## FASE 10 — Tests dbt (38 en total, todos PASS)
| Capa | Test | Regla de negocio / de datos que protege |
|---|---|---|
| Bronze (source) | `not_null` FECHA, _PERIODO + freshness 45/75 días | la ingesta sigue llegando y no trae filas vacías |
| Silver | `unique`/`not_null` incidente_id | un mes cargado 2 veces (idempotencia de la ingesta) |
| Silver | `silver_conserva_filas_de_bronze` | la limpieza marca, no borra |
| Silver | `silver_fecha_dentro_de_su_mes` | confusión día/mes al parsear fechas |
| Silver | **`silver_meses_completos`** (nuevo) | cada archivo trae todos los días de su mes (si no, ceros falsos) |
| Silver | **`silver_volumen_mensual_estable`** (nuevo) | volumen/día de cada mes a ±20% de la mediana de vecinos (±3 meses); excluye `periodos_anomalos` |
| Silver | **`silver_pct_sin_ubicacion_bajo`** (nuevo) | filas sin cantón ≤ 0.1% (hoy 0.012%) |
| Seeds | `unique`/`not_null` fecha de feriado | un feriado duplicado duplicaría filas de dim_fecha |
| Gold dims | `unique`/`not_null` PK; `accepted_values` región y día de semana | un código con 2 nombres; mapeo de región incompleto |
| Gold hecho | `relationships` → dims; `fct_grain_canton_dia_unico`; `fct_spine_completo` | grain e integridad referencial; ceros completos |
| Gold hecho | `fct_conserva_emergencias_de_silver`; `fct_servicios_suman_total` | la agregación no pierde ni inventa; servicio nuevo sin columna |
| Gold hecho | **`fct_canton_con_datos_cada_mes`** (nuevo) | cantón-mes en 0 = cambio de código/límites (habría detectado el hallazgo #11) |
- Umbrales basados en datos: desviación por día del mes normal más alejado ≈ 12% (2025-02 por mes, se normaliza por día);
  2024-01 = −28.2% por día (6,716 vs mediana 9,359). Ningún mes con días faltantes; ningún cantón-mes en 0.
- **Test que falla a propósito:** `dbt build --vars "{periodos_anomalos: []}"` (sin tocar código) →
  `FAIL 1 silver_volumen_mensual_estable` (2024-01, −28.2%) → `SKIP GOLD.dim_canton` y `GOLD.fct_emergencias_canton_dia`;
  `Done. PASS=20 ERROR=1 SKIP=17`. Observación: Silver y dim_fecha SÍ se reconstruyeron antes del test (dbt build materializa
  y luego prueba; un test fallido no revierte) → se restauró con el build normal desde Kestra (PASS=38).
- **Bug encontrado en la demo:** el primer intento no falló el test sino los modelos: con la lista vacía el `for` generaba
  `periodo in ()` → `SQL compilation error: unexpected ')'`. Snowflake no reemplazó las tablas (el CREATE falló), nada se dañó.
  Corrección: macro `es_periodo_anomalo(columna)` que devuelve `false` si la lista está vacía; reemplaza el bloque repetido en
  Silver, dim_fecha y el test.
- Namespace files ahora: 25 archivos.

## FASE 11 — OBT con Spark
- Job `spark/jobs/build_obt.py` (PySpark 3.5.9) con conector `net.snowflake:spark-snowflake_2.12:3.2.2-spark_3.5`
  (trae snowflake-jdbc 4.0.2) vía `--packages` (caché Ivy en `/tmp/.ivy2`).
- Dos formas de correrlo: `docker compose exec spark-master spark-submit ...` (driver en spark-master, que ahora recibe SNOWFLAKE_*)
  y flow **`ecu911.spark_obt`** (shell Commands + Docker runner con la imagen de Spark, `networkMode: pset2`,
  `spark.driver.host=$(hostname -i)` para que el executor del worker se conecte al driver efímero). `kestra-init` sube `spark/jobs/*.py`
  a namespace files.
- Pasos: leer GOLD (hecho + 2 dims) → **LEFT join + broadcast** de dims (224 y 2,191 filas: se copian a cada executor, sin shuffle)
  → features con `Window.partitionBy(cod_canton).orderBy(fecha)` → validar → escribir `OBT.OBT_EMERGENCIAS_CANTON_DIA` (overwrite, idempotente).
- **Grain OBT = 1 cantón × 1 día** (igual que el hecho). 36 columnas: grain, atributos de cantón y fecha, 9 medidas, features
  `lag_1d/7d/14d`, `media_7d/28d` (incluyen el día t), `es_feriado_t3/t7` (futuro conocido), e insumos del target
  `obj_n_emergencias_t3/t7` (volumen real en t+3/t+7; el y_h con P90 de train se calcula en ML → prefijo `obj_` = NO usar como feature).
- Lags por posición de fila son correctos porque el spine no tiene huecos (test dbt `fct_spine_completo`).
- **Validaciones antes de escribir (el job sale con error y no escribe si alguna falla):** filas OBT = filas hecho (422,912);
  claves (cantón, fecha) distintas = filas; suma n_emergencias igual (17,678,093); 0 filas sin match de cantón; 0 sin match de fecha.
  Después de escribir se relee el COUNT en Snowflake (422,912).
- Verificación de contenido: nulos de `lag_14d` = 3,136 (224×14), de `obj_n_emergencias_t7` = 1,568 (224×7); `media_28d` sin nulos
  (los primeros 27 días promedian una ventana parcial). Quito 22-dic-2025: `obj_t3` = 1,809 = valor del 25-dic y `es_feriado_t3` = true.
- Tiempo: ~4 min (la escritura en Snowflake es la parte más lenta). Manual y desde Kestra: SUCCESS.
- Ruido en logs (no son fallas): el JDBC de Snowflake prueba metadatos de AWS (169.254.169.254) y GCP (metadata.google.internal);
  Spark 3.5 muestra `NotSerializableException: StorageStatus ... Ignoring error`. Kestra marca todo stderr como ERROR.
- **Star schema vs OBT:** el star schema (GOLD) es la fuente de verdad para análisis y BI (dimensiones reutilizables, sin redundancia,
  fácil de extender con nuevas dims/hechos); la OBT es la vista desnormalizada para el modelo de ML: 1 fila = 1 observación con todas
  las features, sin joins en el entrenamiento. Si cambia una dimensión se actualiza GOLD y se regenera la OBT.

## FASE 12 — Orquestación y validación end-to-end
- Nuevo flow **`ecu911.transform`**: Subflow `dbt_build` → Subflow `spark_obt` (ambos `wait` + `transmitFailed`: si un test dbt falla,
  Spark no corre y la OBT conserva su última versión buena) → Query `reconciliar` (conteos de las 4 capas) → `Fail` si descuadra → Log.
- `ingest_scheduled` ahora tiene input **`transformar`** (BOOLEAN, default true): al terminar el ForEach de meses lanza `transform`.
  Backfill: usar `meses = 1` y `transformar = false` (evita 62 builds de dbt + Spark) y luego ejecutar `transform` una vez.
- Cadena completa: `Schedule (día 10) → ingest_scheduled → ingest_month ×N → transform → dbt_build → spark_obt → reconciliar`.
- **Prueba real (29-sep-2026):** `ingest_scheduled` manual con `meses=1` → recargó 2026-08 desde el portal → todo SUCCESS en 6 min 42 s
  (ingest_month 39 s · dbt_build 47 s · spark_obt 5 min 7 s). Log de `transform`:
  `BRONZE 17680253 = SILVER 17680253 filas | SILVER válidas 17678093 = GOLD 17678093 = OBT 17678093 emergencias |
  GOLD 422912 = OBT 422912 cantón-días | datos hasta 2026-08-31`.
- Idempotencia end-to-end: 2026-08 pasó de la ejecución `4Pn4Vx5t…` (28-sep) a `35gvNDU4…` (29-sep) con las mismas 280,867 filas
  y 1 sola ejecución por mes; ninguna capa cambió de tamaño.

## FASE 13 — Ajustes de features de la OBT y test de meses faltantes (02-oct-2026)
Cuatro cambios, uno a la vez, cada uno ejecutado y verificado antes del siguiente.

| # | Cambio | Motivo |
|---|---|---|
| 1 | `es_feriado_t3/t7` desde **DIM_FECHA** (join broadcast sobre `date_add(fecha, 3/7)`) en vez de `lead` | `lead` mira "la fila siguiente" del hecho, que termina en el último día con datos → los últimos 3/7 días de cada cantón quedaban NULL aunque el feriado se conoce de antemano. Nueva validación: 0 nulos; si `dim_fecha` no cubre fecha máx + 7, el job falla e indica hasta dónde llega |
| 2 | Test singular **`silver_sin_meses_faltantes`** | Un mes ausente en medio del rango se convertiría en ceros falsos en el spine de Gold. `silver_meses_completos` no lo ve (solo revisa meses presentes) y `fct_canton_con_datos_cada_mes` lo ve tarde (después de construir Gold). En Silver: si falla, se salta Gold y Spark no corre |
| 3 | **`lag_28d`** | 4 semanas atrás = mismo día de la semana que t (el target compara contra P90 por día de semana) |
| 4 | `media_7d/28d` → **`media_prev_7d/28d`** (`rowsBetween(-7,-1)` / `(-28,-1)`, NULL si la ventana no está completa) | Las medias anteriores incluían el día t (redundante con `n_emergencias`) y promediaban ventanas parciales al inicio (la "media de 28 días" del día 1 era 1 solo día). Convención documentada en el docstring de `build_obt.py`: fila = información al cierre del día t; lags/medias solo días anteriores; `obj_*` no son features |

| Verificación | Esperado | Obtenido |
|---|---|---|
| Nulos `es_feriado_t3` / `es_feriado_t7` | 0 / 0 (antes 672 / 1,568) | **0 / 0** |
| Quito 22-dic-2025 `es_feriado_t3` | true (25-dic) | **true** |
| `dbt build` | PASS=39 (antes 38) | **PASS=39** |
| Nulos `lag_28d` | 6,272 (224×28) | **6,272** |
| Nulos `media_prev_7d` | 1,568 (224×7) | **1,568** |
| Nulos `media_prev_28d` | 6,272 (224×28) | **6,272** |
| Nulos fuera de los primeros k días | 0 | **0** |
| Quito 22-dic-2025 `media_prev_7d` | promedio 15–21 dic = 15,016 / 7 = 2,145.14 | **2,145.14** (la fórmula anterior, con t, daba 2,168.14) |
| OBT | 422,912 filas, 37 columnas, 7 validaciones OK | **422,912 filas, 37 columnas, 7/7 OK** |

- **Pipeline completo desde Kestra** (`ecu911.transform`, ejecución `5ZiEvnYB…`, 4 min 8 s): `dbt_build` SUCCESS (49 s,
  `PASS=39 WARN=0 ERROR=0 SKIP=0`) · `spark_obt` SUCCESS (3 min 11 s, 7 `[validar] OK`) · `reconciliar` SUCCESS. Log:
  `Pipeline OK. BRONZE 17680253 = SILVER 17680253 filas | SILVER válidas 17678093 = GOLD 17678093 = OBT 17678093 emergencias |
  GOLD 422912 = OBT 422912 cantón-días | datos hasta 2026-08-31`. Los nulos de la OBT escrita por Kestra coinciden con la tabla de arriba.

- **Demo del test 2 sin tocar datos:** la misma lógica del test como consulta ad hoc (`dbt show --inline`) quitando `2024-01`
  de `con_datos` → devuelve `periodo_faltante = 2024-01` (secuencia de 62 meses). Snowflake no se modificó.
- Namespace files re-subidos con `docker compose up kestra-init` (6 flows OK, dbt 26 archivos, job de Spark).
- Columnas de la OBT (37): grain (2) + cantón (5) + fecha (11) + medidas (9) + `lag_1d, lag_7d, lag_14d, lag_28d,
  media_prev_7d, media_prev_28d, es_feriado_t3, es_feriado_t7` (8) + `obj_n_emergencias_t3, obj_n_emergencias_t7` (2).

**Preguntas del profesor:**
1. *¿Por qué `es_feriado_t7` puede usar el futuro y `obj_n_emergencias_t7` no?* — El feriado se conoce de antemano (calendario
   oficial): al cierre del día t ya se sabe si t+7 es feriado. El volumen de t+7 no se conoce en t; es lo que se quiere predecir.
2. *¿Por qué dejar NULL en vez de promediar los días disponibles al inicio?* — Una media de 3 días tiene mucha más varianza que una
   de 28, pero tendría el mismo nombre: el modelo trataría igual dos cosas distintas. Con NULL, la etapa de ML decide (descartar
   los primeros 28 días de cada cantón o imputar), y son solo 6,272 de 422,912 filas (1.5%).
3. *¿Qué pasa con los lags si falta un mes completo?* — Los lags son por posición de fila y el spine cubre todo el rango: un mes
   ausente sería ~30 días de ceros falsos que contaminan `lag_*` y `media_prev_*` hasta 28 días después. Por eso
   `silver_sin_meses_faltantes` lo detiene en Silver, antes de construir Gold y la OBT.

---

## Pendiente
- **FASE 14 — demo de retries y backfill:** preparar/demostrar en vivo (retry del portal, backfill con transformar=false). · FASE 15 — README/documentación · FASE 16 — defensa.
- Ideas/limitaciones anotadas: correcciones de meses viejos en la fuente no se detectan solas (solo se recargan los 2 últimos
  meses; recargar con backfill/ingest_month); población INEC y feriados como seeds de dbt (feriados recomendado).
