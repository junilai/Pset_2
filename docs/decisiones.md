# Decisiones de arquitectura, batch vs. streaming y limitaciones

## Batch vs. streaming

**Se usa procesamiento por lotes (mensual)** porque:

- **Frecuencia de la fuente:** el ECU 911 publica un CSV por mes, con ~1 mes de retraso. No existe
  un flujo de eventos público; procesar más seguido que la publicación no aporta datos nuevos.
- **Latencia que requiere el negocio:** el modelo del PSet #1 anticipa demanda alta a t+3 y t+7
  días para apoyar la planificación de turnos y la coordinación semanal. Una decisión que se toma
  con días de anticipación no necesita datos con segundos de latencia.
- **Costo y simplicidad:** un lote mensual consume un warehouse XS unos minutos; un pipeline
  streaming (Kafka, Snowpipe Streaming, procesamiento continuo) tendría costo fijo y más puntos de
  falla sin mejorar la decisión.

**Qué tendría que cambiar para justificar streaming:**

- Acceso a la fuente operativa del ECU 911 (eventos de llamadas/alertas en tiempo real) en lugar de
  la publicación mensual de datos abiertos.
- Un caso de uso de horizonte intradía: por ejemplo, detectar en minutos un pico anómalo en un
  cantón (evento masivo, desastre) para reasignar recursos durante el mismo turno.
- Datos con hora del incidente (hoy solo hay fecha) y una acción operativa que pueda ejecutarse en
  minutos. Con eso, la arquitectura pasaría a ingesta continua (p. ej. Kafka → Snowpipe Streaming)
  y features en ventanas deslizantes cortas.

## Decisiones clave

| Decisión | Alternativa descartada | Motivo |
|---|---|---|
| Un mes por ejecución de Kestra + subflows | Un solo script que carga todo | Retries, logs y backfill por mes; un fallo afecta solo a ese mes. |
| URL resuelta con la API CKAN | URLs fijas en el código | Las URLs llevan IDs aleatorios del portal; un mes nuevo se descubre solo. |
| Bronze todo en `VARCHAR` | Tipar al cargar | Conserva el dato original; un formato de fecha inesperado no rompe la carga, se trata en Silver. |
| Carga `DELETE` + `COPY` en transacción | `COPY` con historial de carga | Idempotencia explícita: re-cargar un mes lo reemplaza, nunca duplica. |
| No deduplicar | Deduplicar filas idénticas | Sin ID ni hora, las filas idénticas son emergencias distintas (ver calidad_datos.md). |
| Llave de usuario de servicio RSA | Usuario/contraseña | Snowflake exige MFA a usuarios con contraseña; una tubería automática no puede responderlo. |
| Spark con driver en el contenedor de Kestra | Montar el socket de Docker | Kestra ejecuta `spark-submit` directamente (task runner Process) contra el cluster del compose. |
| Target fuera de la OBT | P90 calculado en la OBT | Evita fuga de información: el P90 se calcula solo con el período de entrenamiento. |

## Limitaciones

1. **Sin hora ni ID de incidente:** impide deduplicar y predecir por hora. La llave del hecho
   depende del orden de filas del archivo; si el ECU 911 republica un mes con otro orden, las
   llaves de ese mes cambian (la recarga es consistente, pero no se puede rastrear una emergencia
   individual entre versiones).
2. **Meses incompletos en la fuente:** ene-2024 parece publicado parcialmente. Se marca
   (`mes_cobertura_sospechosa`) pero no se puede corregir sin la fuente operativa.
3. **Población:** son proyecciones INEC al 30 de junio de cada año (no conteos diarios) y no cubren
   las zonas no delimitadas.
4. **Feriados:** solo nacionales (librería `holidays`); faltan fiestas cantonales y los "puentes"
   decretados ad hoc, que pueden explicar picos locales.
5. **Recategorización de subtipos:** el número de subtipos cambia entre meses; análisis por subtipo
   a lo largo del tiempo no son comparables sin un mapeo adicional.
6. **Publicación con retraso y formatos variables:** abril y mayo 2026 se publicaron en XLSX; la
   ingesta solo acepta CSV y fallaría (con aviso) si un mes futuro llega en otro formato.
7. **Infraestructura local:** Spark corre con un worker de 2 cores / 2 GB; suficiente para
   ~10 M de filas, pero no es un cluster productivo.
