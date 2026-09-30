# Calidad de datos

> **Estado:** la evidencia de esta versión viene del perfilamiento previo de 3 meses de muestra
> (ene-2023, ene-2024, mar-2026) y de la revisión de los encabezados de los 39 archivos. Las
> cifras sobre el histórico completo (2023-01 a 2026-03) se obtienen con
> [`consultas_calidad.sql`](consultas_calidad.sql) después de la primera corrida.

## Dimensiones revisadas

| Dimensión | Qué se revisó |
|---|---|
| Completitud | Nulos por columna, meses faltantes, meses publicados incompletos, filas cargadas vs filas del archivo. |
| Precisión | Fechas que no existen, códigos DPA mal formados, codificación de caracteres. |
| Consistencia | Formatos de fecha entre meses, nombres escritos de varias formas, servicios fuera del catálogo, cantón coherente con parroquia. |
| Validez | Fecha dentro del mes del archivo, código de parroquia de 6 dígitos, servicio en el diccionario oficial. |

## Problemas principales

| Problema | Evidencia | Acción | Justificación |
|---|---|---|---|
| Filas idénticas ("duplicados") | 65,2 % de las filas en ene-2023 (186.821 de 286.435), 73,2 % en ene-2024, 65,3 % en mar-2026 son idénticas a otra fila. | **No se deduplica.** PK = período + número de fila. | La fuente no tiene ID ni hora; la fecha es diaria. Dos emergencias del mismo subtipo el mismo día en la misma parroquia son legítimas y distintas. Deduplicar borraría ~2/3 de la demanda real, que es justamente el target del proyecto. |
| Mes publicado incompleto | Ene-2024: Gestión Sanitaria 1.162 filas vs 35.455 (ene-2023) y 36.622 (mar-2026); Tránsito 2.634 vs ~30.000; total 208.194 vs ~285.000. | Se conserva, pero `slv_cobertura_mensual` lo marca y `dim_fecha.mes_cobertura_sospechosa` lo propaga a la OBT. | No es error de carga (filas cargadas = filas del archivo). Borrarlo abriría un hueco en los lags; mantenerlo sin marca sesgaría el P90 del target. El modelado decide si excluirlo del entrenamiento. |
| Formato de fecha inconsistente | `01/01/2023` (dd/mm) en unos meses, `1/2/2023` y `4/1/2024` (d/m) en otros. | Normalizar a dd/mm/yyyy y `TRY_TO_DATE`; si no es una fecha válida → cuarentena `fecha_invalida`. | `TRY_TO_DATE` rechaza fechas inexistentes (31/02) en lugar de desplazarlas al mes siguiente. |
| Registros de otro mes | Regla de validez: la fecha debe caer en el mes del archivo. | Cuarentena `fecha_fuera_de_periodo`. | Cada archivo es la publicación de un mes; una fecha ajena indica error de captura y duplicaría conteos al cruzar meses. |
| Ubicación faltante | 11 filas (0,004 %) en ene-2023, 4 en ene-2024, 452 (0,16 %) en mar-2026 sin provincia/cantón/parroquia. | Cuarentena `ubicacion_faltante`. | La unidad de predicción es el cantón: una emergencia sin ubicación no puede atribuirse a ningún cantón. Volumen despreciable. |
| Codificación mixta | Los archivos vienen en UTF-8 con BOM, salvo algunos en CP1252 (ago-2024 muestra `CA�AR` si se lee como UTF-8). | El cargador detecta la codificación por archivo, transcodifica a UTF-8 y la registra en `_SOURCE_ENCODING`. | Sin esto, "CAÑAR" y los servicios con tilde quedarían corruptos y se partirían en categorías distintas. |
| Columnas vacías extra | Mar-2026 trae `;;;;` al final de cada fila (11 columnas en vez de 7). | Bronze carga solo las 7 columnas del diccionario (`error_on_column_count_mismatch = false`). | Las columnas extra no tienen encabezado ni contenido. |
| Textos con variantes | Tildes, mayúsculas y espacios varían (`Gestión Sanitaria` / `GESTION SANITARIA`). | `norm_text`: sin tildes (conserva Ñ), mayúsculas, espacios colapsados. Servicio mapeado al catálogo del diccionario (7 + "no especificado"); nombre de cantón/parroquia = variante más frecuente por código. | El código DPA es la llave confiable; el texto solo se usa para mostrar. |
| Código DPA sin cero inicial (riesgo) | Todos los códigos de la muestra tienen 6 dígitos, pero provincias 01–09 empiezan con 0. | Se guardan como texto; si llegan 5 dígitos se repone el 0. | Un código numérico convertiría `070150` en `70150` y rompería la jerarquía parroquia → cantón. |
| Subtipos cambian en el tiempo | 532 subtipos distintos en ene-2023, 378 en ene-2024, 529 en mar-2026. | Se conservan (dim_tipo_emergencia = servicio + subtipo). | Recategorización del operador; el proyecto predice volumen total por cantón, no por subtipo. |
| "ZONA NO DELIMITADA" | 42–62 filas/mes con provincia "ZONA NO DELIMITADA" (código DPA 90). | Se conservan como cantones `90xx` con `es_zona_no_delimitada = true`. | Son territorios reales sin cantón asignado; el modelo puede excluirlos. No tienen población INEC. |
| Errores en fuentes complementarias | Proyecciones INEC: la hoja `pablo_sexto_m` (Morona Santiago) tiene como título "Portovelo". Portal: formatos mal etiquetados (CSV marcados como XLSX). | El seed de población empareja por nombre de hoja y orden de códigos, no por título; la ingesta decide por la extensión de la URL. | Evita asignar la población de otro cantón y descargar el recurso equivocado. |

## Tests que validan estas reglas

- `assert_bronze_matches_load_audit`: filas en Bronze = filas contadas en el archivo (completitud de la ingesta).
- `assert_bronze_reconciles_with_silver`: Bronze = Silver + cuarentena, por mes (no se pierde nada).
- `assert_sin_meses_faltantes`: ningún mes faltante entre el primero y el último.
- `assert_fecha_dentro_del_periodo`, `assert_parroquia_pertenece_al_canton`: validez y consistencia DPA.
- `unique` / `not_null` / `relationships` en todas las PK y FK de Gold.
- `assert_cantones_con_poblacion` (aviso): cantones con emergencias sin población INEC.
