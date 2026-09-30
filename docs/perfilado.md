# Perfilado de calidad de BRONZE.EMERGENCIAS

Análisis de los 60 periodos mensuales de la Base de Emergencias del ECU 911 cargados en `ECU911.BRONZE.EMERGENCIAS`, y reglas de limpieza acordadas para la capa Silver.

## Alcance y método

- **Fuente:** dataset `base-de-emergencias` de datosabiertos.gob.ec (CKAN, id `00c650e7-42a9-4f3c-9f29-45000091f8b3`).
- **Periodos cargados:** 60, de 202107 a 202608: **17,120,516 filas** en 2,140,679,979 bytes de CSV.
- **Periodos excluidos:** 202604 y 202605, publicados solo en xlsx (`COPY INTO` no carga Excel).
- **Cómo se midió:** el flow `perfilar_bronze` guarda 100 métricas por periodo en `BRONZE.PERFIL_CALIDAD`. Para 202107, las 99 métricas se recalcularon por separado en Python sobre el CSV original y coincidieron todas. En los 60 periodos, las filas perfiladas coinciden con las que contó `validar_archivo` antes de la carga.
- **Revisión manual:** los casos anómalos de 202110, 202302, 202204 y 202603 se revisaron sobre los archivos originales.

## Estructura de los archivos

Todos los archivos son UTF-8 con BOM, usan `;` como separador y tienen el encabezado `Fecha;provincia;Canton;Cod_Parroquia;Parroquia;Servicio;Subtipo`.

| Columnas por línea | Periodos | Causa |
|---|---|---|
| 7 | 58 | formato esperado |
| 8 | 202110 | un `;` al final de cada línea |
| 11 | 202603 | cuatro `;` al final de cada línea |

Las columnas sobrantes no tienen nombre y vienen vacías en todas las filas (en 202110 hay una sola fila con un espacio). `validar_archivo` las acepta solo en esas condiciones y rechaza el archivo si alguna trae datos. El `COPY INTO` toma únicamente `$1..$7`.

## Hallazgos

### Sin problemas en los 60 periodos

- **`FECHA`:** se puede convertir en el 100% de las filas, ninguna cae fuera de su periodo y cada mes tiene todos sus días. En 24 periodos aparecen fechas con ceros (`01/10/2021`) junto a fechas sin ceros (`1/10/2021`); las dos formas se convierten igual.
- **`SERVICIO`:** los mismos 7 valores en todos los periodos: Seguridad Ciudadana (69.5% de las filas), Gestión Sanitaria, Tránsito y Movilidad, Servicios Municipales, Gestión de Siniestros, Servicio Militar y Gestión de Riesgos.
- **`CANTON`:** entre 221 y 222 valores distintos por mes.
- **Espacios y mayúsculas:** 0 casos en `PROVINCIA`, `CANTON`, `COD_PARROQUIA`, `PARROQUIA` y `SERVICIO`.

### H1. Filas idénticas en las 7 columnas: entre 63.6% y 87.8% por mes

El archivo registra solo la fecha, sin hora ni identificador de incidente. Por eso dos emergencias del mismo subtipo, el mismo día y en la misma parroquia generan filas idénticas. En 202107 una misma combinación aparece 707 veces. **No son errores de carga.**

### H2. La falta de ubicación viene escrita de tres maneras

| Periodos | Cómo viene | Filas |
|---|---|---|
| 202107 | `PROVINCIA = '0'` y `COD_PARROQUIA = '0'` | 16 |
| 202108 | el texto `'NULL'` en provincia, cantón, parroquia y código | 17 |
| otros 58 | campos vacíos (llegan como NULL) | 2,005 |

En todos los casos faltan juntas provincia, cantón y parroquia. 202603 tiene muchas más filas así que el resto de los meses (452). Además hay 4 filas con provincia, cantón y parroquia pero sin código (202407: 3, 202408: 1).

### H3. `COD_PARROQUIA` sin el cero inicial en 10 periodos

En total son **1,265,543 filas** con código de 5 dígitos, en 202107, 202108, 202204, 202205, 202206, 202209, 202212, 202308 (solo una parte del mes), 202402 y 202606. Corresponden a las provincias 01 a 09. En los demás periodos el código viene con 6 dígitos.

### H4. Códigos asignados a una ubicación equivocada

En 10 periodos hay un código que aparece en más de una ubicación. Por ejemplo, en 202204 hay 11 filas de Morona Santiago (Macas, Sucúa, Limón Indanza, Huamboya, Pablo Sexto y Santiago) con el código `090150`, que corresponde a Guayaquil, en lugar de los códigos de su propia provincia (`14xxxx`). Son pocas filas por mes, pero si el código se usa como clave, esas emergencias quedan en la provincia equivocada.

### H5. 202302: `SUBTIPO` no trae subtipos

En 202302 la columna `SUBTIPO` solo tiene 7 valores distintos, y son las mismas categorías de `SERVICIO`. En 268,277 de 270,613 filas (99.1%) es igual a `SERVICIO`. El subtipo real de ese mes no está en el archivo publicado. En el resto de los periodos hay entre 378 y 548 subtipos distintos por mes.

### H6. Nombres de parroquia inconsistentes

- Vienen cortados a 50 caracteres (por ejemplo, `GENERAL LEONIDAS PLAZA GUTIÉRREZ (LIMÓN), CABECE`).
- Según el mes, llevan o no un punto final (`… CAPITAL PROVINCIAL.`) o marcas como `, *`.
- Hay entre 898 y 952 nombres distintos por mes.

### H7. Problemas sueltos

- `SERVICIO` nulo: 2,217 filas en 11 periodos; 2,154 son de 202404.
- `SUBTIPO` nulo: 83 filas en 16 periodos, más 1 que solo tiene espacios (202110).
- `SUBTIPO` con variantes de mayúsculas o espacios: 1 valor en cada uno de 8 periodos.

### H8. `Ñ` mal codificada como `Ð` (encontrado al construir Silver)

El cantón `LOGROÑO` aparece como `LOGROÐO` en 4,418 filas de 57 periodos. Es el único valor con caracteres fuera del alfabeto español en `PROVINCIA`, `CANTON`, `PARROQUIA`, `SERVICIO` y `SUBTIPO`. El perfilado no lo detectó, porque el valor no está vacío, no tiene espacios y no tiene variantes de mayúsculas. Como la grafía corrupta es la más frecuente, sin corregirla el catálogo de parroquias habría elegido `LOGROÐO`.

### H9. Filas de 202201 con ubicación contradictoria (encontrado al construir Silver)

Hay 10 filas con provincia y cantón de Morona Santiago, pero con parroquia y código de Guayaquil (`GUAYAQUIL, CABECERA CANTONAL Y CAPITAL PROVINCIAL`, `090150`). A diferencia de H4, los nombres no permiten encontrar el código correcto.

## Reglas para Silver

| # | Regla | Hallazgo |
|---|---|---|
| 1 | **No quitar las filas idénticas.** Cada fila es una emergencia. | H1 |
| 2 | Completar `COD_PARROQUIA` con ceros a la izquierda hasta 6 dígitos. | H3 |
| 3 | Conservar las filas sin ubicación y marcarlas con un indicador `sin_ubicacion`. | H2 |
| 4 | Mantener `ZONA NO DELIMITADA` como un valor válido de provincia: es una categoría oficial del INEC, no un error. | — |
| 5 | Tratar `'0'`, `'NULL'` y vacío como la misma ausencia de ubicación (regla 3). | H2 |
| 6 | Crear un catálogo de parroquias (código → provincia, cantón y nombre) con la asociación más frecuente de los 60 periodos. Si el código de una fila no corresponde a su provincia, corregirlo a partir de los nombres. | H4, H6 |
| 7 | Poner `SUBTIPO` en NULL para 202302 y marcarlo con un indicador `subtipo_no_disponible`. | H5 |
| 8 | Aplicar `TRIM` a todas las columnas y unificar mayúsculas en `SUBTIPO`. | H7 |
| 9 | Dejar `SERVICIO` nulo como NULL; se reporta como dato faltante, no se imputa. | H7 |
| 10 | Reemplazar `Ð` por `Ñ` en las columnas de ubicación. | H8 |
| 11 | Si el código contradice la provincia y no se puede corregir con los nombres, conservar provincia y cantón y dejar parroquia y código en NULL (`ajuste_codigo = 'ANULADO'`). | H9 |

### Resultado en `SILVER.EMERGENCIAS`

| Cifra | Valor |
|---|---|
| Filas (iguales a Bronze, periodo por periodo) | 17,120,516 |
| Parroquias en `SILVER.DIM_PARROQUIA` | 1,041 |
| `ajuste_codigo = 'CORREGIDO'` / `'INFERIDO'` / `'ANULADO'` | 34 (8 periodos) / 3 / 10 |
| `sin_ubicacion` | 2,038 |
| `subtipo_no_disponible` (202302) | 270,613 |
| Subtipos distintos después de unificar la grafía | 689 |

Los 19 tests de dbt pasan, entre ellos: conservación de filas por periodo, fecha dentro del periodo, código de 6 dígitos, relación con `dim_parroquia` y ubicación de origen coincidente con el código.

## Anexo: cifras por periodo

"Sin ubicación" suma los tres formatos de H2. "Duplicados exactos" es el porcentaje de filas que repiten otra fila en las 7 columnas.

| Periodo | Filas | Columnas en archivo | Códigos de 5 dígitos | Sin ubicación | `SERVICIO` nulo | Duplicados exactos |
|---|---:|---:|---:|---:|---:|---:|
| 202107 | 314,507 | 7 | 144,702 | 16 | — | 67.3% |
| 202108 | 319,989 | 7 | 148,420 | 17 | — | 67.3% |
| 202109 | 307,665 | 7 | — | 9 | — | 67.6% |
| 202110 | 327,820 | 8 | — | 12 | — | 68.2% |
| 202111 | 299,013 | 7 | — | 9 | — | 66.9% |
| 202112 | 334,055 | 7 | — | 4 | — | 68.5% |
| 202201 | 304,575 | 7 | — | 17 | — | 66.7% |
| 202202 | 294,034 | 7 | — | 11 | — | 67.6% |
| 202203 | 312,801 | 7 | — | 15 | — | 66.9% |
| 202204 | 311,739 | 7 | 141,940 | 19 | — | 67.8% |
| 202205 | 318,799 | 7 | 145,366 | 8 | — | 68.4% |
| 202206 | 291,802 | 7 | 138,581 | 12 | — | 67.0% |
| 202207 | 309,337 | 7 | — | 23 | — | 67.0% |
| 202208 | 296,460 | 7 | — | 28 | — | 66.0% |
| 202209 | 294,140 | 7 | 129,435 | 20 | — | 66.2% |
| 202210 | 306,756 | 7 | — | 33 | — | 66.4% |
| 202211 | 287,294 | 7 | — | 24 | — | 66.0% |
| 202212 | 302,367 | 7 | 130,377 | 10 | — | 66.0% |
| 202301 | 286,435 | 7 | — | 11 | — | 65.2% |
| 202302 | 270,613 | 7 | — | 13 | 2 | 87.8% |
| 202303 | 289,472 | 7 | — | 20 | — | 65.6% |
| 202304 | 289,411 | 7 | — | 19 | — | 66.4% |
| 202305 | 291,364 | 7 | — | 20 | — | 66.2% |
| 202306 | 290,245 | 7 | — | 17 | — | 66.6% |
| 202307 | 295,384 | 7 | — | 20 | — | 65.9% |
| 202308 | 284,365 | 7 | 43,821 | 19 | — | 65.2% |
| 202309 | 287,146 | 7 | — | 23 | — | 65.4% |
| 202310 | 289,047 | 7 | — | 23 | — | 65.2% |
| 202311 | 273,956 | 7 | — | 45 | — | 65.5% |
| 202312 | 299,651 | 7 | — | 43 | — | 66.9% |
| 202401 | 208,194 | 7 | — | 4 | 4 | 73.2% |
| 202402 | 276,844 | 7 | 122,694 | 34 | — | 66.5% |
| 202403 | 291,219 | 7 | — | 36 | — | 66.5% |
| 202404 | 271,992 | 7 | — | 21 | 2,154 | 65.9% |
| 202405 | 278,104 | 7 | — | 21 | — | 65.9% |
| 202406 | 277,077 | 7 | — | 17 | — | 65.9% |
| 202407 | 268,289 | 7 | — | 15 | — | 64.4% |
| 202408 | 269,571 | 7 | — | 9 | — | 63.6% |
| 202409 | 279,947 | 7 | — | 20 | — | 64.8% |
| 202410 | 294,494 | 7 | — | 17 | 2 | 66.1% |
| 202411 | 279,589 | 7 | — | 13 | 5 | 65.6% |
| 202412 | 297,323 | 7 | — | 16 | — | 67.1% |
| 202501 | 267,790 | 7 | — | 16 | 1 | 65.5% |
| 202502 | 244,669 | 7 | — | 25 | — | 64.9% |
| 202503 | 277,160 | 7 | — | 45 | — | 64.9% |
| 202504 | 263,466 | 7 | — | 51 | — | 65.2% |
| 202505 | 277,968 | 7 | — | 40 | — | 66.0% |
| 202506 | 271,175 | 7 | — | 39 | — | 65.9% |
| 202507 | 268,119 | 7 | — | 54 | 1 | 65.2% |
| 202508 | 275,442 | 7 | — | 66 | — | 65.2% |
| 202509 | 267,225 | 7 | — | 60 | — | 64.6% |
| 202510 | 266,910 | 7 | — | 64 | 1 | 64.7% |
| 202511 | 269,066 | 7 | — | 58 | — | 65.4% |
| 202512 | 289,009 | 7 | — | 54 | 19 | 65.7% |
| 202601 | 267,009 | 7 | — | 55 | — | 64.6% |
| 202602 | 250,314 | 7 | — | 58 | 27 | 64.7% |
| 202603 | 282,649 | 11 | — | 452 | — | 65.3% |
| 202606 | 268,513 | 7 | 120,207 | 42 | 1 | 65.6% |
| 202607 | 260,280 | 7 | — | 44 | — | 63.7% |
| 202608 | 280,867 | 7 | — | 32 | — | 64.7% |
| **Total** | **17,120,516** | | **1,265,543** | **2,038** | **2,217** | |
