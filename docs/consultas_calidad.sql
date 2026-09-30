-- Métricas de calidad sobre el histórico completo (ejecutar en Snowsight con rol PSET2_ROLE).
use warehouse PSET2_WH;
use database PSET2_DB;

-- 1. Volumen por capa
select
    (select count(*) from BRONZE.EMERGENCIAS)             as bronze,
    (select count(*) from SILVER.SLV_EMERGENCIAS)         as silver,
    (select count(*) from SILVER.SLV_EMERGENCIAS_RECHAZADAS) as rechazadas,
    (select count(*) from GOLD.FCT_EMERGENCIAS)           as hechos,
    (select count(*) from OBT.OBT_CANTON_DIA)             as obt;

-- 2. Motivos de rechazo (% sobre bronze)
select rejection_reason, count(*) as filas,
       round(100 * count(*) / (select count(*) from BRONZE.EMERGENCIAS), 3) as pct_bronze
from SILVER.SLV_EMERGENCIAS_RECHAZADAS
group by 1 order by 2 desc;

-- 3. Nulos por columna en bronze (%)
select
    round(100 * count_if(fecha is null) / count(*), 3)          as pct_fecha_nula,
    round(100 * count_if(provincia is null) / count(*), 3)      as pct_provincia_nula,
    round(100 * count_if(canton is null) / count(*), 3)         as pct_canton_nulo,
    round(100 * count_if(cod_parroquia is null) / count(*), 3)  as pct_parroquia_nula,
    round(100 * count_if(servicio is null) / count(*), 3)       as pct_servicio_nulo,
    round(100 * count_if(subtipo is null) / count(*), 3)        as pct_subtipo_nulo
from BRONZE.EMERGENCIAS;

-- 4. Filas idénticas (no se deduplican: ver calidad_datos.md)
select count(*) as filas, count(*) - count(distinct fecha, provincia, canton, cod_parroquia, parroquia, servicio, subtipo, _source_period) as filas_repetidas,
       round(100 * filas_repetidas / filas, 1) as pct_repetidas
from BRONZE.EMERGENCIAS;

-- 5. Codificación por archivo
select _source_encoding, count(distinct _source_period) as meses
from BRONZE.EMERGENCIAS group by 1;

-- 6. Cobertura mensual (meses sospechosos)
select * from SILVER.SLV_COBERTURA_MENSUAL order by source_period;

-- 7. Variantes de escritura por código (consistencia)
select canton_codigo, count(distinct canton_nombre) as variantes, listagg(distinct canton_nombre, ' | ') as nombres
from SILVER.SLV_EMERGENCIAS group by 1 having variantes > 1 order by 2 desc;

-- 8. Servicios fuera del catálogo
select servicio_original, count(*) from SILVER.SLV_EMERGENCIAS where servicio_id = 0 group by 1 order by 2 desc;

-- 9. Validaciones de la OBT (Spark)
select * from OBT.OBT_VALIDACIONES order by run_at desc;
