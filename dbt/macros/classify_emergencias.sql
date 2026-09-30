{#-
  Estandariza y clasifica cada fila de BRONZE.EMERGENCIAS. La usan
  slv_emergencias (filas válidas) y slv_emergencias_rechazadas (cuarentena),
  así ambas comparten exactamente las mismas reglas. Justificación en docs/calidad_datos.md.

  NO se deduplica: la fuente no tiene ID ni hora, así que dos filas idénticas
  (misma fecha, parroquia, servicio y subtipo) son emergencias distintas.
-#}
{% macro classify_emergencias() -%}
with bronze as (

    select *
    from {{ source('bronze', 'emergencias') }}
    where {{ periods_to_process(source('bronze', 'emergencias'), '_source_period', '_loaded_at', 'bronze_loaded_at') }}

),

parsed as (

    select
        b.*,
        -- La fecha llega como d/m/yyyy o dd/mm/yyyy según el mes. Se normaliza a
        -- dd/mm/yyyy antes de convertir; try_to_date devuelve NULL si la fecha no existe (31/02).
        case
            when regexp_like(trim(b.fecha), '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$') then
                try_to_date(
                    lpad(split_part(trim(b.fecha), '/', 1), 2, '0') || '/' ||
                    lpad(split_part(trim(b.fecha), '/', 2), 2, '0') || '/' ||
                    split_part(trim(b.fecha), '/', 3),
                    'DD/MM/YYYY')
            when regexp_like(trim(b.fecha), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then
                try_to_date(trim(b.fecha), 'YYYY-MM-DD')
        end                                                             as fecha_parsed,
        regexp_replace(coalesce(b.cod_parroquia, ''), '[^0-9]', '')     as cod_digitos
    from bronze b

),

standardized as (

    select
        -- PK: posición de la fila en el archivo del mes (la fuente no trae ID)
        md5(p._source_period || '|' || p._source_row_number)            as emergencia_key,
        p.fecha_parsed                                                   as fecha,
        -- Código DPA de parroquia: 6 dígitos (PPCCPP). Si perdió el cero inicial, se repone.
        case when length(p.cod_digitos) = 5 then lpad(p.cod_digitos, 6, '0')
             else p.cod_digitos end                                      as parroquia_codigo,
        {{ norm_text('p.provincia') }}                                   as provincia_nombre,
        {{ norm_text('p.canton') }}                                      as canton_nombre,
        {{ norm_text('p.parroquia') }}                                   as parroquia_nombre,
        -- Servicio alineado al catálogo oficial (diccionario de datos del ECU 911)
        coalesce(s.servicio_id, 0)                                       as servicio_id,
        trim(p.servicio)                                                 as servicio_original,
        coalesce({{ norm_text('p.subtipo') }}, 'NO ESPECIFICADO')        as subtipo,
        -- linaje
        p._source_file                                                   as source_file,
        p._source_period                                                 as source_period,
        p._source_row_number                                             as source_row_number,
        p._source_encoding                                               as source_encoding,
        p._loaded_at                                                     as bronze_loaded_at
    from parsed p
    left join {{ ref('servicios') }} s
        on s.servicio_clave = {{ norm_text('p.servicio') }}

)

select
    *,
    left(parroquia_codigo, 4)                                            as canton_codigo,
    left(parroquia_codigo, 2)                                            as provincia_codigo,
    -- Primera regla que falla = motivo de rechazo (NULL = fila válida)
    case
        when fecha is null
            then 'fecha_invalida'
        when date_trunc('month', fecha) <> source_period
            then 'fecha_fuera_de_periodo'
        when length(parroquia_codigo) <> 6
            then 'ubicacion_faltante'
    end                                                                  as rejection_reason
from standardized
{%- endmacro %}
