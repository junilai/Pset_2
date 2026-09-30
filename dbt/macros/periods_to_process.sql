{#-
  Filtro incremental por período (mes del archivo).
  En corridas incrementales solo se re-procesan los meses cuyo upstream cambió
  desde la última corrida del modelo actual. Junto con incremental_strategy
  'delete+insert' y unique_key 'source_period', un mes re-procesado se reemplaza
  completo: re-ejecutar la tubería no duplica datos.
-#}
{% macro periods_to_process(upstream, upstream_period_col, upstream_ts_col, this_ts_col) -%}
    {%- if is_incremental() -%}
    {{ upstream_period_col }} in (
        select {{ upstream_period_col }}
        from {{ upstream }}
        group by 1
        having max({{ upstream_ts_col }}) > (
            select coalesce(max({{ this_ts_col }}), '1900-01-01'::timestamp_ltz) from {{ this }}
        )
    )
    {%- else -%}
    true
    {%- endif -%}
{%- endmacro %}
