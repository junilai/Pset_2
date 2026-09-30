-- Toda emergencia válida de silver llega exactamente una vez a la tabla de hechos.
select * from (
    select (select count(*) from {{ ref('slv_emergencias') }}) as silver_rows,
           (select count(*) from {{ ref('fct_emergencias') }}) as fact_rows
)
where silver_rows <> fact_rows
