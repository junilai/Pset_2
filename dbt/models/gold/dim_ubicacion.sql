{#- Parroquias de SILVER.DIM_PARROQUIA mas miembros especiales, para que toda emergencia tenga clave:
    <cod_canton>00 = parroquia desconocida dentro de un canton conocido; 000000 = sin ubicacion. -#}
with parroquias as (
    select
        cod_parroquia   as ubicacion_key,
        cod_provincia,
        provincia,
        cod_canton,
        canton,
        cod_parroquia,
        parroquia,
        'PARROQUIA'     as nivel
    from {{ ref('dim_parroquia') }}
),

cantones as (
    select
        cod_canton || '00'          as ubicacion_key,
        cod_provincia,
        provincia,
        cod_canton,
        canton,
        null                        as cod_parroquia,
        'PARROQUIA DESCONOCIDA'     as parroquia,
        'CANTON'                    as nivel
    from {{ ref('dim_parroquia') }}
    group by all
),

sin_ubicacion as (
    select
        '000000'        as ubicacion_key,
        null            as cod_provincia,
        'SIN UBICACIÓN' as provincia,
        null            as cod_canton,
        'SIN UBICACIÓN' as canton,
        null            as cod_parroquia,
        'SIN UBICACIÓN' as parroquia,
        'NINGUNO'       as nivel
)

select * from parroquias
union all
select * from cantones
union all
select * from sin_ubicacion
