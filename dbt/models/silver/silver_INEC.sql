with source as (

    select *
    from {{ source('bronze', 'INEC_POBLACION_RAW') }}

),

cleaned as (

    select
        nullif(trim(id), '') as canton_id,

        nullif(trim(upper(name_es)), '') as canton,

        translate(
            nullif(
                trim(
                    regexp_replace(
                        replace(upper(name_es), chr(160), ' '),
                        '\\s+',
                        ' '
                    )
                ),
                ''
            ),
            'ÁÉÍÓÚÜÑ',
            'AEIOUUN'
        ) as canton_normalizado,

        try_to_number(value) as poblacion,

        _source_file,
        _ingested_at

    from source

),

valid_rows as (

    select *
    from cleaned
    where canton_id is not null
      and canton is not null
      and poblacion is not null

),

mapa_provincias as (

    select
        canton_id,

        upper(trim(provincia)) as provincia,

        translate(
            upper(trim(provincia)),
            'ÁÉÍÓÚÜÑ',
            'AEIOUUN'
        ) as provincia_normalizada

    from {{ ref('canton_provincia') }}

),

aliases as (

    select
        provincia_normalizada,
        canton_inec,
        canton_ecu911

    from {{ ref('canton_aliases') }}

)

select
    i.canton_id,

    m.provincia,
    m.provincia_normalizada,

    i.canton,
    i.canton_normalizado,

    coalesce(
        a.canton_ecu911,
        i.canton_normalizado
    ) as canton_ecu911,

    i.poblacion,

    i._source_file,
    i._ingested_at

from valid_rows i

left join mapa_provincias m
    on i.canton_id = m.canton_id

left join aliases a
    on m.provincia_normalizada = a.provincia_normalizada
   and i.canton_normalizado = a.canton_inec