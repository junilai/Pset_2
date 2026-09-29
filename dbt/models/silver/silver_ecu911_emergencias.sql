with source as (

    select *
    from {{ source('bronze', 'ECU911_EMERGENCIAS_RAW') }}

),

cleaned as (

    select
        try_to_date(fecha, 'DD/MM/YYYY') as fecha,

        case
            when upper(trim(provincia)) in ('', '0', 'NULL') then null
            else upper(trim(provincia))
        end as provincia,

        translate(
            case
                when upper(trim(provincia)) in ('', '0', 'NULL') then null
                else upper(trim(provincia))
            end,
            'ÁÉÍÓÚÜÑ',
            'AEIOUUN'
        ) as provincia_normalizada,

        case
            when upper(trim(canton)) in ('', '0', 'NULL') then null
            else upper(trim(canton))
        end as canton,

        translate(
            replace(
                case
                    when upper(trim(canton)) in ('', '0', 'NULL') then null
                    else upper(trim(canton))
                end,
                'Ð',
                'Ñ'
            ),
            'ÁÉÍÓÚÜÑ',
            'AEIOUUN'
        ) as canton_normalizado,

        nullif(trim(cod_parroquia), '') as cod_parroquia,

        nullif(
            trim(
                regexp_replace(
                    upper(parroquia),
                    '\\s+',
                    ' '
                )
            ),
            ''
        ) as parroquia,

        translate(
            nullif(
                trim(
                    regexp_replace(
                        upper(parroquia),
                        '\\s+',
                        ' '
                    )
                ),
                ''
            ),
            'ÁÉÍÓÚÜÑ',
            'AEIOUUN'
        ) as parroquia_normalizada,

        nullif(trim(upper(servicio)), '') as servicio,

        nullif(
            trim(
                regexp_replace(
                    upper(subtipo),
                    '\\s+',
                    ' '
                )
            ),
            ''
        ) as subtipo,

        _source_month,
        _source_file,
        _ingested_at

    from source

),

valid_rows as (

    select *
    from cleaned
    where fecha is not null
      and provincia is not null
      and canton is not null

)

select *
from valid_rows