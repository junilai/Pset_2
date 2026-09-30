{{ config(materialized='table') }}

with bounds as (
    select
        min(event_date) as min_date,
        dateadd(day, 7, max(event_date)) as max_date
    from {{ ref('stg_emergencias') }}
    where event_date is not null
),

numbers as (
    select row_number() over (order by seq4()) - 1 as day_offset
    from table(generator(rowcount => 4000))
),

dates as (
    select dateadd(day, n.day_offset, b.min_date)::date as calendar_date
    from numbers n
    cross join bounds b
    where dateadd(day, n.day_offset, b.min_date) <= b.max_date
)

select
    to_number(to_char(calendar_date, 'YYYYMMDD')) as date_key,
    calendar_date,
    year(calendar_date) as year_number,
    quarter(calendar_date) as quarter_number,
    month(calendar_date) as month_number,
    day(calendar_date) as day_of_month,
    dayofweekiso(calendar_date) as day_of_week_iso,
    weekofyear(calendar_date) as week_of_year,
    dayofweekiso(calendar_date) in (6, 7) as is_weekend,
    last_day(calendar_date, 'month') = calendar_date as is_month_end
from dates
