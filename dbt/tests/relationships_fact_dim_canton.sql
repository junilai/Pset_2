select
    f.provincia,
    f.canton

from {{ ref('fact_emergencias_canton_dia') }} f

left join {{ ref('dim_canton') }} d
    on f.provincia = d.provincia
   and f.canton = d.canton

where d.canton is null