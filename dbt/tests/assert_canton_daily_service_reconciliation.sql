select
    event_date,
    canton_key,
    total_incidents,
    security_incidents + health_incidents + transit_incidents
      + municipal_incidents + incident_service_incidents + military_incidents
      + risk_incidents + other_incidents as service_total
from {{ ref('fct_canton_daily') }}
where total_incidents < 0
   or total_incidents <>
      security_incidents + health_incidents + transit_incidents
      + municipal_incidents + incident_service_incidents + military_incidents
      + risk_incidents + other_incidents
