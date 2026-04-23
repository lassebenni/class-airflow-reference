select
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id
from {{ ref('stg_trips') }}
where pickup_datetime > dropoff_datetime
