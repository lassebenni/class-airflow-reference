{{ config(materialized='table') }}

select
    t.pickup_datetime,
    t.dropoff_datetime,
    t.fare_amount,
    t.tip_amount,
    t.trip_distance,
    t.tip_pct,
    t.fare_per_mile,
    t.payment_type_label,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone
from {{ ref('stg_trips') }} t
left join {{ ref('stg_zones') }} pz
    on t.pickup_location_id = pz.location_id
left join {{ ref('stg_zones') }} dz
    on t.dropoff_location_id = dz.location_id
