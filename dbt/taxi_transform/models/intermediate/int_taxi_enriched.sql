with trips as (
 select * from {{ ref("stg_taxi_trips") }}
),
zones as (
 select * from {{ ref("stg_taxi_zones") }}
),
enriched as (
 select
 -- Pass-through columns from trips
 t.vendor_id,
 t.pickup_at,
 t.dropoff_at,
 t.passenger_count,
 t.trip_distance,
 t.payment_method,
 t.fare_amount,
 t.tip_amount,
 t.total_amount,
 t.pickup_location_id,
 t.dropoff_location_id,
 -- Zone names (zones joined TWICE: pickup + dropoff)
 pu.zone_name as pickup_zone,
 pu.borough as pickup_borough,
 doz.zone_name as dropoff_zone,
 doz.borough as dropoff_borough,
 -- Calculated: trip duration in minutes
 extract(epoch from (t.dropoff_at - t.pickup_at)) / 60.0
 as trip_duration_minutes,
 -- Calculated: time dimensions for grouping in mart models
 extract(hour from t.pickup_at) as pickup_hour,
 extract(dow from t.pickup_at) as day_of_week, -- 0=Sun, 6=Sat
 t.pickup_at::date as trip_date,
 -- Calculated: trip length bucket
 case
 when t.trip_distance < 1 then 'short'
 when t.trip_distance < 5 then 'medium'
 when t.trip_distance < 15 then 'long'
 else 'very_long'
 end as distance_category,
 -- Calculated: true if trip starts OR ends at an airport
 case when pu.is_airport or doz.is_airport
 then true else false
 end as is_airport_trip,
 -- Calculated: revenue per mile
 -- "case when > 0" avoids division-by-zero crash
 case
 when t.trip_distance > 0
 then round((t.total_amount / t.trip_distance)::numeric, 2)
 else null
 end as revenue_per_mile
 from trips t
 left join zones pu on t.pickup_location_id = pu.location_id
 left join zones doz on t.dropoff_location_id = doz.location_id
 -- Remove impossible trips (under 1 min or over 5 hours)
 where extract(epoch from (t.dropoff_at - t.pickup_at)) / 60.0
 between 1 and 300
)
select * from enriched