with enriched as (
 select * from {{ ref("int_taxi_enriched") }}
)
select
 -- Grain: one row per pickup zone
 pickup_location_id,
 pickup_zone,
 pickup_borough,
 count(*) as total_pickups,
 round(avg(total_amount)::numeric, 2) as avg_fare,
 round(sum(total_amount)::numeric, 2) as total_revenue,
 -- NULLIF prevents crash when fare_amount = 0
 round(avg(tip_amount / nullif(fare_amount, 0) * 100)::numeric, 1)
 as avg_tip_pct,
 round(avg(trip_distance)::numeric, 2) as avg_distance_miles,
 round(avg(trip_duration_minutes)::numeric, 1) as avg_duration_mins,
 round(avg(revenue_per_mile)::numeric, 2) as avg_revenue_per_mile,
 count(*) filter (where is_airport_trip) as airport_trips
from enriched
where pickup_zone is not null
group by pickup_location_id, pickup_zone, pickup_borough
order by total_pickups desc