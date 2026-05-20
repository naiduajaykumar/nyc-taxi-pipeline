with enriched as (
 select * from {{ ref("int_taxi_enriched") }}
)
select
 -- Grain: one row per calendar day
 trip_date,
 day_of_week,
 -- Volume
 count(*) as total_trips,
 sum(passenger_count) as total_passengers,
 -- Revenue
 round(sum(fare_amount)::numeric, 2) as total_fare_revenue,
 round(sum(tip_amount)::numeric, 2) as total_tip_revenue,
 round(sum(total_amount)::numeric, 2) as total_revenue,
 round(avg(total_amount)::numeric, 2) as avg_trip_revenue,
 -- Trip quality
 round(avg(trip_distance)::numeric, 2) as avg_distance_miles,
 round(avg(trip_duration_minutes)::numeric, 1) as avg_duration_mins,
 -- Segment counts using modern FILTER syntax
 count(*) filter (where is_airport_trip) as airport_trips,
 count(*) filter (where payment_method = 'Credit card') as credit_card_trips,
 count(*) filter (where payment_method = 'Cash') as cash_trips,
 round(avg(revenue_per_mile)::numeric, 2) as avg_revenue_per_mile
from enriched
group by trip_date, day_of_week
order by trip_date