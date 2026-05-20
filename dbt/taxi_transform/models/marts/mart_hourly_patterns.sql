with enriched as (
 select * from {{ ref("int_taxi_enriched") }}
)
select
 -- Grain: one row per hour x day-of-week combination
 pickup_hour,
 day_of_week,
 case day_of_week
 when 0 then 'Sunday' when 1 then 'Monday'
 when 2 then 'Tuesday' when 3 then 'Wednesday'
 when 4 then 'Thursday' when 5 then 'Friday'
 when 6 then 'Saturday'
 end as day_name,
 count(*) as total_trips,
 round(avg(passenger_count)::numeric, 1) as avg_passengers,
 round(avg(total_amount)::numeric, 2) as avg_fare,
 round(sum(total_amount)::numeric, 2) as total_revenue,
 round(avg(revenue_per_mile)::numeric,2) as avg_revenue_per_mile
from enriched
group by pickup_hour, day_of_week
order by day_of_week, pickup_hour