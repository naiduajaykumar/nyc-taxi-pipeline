-- stg_taxi_trips.sql
with source as (
    select * from {{ source("raw", "yellow_taxi_trips") }}
),
renamed as (
    select
        -- IDs: rename cryptic names to readable names
        vendorid as vendor_id,
        ratecodeid as rate_code_id,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        -- Timestamps: shorter names
        tpep_pickup_datetime::timestamp as pickup_at,
        tpep_dropoff_datetime::timestamp as dropoff_at,
        -- Trip details (keep as-is)
        passenger_count,
        trip_distance,
        -- Payment: decode integer code to human label
        payment_type,
        case payment_type
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            else 'Other'
        end as payment_method,
        -- Fares
        fare_amount,
        tip_amount,
        total_amount,
        congestion_surcharge
        
    from source
    where fare_amount > 0 -- remove refunds and bad rows
        and trip_distance >= 0
        and tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
)
select * from renamed