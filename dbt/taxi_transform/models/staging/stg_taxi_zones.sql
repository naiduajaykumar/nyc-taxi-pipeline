with source as (
    -- ref() because dbt loaded this via dbt seed, not Python
    select * from {{ ref("taxi_zones") }}
),
renamed as (
    select
        "LocationID"::integer as location_id,
        "Borough" as borough,
        "Zone" as zone_name,
        "service_zone" as service_zone,
        -- Calculate airport flag once here.
        -- All 3 mart models get is_airport for free.
        case
            when "Zone" ilike '%Airport%'
                or "service_zone" = 'EWR'
            then true
            else false
        end as is_airport
    from source
)
select * from renamed