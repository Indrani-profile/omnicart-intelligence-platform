-- Staging model: light cleanup pass over the raw external table.
-- No business logic here — just column selection, renaming for clarity
-- where useful, and establishing this as the single point other models
-- reference instead of querying the source directly.

with source as (

    select * from {{ source('gold', 'ext_daily_delivery_summary') }}

),

renamed as (

    select
        pickup_date,
        trip_count,
        avg_fare_amount,
        total_revenue,
        avg_trip_distance_miles,
        avg_trip_duration_minutes,
        precipitation_mm,
        snowfall_cm,
        temp_max_c,
        temp_min_c

    from source

)

select * from renamed
