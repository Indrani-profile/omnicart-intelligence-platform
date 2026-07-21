with source as (

    select * from {{ source('gold', 'ext_weather_delay_impact') }}

),

renamed as (

    select
        pickup_date,
        weather_severity,
        total_trips,
        on_time_trips,
        delayed_trips,
        on_time_rate,
        avg_delay_minutes

    from source

)

select * from renamed
