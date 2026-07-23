with source as (

    select * from {{ source('gold', 'ext_demand_forecast') }}

),

renamed as (

    select
        cast(forecast_date as date) as forecast_date,
        actual_trip_count,
        predicted_trip_count,
        predicted_lower_bound,
        predicted_upper_bound,
        is_future

    from source

)

select * from renamed
