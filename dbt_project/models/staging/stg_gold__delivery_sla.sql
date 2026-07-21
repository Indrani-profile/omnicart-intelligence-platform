with source as (

    select * from {{ source('gold', 'ext_delivery_sla') }}

),

renamed as (

    select
        pickup_date,
        pickup_location_id,
        total_trips,
        on_time_trips,
        delayed_trips,
        on_time_rate,
        avg_delay_minutes

    from source

)

select * from renamed
