with source as (

    select * from {{ source('gold', 'ext_order_status_current') }}

),

renamed as (

    select
        order_id,
        delivery_id,
        pickup_location_id,
        vendor_id,
        order_value,
        current_status,
        last_event_timestamp,
        updated_at

    from source

)

select * from renamed
