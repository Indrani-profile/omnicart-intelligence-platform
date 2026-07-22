-- Aggregates order outcomes by vendor: volume, delivered vs. cancelled
-- rate, and average order value. This is the operational counterpart to
-- mart_weather_delay_correlation — an "is this vendor healthy" view
-- rather than a system-wide analytical question.

with orders as (

    select * from {{ ref('stg_gold__order_status_current') }}

),

by_vendor as (

    select
        vendor_id,
        count(*) as total_orders,
        sum(case when current_status = 'delivered' then 1 else 0 end) as delivered_orders,
        sum(case when current_status = 'cancelled' then 1 else 0 end) as cancelled_orders,
        div0(
            sum(case when current_status = 'delivered' then 1 else 0 end),
            count(*)
        ) as delivered_rate,
        div0(
            sum(case when current_status = 'cancelled' then 1 else 0 end),
            count(*)
        ) as cancelled_rate,
        avg(order_value) as avg_order_value,
        sum(order_value) as total_order_value,
        min(last_event_timestamp) as first_order_timestamp,
        max(last_event_timestamp) as last_order_timestamp

    from orders
    group by vendor_id

)

select * from by_vendor
order by total_orders desc
