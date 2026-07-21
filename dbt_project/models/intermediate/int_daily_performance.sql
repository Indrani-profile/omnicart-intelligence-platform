-- Combines daily delivery summary, SLA performance, and weather impact
-- into a single daily-grain table. delivery_sla is grouped by
-- pickup_date + pickup_location_id, so it's aggregated up to just
-- pickup_date here to match the grain of the other two sources.
-- weather_delay_impact is grouped by pickup_date + weather_severity;
-- a given day only has one weather_severity value in this dataset,
-- so this is a safe 1:1 join, not a fan-out.

with daily_summary as (

    select * from {{ ref('stg_gold__daily_delivery_summary') }}

),

sla_daily as (

    select
        pickup_date,
        sum(total_trips) as total_trips,
        sum(on_time_trips) as on_time_trips,
        sum(delayed_trips) as delayed_trips,
        div0(sum(on_time_trips), nullif(sum(total_trips), 0)) as on_time_rate,
        avg(avg_delay_minutes) as avg_delay_minutes

    from {{ ref('stg_gold__delivery_sla') }}
    group by pickup_date

),

weather as (

    select
        pickup_date,
        weather_severity,
        on_time_rate as weather_on_time_rate,
        avg_delay_minutes as weather_avg_delay_minutes

    from {{ ref('stg_gold__weather_delay_impact') }}

),

joined as (

    select
        daily_summary.pickup_date,
        daily_summary.trip_count,
        daily_summary.total_revenue,
        daily_summary.precipitation_mm,
        daily_summary.snowfall_cm,
        daily_summary.temp_max_c,
        daily_summary.temp_min_c,
        sla_daily.total_trips as sla_total_trips,
        sla_daily.on_time_trips,
        sla_daily.delayed_trips,
        sla_daily.on_time_rate,
        sla_daily.avg_delay_minutes,
        weather.weather_severity,
        weather.weather_on_time_rate,
        weather.weather_avg_delay_minutes

    from daily_summary
    left join sla_daily on daily_summary.pickup_date = sla_daily.pickup_date
    left join weather on daily_summary.pickup_date = weather.pickup_date

)

select * from joined
