-- Exposes the Prophet forecast alongside a per-day pct_error for historical
-- rows, so the dashboard can visualize accuracy trend over time (e.g. did
-- error spike around specific periods), not just a single static MAPE
-- headline. pct_error is null for future rows, since there's no actual
-- value to compare against yet.

with forecast as (

    select * from {{ ref('stg_gold__demand_forecast') }}

),

with_error as (

    select
        forecast_date,
        actual_trip_count,
        predicted_trip_count,
        predicted_lower_bound,
        predicted_upper_bound,
        is_future,
        case
            when not is_future then abs(actual_trip_count - predicted_trip_count) / actual_trip_count
            else null
        end as pct_error

    from forecast

)

select * from with_error
order by forecast_date
