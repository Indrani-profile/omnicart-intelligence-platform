-- Answers the project's core cross-domain question directly: does weather
-- affect delivery delays? Two angles:
--   1. Linear correlation coefficients (CORR) between continuous weather
--      metrics (precipitation, snowfall) and delay outcomes.
--   2. A categorical breakdown by weather_severity tier, with each tier's
--      avg_delay_minutes and on_time_rate compared against the 'Clear'
--      day baseline as an explicit lift/delta — answers "how much worse,
--      specifically" rather than leaving the reader to eyeball a table.

with daily as (

    select * from {{ ref('int_daily_performance') }}

),

correlations as (

    select
        corr(precipitation_mm, avg_delay_minutes) as corr_precipitation_delay,
        corr(snowfall_cm, avg_delay_minutes) as corr_snowfall_delay,
        corr(precipitation_mm, on_time_rate) as corr_precipitation_on_time_rate,
        corr(snowfall_cm, on_time_rate) as corr_snowfall_on_time_rate

    from daily

),

clear_baseline as (

    select
        avg(avg_delay_minutes) as baseline_avg_delay_minutes,
        avg(on_time_rate) as baseline_on_time_rate

    from daily
    where weather_severity = 'Clear'

),

by_severity as (

    select
        daily.weather_severity,
        count(*) as day_count,
        sum(daily.trip_count) as total_trips,
        avg(daily.avg_delay_minutes) as avg_delay_minutes,
        avg(daily.on_time_rate) as on_time_rate,
        avg(daily.avg_delay_minutes) - clear_baseline.baseline_avg_delay_minutes as delay_lift_vs_clear_minutes,
        avg(daily.on_time_rate) - clear_baseline.baseline_on_time_rate as on_time_rate_delta_vs_clear

    from daily
    cross join clear_baseline
    group by daily.weather_severity, clear_baseline.baseline_avg_delay_minutes, clear_baseline.baseline_on_time_rate

)

select
    by_severity.weather_severity,
    by_severity.day_count,
    by_severity.total_trips,
    by_severity.avg_delay_minutes,
    by_severity.on_time_rate,
    by_severity.delay_lift_vs_clear_minutes,
    by_severity.on_time_rate_delta_vs_clear,
    correlations.corr_precipitation_delay,
    correlations.corr_snowfall_delay,
    correlations.corr_precipitation_on_time_rate,
    correlations.corr_snowfall_on_time_rate

from by_severity
cross join correlations

order by by_severity.avg_delay_minutes desc
