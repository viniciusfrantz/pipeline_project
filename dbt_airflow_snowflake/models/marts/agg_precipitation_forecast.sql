{{ config(materialized='table', schema='marts') }}

with forecast as (
    select * from {{ ref('stg_weather_forecast') }}
),

agg_1 as (
    select 
        updated_date,
        forecast_time,
        forecast_day,
        avg(precipitation) as avg_precipitation
    from forecast
    group by updated_date, forecast_time, forecast_day
),

agg_2 as (
    select 
        updated_date,
        forecast_day,
        sum(avg_precipitation) as total_precipitation
    from agg_1
    group by updated_date, forecast_day
),

final as (
    select 
        *,
        datediff(day, forecast_day, updated_date) as days_diff,
        round(total_precipitation, 2) as precipitation
    from agg_2
)

select 
    updated_date,
    forecast_day as date,
    precipitation,
    days_diff
from final