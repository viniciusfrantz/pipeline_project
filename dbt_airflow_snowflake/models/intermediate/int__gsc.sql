with base as (
    SELECT
        *
    FROM {{ref('stg_weather_data__gsc')}}
        
    
),

agg_mean as (
    select 
        updated_date,
        forecast_time,
        forecast_date,
        AVG(precipitation) AS avg_precipitation
    from base
    GROUP BY updated_date,forecast_time, forecast_date
),

agg_sum AS (
    SELECT
        updated_date,
        forecast_date,
        ROUND(SUM(avg_precipitation), 2) as daily_precipitation_forecast
    FROM
        agg_mean
    GROUP BY updated_date, forecast_date
),

final AS(
    SELECT
        *,
        DATEDIFF(DAY, updated_date, forecast_date) AS days_diff
    FROM
        agg_sum
),

pivoted AS (
    SELECT
        forecast_date,
        MAX(CASE WHEN days_diff = 0 THEN daily_precipitation_forecast END) AS prev_d0,
        MAX(CASE WHEN days_diff = 1 THEN daily_precipitation_forecast END) AS prev_d1,
        MAX(CASE WHEN days_diff = 2 THEN daily_precipitation_forecast END) AS prev_d2,
        MAX(CASE WHEN days_diff = 3 THEN daily_precipitation_forecast END) AS prev_d3,
        MAX(CASE WHEN days_diff = 4 THEN daily_precipitation_forecast END) AS prev_d4,
        MAX(CASE WHEN days_diff = 5 THEN daily_precipitation_forecast END) AS prev_d5,
        MAX(CASE WHEN days_diff = 6 THEN daily_precipitation_forecast END) AS prev_d6,
        MAX(CASE WHEN days_diff = 7 THEN daily_precipitation_forecast END) AS prev_d7,
        MAX(CASE WHEN days_diff = 8 THEN daily_precipitation_forecast END) AS prev_d8,
        MAX(CASE WHEN days_diff = 9 THEN daily_precipitation_forecast END) AS prev_d9
    FROM final
    GROUP BY forecast_date
)

SELECT * FROM pivoted
