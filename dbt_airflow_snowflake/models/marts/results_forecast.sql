with 
final_table as(
    SELECT
        last_updated_date,
        CAST(COALESCE(forecast_gsc.forecast_date, station.data) AS DATE) AS forecast_date,
        forecast_gsc.prev_d0,
        forecast_gsc.prev_d1,
        forecast_gsc.prev_d2,
        forecast_gsc.prev_d3,
        forecast_gsc.prev_d4,
        forecast_gsc.prev_d5,
        forecast_gsc.prev_d6,
        forecast_gsc.prev_d7,
        forecast_gsc.prev_d8,
        forecast_gsc.prev_d9,
        station.precipitacao
    FROM {{ref('int__gsc')}} forecast_gsc
    FULL OUTER JOIN {{ref('stg_station__real')}} station
        ON station.data=forecast_gsc.forecast_date
    )
SELECT * 
FROM final_table 
ORDER BY
    forecast_date ASC

