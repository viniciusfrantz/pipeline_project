with 
final_table as(
    SELECT
        forecast_gsc.*,
        station.precipitacao
    FROM {{ref('int__gsc')}} forecast_gsc
    LEFT JOIN {{ref('stg_station__real')}} station
        ON station.data=forecast_gsc.forecast_date
    ORDER BY
        forecast_date ASC
    )
SELECT * FROM final_table
