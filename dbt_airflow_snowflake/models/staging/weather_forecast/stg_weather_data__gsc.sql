with
weather_data as (
    SELECT * FROM {{ source('weather_forecast', 'stg_weather_data')}}
),

weather_data_gsc as (
    
    SELECT
        CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', forecast_time) as forecast_time,
        CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', forecast_time)::DATE AS forecast_date,
        updated_date,
        updated_hour,
        location,
        precipitation,
        air_temperature,
        forecast_time AS forecast_day
    FROM
        weather_data
    WHERE
        location = 'Granja_Santa_Catarina'
    )

SELECT * FROM weather_data_gsc

