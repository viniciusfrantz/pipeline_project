{{ config(materialized='view', schema='staging') }}

with base as (
    select 
        CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', forecast_time) as forecast_time,
        updated_date,
        updated_hour,
        location,
        precipitation
    from {{ ref('raw_weather_forecast') }}
    where location = 'Granja_Santa_Catarina'
),

final as (
    select 
        forecast_time,
        updated_hour,
        forecast_time::date as forecast_day,
        updated_date,
        location,
        precipitation
    from base
)

select * from final