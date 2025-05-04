{{ config(materialized='table', schema='raw') }}

SELECT
    $1::TIMESTAMP           AS time,
    $2::VARCHAR(13)         AS updated_time, 
    $3::DATE                AS updated_date,
    $4::INTEGER             AS updated_hour,
    $5::VARCHAR(150)        AS location,
    $6::FLOAT               AS precipitation,
    $7::FLOAT               AS air_pressure_sea_level,
    $8::FLOAT               AS air_temperature,
    $9::FLOAT               AS cloud_area_fraction,
    $10::FLOAT              AS relative_humidity,
    $11::FLOAT              AS wind_from_direction,
    $12::FLOAT              AS wind_speed
FROM @DBT_AIRFLOW_DB.DBT_SCHEMA.my_s3_stage