CREATE TABLE IF NOT EXISTS dbt_airflow_db.dbt_schema.stg_weather_data (
            forecast_time           TIMESTAMP,
            updated_time            VARCHAR(13),
            updated_date            DATE,
            updated_hour            INTEGER,
            location                VARCHAR(150),
            precipitation           FLOAT,
            air_pressure_sea_level  FLOAT,
            air_temperature         FLOAT,
            cloud_area_fraction     FLOAT,
            relative_humidity       FLOAT,
            wind_from_direction     FLOAT,
            wind_speed              FLOAT
        );
COPY INTO dbt_airflow_db.dbt_schema.stg_weather_data
FROM @dbt_airflow_db.dbt_schema.my_s3_stage/weather_combined_Granja_Santa_Catarina.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);