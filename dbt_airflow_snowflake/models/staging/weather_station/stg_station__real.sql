WITH 

weather_station AS(
SELECT *
FROM {{ref('dados_estacao_gsc')}}
),

weather_station_fin AS (
    SELECT
        estacao,
        data,
        precipitacao,
        temp_media,
        temp_max,
        temp_min
    FROM  weather_station -- Nome da tabela que foi criada a partir do seed
)

SELECT *
FROM weather_station_fin

