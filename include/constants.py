from pathlib import Path
from cosmos import ExecutionConfig

dbt_snowflake_project_path = Path("/usr/local/airflow/dbt_airflow_snowflake")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)

# getting data from API
weather_csv_file = "weather_combined_Granja_Santa_Catarina.csv"
s3_bucket_path = "s3://meu-bucket-de-dados/weather/"  # atualize com seu caminho
snowflake_table = "minha_tabela_weather"
snowflake_stage = "weather_stage"  # nome do STAGE já criado no Snowflake