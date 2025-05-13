from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 


from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProfileConfig, ExecutionConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from include.constants import dbt_snowflake_project_path, venv_execution_config
from include.weather_utils import get_weather_data, normalize_csv


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2025, 5, 1),
    'catchup': False
}


with DAG(
    'weather_dag',
    default_args=default_args,
    schedule = '0 11 * * *',
    catchup=False,
    tags=['weather', 's3'],
    description="get weather data and save in S3 bucket, then run dbt models"
    ) as dag:


    def load_weather_data():
        s3_hook = S3Hook(aws_conn_id='aws_conn')  # S3 connection
        bucket_name = 'vinicius-airflow'  # S3 Bucket
        object_key = 'weather_forecast/weather_combined_Granja_Santa_Catarina.csv'  # File Name (S3)
        get_weather_data(s3_hook, bucket_name, object_key)


    get_weather_data_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
        retries=2,
        retry_delay=timedelta(seconds=10)
    )


    def normalize_seed_file():
        normalize_csv('dados_estacao_gsc.csv')

    normalize_csv_task = PythonOperator(
        task_id='normalize_ws_seed_file',
        python_callable=normalize_seed_file,
        )

     
    copy_into_task = SnowflakeOperator(
        task_id='copy_into_staging',
        sql=open('/usr/local/airflow/include/copy_into_stg_weather.sql').read(),
        snowflake_conn_id='snowflake_conn'
    )


    # Define task to run dbt seed
    #dbt_seed_task = DbtSeedOperator(
    #   task_id="dbt_seed",
    #    project_dir=dbt_snowflake_project_path,
    #    profile_config=profile_config_dbt,
    #    execution_config=venv_execution_config,
    #   operator_args={"install_deps": True},
#)

    # Defined inside the DAG to avoid import timeout issues during DAG parsing.
    profile_config_dbt = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",  # ← nome da conexão no Airflow
        profile_args={
            "database": "dbt_airflow_db",
            "schema": "dbt_schema"
            }
        )
    )


    dbt_task_group = DbtTaskGroup(
        group_id="dbt_snowflake_models",
        project_config=ProjectConfig(dbt_snowflake_project_path),
        profile_config=profile_config_dbt,
        execution_config=venv_execution_config,
        operator_args={"install_deps": True},
    )


    export_results_to_s3_task = SnowflakeOperator(
    task_id='export_results_forecast_to_s3',
    sql="""
        COPY INTO @my_s3_stage/results_forecast_export
        FROM DBT_AIRFLOW_DB.DBT_SCHEMA_MARTS.RESULTS_FORECAST
        FILE_FORMAT = (TYPE = 'PARQUET')
        SINGLE = TRUE
        HEADER = TRUE
        OVERWRITE = TRUE;
    """,
    snowflake_conn_id='snowflake_conn',
)

    get_weather_data_task >> [normalize_csv_task, copy_into_task] >> dbt_task_group >> export_results_to_s3_task
