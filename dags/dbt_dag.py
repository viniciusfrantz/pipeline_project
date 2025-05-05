from datetime import datetime
from airflow import DAG
from cosmos import DbtDag, ProfileConfig, ExecutionConfig, ProjectConfig
from include.constants import dbt_snowflake_project_path, venv_execution_config
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config_dbt = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",  # ← este é o nome da conexão no Airflow
        profile_args={
            "database": "dbt_airflow_db",
            "schema": "dbt_schema"
        }
    )
)

dbt_snowflake_dag = DbtDag(    
    project_config=ProjectConfig(dbt_snowflake_project_path),
    profile_config = profile_config_dbt,
    execution_config=venv_execution_config,
    operator_args={"install_deps":True},
    schedule="@daily",
    start_date=datetime(2025, 5, 1),
    catchup=False,
    dag_id="dbt_snowflake_dag",
    tags=['dbt'],
)
