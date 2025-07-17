from airflow.utils.email import send_email
from airflow.models import TaskInstance
from airflow.utils.state import State
from datetime import datetime
import logging

def dag_success_callback(**context):
    dag_run = context.get('dag_run' )
    print("Running dag_success_callback") 
    
    subject = f"✅ DAG Success: {dag_run.dag_id}"
    body = f"""
    DAG: {dag_run.dag_id}
    Run ID: {dag_run.run_id}
    Logical Date: {dag_run.logical_date}
    Status: SUCCESS ✅
    """

    send_email(to=["viniciusfrantz@hotmail.com"], 
               subject=subject, 
               html_content=body,
               smtp_conn_id='gmail_conn')

def dag_failure_callback(context):
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    execution_date = dag_run.execution_date

    # Coletar falhas por task
    session = context['session']
    failed_tasks = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dag_id)
        .filter(TaskInstance.execution_date == execution_date)
        .filter(TaskInstance.state == State.FAILED)
        .all()
    )

    details = ""
    for task in failed_tasks:
        details += f"""
        - Task ID: {task.task_id}
        - Start: {task.start_date}
        - End: {task.end_date}
        - Log URL: {task.log_url}
        ----------------------------
        """

    subject = f"❌ DAG Failed: {dag_id}"
    body = f"""
    DAG: {dag_id}
    Run ID: {run_id}
    Execution Date: {execution_date}
    Status: FAILED ❌

    Failed Task Details:
    {details}
    """

    send_email(to=["viniciusfrantz@hotmail.com"], subject=subject, html_content=body)