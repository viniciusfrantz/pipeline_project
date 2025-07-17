from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

def send_test_email():
    send_email(
        to=["viniciusfrantz@hotmail.com"],
        subject="✅ Teste de Email - Airflow",
        html_content="<p>Este é um teste de envio de email via Airflow usando a conexão SMTP configurada.</p>",
        smtp_conn_id='gmail_conn'  # 👉 Aqui você usa o nome da sua conexão criada no UI
    )

with DAG(
    dag_id='test_email_dag',
    schedule = '0 11 * * *',
    catchup=False,
) as dag:

    test_email_task = PythonOperator(
        task_id='send_test_email_task',
        python_callable=send_test_email,
    )