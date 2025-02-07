from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def sample_task():
    print("This is a test task!")

with DAG(
    dag_id='test_dag',
    start_date=datetime(2025, 2, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='sample_task',
        python_callable=sample_task,
    )