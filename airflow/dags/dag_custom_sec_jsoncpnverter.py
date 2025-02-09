import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='json_convert_and upload_to_s3',
    default_args=default_args,
    description='Convert txt files into JSON and load into S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_converter(**kwargs):
        year = kwargs['year']
        quarter = kwargs['quarter']
        base_path = f"{year}/{quarter}/json"
        extracted_files = (year, quarter, base_path)
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        for file_name, file_bytes in extracted_files:
            s3_key = f"{base_path}/{file_name}"
            s3_hook.load_file_obj(file_obj=file_bytes, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"Uploaded {file_name} to S3 at {s3_key}")

    scrape_task = PythonOperator(
        task_id='json_convert_and_upload_to_s3',
        python_callable=run_converter,
        op_kwargs={'year': 2024, 'quarter': 4},
        provide_context=True,
    )

scrape_task
