import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from sec_webpage_scraper.scrape import scrape_sec_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='sec_data_to_s3',
    default_args=default_args,
    description='Extract SEC financial data and load into S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_scraper(**kwargs):
        year = kwargs['year']
        quarter = kwargs['quarter']
        base_path = f"DAMG7245_Assignment02/data/{year}/{quarter}"
        extracted_files = scrape_sec_data(year, quarter, base_path)
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        for file_path in extracted_files:
            file_name = os.path.basename(file_path)
            s3_key = f"{base_path}/{file_name}"
            s3_hook.load_file(filename=file_path, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"Uploaded {file_name} to S3 at {s3_key}")

    scrape_task = PythonOperator(
        task_id='scrape_and_upload_sec_data',
        python_callable=run_scraper,
        op_kwargs={'year': 2024, 'quarter': 3},
        provide_context=True,
    )

scrape_task