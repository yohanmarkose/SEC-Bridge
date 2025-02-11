import os
from io import BytesIO, StringIO
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from sec_json_converter.json_convert import load_data 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='json_convert_and_upload_to_s3',
    default_args=default_args,
    description='Convert txt files into JSON and load into S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_converter(**kwargs):
        # year = kwargs['year']
        # quarter = kwargs['quarter']
        year = "2024"
        quarter = "1"
        csv_base_path = f"data/{year}/{quarter}/csv"
        json_base_path = f"data/{year}/{quarter}/json"
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=csv_base_path)
        csv_files = [key for key in keys if key.endswith('.csv')]
        
        print(csv_files)
        
        files = []
        
        for file_key in csv_files:
            try:
                print("Reading file: ", file_key)
                file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                # Read file content from S3
                file_content = file_obj.get()["Body"].read()
                print(file_content[:100])  # Print first 100 bytes for debugging
                # Convert binary content to BytesIO
                file_buffer = BytesIO(file_content)
                # Convert content to a Pandas DataFrame
                # df = pd.read_csv(StringIO(file_content), low_memory=False)
                files.append((file_key, file_buffer))
                print(f"File from {file_key}: {file_buffer}")
            except Exception as e:
                print(f"Error reading {file_key}: {e}")        
        
        dataframes = load_data(files)
        print(dataframes.keys())

    convert_task = PythonOperator(
        task_id='json_convert_and_upload_to_s3',
        python_callable=run_converter,
        op_kwargs={'year': 2024, 'quarter': 4},
        provide_context=True,
    )

convert_task
