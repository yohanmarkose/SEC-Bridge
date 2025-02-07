import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from sec_webpage_scraper.scrape import scrape_sec_data
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='sec_data_to_s3',
    default_args=default_args,
    description='Extract SEC financial data, transform it, and load into S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_scraper_and_transform(**kwargs):
        # year = kwargs['year']
        # quarter = kwargs['quarter']
        #base_path = f"/tmp/DAMG7245_Assignment02/data/{year}/{quarter}"  # Save locally in /tmp
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
        extracted_files = scrape_sec_data(year, quarter)
        
        transformed_files = []
        
        # Transform .txt files to .csv
        for file_path in extracted_files:
            file_name = os.path.basename(file_path)
            if file_name.endswith('.txt'):
                csv_file_path = os.path.splitext(file_path)[0] + '.csv'  # Change extension to .csv
                
                # Transform .txt to .csv using pandas
                try:
                    df_load = pd.read_table(file_path, delimiter="\t", low_memory=False)
                    df_load["year"] = year
                    df_load["quarter"] = quarter
                    df_load.to_csv(csv_file_path, index=False)
                    transformed_files.append(csv_file_path)
                    print(f"Transformed {file_name} to {csv_file_path}")
                except Exception as e:
                    print(f"Failed to transform {file_name}: {e}")

        # Upload transformed .csv files to S3
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        for csv_file_path in transformed_files:
            csv_file_name = os.path.basename(csv_file_path)
            s3_key = f"DAMG7245_Assignment02/data/{year}/{quarter}/{csv_file_name}"
            s3_hook.load_file(filename=csv_file_path, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"Uploaded {csv_file_name} to S3 at {s3_key}")

    scrape_and_transform_task = PythonOperator(
        task_id='scrape_transform_and_upload_sec_data',
        python_callable=run_scraper_and_transform,
        #op_kwargs={'year': 2024, 'quarter': 3},
        provide_context=True,
    )

scrape_and_transform_task