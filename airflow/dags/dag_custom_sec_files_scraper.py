from io import BytesIO
import base64
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_csv import csv_transformer  
from sec_scraper.convert_to_parquet import parquet_transformer

# Fetch Airflow variable
bucket_name = Variable.get("s3_bucket_name")
AWS_KEY_ID = Variable.get("AWS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
s3_path = f"s3://{bucket_name}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='sec_raw_data_to_snowflake',
    default_args=default_args,
    tags=['sec_datapipelines'],
    description='Extract SEC financial data, transform it, and load into S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_scraper_and_transform(**kwargs):
        source = kwargs['dag_run'].conf.get('source')
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split("Q")[1]

        # Scrape SEC data
        extracted_files = scrape_sec_data(year, quarter)
        
        # Serialize BytesIO objects to base64
        serialized_files = []
        for file_name, file_bytes in extracted_files:
            file_bytes.seek(0)  # Ensure the stream is at the beginning
            encoded_content = base64.b64encode(file_bytes.read()).decode('utf-8')
            serialized_files.append((file_name, encoded_content))
            
        # Push extracted files to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='extracted_files', value=serialized_files)
        kwargs['ti'].xcom_push(key='year', value=year)
        kwargs['ti'].xcom_push(key='quarter', value=quarter)
    
    scrape_task = PythonOperator(
        task_id='scrape_sec_data',
        python_callable=run_scraper_and_transform,
    )
            
    def convert_to_csv(**kwargs):
        ti = kwargs['ti']
        serialized_files = ti.xcom_pull(task_ids='scrape_sec_data', key='extracted_files')
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        
        # Deserialize base64 content back to BytesIO
        extracted_files = []
        for file_name, encoded_content in serialized_files:
            file_bytes = BytesIO(base64.b64decode(encoded_content))
            extracted_files.append((file_name, file_bytes))
        
        # Convert Extracted files to CSV
        csv_transformed_files = csv_transformer(extracted_files, year, quarter)
        
        # Serialize BytesIO objects to base64
        serialized_files = []
        for file_name, file_bytes in csv_transformed_files:
            file_bytes.seek(0)  # Ensure the stream is at the beginning
            encoded_content = base64.b64encode(file_bytes.read()).decode('utf-8')
            serialized_files.append((file_name, encoded_content))

        # Push transformed CSV files to XCom for upload task
        ti.xcom_push(key='csv_transformed_files', value=serialized_files)
    
    convert_to_csv_task = PythonOperator(
        task_id='convert_to_csv',
        python_callable=convert_to_csv,
    )
        
    def convert_to_parquet(**kwargs):
        ti = kwargs['ti']
        serialized_files = ti.xcom_pull(task_ids='scrape_sec_data', key='extracted_files')
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        
        # Deserialize base64 content back to BytesIO
        extracted_files = []
        for file_name, encoded_content in serialized_files:
            file_bytes = BytesIO(base64.b64decode(encoded_content))
            extracted_files.append((file_name, file_bytes))

        # Convert Extracted files to Parquet
        parquet_transformed_files = parquet_transformer(extracted_files, year, quarter)
        
        # Serialize BytesIO objects to base64
        serialized_files = []
        for file_name, file_bytes in parquet_transformed_files:
            file_bytes.seek(0)  # Ensure the stream is at the beginning
            encoded_content = base64.b64encode(file_bytes.read()).decode('utf-8')
            serialized_files.append((file_name, encoded_content))

        # Push transformed Parquet files to XCom for upload task
        ti.xcom_push(key='parquet_transformed_files', value=serialized_files)
        
    convert_to_parquet_task = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet,
    )

    def upload_to_s3(**kwargs):
        ti = kwargs['ti']
        serialized_csv_files = ti.xcom_pull(task_ids='convert_to_csv', key='csv_transformed_files')
        serialized_parquet_files = ti.xcom_pull(task_ids='convert_to_parquet', key='parquet_transformed_files')

        # Deserialize base64 content back to BytesIO
        csv_transformed_files = []
        for file_name, encoded_content in serialized_csv_files:
            file_bytes = BytesIO(base64.b64decode(encoded_content))
            csv_transformed_files.append((file_name, file_bytes))
            
        parquet_transformed_files = []
        for file_name, encoded_content in serialized_parquet_files:
            file_bytes = BytesIO(base64.b64decode(encoded_content))
            parquet_transformed_files.append((file_name, file_bytes))
        
        all_transformed_files = csv_transformed_files + parquet_transformed_files
        
        # Upload transformed files to S3
        s3_hook = S3Hook(aws_conn_id='aws_default')

        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')

        for file_name, file_bytes in all_transformed_files:
            file_type = file_name.split(".")[1]
            s3_key = f"data/{year}/{quarter}/{file_type}/{file_name}"
            if isinstance(file_bytes, BytesIO):
                file_obj = file_bytes
            else:
                file_obj = BytesIO(file_bytes)
            s3_hook.load_file_obj(file_obj=file_obj, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"Uploaded {file_name} to S3 at {s3_key}")

        print("All transformed data uploaded to S3 successfully!")
    
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )
    


    # Task dependencies
    scrape_task >> [convert_to_csv_task, convert_to_parquet_task]
    [convert_to_csv_task, convert_to_parquet_task] >> upload_task