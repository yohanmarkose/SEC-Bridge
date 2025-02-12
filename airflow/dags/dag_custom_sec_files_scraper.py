from io import BytesIO
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_parquet import parquet_transformer
from sec_scraper.convert_to_csv import csv_transformer  

# Fetch Airflow variable
bucket_name = Variable.get("s3_bucket_name")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='sec_data_to_s3_scraper',
    default_args=default_args,
    tags=['sec_datapipelines'],
    description='Extract SEC financial data, transform it, and load into S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_scraper_and_transform(**kwargs):
        source = kwargs['dag_run'].conf.get('source')
        print(source)
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split("Q")[1]

        extracted_files = scrape_sec_data(year, quarter)
        csv_transformed_files = csv_transformer(extracted_files, year, quarter)
        parquet_transformed_files = parquet_transformer(extracted_files, year, quarter)

        all_transformed_files = csv_transformed_files + parquet_transformed_files

        # Upload transformed files to S3
        s3_hook = S3Hook(aws_conn_id='aws_default')

        for file_name, file_bytes in all_transformed_files:
            file_type = file_name.split(".")[1]
            s3_key = f"data/{year}/{quarter}/{file_type}/{file_name}"
            if isinstance(file_bytes, BytesIO):
                file_obj = file_bytes
            else:
                file_obj = BytesIO(file_bytes)
            s3_hook.load_file_obj(file_obj=file_obj, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"Uploaded {file_name} to S3 at {s3_key}")

        print("Scraping and transformed data uploaded to S3 successfully!")

    scrape_and_transform_task = PythonOperator(
        task_id='scrape_transform_and_upload_sec_data',
        python_callable=run_scraper_and_transform,
    )

    scrape_and_transform_task