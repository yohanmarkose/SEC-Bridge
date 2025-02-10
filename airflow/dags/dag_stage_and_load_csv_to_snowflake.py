import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable

# Fetch variables from Airflow Variables
bucket_name = Variable.get("s3_bucket_name")
AWS_KEY_ID = Variable.get("AWS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
s3_path = f"s3://{bucket_name}"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='stage_and_load_csv_to_snowflake',
    default_args=default_args,
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Create External Stage in Snowflake
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_v2',
        sql=f"""
            USE ROLE dbt_role;
            CREATE OR REPLACE STAGE sec_s3_stage_csv
            URL='{s3_path}'
            CREDENTIALS = (
                AWS_KEY_ID = '{AWS_KEY_ID}',
                AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
            );
        """
    )

    # Task 2: Create File Format for CSV Files
    create_fileformat_csv = SnowflakeOperator(
        task_id='create_fileformat_csv',
        snowflake_conn_id='snowflake_v2',
        sql="""
            CREATE OR REPLACE FILE FORMAT csv_fileformat
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            NULL_IF = ('', 'NULL')
            PARSE_HEADER = TRUE
            DATE_FORMAT = 'YYYYMMDD';
        """
    )

    # Task 3: Create Schema Dynamically for "sub" Table
    def schema_def_sub(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
        return f"""
            CREATE OR REPLACE TABLE sub_{year}_{quarter}_csv (
                    adsh VARCHAR PRIMARY KEY, -- Unique submission ID
                    cik VARCHAR, -- Central Index Key (company identifier)
                    name VARCHAR, -- Company name
                    sic VARCHAR, -- Standard Industrial Classification code
                    countryba VARCHAR, -- Country of business address
                    stprba VARCHAR, -- State/province of business address
                    cityba VARCHAR, -- City of business address
                    zipba VARCHAR, -- ZIP code of business address
                    bas1 VARCHAR, -- Business address street 1
                    bas2 VARCHAR, -- Business address street 2
                    baph VARCHAR, -- Business address phone
                    countryma VARCHAR, -- Country of mailing address
                    stprma VARCHAR, -- State/province of mailing address
                    cityma VARCHAR, -- City of mailing address
                    zipma VARCHAR, -- ZIP code of mailing address
                    mas1 VARCHAR, -- Mailing address street 1
                    mas2 VARCHAR, -- Mailing address street 2
                    countryinc VARCHAR, -- Country of incorporation
                    stprinc VARCHAR, -- State/province of incorporation
                    ein VARCHAR, -- Employer Identification Number
                    former VARCHAR, -- Former company name
                    changed VARCHAR, -- Date of name change
                    afs VARCHAR, -- Accounting standards (e.g., 1-LAF, 4-NON)
                    wksi VARCHAR, -- Well-known seasoned issuer (0 or 1)
                    fye VARCHAR, -- Fiscal year end (e.g., 1231 for December 31)
                    form VARCHAR, -- Form type (e.g., 10-Q, 10-K)
                    period VARCHAR, -- Period end date
                    fy VARCHAR, -- Fiscal year
                    fp VARCHAR, -- Fiscal period (e.g., Q1, Q2, Q3, Q4)
                    filed VARCHAR, -- Filing date
                    accepted VARCHAR, -- Time accepted by SEC
                    prevrpt VARCHAR, -- Previous report (0 or 1)
                    detail VARCHAR, -- Detailed submission (0 or 1)
                    instance VARCHAR, -- Instance document name
                    nciks VARCHAR, -- Number of CIKs
                    aciks VARCHAR NULL, -- Additional CIKs (comma-separated)
                    year INT,
                    quarter INT
            );
        """

    generate_schema_task = PythonOperator(
        task_id='generate_schema_sql',
        python_callable=schema_def_sub,
        provide_context=True,
        dag=dag
    )

    schema_creation_sub = SnowflakeOperator(
        task_id='schema_creation_sub',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_schema_sql') }}",
        dag=dag
    )

    # Task 4: Copy Data from S3 to Snowflake Table Dynamically for "sub"
    def copyinto_sub(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
        return f"""
            COPY INTO sub_{year}_{quarter}_csv
            FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/sub.csv
            FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
    generate_copy_task = PythonOperator(
        task_id='generate_copy_sql',
        python_callable=copyinto_sub,
        provide_context=True,
        dag=dag
    )

    copy_from_s3_sub = SnowflakeOperator(
        task_id='copy_from_s3_sub',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_copy_sql') }}",
        dag=dag
    )
    # Define task dependencies in the correct order:
    create_stage >> create_fileformat_csv >> generate_schema_task >> schema_creation_sub >> generate_copy_task >> copy_from_s3_sub
