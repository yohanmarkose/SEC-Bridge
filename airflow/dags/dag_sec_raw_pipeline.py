from io import BytesIO
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
        print("All files extracted successfully!")
        # Convert Extracted files to CSV
        csv_transformed_files = csv_transformer(extracted_files, year, quarter)
        print("All files converted to CSV successfully!")
        # Convert Extracted files to Parquet
        parquet_transformed_files = parquet_transformer(extracted_files, year, quarter)
        print("All files converted to Parquet successfully!")
        all_transformed_files = csv_transformed_files + parquet_transformed_files
        print("Uploading transformed files to S3...")
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
        kwargs['ti'].xcom_push(key='year', value=year)
        kwargs['ti'].xcom_push(key='quarter', value=quarter)
        print("All transformed data uploaded to S3 successfully!")
    
    scrape_task = PythonOperator(
        task_id='scrape_sec_data',
        python_callable=run_scraper_and_transform,
    )
                    
    
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
            DATE_FORMAT = 'YYYYMMDD'
            EMPTY_FIELD_AS_NULL = TRUE
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
        """
    )
    
    # Task 3: Create Schema for "sub" Table
    def schema_def_sub(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f"""
            CREATE OR REPLACE TABLE sub_{year}_Q{quarter} (
                    adsh VARCHAR PRIMARY KEY,
                    cik VARCHAR,
                    name VARCHAR,
                    sic VARCHAR,
                    countryba VARCHAR,
                    stprba VARCHAR,
                    cityba VARCHAR,
                    zipba VARCHAR,
                    bas1 VARCHAR,
                    bas2 VARCHAR,
                    baph VARCHAR,
                    countryma VARCHAR,
                    stprma VARCHAR,
                    cityma VARCHAR,
                    zipma VARCHAR,
                    mas1 VARCHAR,
                    mas2 VARCHAR,
                    countryinc VARCHAR,
                    stprinc VARCHAR,
                    ein VARCHAR,
                    former VARCHAR,
                    changed VARCHAR,
                    afs VARCHAR,
                    wksi VARCHAR,
                    fye VARCHAR,
                    form VARCHAR,
                    period VARCHAR,
                    fy VARCHAR,
                    fp VARCHAR,
                    filed VARCHAR,
                    accepted VARCHAR,
                    prevrpt VARCHAR,
                    detail VARCHAR,
                    instance VARCHAR,
                    nciks VARCHAR,
                    aciks VARCHAR NULL,
                    year INT,
                    quarter INT
            );
        """

    generate_schema_sub = PythonOperator(
        task_id='generate_schema_sql_sub',
        python_callable=schema_def_sub,
        provide_context=True,
    )

    schema_creation_sub = SnowflakeOperator(
        task_id='schema_creation_sub',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_schema_sql_sub') }}"
    )
    
    # Task 4: Copy Data from S3 to Snowflake for "sub"
    def copyinto_sub(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        # year = kwargs['dag_run'].conf.get('year')
        # quarter = kwargs['dag_run'].conf.get('quarter')
        return f"""
            COPY INTO sub_{year}_Q{quarter}
            FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/sub.csv
            FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """

    generate_copy_sub = PythonOperator(
        task_id='generate_copy_sql_sub',
        python_callable=copyinto_sub,
        provide_context=True,
    )

    copy_from_s3_sub = SnowflakeOperator(
        task_id='copy_from_s3_sub',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_sub') }}"
    )
    
    # Task 3: Create Schema for "num" Table
    def schema_def_num(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f""" 
            CREATE OR REPLACE TABLE num_{year}_Q{quarter} (
                adsh VARCHAR PRIMARY KEY,
                tag VARCHAR,
                version VARCHAR,
                ddate VARCHAR,
                qtrs VARCHAR,
                uom VARCHAR,
                segments VARCHAR,
                coreg VARCHAR,
                value VARCHAR,
                footnote VARCHAR,
                year INT,
                quarter INT
            );
        """

    generate_schema_num = PythonOperator(
        task_id='generate_schema_sql_num',
        python_callable=schema_def_num,
        provide_context=True,
    )

    schema_creation_num = SnowflakeOperator(
        task_id='schema_creation_num',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_schema_sql_num') }}"
    )

    # Task 4: Copy Data from S3 to Snowflake for "num"
    def copyinto_num(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f"""
            COPY INTO num_{year}_Q{quarter}
            FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/num.csv
            FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """

    generate_copy_num = PythonOperator(
        task_id='generate_copy_sql_num',
        python_callable=copyinto_num,
        provide_context=True,
    )

    copy_from_s3_num = SnowflakeOperator(
        task_id='copy_from_s3_num',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_num') }}"
    )
    
    # Task 3: Create Schema for "tag" Table
    def schema_def_tag(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f""" 
            CREATE OR REPLACE TABLE tag_{year}_Q{quarter} (
                tag VARCHAR,
                version VARCHAR,
                custom VARCHAR,
                abstract VARCHAR,
                datatype VARCHAR,
                iord VARCHAR,
                crdr VARCHAR,
                tlabel VARCHAR,
                doc VARCHAR,
                year INT,
                quarter INT
            );
        """

    generate_schema_sql_tag = PythonOperator(
        task_id='generate_schema_sql_tag',
        python_callable=schema_def_tag,
        provide_context=True,
    )

    schema_creation_tag = SnowflakeOperator(
        task_id='schema_creation_tag',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_schema_sql_tag') }}"
    )

   # Task 4: Copy Data from S3 to Snowflake for "tag"
    def copyinto_tag(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f"""
            COPY INTO tag_{year}_Q{quarter}
            FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/tag.csv
            FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """

    generate_copy_sql_tag = PythonOperator(
        task_id='generate_copy_sql_tag',
        python_callable=copyinto_tag,
        provide_context=True,
    )

    copy_from_s3_tag = SnowflakeOperator(
        task_id='copy_from_s3_tag',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_tag') }}"
    )

    # Task 3: Create Schema for "pre" Table
    def schema_def_pre(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f""" 
            CREATE OR REPLACE TABLE pre_{year}_Q{quarter} (
                adsh VARCHAR PRIMARY KEY,
                report VARCHAR,
                line VARCHAR,
                stmt VARCHAR,
                inpth VARCHAR,
                rfile VARCHAR,
                tag VARCHAR,
                version VARCHAR,
                plabel VARCHAR,
                negating VARCHAR,
                year INT,
                quarter INT
            );
        """

    generate_schema_sql_pre = PythonOperator(
        task_id='generate_schema_sql_pre',
        python_callable=schema_def_pre,
        provide_context=True,
    )

    schema_creation_pre = SnowflakeOperator(
        task_id='schema_creation_pre',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_schema_sql_pre') }}"
    )

   # Task 4: Copy Data from S3 to Snowflake for "pre"
    def copyinto_pre(**kwargs):
        ti = kwargs['ti']
        year = ti.xcom_pull(task_ids='scrape_sec_data', key='year')
        quarter = ti.xcom_pull(task_ids='scrape_sec_data', key='quarter')
        return f"""
            COPY INTO pre_{year}_Q{quarter}
            FROM @sec_s3_stage_csv/data/{year}/{quarter}/csv/pre.csv
            FILE_FORMAT = (FORMAT_NAME = csv_fileformat)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """

    generate_copy_sql_pre = PythonOperator(
        task_id='generate_copy_sql_pre',
        python_callable=copyinto_pre,
        provide_context=True,
    )

    copy_from_s3_pre = SnowflakeOperator(
        task_id='copy_from_s3_pre',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_pre') }}"
    )

    # Snowflake tasks: Create Stage and File Format
    scrape_task >> create_stage >> create_fileformat_csv 
    
    # Snowflake tasks: Create Schema and Copy Data for "sub" Table
    create_fileformat_csv >> generate_schema_sub >> schema_creation_sub >> generate_copy_sub >> copy_from_s3_sub
    
    # Snowflake tasks: Create Schema and Copy Data for "num" Table
    create_fileformat_csv >> generate_schema_num >> schema_creation_num >> generate_copy_num >> copy_from_s3_num

    # Snowflake tasks: Create Schema and Copy Data for "tag" Table
    create_fileformat_csv >> generate_schema_sql_tag >> schema_creation_tag >> generate_copy_sql_tag >> copy_from_s3_tag

    # Snowflake tasks: Create Schema and Copy Data for "pre" Table
    create_fileformat_csv >> generate_schema_sql_pre >> schema_creation_pre >> generate_copy_sql_pre >> copy_from_s3_pre