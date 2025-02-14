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
    dag_id='sec_raw_data_to_snowflake_v1',
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

    # Processing Raw Tables

    #Processing Num table
    def table_def_num(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split("Q")[1]
        return f"""
            CREATE TABLE STG_NUM_{year}_Q{quarter} AS
            SELECT
                adsh AS submission_id,
                tag AS tag,
                version as version,
                TRY_TO_DATE(ddate, 'YYYYMMDD') AS period_end_date, 
                TRY_CAST(qtrs AS NUMBER) AS num_quaters_covered,
                uom AS unit,
                segments as segments,
                coreg as coreg,
                TRY_CAST(value AS NUMBER) AS reported_amount,
                footnote as footnote
            FROM NUM_{year}_Q{quarter}
        """
    generate_table_stg_num = PythonOperator(
        task_id='generate_table_stg_num',
        python_callable=table_def_num,
        provide_context=True,
    )

    table_creation_stg_num = SnowflakeOperator(
        task_id='table_creation_stg_num',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_num') }}"
    )

    # Processing the raw sub table
    def table_def_sub(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split("Q")[1]
        return f"""
            CREATE TABLE STG_SUB_{year}_Q{quarter} AS
            SELECT
                adsh AS submission_id,
                TRY_CAST(cik AS NUMBER) AS company_id,
                name AS company_name,
                TRY_CAST(sic AS NUMBER) AS sic_code,
                countryba AS business_country,
                stprba AS business_state,
                cityba AS business_city,
                zipba AS business_zip,
                bas1 AS business_add_1,
                bas2 AS business_add_2,
                baph AS business_ph_no,
                countryma AS mailing_country,
                stprma AS mailing_state,
                cityma AS mailing_city,
                zipma AS mailing_zip,
                mas1 AS registrant_mail_add_1,
                mas2 AS registrant_mail_add_2,
                countryinc AS countryinc,
                stprinc AS state_prov_inc,
                TRY_CAST(ein AS NUMBER) as employer_id,
                former as former,
                changed as changed,
                afs as afs,
                wksi as wksi,
                fye as fye,
                form as form,
                TRY_TO_DATE(period, 'YYYYMMDD') AS period, 
                TRY_TO_DATE(filed, 'YYYYMMDD') AS filing_date,
                TRY_CAST(fy AS INTEGER) AS fiscal_year,
                fp AS fiscal_period,
                accepted as accepted,
                prevrpt as prevrpt,
                detail as detail,
                instance as instance,
                nciks as nciks,
                aciks as aciks
            FROM SUB_{year}_Q{quarter}
        """

    generate_table_stg_sub = PythonOperator(
        task_id='generate_table_stg_sub',
        python_callable=table_def_sub,
        provide_context=True,
    )

    table_creation_stg_sub = SnowflakeOperator(
        task_id='table_creation_stg_sub',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_sub') }}"
    )

    # Processing the raw pre table
    def table_def_pre(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split("Q")[1]
        return f"""
            CREATE TABLE STG_PRE_{year}_Q{quarter} AS
            SELECT
                adsh AS submission_id,
                TRY_CAST(report AS NUMBER) AS report,
                TRY_CAST(line AS NUMBER) AS line,
                stmt AS statement_type,
                TRY_CAST(inpth AS BOOLEAN) AS directly_reported,
                rfile AS rfile,
                tag AS tag,
                version as version,
                plabel AS preferred_label,
                TRY_CAST(negating AS BOOLEAN) AS negating
            FROM PRE_{year}_Q{quarter}
        """

    generate_table_stg_pre = PythonOperator(
        task_id='generate_table_stg_pre',
        python_callable=table_def_pre,
        provide_context=True,
    )

    table_creation_stg_pre = SnowflakeOperator(
        task_id='table_creation_stg_pre',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_pre') }}"
    )

    # Processing the raw tag table
    def table_def_tag(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split("Q")[1]
        return f"""
            CREATE TABLE STG_TAG_{year}_Q{quarter} AS
            SELECT
                tag AS tag,
                version as version,
                TRY_CAST(custom AS BOOLEAN) AS custom,
                TRY_CAST(abstract AS BOOLEAN) AS abstract,
                datatype as datatype,
                iord AS item_order,
                crdr AS balance_type,
                tlabel AS tag_label,
                doc AS documentation,
            FROM TAG_{year}_Q{quarter}
        """

    generate_table_stg_tag = PythonOperator(
        task_id='generate_table_stg_tag',
        python_callable=table_def_tag,
        provide_context=True,
    )

    table_creation_stg_tag = SnowflakeOperator(
        task_id='table_creation_stg_tag',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_tag') }}"
    )



    # Snowflake tasks: Create Stage and File Format
    scrape_task >> create_stage >> create_fileformat_csv 
    
    # Snowflake tasks: Create Schema and Copy Data for "sub" Table
    create_fileformat_csv >> generate_schema_sub >> schema_creation_sub >> generate_copy_sub >> copy_from_s3_sub >> generate_table_stg_sub >> table_creation_stg_sub

    # Snowflake tasks: Create Schema and Copy Data for "num" Table
    create_fileformat_csv >> generate_schema_num >> schema_creation_num >> generate_copy_num >> copy_from_s3_num >> generate_table_stg_num >> table_creation_stg_num

    # Snowflake tasks: Create Schema and Copy Data for "tag" Table
    create_fileformat_csv >> generate_schema_sql_tag >> schema_creation_tag >> generate_copy_sql_tag >> copy_from_s3_tag >> generate_table_stg_tag >> table_creation_stg_tag

    # Snowflake tasks: Create Schema and Copy Data for "pre" Table
    create_fileformat_csv >> generate_schema_sql_pre >> schema_creation_pre >> generate_copy_sql_pre >> copy_from_s3_pre >> generate_table_stg_pre >> table_creation_stg_pre