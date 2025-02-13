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
    dag_id='process_raw_tables',
    default_args=default_args,
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task 1: Process the raw num table
    def schema_def_num(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
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
            FROM NUM_{year}_Q{quarter}_csv
            );
        """

    generate_table_stg_num = PythonOperator(
        task_id='generate_table_stg_num',
        python_callable=schema_def_num,
        provide_context=True,
    )

    table_creation_num = SnowflakeOperator(
        task_id='table_creation_num',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_num') }}"
    )

    # Task 2: Process the raw sub table
    def schema_def_sub(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
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
            FROM SUB_{year}_Q{quarter}_csv
            );
        """

    generate_table_stg_sub = PythonOperator(
        task_id='generate_table_stg_sub',
        python_callable=schema_def_sub,
        provide_context=True,
    )

    table_creation_sub = SnowflakeOperator(
        task_id='table_creation_sub',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_sub') }}"
    )

    # Task 3: Process the raw pre table
    def schema_def_pre(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
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
            FROM PRE_{year}_Q{quarter}_csv
            );
        """

    generate_table_stg_pre = PythonOperator(
        task_id='generate_table_stg_pre',
        python_callable=schema_def_pre,
        provide_context=True,
    )

    table_creation_pre = SnowflakeOperator(
        task_id='table_creation_pre',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_pre') }}"
    )

    # Task 4: Process the raw tag table
    def schema_def_tag(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter')
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
            FROM TAG_{year}_Q{quarter}_csv
            );
        """

    generate_table_stg_tag = PythonOperator(
        task_id='generate_table_stg_tag',
        python_callable=schema_def_tag,
        provide_context=True,
    )

    table_creation_tag = SnowflakeOperator(
        task_id='table_creation_tag',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_table_stg_tag') }}"
    )


    