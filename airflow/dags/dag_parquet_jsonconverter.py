import os
from io import BytesIO
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='json_convert_via_parquet_and_upload_to_s3',
    default_args=default_args,
    description='Convert Parquet files into JSON and load into S3',
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def load_data(**kwargs):
        """Loads Parquet file paths from S3 and pushes them to XCom."""
        year = '2019'
        quarter = '1'
        parquet_base_path = f"data/{year}/{quarter}/parquet/"
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")

        parquet_files = {}
        for file_name in ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet"]:
            file_key = f"{parquet_base_path}{file_name}"
            if s3_hook.check_for_key(file_key, bucket_name):
                parquet_files[file_name] = file_key

        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)

    def get_df_length(**kwargs):
        """Reads the 'sub.parquet' file and pushes its length to XCom."""
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        parquet_files = ti.xcom_pull(task_ids='load_data', key='parquet_files')

        if 'sub.parquet' in parquet_files:
            file_obj = s3_hook.get_key(parquet_files['sub.parquet'], bucket_name)
            buffer = BytesIO(file_obj.get()["Body"].read())
            dfSub = pd.read_parquet(buffer)
            df_length = len(dfSub)
        else:
            df_length = 0

        ti.xcom_push(key='dfSub_length', value=df_length)

    def compute_range(task_index, num_chunks, **kwargs):
        """Computes start and end indices for each processing task."""
        ti = kwargs['ti']
        df_length = ti.xcom_pull(task_ids='get_df_length', key='dfSub_length')

        if df_length is None:
            raise ValueError("dfSub_length XCom value is missing!")

        chunk_size = df_length // num_chunks
        start_idx = chunk_size * task_index
        end_idx = chunk_size * (task_index + 1) if task_index < num_chunks - 1 else df_length

        ti.xcom_push(key=f'process_range_{task_index}', value=(start_idx, end_idx))

    def process_rows(task_index, **kwargs):
        year = '2019'
        quarter = '1'
        """Processes rows from the Parquet files in chunks and uploads JSON to S3."""
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        basepath = f"data/{year}/{quarter}/json/"

        parquet_files = ti.xcom_pull(task_ids='load_data', key='parquet_files')
        start_idx, end_idx = ti.xcom_pull(task_ids=f'compute_range_{task_index}', key=f'process_range_{task_index}')
        
        if start_idx is None or end_idx is None:
            raise ValueError(f"Missing range values for task {task_index}")

        def read_parquet(file_name):
            """Helper function to read a parquet file from S3."""
            if file_name in parquet_files:
                file_obj = s3_hook.get_key(parquet_files[file_name], bucket_name)
                buffer = BytesIO(file_obj.get()["Body"].read())
                return pd.read_parquet(buffer)
            return pd.DataFrame()

        dfSub = read_parquet("sub.parquet")
        dfNum = read_parquet("num.parquet")
        dfPre = read_parquet("pre.parquet")
        dfTag = read_parquet("tag.parquet")

        if dfSub.empty:
            return  # No data to process

        # Process rows within the given range
        for subId in range(start_idx, min(end_idx, len(dfSub))):
            submitted = dfSub.iloc[subId]
            symbol_financials = {}

            filteredDfNum = dfNum[dfNum['adsh'] == submitted['adsh']] if not dfNum.empty else pd.DataFrame()

            for _, myNum in filteredDfNum.iterrows():
                myTag = dfTag[dfTag["tag"] == myNum['tag']] if not dfTag.empty else pd.DataFrame()
                label = myTag["doc"].to_string(index=False) if not myTag.empty else "N/A"
                concept = myNum["tag"]
                unit = myNum["uom"]
                value = myNum["value"]

                myPre = dfPre[(dfPre['adsh'] == submitted["adsh"]) & (dfPre['tag'] == myNum['tag'])] if not dfPre.empty else pd.DataFrame()
                info = myPre["plabel"].to_string(index=False).replace('"', "'") if not myPre.empty else "N/A"

                stmt = myPre['stmt'].to_string(index=False).strip() if not myPre.empty else "Other"
                if stmt not in symbol_financials:
                    symbol_financials[stmt] = []
                symbol_financials[stmt].append({
                    "label": label,
                    "concept": concept,
                    "unit": unit,
                    "value": value,
                    "info": info
                })

            result = json.dumps(symbol_financials, default=str)
            json_filename = f"{basepath}{submitted['adsh']}.json"
            print(f"Uploaded {json_filename} to S3")

        # Upload JSON to S3
        s3_hook.load_string(
            string_data=result,
            key=json_filename,
            bucket_name=bucket_name,
            replace=True
        )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    length_task = PythonOperator(
        task_id='get_df_length',
        python_callable=get_df_length,
        provide_context=True
    )

    num_chunks = 3
    compute_tasks = []

    # Compute ranges separately (outside the TaskGroup)
    for i in range(num_chunks):
        compute_task = PythonOperator(
            task_id=f'compute_range_{i}',
            python_callable=compute_range,
            op_kwargs={'task_index': i, 'num_chunks': num_chunks},
            provide_context=True,
        )
        compute_tasks.append(compute_task)

    # Process rows inside TaskGroup
    with TaskGroup("parallel_processing") as processing_group:
        process_tasks = []

        for i in range(num_chunks):
            process_task = PythonOperator(
                task_id=f'process_rows_{i}',
                python_callable=process_rows,
                op_kwargs={'task_index': i},
                provide_context=True,
            )
            process_tasks.append(process_task)

    # Set dependencies
    load_task >> length_task >> compute_tasks >> processing_group