from airflow.utils.task_group import TaskGroup
from io import BytesIO
import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import date, datetime
from marshmallow import Schema, fields
import pandas as pd

# Import custom functions
from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_parquet import get_ticker_file, parquet_transformer

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='json_convert_via_parquet_and_upload_to_s3_v2',
    default_args=default_args,
    description='Convert Parquet files into JSON and load into S3',
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    class FinancialElementImportDto:
        label = ""
        concept = ""        #Num.tag
        info = ""           #Pre.plabel
        unit = ""           #Num.uom
        value = 0.0         #Num.value

    class FinancialsDataDto:
        bs = []  
        cf = []  
        ic = []

    class SymbolFinancialsDto:
        startDate = date.today()   # Sub.period 
        endDate = date.today()     # Sub.period + Sub.fp
        year = 0                   # Sub.fy
        quarter = ""               # Sub.fp
        symbol = ""                # Sub.cik -> Sym.cik -> Sym.symbol
        name = ""                  # Sub.name
        country = ""               # Sub.countryma
        city = ""                  # Sub.cityma
        data = FinancialsDataDto()

    # Define marshmallow schemas
    class FinancialElementImportSchema(Schema):
        label = fields.String()
        concept = fields.String()
        info = fields.String()
        unit = fields.String()
        value = fields.Int()

    class FinancialsDataSchema(Schema):
        bs = fields.List(fields.Nested(FinancialElementImportSchema()))
        cf = fields.List(fields.Nested(FinancialElementImportSchema()))
        ic = fields.List(fields.Nested(FinancialElementImportSchema()))

    class SymbolFinancialsSchema(Schema):
        startDate = fields.DateTime()
        endDate = fields.DateTime()
        year = fields.Int()
        quarter = fields.String()
        symbol = fields.String()
        name = fields.String()
        country = fields.String()
        city = fields.String()
        data = fields.Nested(FinancialsDataSchema)

    def check_s3_files(**kwargs):
        """Check if all required parquet files exist in S3 for the requested year and quarter"""
        year = '2024'
        quarter = '4'
        bucket_name = Variable.get("s3_bucket_name")
        required_files = ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet"]
        s3_hook = S3Hook(aws_conn_id='aws_default')
        file_exists_list = []
        for file_name in required_files:
            key = f"data/{year}/{quarter}/parquet/{file_name}"
            exists = s3_hook.check_for_key(key=key, bucket_name=bucket_name)
            print(f"Checking {key} in bucket {bucket_name}: {exists}")
            file_exists_list.append(exists)
        all_files_exist = all(file_exists_list)
        print(f"All files exist: {all_files_exist}")
        kwargs['ti'].xcom_push(key='files_exist', value=all_files_exist)
        return all_files_exist
    
    def decide_on_loading(**kwargs):
        """Deciding if data needs to be loaded in S3 for the requested year and quarter"""
        files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
        return 'load_data' if not files_exist else 'skip_load_data'

    def load_data(**kwargs):
        """Scrape and loads Parquet file paths from S3."""
        year = '2024'
        quarter = '4'
        parquet_base_path = f"data/{year}/{quarter}/parquet"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        extracted_files = scrape_sec_data(year, quarter)
        parquet_transformed_files = parquet_transformer(extracted_files, year, quarter)
        ticker_files = get_ticker_file()
        all_parquet_files = parquet_transformed_files + ticker_files
        s3_hook = S3Hook(aws_conn_id='aws_default')
        parquet_files = {}
        for file_name, file_bytes in all_parquet_files:
            s3_key = f"{parquet_base_path}/{file_name}"
            if isinstance(file_bytes, BytesIO):
                file_obj = file_bytes
            else:
                file_obj = BytesIO(file_bytes)
            s3_hook.load_file_obj(file_obj=file_obj, bucket_name=bucket_name, key=s3_key, replace=True)
            parquet_files[file_name] = s3_key
            print(f"Uploaded {file_name} to S3 at {s3_key}")
        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)

    def skip_load_data(**kwargs):
        """Generating the S3 paths for existing parquet files."""
        year = '2024'
        quarter = '4'
        parquet_base_path = f"data/{year}/{quarter}/parquet/"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        parquet_files = {}
        for file_name in ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet","ticker.parquet"]:
            file_key = f"{parquet_base_path}{file_name}"
            if s3_hook.check_for_key(file_key, bucket_name):
                parquet_files[file_name] = file_key
            else : 
                raise Exception(f"Check parquet files for the {year} and {quarter} all required files have not been loaded!!")
        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)
    
    def get_df_length(**kwargs):
        """Reads the 'sub.parquet' file and pushes its length to XCom."""
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        parquet_files = kwargs['ti'].xcom_pull(task_ids='load_data', key='parquet_files')
        if parquet_files is not None:
            print(f"Using data from 'load_data': {parquet_files}")
        else:
            parquet_files = kwargs['ti'].xcom_pull(task_ids='skip_load_data', key='parquet_files')
            print(f"Using data from 'skip_load_data': {parquet_files}")
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

    # Convert DTOs to JSON
    def convert_to_json(symbol_financials):
        schema = SymbolFinancialsSchema(many=True)
        return schema.dump(symbol_financials)

    def process_rows(task_index, **kwargs):
        """Processes rows from the Parquet files in chunks and uploads JSON to S3."""
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")

        parquet_files = kwargs['ti'].xcom_pull(task_ids='load_data', key='parquet_files')
        if parquet_files is not None:
            print(f"Using data from 'load_data': {parquet_files}")
        else:
            parquet_files = kwargs['ti'].xcom_pull(task_ids='skip_load_data', key='parquet_files')
            print(f"Using data from 'skip_load_data': {parquet_files}")
        
        start_idx, end_idx = ti.xcom_pull(task_ids=f'compute_range_{task_index}', key=f'process_range_{task_index}')
        
        if start_idx is None or end_idx is None:
            raise ValueError(f"Missing range values for task {task_index}")

        def read_parquet(file_name):
            """Helper function to read a parquet file from S3."""
            parquet_files = kwargs['ti'].xcom_pull(task_ids='load_data', key='parquet_files')
            if parquet_files is not None:
                print(f"Using data from 'load_data': {parquet_files}")
            else:
                parquet_files = kwargs['ti'].xcom_pull(task_ids='skip_load_data', key='parquet_files')
                print(f"Using data from 'skip_load_data': {parquet_files}")
                file_obj = s3_hook.get_key(parquet_files[file_name], bucket_name)
                buffer = BytesIO(file_obj.get()["Body"].read())
                return pd.read_parquet(buffer)
            return pd.DataFrame()

        dfSub = read_parquet("sub.parquet")
        dfNum = read_parquet("num.parquet")
        dfPre = read_parquet("pre.parquet")
        dfTag = read_parquet("tag.parquet")
        dfSym = read_parquet("ticker.parquet")

        if dfSub.empty:
            return  # No data to process
        
        def get_month_for_fiscal_period(fp):
            """Map fiscal period (e.g., Q1, Q2, Q3, Q4) to the corresponding month."""
            fiscal_periods = {
                'Q1': 1,  # January - March
                'Q2': 4,  # April - June
                'Q3': 7,  # July - September
                'Q4': 10  # October - December
            }
            return fiscal_periods.get(fp, 1)

        def map_to_dto(dfSub, dfNum, dfPre, dfTag, dfSym):
            symbol_financials = []
            for _, sub_row in dfSub.iterrows():
                # Skip rows with missing 'period'
                if pd.isnull(sub_row['period']):
                    print("Skipping row due to missing 'period' value")
                    continue

                symbol = SymbolFinancialsDto()
                symbol.year = sub_row['fy']
                symbol.quarter = sub_row['fp']

                # Lookup symbol using 'cik'; fallback if not found.
                matching_symbol = dfSym.loc[dfSym['cik'] == sub_row['cik'], 'symbol']
                symbol.symbol = matching_symbol.iloc[0] if not matching_symbol.empty else 'UNKNOWN'

                symbol.name = sub_row['name']
                symbol.country = sub_row['countryma']
                symbol.city = sub_row['cityma']

                # Parse period (assumed format: YYYYMMDD) to set startDate and endDate
                period_str = str(int(sub_row['period']))
                year_int = int(period_str[:4])
                month_int = int(period_str[4:6])
                symbol.startDate = datetime(year_int, month_int, 1)
                fiscal_month = get_month_for_fiscal_period(sub_row['fp'])
                if fiscal_month == 1:  # Q1
                    symbol.endDate = datetime(year_int, 3, 31)
                elif fiscal_month == 4:  # Q2
                    symbol.endDate = datetime(year_int, 6, 30)
                elif fiscal_month == 7:  # Q3
                    symbol.endDate = datetime(year_int, 9, 30)
                elif fiscal_month == 10:  # Q4
                    symbol.endDate = datetime(year_int, 12, 31)

                financial_data = FinancialsDataDto()

                for _, pre_row in dfPre.iterrows():
                    if pre_row['stmt'] == 'BS':
                        fe = FinancialElementImportDto()
                        matching_label = dfTag.loc[dfTag['doc'] == pre_row['plabel'], 'doc']
                        fe.label = matching_label.iloc[0] if not matching_label.empty else pre_row['plabel']

                        matching_concept = dfNum.loc[dfNum['tag'] == pre_row['tag'], 'tag']
                        fe.concept = matching_concept.iloc[0] if not matching_concept.empty else pre_row['tag']

                        fe.info = pre_row['plabel']

                        matching_unit = dfNum.loc[dfNum['tag'] == pre_row['tag'], 'uom']
                        fe.unit = matching_unit.iloc[0] if not matching_unit.empty else 'UNKNOWN'

                        fe.value = pre_row.get('value', 0.0)
                        financial_data.bs.append(fe)

                    elif pre_row['stmt'] == 'CF':
                        fe = FinancialElementImportDto()
                        matching_label = dfTag.loc[dfTag['doc'] == pre_row['plabel'], 'doc']
                        fe.label = matching_label.iloc[0] if not matching_label.empty else pre_row['plabel']

                        matching_concept = dfNum.loc[dfNum['tag'] == pre_row['tag'], 'tag']
                        fe.concept = matching_concept.iloc[0] if not matching_concept.empty else pre_row['tag']

                        fe.info = pre_row['plabel']

                        matching_unit = dfNum.loc[dfNum['tag'] == pre_row['tag'], 'uom']
                        fe.unit = matching_unit.iloc[0] if not matching_unit.empty else 'UNKNOWN'

                        fe.value = pre_row.get('value', 0.0)
                        financial_data.cf.append(fe)

                    elif pre_row['stmt'] == 'IS':
                        fe = FinancialElementImportDto()
                        matching_label = dfTag.loc[dfTag['doc'] == pre_row['plabel'], 'doc']
                        fe.label = matching_label.iloc[0] if not matching_label.empty else pre_row['plabel']

                        matching_concept = dfNum.loc[dfNum['tag'] == pre_row['tag'], 'tag']
                        fe.concept = matching_concept.iloc[0] if not matching_concept.empty else pre_row['tag']

                        fe.info = pre_row['plabel']

                        matching_unit = dfNum.loc[dfNum['tag'] == pre_row['tag'], 'uom']
                        fe.unit = matching_unit.iloc[0] if not matching_unit.empty else 'UNKNOWN'

                        fe.value = pre_row.get('value', 0.0)
                        financial_data.ic.append(fe)

                symbol.data = financial_data
                symbol_financials.append(symbol)

            return symbol_financials

        # Process the data and load it into S3
        symbol_financials = map_to_dto(dfSub, dfNum, dfPre, dfTag, dfSym)
        json_data = convert_to_json(symbol_financials)
        year='2024'
        quater='4'
        s3_key=f"/data/{year}/{quater}/json"
        s3_hook.load_file_obj(file_obj=json_data, bucket_name=bucket_name, key=s3_key, replace=True)

    # Check files task
    check_files_task = PythonOperator(
        task_id='check_s3_files',
        python_callable=check_s3_files,
        provide_context=True,
    )
    
    # Branching task to decide the next step
    load_decider_task = BranchPythonOperator(
        task_id='decide_on_loading',
        python_callable=decide_on_loading,
        provide_context=True,
    )
    
    # Load data task (executed if files are missing)
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )
    
    # Skip load data task generating the S3 paths for files
    skip_load_data_task = PythonOperator(
        task_id='skip_load_data',
        python_callable=skip_load_data,
        provide_context=True,
    )
    
    # Join point after branching
    downstream_tasks = DummyOperator(
        task_id='join',
        trigger_rule='none_failed'
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

    end_task = DummyOperator(task_id="end")
    start_task = DummyOperator(task_id="start")

    # Define task dependencies
    start_task >> check_files_task >> load_decider_task
    load_decider_task >> [load_data_task, skip_load_data_task] >> downstream_tasks
    downstream_tasks >> length_task >> compute_tasks >> processing_group >> end_task