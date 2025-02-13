from airflow.utils.task_group import TaskGroup
from io import BytesIO
import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from datetime import date, datetime
from marshmallow import Schema, fields
import numpy as np
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
    dag_id='json_convert_via_parquet_and_print_heads',
    default_args=default_args,
    description='Load parquet files into S3 (if needed) and print their head rows',
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # DTO and Schema definitions (unchanged)
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
        print(f"All required files exist: {all_files_exist}")
        kwargs['ti'].xcom_push(key='files_exist', value=all_files_exist)
        return all_files_exist
    
    def decide_on_loading(**kwargs):
        """Decide whether to load the parquet files (if missing) or skip loading."""
        files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
        return 'load_data' if not files_exist else 'skip_load_data'

    def load_data(**kwargs):
        """Scrape and load Parquet file paths from S3."""
        year = '2024'
        quarter = '4'
        parquet_base_path = f"data/{year}/{quarter}/parquet"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        # Scrape and convert to parquet files
        extracted_files = scrape_sec_data(year, quarter)
        parquet_transformed_files = parquet_transformer(extracted_files, year, quarter)
        ticker_files = get_ticker_file()
        all_parquet_files = parquet_transformed_files + ticker_files
        
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
        """Generate the S3 paths for existing parquet files."""
        year = '2024'
        quarter = '4'
        parquet_base_path = f"data/{year}/{quarter}/parquet/"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        parquet_files = {}
        for file_name in ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet", "ticker.parquet"]:
            file_key = f"{parquet_base_path}{file_name}"
            if s3_hook.check_for_key(file_key, bucket_name):
                parquet_files[file_name] = file_key
            else: 
                raise Exception(f"Not all required parquet files for {year} Q{quarter} have been loaded!")
        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)

    def parquet_to_json(**kwargs):
        """Read parquet files from S3 and print their head rows."""
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        parquet_files = ti.xcom_pull(task_ids='load_data', key='parquet_files')
        if not parquet_files:
            parquet_files = ti.xcom_pull(task_ids='skip_load_data', key='parquet_files')
        
        if not parquet_files:
            raise ValueError("Parquet files information not found in XCom.")
        
        def read_parquet(file_name):
            if file_name not in parquet_files:
                raise Exception(f"Missing parquet file: {file_name}")
            s3_key = parquet_files[file_name]
            file_obj = s3_hook.get_key(s3_key, bucket_name)
            buffer = BytesIO(file_obj.get()["Body"].read())
            return pd.read_parquet(buffer)
        
        def npInt_to_str(var):
            return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

        def formatDateNpNum(var):
            dateStr = npInt_to_str(var)
            return dateStr[0:4]+"-"+dateStr[4:6]+"-"+dateStr[6:8]
        
        dfNum = read_parquet("num.parquet")
        dfPre = read_parquet("pre.parquet")
        dfSub = read_parquet("sub.parquet")
        dfTag = read_parquet("tag.parquet")
        dfSym = read_parquet("ticker.parquet")

        
        for subId in range(len(dfSub)):
            submitted = dfSub.iloc[subId]
            sfDto = SymbolFinancialsDto()
            sfDto.data = FinancialsDataDto()
            sfDto.data.bs = []
            sfDto.data.cf = []
            sfDto.data.ic = []
            try:
                periodStartDate = date.fromisoformat(formatDateNpNum(submitted["period"]))
            except ValueError:
                print(submitted["adsh"]+".json has Period: "+str(submitted["period"]))
                continue   
            sfDto.startDate = periodStartDate
            sfDto.endDate = date.today()
            if pd.isna(submitted["fy"]):
                sfDto.year = 0
            else: 
                sfDto.year = submitted["fy"].astype(int)
            sfDto.quarter = str(submitted["fp"]).strip().upper()

            if sfDto.quarter == "FY" or sfDto.quarter == "CY":
                sfDto.endDate = periodStartDate + relativedelta(months=+12, days=-1)
            elif sfDto.quarter == "H1" or sfDto.quarter == "H2":
                sfDto.endDate = periodStartDate + relativedelta(months=+6, days=-1)
            elif sfDto.quarter == "T1" or sfDto.quarter == "T2" or sfDto.quarter == "T3":
                sfDto.endDate = periodStartDate + relativedelta(months=+4, days=-1)
            elif sfDto.quarter == "Q1" or sfDto.quarter == "Q2" or sfDto.quarter == "Q3" or sfDto.quarter == "Q4":
                sfDto.endDate = periodStartDate + relativedelta(months=+3, days=-1)
            else:
                continue

            val = dfSym[dfSym["cik"]==submitted["cik"]]
            if len(val) > 0:
                sfDto.symbol = val["symbol"].to_string(index = False).split("\n")[0].split("\\n")[0].strip().split(" ")[0].strip().upper()
                if len(sfDto.symbol) > 19 or len(sfDto.symbol) < 1:
                    print(submitted["adsh"]+".json has Symbol "+sfDto.symbol)
            else:
                print(submitted["adsh"]+".json has Symbol "+val["symbol"].to_string(index = False))
                continue    
            sfDto.name = submitted["name"]
            sfDto.country = submitted["countryma"]
            sfDto.city = submitted["cityma"]
            sdDto = FinancialsDataDto()
            dfNum['value'] = pd.to_numeric(dfNum['value'], errors='coerce')
            dfNum = dfNum.dropna(subset=['value'])
            dfNum['value'] = dfNum['value'].astype(int)
            filteredDfNum = dfNum[dfNum['adsh'] == submitted['adsh']]
            filteredDfNum = filteredDfNum.reset_index(drop=True)
            for myId in range(len(filteredDfNum)):
                submitted["adsh"] = str(submitted["adsh"])
                myNum = filteredDfNum.iloc[myId]
                myDto = FinancialElementImportDto()
                
                # Match tag
                myTag = dfTag[dfTag["tag"] == myNum['tag']]
                myDto.label = myTag["doc"].to_string(index=False)
                myDto.concept = myNum["tag"]
                
                # Match presentation data
                myPre = dfPre[(dfPre['adsh'] == submitted["adsh"]) & (dfPre['tag'] == myNum['tag'])]
                if myPre.empty:
                    print(f"No matching presentation data found for TAG: {myNum['tag']} in ADSH: {submitted['adsh']}")
                    continue
                
                # Extract info
                myDto.info = myPre["plabel"].to_string(index=False).replace('"', "'")
                myDto.unit = myNum["uom"]
                myDto.value = myNum["value"]
                
                # Debugging stmt value
                stmt_value = myPre['stmt'].iloc[0]  # Directly access the first value
                
                # Append to appropriate list based on stmt value
                if stmt_value == 'BS':
                    sfDto.data.bs.append(myDto)
                elif stmt_value == 'CF':
                    sfDto.data.cf.append(myDto)
                elif stmt_value == 'IS':
                    sfDto.data.ic.append(myDto)
            result = SymbolFinancialsSchema().dump(sfDto)
            result = str(json.dumps(result))
            result = result.replace('\\r', '').replace('\\n', ' ')
            print(result)


     # Define tasks
    start_task = DummyOperator(task_id="start")
    
    check_files_task = PythonOperator(
        task_id='check_s3_files',
        python_callable=check_s3_files,
        provide_context=True,
    )
    
    load_decider_task = BranchPythonOperator(
        task_id='decide_on_loading',
        python_callable=decide_on_loading,
        provide_context=True,
    )
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )
    
    skip_load_data_task = PythonOperator(
        task_id='skip_load_data',
        python_callable=skip_load_data,
        provide_context=True,
    )
    
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
                python_callable=parquet_to_json,
                op_kwargs={'task_index': i},
                provide_context=True,
            )
            process_tasks.append(process_task)
    
    end_task = DummyOperator(task_id="end")

    # Set task dependencies
    start_task >> check_files_task >> load_decider_task
    load_decider_task >> [load_data_task, skip_load_data_task] >> downstream_tasks
    downstream_tasks >> length_task >> compute_tasks >> processing_group >> end_task