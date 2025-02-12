import os
from io import BytesIO, StringIO
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from sec_json_converter.json_convert import load_data 
from marshmallow import Schema, fields

class FinancialElementImportDto:
    label = ""
    concept = ""
    info = ""
    unit = ""
    value = 0.0

class FinancialsDataDto:
    bs = []
    cf = []
    ic = []

class SymbolFinancialsDto:
    startDate = date.today()
    endDate = date.today()
    year = 0
    quarter = ""
    symbol = ""
    name = ""
    country = ""
    city = ""
    data = FinancialsDataDto()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='json_convert_and_upload_to_s3',
    default_args=default_args,
    description='Convert csv files into JSON and load into S3',
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_converter(**kwargs):
        # year = kwargs['year']
        # quarter = kwargs['quarter']
        year ='2019'
        quarter = '1'
        csv_base_path = f"data/{year}/{quarter}/csv"
        json_base_path = f"data/{year}/{quarter}/json"
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=csv_base_path)
        csv_files = [key for key in keys if key.endswith('.csv')]
        
        print(csv_files)
        
        files = []
        
        for file_key in csv_files:
            try:
                print("Reading file: ", file_key)
                file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                file_content = file_obj.get()["Body"].read()
                print(file_content[:100])  
                file_buffer = BytesIO(file_content)
                files.append((file_key, file_buffer))
            except Exception as e:
                print(f"Error reading {file_key}: {e}")
        
        dataframes = load_data(files)
        print(dataframes.keys())
    
        # Loading into individual dataframes    
        dfSub = dataframes.get("sub")
        dfNum = dataframes.get("num")
        dfPre = dataframes.get("pre")
        dfTag = dataframes.get("tag")
        
        for subId in range(len(dfSub)):
            submitted = dfSub.iloc[subId]
            sfDto = SymbolFinancialsDto()
            sfDto.data = FinancialsDataDto()
            sfDto.data.bs = []
            sfDto.data.cf = []
            sfDto.data.ic = []
            
            try:
                period_value = int(submitted["period"])
                periodStartDate = date.fromisoformat(str(period_value)[:4] + "-" + str(period_value)[4:6] + "-" + str(period_value)[6:])
            except ValueError:
                continue       
            
            sfDto.startDate = periodStartDate
            sfDto.year = int(submitted["fy"]) if not np.isnan(submitted["fy"]) else 0        
            sfDto.quarter = str(submitted["fp"]).strip().upper()
            
            if sfDto.quarter in ["FY", "CY"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+12, days=-1)
            elif sfDto.quarter in ["H1", "H2"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+6, days=-1)
            elif sfDto.quarter in ["T1", "T2", "T3"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+4, days=-1)
            elif sfDto.quarter in ["Q1", "Q2", "Q3", "Q4"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+3, days=-1)
            else:
                continue
            
            sfDto.name = submitted["name"]
            sfDto.country = submitted["countryma"]
            sfDto.city = submitted["cityma"]
            
            dfNum["value"] = pd.to_numeric(dfNum["value"], errors='coerce').dropna().astype(int)
            filteredDfNum = dfNum[dfNum['adsh'] == submitted['adsh']]
            
            for _, myNum in filteredDfNum.iterrows():
                myDto = FinancialElementImportDto()
                myTag = dfTag[dfTag["tag"] == myNum['tag']]
                myDto.label = myTag["doc"].to_string(index=False)
                myDto.concept = myNum["tag"]
                myDto.unit = myNum["uom"]
                myDto.value = myNum["value"]
                
                myPre = dfPre[(dfPre['adsh'] == submitted["adsh"]) & (dfPre['tag'] == myNum['tag'])]
                myDto.info = myPre["plabel"].to_string(index=False).replace('"',"'")
                
                stmt = myPre['stmt'].to_string(index=False).strip()
                if stmt == 'BS':
                    sfDto.data.bs.append(myDto)
                elif stmt == 'CF':
                    sfDto.data.cf.append(myDto)
                elif stmt == 'IC':
                    sfDto.data.ic.append(myDto)
            
            result = json.dumps(sfDto.__dict__, default=str)
            print(result)
    
    convert_task = PythonOperator(
        task_id='json_convert_and_upload_to_s3',
        python_callable=run_converter,
        op_kwargs={'year': 2024, 'quarter': 4},
        provide_context=True,
    )

convert_task