import os
from datetime import date, datetime, timedelta
from marshmallow import Schema, fields
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import json

# Constants
dirname = "data/2024/2"

# Load DataFrames
def load_data():
    return {
        "num": pd.read_table(f"{dirname}/num.txt", delimiter="\t", low_memory=False),
        "pre": pd.read_table(f"{dirname}/pre.txt", delimiter="\t", low_memory=False),
        "sub": pd.read_table(f"{dirname}/sub.txt", delimiter="\t", low_memory=False),
        "tag": pd.read_table(f"{dirname}/tag.txt", delimiter="\t", low_memory=False),
        "sym": pd.read_table(f"{dirname}/ticker.txt", delimiter="\t", header=None, names=['symbol', 'cik'], low_memory=False)
    }

# Utility Functions
def npInt_to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

def formatDateNpNum(var):
    dateStr = npInt_to_str(var)
    return f"{dateStr[0:4]}-{dateStr[4:6]}-{dateStr[6:8]}"

# DTO and Schema Classes
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

# Processing Functions
def calculate_end_date(start_date, quarter):
    quarter_map = {
        "FY": 12, "CY": 12,
        "H1": 6, "H2": 6,
        "T1": 4, "T2": 4, "T3": 4,
        "Q1": 3, "Q2": 3, "Q3": 3, "Q4": 3
    }
    
    months_to_add = quarter_map.get(quarter.upper(), None)
    
    if months_to_add is not None:
        return start_date + relativedelta(months=+months_to_add, days=-1)
    
def process_submission(submitted, dfSym, dfNum, dfPre, dfTag):
    sfDto = SymbolFinancialsDto()
    
    try:
        sfDto.startDate = date.fromisoformat(formatDateNpNum(submitted["period"]))
        sfDto.endDate = calculate_end_date(sfDto.startDate, submitted["fp"].strip().upper())
        sfDto.year = int(submitted["fy"]) if not pd.isna(submitted["fy"]) else 0
        sfDto.quarter = submitted["fp"].strip().upper()

        # Map Symbol
        symbol_row = dfSym[dfSym["cik"] == submitted["cik"]]
        if not symbol_row.empty:
            sfDto.symbol = symbol_row.iloc[0]["symbol"].strip().upper()

        # Populate other attributes
        sfDto.name, sfDto.country, sfDto.city = submitted["name"], submitted["countryma"], submitted["cityma"]

        # Process Financial Data
        process_financial_data(sfDto.data, submitted["adsh"], dfNum, dfPre, dfTag)

        return sfDto
    
    except Exception as e:
        print(f"Error processing submission {submitted['adsh']}: {e}")
        return None

def process_financial_data(data_dto, adsh, dfNum, dfPre, dfTag):
    filtered_df_num = dfNum[dfNum['adsh'] == adsh].dropna(subset=['value'])
    
    for _, row in filtered_df_num.iterrows():
        myDto = FinancialElementImportDto()
        
        tag_row = dfTag[dfTag["tag"] == row['tag']]
        myDto.label = tag_row.iloc[0]["doc"] if not tag_row.empty else ""
        
        myPre_row = dfPre[(dfPre['adsh'] == adsh) & (dfPre['tag'] == row['tag'])]
        
        if not myPre_row.empty:
            myDto.info = myPre_row.iloc[0]["plabel"]
            stmt_type = myPre_row.iloc[0]["stmt"].strip().upper()

            if stmt_type == 'BS':
                data_dto.bs.append(myDto)
            elif stmt_type == 'CF':
                data_dto.cf.append(myDto)
            elif stmt_type == 'IS':
                data_dto.ic.append(myDto)

# Export Functions
def export_to_json(sfDto):
    result_json_str = json.dumps(SymbolFinancialsSchema().dump(sfDto))
    
    output_dir_path = f"exportFiles/{dirname}"
    
