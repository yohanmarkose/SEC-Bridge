import os
from datetime import date, datetime, timedelta
from marshmallow import Schema, fields
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import json

def load_data(files):
    """
    Load data from the specified year and quarter
    """
    dataframes = {}
    for file_name, file_buffer in files:
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_buffer, low_memory=False)
            # Use only the base name of the file as the key
            base_name = file_name.split('/')[-1].split('.')[0]
            dataframes[base_name] = df
        except Exception as e:
            print(f"Error loading {file_name}: {e}")
    return dataframes  

