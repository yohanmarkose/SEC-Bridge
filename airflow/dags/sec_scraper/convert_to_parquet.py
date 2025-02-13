import pandas as pd
from io import BytesIO, StringIO

import requests

def get_ticker_file():
    """
    Fetches the SEC ticker file and converts it to a Parquet format.
    
    Returns:
        transformed_files (list): List containing the transformed file name and BytesIO object.
    """
    url = "https://www.sec.gov/include/ticker.txt"
    headers = {"User-Agent": "YourName (your_email@example.com)"}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        try:
            data_frame = pd.read_table(StringIO(response.text), delimiter="\t", header=None, names=["symbol", "cik"])
            bytes_io = BytesIO()
            data_frame.to_parquet(
                bytes_io,
                index=False,
                compression="snappy",
                engine="pyarrow"
            )
            bytes_io.seek(0)
            print("Successfully transformed ticker.txt to ticker.parquet in BytesIO format.")
            return [("ticker.parquet", bytes_io)]
        except Exception as e:
            print(f"Failed to transform ticker.txt: {e}")
    else:
        print(f"Failed to fetch data: {response.status_code}")
    return []



def parquet_transformer(extracted_files, year, quarter):
    """
    Transforms extracted .txt files into optimized Parquet format with encoding and compression.
    
    Parameters:
        extracted_files (list): List of tuples containing file names and file content as BytesIO objects.
        year (int): The year associated with the data.
        quarter (int): The quarter associated with the data.
    
    Returns:
        transformed_files (list): List of tuples containing transformed file names and BytesIO objects.
    """
    transformed_files = []
    
    for file_name, file_bytes in extracted_files:
        # Change file extension to .parquet
        if file_name.split(".")[1] == 'txt':
            parquet_file_name = file_name.split(".")[0] + ".parquet"
            
            try:
                file_bytes.seek(0)
                data_frame = pd.read_table(file_bytes, delimiter="\t", low_memory=False)
                data_frame["year"] = year
                data_frame["quarter"] = quarter
                categorical_columns = ["tag", "version", "uom", "segments", "form", "stmt"]
                for col in categorical_columns:
                    if col in data_frame.columns:
                        data_frame[col] = data_frame[col].astype("category")  # Convert to Pandas category type
                bytes_io = BytesIO()
                data_frame.to_parquet(
                    bytes_io,
                    index=False,
                    compression="snappy",
                    engine="pyarrow"       # Use PyArrow for better compatibility with Snowflake
                )
                
                bytes_io.seek(0)  # Reset pointer for further use
                transformed_files.append((parquet_file_name, bytes_io))
                print(f"Successfully transformed {file_name} to {parquet_file_name} in BytesIO format.")
            
            except Exception as e:
                print(f"Failed to transform {file_name}: {e}")
    
    return transformed_files