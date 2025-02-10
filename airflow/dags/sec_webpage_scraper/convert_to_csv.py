import pandas as pd
from io import BytesIO

def get_ticker_file():
    import pandas as pd
    import requests

    url = "https://www.sec.gov/include/ticker.txt"
    headers = {"User-Agent": "YourName (your_email@example.com)"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Save content to a DataFrame
        from io import StringIO
        data_frame = pd.read_table(StringIO(response.text), delimiter="\t", header=None, names=["symbol", "cik"])
    else:
        print(f"Failed to fetch data: {response.status_code}")
    
    return(data_frame)

def convert_to_bytes(data_frame):
    bytes_io = BytesIO()
    data_frame.to_csv(bytes_io, index=False)
    bytes_io.seek(0)
    return bytes_io

def csv_transformer(extracted_files, year, quarter):
    transformed_files = []
    # Transform .txt files to .csv
    for file_name, file_bytes in extracted_files:
        file_name = file_name.split(".")[0] + ".csv"
        # Transform BytesIO Stream to .csv using pandas
        try:
            file_bytes.seek(0)  # Reset file pointer to the beginning
            data_frame = pd.read_table(file_bytes, delimiter="\t", low_memory=False)
            data_frame["year"] = year
            data_frame["quarter"] = quarter
            # bytes_io = BytesIO()
            # data_frame.to_csv(bytes_io, index=False)
            # bytes_io.seek(0)
            # df_load.to_parquet(csv_file_path, index=False, compression='gzip')
            transformed_files.append((file_name, convert_to_bytes(data_frame)))
            print(f"Transformed {file_name} to {file_name} in BytesIO format")
        except Exception as e:
            print(f"Failed to transform {file_name}: {e}")
    
    data_frame = get_ticker_file()
    transformed_files.append(("ticker.csv", convert_to_bytes(data_frame)))
    
    return transformed_files    
