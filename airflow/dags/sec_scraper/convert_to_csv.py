import pandas as pd
from io import BytesIO

def csv_transformer(extracted_files, year, quarter):
    transformed_files = []
    # Transform .txt files to .csv
   
    for file_name, file_bytes in extracted_files:
        if file_name.split(".")[1] == 'txt':
            file_name = file_name.split(".")[0] + ".csv"
            # Transform BytesIO Stream to .csv using pandas
            try:
                file_bytes.seek(0)  
                data_frame = pd.read_table(file_bytes, delimiter="\t", low_memory=False)
                data_frame["year"] = year
                data_frame["quarter"] = quarter
                bytes_io = BytesIO()
                data_frame.to_csv(bytes_io, index=False)
                bytes_io.seek(0)
                # df_load.to_parquet(csv_file_path, index=False, compression='gzip')
                transformed_files.append((file_name, bytes_io))
                print(f"Transformed {file_name} to {file_name} in BytesIO format")
    
            except Exception as e:
                print(f"Failed to transform {file_name}: {e}")

    return transformed_files