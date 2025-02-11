import os
import requests
from tempfile import NamedTemporaryFile
from io import BytesIO
from zipfile import ZipFile

def extract_zip_to_bytes(response):
    # Create a temporary file to store the ZIP content
    with NamedTemporaryFile() as temp_zip_file:
        # Write the response content to the temporary ZIP file
        temp_zip_file.write(response.content)
        temp_zip_file.seek(0)  # Reset file pointer to the beginning

        # Open the ZIP file and extract its contents
        with ZipFile(temp_zip_file, 'r') as zip_file:
            extracted_files = []
            
            for file_name in zip_file.namelist():
                # Read each file in the ZIP as a byte stream
                with zip_file.open(file_name) as file:
                    extracted_files.append((file_name, BytesIO(file.read())))
            
            if not extracted_files:
                raise Exception("No files were extracted from the ZIP archive.")
            
            return extracted_files

def scrape_sec_data(year, quarter):
    """
    Downloads and extracts SEC financial statement data for a given year and quarter.
    
    Args:
        year (int): The year of the data to download.
        quarter (int): The quarter of the data to download (1, 2, 3, or 4).
        base_path (str): Local base path where extracted files will be saved temporarily.

    Returns:
        list: A list of file paths for the extracted files.
    """
    headers = {
        "User-Agent": "contact@example.com",  # Replace with your email per SEC guidelines
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",
        "Host": "www.sec.gov",
        "Referer": "https://www.sec.gov/"
    }

    # Construct URL for the SEC dataset
    url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"

    print(f"Downloading SEC data from {url}...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  
        #os.makedirs(base_path, exist_ok=True)
        
        return extract_zip_to_bytes(response)

    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download SEC data: {e}")
    except Exception as e:
        raise Exception(f"Failed to process SEC data: {e}")

if __name__ == "__main__":
    year = 2024
    quarter = 2
    files = scrape_sec_data(year, quarter)
    print(f"Extracted files: {files}")