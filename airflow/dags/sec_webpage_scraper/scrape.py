import os
import requests
import tempfile
from io import BytesIO
from zipfile import ZipFile


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

        # Create a temporary directory to extract files
        temp_dir="./data"
        with ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(temp_dir)

                # Collect all extracted file paths
                extracted_files = []
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        extracted_files.append(os.path.join(root, file))

                if not extracted_files:
                    raise Exception("No files were extracted from the ZIP archive.")

                print(f"Extracted files: {extracted_files}")
                return extracted_files

    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download SEC data: {e}")
    except Exception as e:
        raise Exception(f"Failed to process SEC data: {e}")

if __name__ == "__main__":
    year = 2024
    quarter = 2
    files = scrape_sec_data(year, quarter)
    print(f"Extracted files: {files}")