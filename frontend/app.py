import os
import streamlit as st
import requests
from dotenv import load_dotenv
load_dotenv()

# Airflow API endpoint
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/sec_data_to_s3/dagRuns"

st.title("SEC Data - Bridge")

# Input fields for year and quarter
year = st.selectbox("Select Year",("2024","2023","2022","2021","2020","2019","2018","2017"))
quarter = st.selectbox("Select Quarter", ("1","2","3","4"))

if st.button("Fetch Data"):
    # Payload for triggering the DAG
    payload = {
        "conf": {
            "year": year,
            "quarter": quarter
        }
    }
    
    AIRFLOW_USER = os.getenv("AIRFLOW_USER")
    AIRFLOW_PASSCODE = os.getenv("AIRFLOW_PASSCODE")

    # Trigger the DAG via Airflow REST API
    response = requests.post(
        AIRFLOW_API_URL,
        json=payload,
        auth=(f"{AIRFLOW_USER}", f"{AIRFLOW_PASSCODE}")
    )

    if response.status_code == 200:
        st.success("DAG triggered successfully!")
    else:
        st.error(f"Failed to trigger DAG: {response.text}")