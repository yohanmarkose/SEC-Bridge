import os
import boto3
import requests
import streamlit as st
from streamlit_option_menu import option_menu
from dotenv import load_dotenv
load_dotenv()

AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PASSCODE = os.getenv("AIRFLOW_PASSCODE")
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')

st.title("SEC Data - Bridge")


# # Input fields for year and quarter
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
    dag_id = "sec_data_to_s3_scraper"
    AIRFLOW_API_URL = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"

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

def populate_airflow_page():
    # Display the airflow page
    st.subheader("Trigger Airflow DAGs")
    # Input fields for source, year and quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT Tables"])
    with col2:
        year = st.selectbox("Select Year", range(2009, 2025))
    with col3:
        quarter = st.selectbox("Select Quarter", ("Q1","Q2","Q3","Q4"))
    
def populate_query_page():
    # Display the query page
    st.subheader("Query Snowflake")

    if "flag" not in st.session_state:
        st.session_state.flag = False
    if "query" not in st.session_state:
        st.session_state.query = None

    # Input fields for source, year and quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT Tables"])
    with col2:
        year = st.selectbox("Select Year", range(2009, 2025))
    with col3:
        quarter = st.selectbox("Select Quarter", ("Q1","Q2","Q3","Q4"))
    # Button to trigger the query
    avail = st.button("Check availability", use_container_width=True)
    # Handle data availability logic
    if avail:
        st.session_state.flag = check_data_availability(source, year, quarter)
        if st.session_state.flag:
            st.success(f"Data is available for **{source}**, Year: **{year}**, Quarter: **{quarter}**.")
        else:
            st.error(f"No data available for **{source}**, Year: **{year}**, Quarter: **{quarter}**.")
    # Show query input only if data is available
    if st.session_state.flag:
        # Text area for query input (persistent using session state)
        st.session_state.query = st.text_area(
            "Enter your query here:", 
            value=st.session_state.query, 
            height=200,
            key="query_input"
        )
        # Button to run the query
        run_query = st.button("Run Query", use_container_width=True)
        if run_query:
            if st.session_state.query is not None:  # Ensure the query is not empty
                # Execute the query
                query_executed = execute_query(st.session_state.query)
                st.write("Query Results:")
                st.dataframe(query_executed)
                st.success(f"Query executed successfully.")
            else:
                st.error("Please enter a valid query before running.")
            # Additional user guidance when no action is taken yet
        
def check_data_availability(source, year, quarter):
    # Placeholder function to check data availability
    try:
        response = requests.get(f"{QUERY_API_URL}/check-availability", params={"source": source}) 
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json().get("data", [])
                # Display the data in a table format if results are available
            if data:
                return(data)
            else:
                st.write("No data returned for the given query.")
        else:
            # Handle errors from the API
            st.error(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        # Handle connection or other exceptions
        st.error(f"An error occurred: {e}")
        
    return(True)
    
def execute_query(query):
    if query.strip():  # Ensure the query is not empty
        try:
            # Send the query to the FastAPI backend
            response = requests.get(f"{QUERY_API_URL}/query-data", params={"query": query})            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json().get("data", [])
                # Display the data in a table format if results are available
                if data:
                    return(data)
                else:
                    st.write("No data returned for the given query.")
            else:
                # Handle errors from the API
                st.error(f"Error: {response.status_code} - {response.text}")
        except Exception as e:
            # Handle connection or other exceptions
            st.error(f"An error occurred: {e}")
    else:
        st.error(f"Failed to trigger DAG: {response.text}")

st.subheader("Load selected data into Snowflake")

# Function to list years, quarters, and file formats from S3
def list_years_quarters_fileformats(bucket_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    years_quarters_fileformats = set()

    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            # Assuming the S3 key format is 'data/{year}/{quarter}/{fileformat}/{file_name}'
            parts = key.split('/')
            if len(parts) > 3:
                year = parts[1]
                quarter = parts[2]
                fileformat = parts[3]
                years_quarters_fileformats.add((year, quarter, fileformat))

    return sorted(years_quarters_fileformats)

# Fetch available data from S3
available_data = list_years_quarters_fileformats(AWS_BUCKET)

# Extract unique values for dropdowns
unique_years = sorted(set(item[0] for item in available_data))
unique_quarters = sorted(set(item[1] for item in available_data))
unique_fileformats = sorted(set(item[2] for item in available_data))

# Dropdown for year selection
selected_year = st.selectbox("Select Year", unique_years)

# Filter quarters based on selected year
filtered_quarters = sorted(set(q for y, q, f in available_data if y == selected_year))

# Dropdown for quarter selection
selected_quarter = st.selectbox("Select Quarter", filtered_quarters)

# Filter file formats based on selected year and quarter
filtered_fileformats = sorted(set(f for y, q, f in available_data if y == selected_year and q == selected_quarter))

# Dropdown for file format selection
selected_fileformat = st.selectbox("Select File Format", filtered_fileformats)

st.write(f"You selected: Year {selected_year}, Quarter {selected_quarter}, File Format {selected_fileformat}")

if st.button("Load Data"):
    # Determine which DAG to trigger based on file format
    if selected_fileformat == "csv":
        dag_id = "stage_and_load_csv_to_snowflake"
    elif selected_fileformat == "parquet":
        dag_id = "stage_and_load_parquet_to_snowflake"
    else:
        st.error("Unsupported file format selected!")
        st.stop()

    # Airflow API endpoint for the selected DAG
    AIRFLOW_API_URL = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"

    # Payload for triggering the DAG
    payload = {
        "conf": {
            "year": selected_year,
            "quarter": selected_quarter,
            "fileformat": selected_fileformat
        }
    }

    # Trigger the DAG via Airflow REST API
    response = requests.post(
        AIRFLOW_API_URL,
        json=payload,
        auth=(f"{AIRFLOW_USER}", f"{AIRFLOW_PASSCODE}")
    )

    if response.status_code == 200:
        st.success(f"DAG '{dag_id}' triggered successfully!")
    else:
        st.error(f"Failed to trigger DAG '{dag_id}': {response.text}")