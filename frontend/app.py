import os, requests
import streamlit as st
from streamlit_option_menu import option_menu
from dotenv import load_dotenv
load_dotenv()

# Airflow API endpoint
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/sec_data_to_s3/dagRuns"

# st.title("US Securities & Exchange Commission - Data Bridge")


# # Input fields for year and quarter
# year = st.selectbox("Select Year",("2024","2023","2022","2021","2020","2019","2018","2017"))
# quarter = st.selectbox("Select Quarter", ("1","2","3","4"))

# if st.button("Fetch Data"):
#     # Payload for triggering the DAG
#     payload = {
#         "conf": {
#             "year": year,
#             "quarter": quarter
#         }
#     }
    
#     AIRFLOW_USER = os.getenv("AIRFLOW_USER")
#     AIRFLOW_PASSCODE = os.getenv("AIRFLOW_PASSCODE")

#     # Trigger the DAG via Airflow REST API
#     response = requests.post(
#         AIRFLOW_API_URL,
#         json=payload,
#         auth=(f"{AIRFLOW_USER}", f"{AIRFLOW_PASSCODE}")
#     )

#     if response.status_code == 200:
#         st.success("DAG triggered successfully!")
#     else:
#         st.error(f"Failed to trigger DAG: {response.text}")

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
                st.success(f"Query executed successfully for Year: {year}, Quarter: {quarter}.")
            else:
                st.error("Please enter a valid query before running.")
            # Additional user guidance when no action is taken yet
        
def check_data_availability(source, year, quarter):
    # Placeholder function to check data availability
    return(True)
    
def execute_query(query):
    print(query)
    return(True)
    
def main():
    # Set the title of the app
    st.title("US Securities and Exchange Commission")
    st.header("Financial Data Pipeline")
    # # Add a sidebar
    # st.sidebar.header("Main Menu")
    # input_format = st.sidebar.selectbox("Choose a format:", ["WebURL", "PDF"])
    
    # Create a sidebar menu
    with st.sidebar:
        selected = option_menu(
            menu_title="Main Menu",  # Title of the menu
            options=["Airflow", "Query Snowflake" ],  # Menu options
            icons=["rocket", "database"],  # Bootstrap icons (optional)
            default_index=0,  # Default selected option
        )
    # Display content based on selection
    if selected == "Airflow":
        st.subheader("Welcome to the Airflow Page")
    elif selected == "Query Snowflake":
        populate_query_page()
            
if __name__ == "__main__":
# Set page configuration
    st.set_page_config(
        page_title="US SEC - Data Bridge",  # Name of the app
        layout="wide",              # Layout: "centered" or "wide"
        initial_sidebar_state="expanded"  # Sidebar: "expanded" or "collapsed"
    )    
    main()