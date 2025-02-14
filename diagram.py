from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.aws.storage import S3
from diagrams.gcp.compute import Run
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from diagrams.programming.framework import FastAPI
from diagrams.digitalocean.compute import Docker
from diagrams.onprem.workflow import Airflow

# Create the diagram
with Diagram("US Securities and Exchange Commission - Data Stream Pipeline", show=False, filename="data_stream", direction="LR"):
    # User interaction
    user = User("User")
    
    # Streamlit Frontend Cluster (Generic Node)
    with Cluster("Frontend (Streamlit)"):
        frontend = Custom("Streamlit UI", "./src/streamlit.png")  # Use a custom icon for Streamlit
        
    # Trigger Airflow DAGS 
    with Cluster("Data Pipelines", direction='LR'):
        
        web = Custom("Scrape Data", "./src/website.png")
        csv = Custom("CSV", "./src/csv.png")    
        json =  Custom("JSON", "./src/json.jpg")
        fact =  Custom("FACT", "./src/fact.png") 
        dbt = Custom("Data Built Tool", "./src/dbt.png")
        s3_storage = S3("Amazon S3")
        snowflake = Custom("Snowflake", "./src/snowflake.png")

    # Input Mechanism Cluster
    with Cluster("User Actions"):
        airflow = Airflow("Airflow") 
        
    # Backend Cluster
    with Cluster("FastAPI"):
        cloud_run = Run("Google Cloud Run")
        docker_image = Docker("Docker Image") 
        backend = FastAPI("FastAPI Service")
    
    # Connections
    user >> frontend
    frontend >> [airflow, backend] 
    airflow >> [csv, json, fact]
    web >> csv >> s3_storage >> snowflake
    csv >> [json, fact]
    json >> s3_storage >> snowflake
    fact >> dbt >> snowflake
    cloud_run >> docker_image >> backend >> snowflake
    snowflake >> backend >> frontend  # Backend sends messages to the frontend for user feedback