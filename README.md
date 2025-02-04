# SEC Bridge

#### Emphasizing financial data processing and pipeline automation

SECBridge is a financial data pipeline and analysis system designed to extract, transform, and validate SEC financial statement data efficiently. The project aims to support analysts conducting fundamental analysis of US public companies by building a robust, scalable, and structured financial database using Snowflake, Airflow, FastAPI, and Streamlit.

## Objectives

**Data Extraction & Processing**: Automate the retrieval of financial statement datasets from the SEC Market Data page.

**Data Storage & Transformation**: Design and evaluate multiple data storage methods, including Raw Staging, JSON Transformation, and Denormalized Fact Tables in Snowflake.

**Data Validation**: Implement Data Validation Tool (DVT) checks to ensure data integrity, schema consistency, and accurate transformations.

**Operational Pipeline**: Develop Airflow-based ETL pipelines using AWS S3 as an intermediary storage layer.

**Post-Upload Testing**: Validate data uploads in Snowflake and ensure robust query execution.

**Visualization & API Access**: Build a Streamlit app for data exploration and a FastAPI backend for structured access to stored financial data.

**Comparative Analysis**: Evaluate the advantages and trade-offs of different data storage approaches.

## Technology

- Cloud & Storage: Snowflake, AWS S3

- ETL & Pipeline: Apache Airflow

- Programming: Python, SQL, JSON Transformation

- Validation & Testing: DVT (Data Validation Tool)

- Backend API: FastAPI

- Frontend UI: Streamlit

`