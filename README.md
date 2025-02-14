# SEC Bridge

#### Emphasizing financial data processing and pipeline automation

EC-Bridge is a financial data pipeline and analysis system designed to extract, transform, and validate SEC financial statement data efficiently. The project aims to support analysts conducting fundamental analysis of US public companies by building a robust, scalable, and structured financial database using Snowflake, Airflow, FastAPI, and Streamlit.

# Architecture Diagram :
![SEC-Bridge](https://github.com/BigDataIA-Spring2025-4/DAMG7245_Assignment02/blob/main/data_stream.png?raw=true)

## Objectives

**1. Data Extraction & Processing**: Automate the retrieval of financial statement datasets from the SEC Market Data page.

**2. Data Storage & Transformation**: Design and evaluate multiple data storage methods, including Raw Staging, JSON Transformation, and Denormalized Fact Tables in Snowflake.

**3. ELT Decentralized Fact Tables Generation:** Utilize DBT (Data Build Tool) for ELT processes, ensuring decentralization of fact tables with rigorous checks for data integrity, schema consistency, and accurate transformations.

**Operational Pipeline**: Develop Airflow-based ETL pipelines using AWS S3 as an intermediary storage layer.

**Post-Upload Testing:** Conduct comprehensive validation of data uploads in Snowflake, ensuring data accuracy, schema adherence, and efficient query execution for downstream processes.

**Visualization & API Access:** Develop a Streamlit application for intuitive data exploration and visualization, complemented by a FastAPI backend to provide structured and secure access to the financial data stored in Snowflake.

**Comparative Analysis:** Perform an in-depth evaluation of the advantages, limitations, and trade-offs of various data storage strategies (e.g., raw staging, JSON transformation, and denormalized fact tables) to determine the most effective approach for financial data processing.

## Technology

- Cloud & Storage: Snowflake, AWS S3

- ETL & Pipeline: Apache Airflow

- Programming: Python, SQL, JSON Transformation

- Transformations Load : DBT (Data Build Tool)

- Backend API: FastAPI

- Frontend UI: Streamlit

# Links 

- **Airflow** :
- **FastAPI** :
- **Streamlit** :
