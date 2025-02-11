USE ROLE ACCOUNTADMIN;

// Warehouse
CREATE WAREHOUSE IF NOT EXISTS dbt_wh with WAREHOUSE_SIZE='x-small';
USE WAREHOUSE dbt_wh;

// Database
CREATE DATABASE IF NOT EXISTS dbt_db;
USE DATABASE dbt_db;

//Role
CREATE ROLE IF NOT EXISTS dbt_role;
GRANT ROLE dbt_role TO USER vedantmane;

GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;
GRANT ALL ON DATABASE dbt_db TO ROLE dbt_role;
 
USE ROLE dbt_role;

CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_schema;
USE SCHEMA dbt_schema;

// Creating JSON File Format
CREATE OR REPLACE FILE FORMAT sec_json_format
TYPE = 'JSON';
// STRIP_OUTER_ARRAY = TRUE; -- Optional: Use if your JSON files are arrays

// CREATE JSON Stage on Snowflake from S3
CREATE OR REPLACE STAGE sec_json_stage
URL = 's3://secdatafiles/data/2024/1/json/'
CREDENTIALS = (AWS_KEY_ID = 'AKIA4SZHNTVUXP4OX3VV'
                AWS_SECRET_KEY = 'sE6Bupmmbmxo5boOvSQedcNJVsFdsstQ0FO151mX')
FILE_FORMAT = sec_json_format;
//STORAGE_INTEGRATION = your_storage_integration_name -- Optional: Use if private bucket

//CREATE Table for JSON Files
CREATE OR REPLACE TABLE SEC_JSON_2024_Q1 (
    json_data VARIANT
);

//INSERT JSON Data into Table
COPY INTO SEC_JSON_2024_Q1
FROM @sec_json_stage
FILE_FORMAT = (FORMAT_NAME = sec_json_format)
PATTERN = '.*\.json'; -- Matches all files with .json extension

SELECT * FROM SEC_JSON_2024_Q1 LIMIT 10;

SELECT COUNT(*) FROM SEC_JSON_2024_Q1;

// CREATE a VIEW for METADATA from JSON Files
CREATE OR REPLACE VIEW metadata_view AS
SELECT 
    json_data:year::NUMBER AS year,
    json_data:quarter::STRING AS quarter,
    json_data:country::STRING AS country,
    json_data:city::STRING AS city,
    json_data:name::STRING AS company_name,
    json_data:symbol::STRING AS symbol,
    json_data:startDate::DATE AS start_date,
    json_data:endDate::DATE AS end_date
FROM SEC_JSON_2024_Q1;

SELECT * FROM METADATA_VIEW LIMIT 10;

CREATE OR REPLACE VIEW JSON_BS_2024_Q1 AS
SELECT 
    t.json_data:year::NUMBER AS year,
    t.json_data:quarter::STRING AS quarter,
    t.json_data:country::STRING AS country,
    t.json_data:city::STRING AS city,
    t.json_data:name::STRING AS company_name,
    t.json_data:symbol::STRING AS symbol,
    
    bs.value:concept::STRING AS concept,
    bs.value:info::STRING AS info,
    bs.value:label::STRING AS label,
    bs.value:unit::STRING AS unit,
    bs.value:value::NUMBER AS value
FROM SEC_JSON_2024_Q1 t, 
     LATERAL FLATTEN(input => t.json_data:data.bs) bs;

SELECT * FROM JSON_BS_2024_Q1;

CREATE OR REPLACE VIEW JSON_CF_2024_Q1 AS
SELECT 
    t.json_data:year::NUMBER AS year,
    t.json_data:quarter::STRING AS quarter,
    t.json_data:country::STRING AS country,
    t.json_data:city::STRING AS city,
    t.json_data:name::STRING AS company_name,
    t.json_data:symbol::STRING AS symbol,

    cf.value:concept::STRING AS concept,
    cf.value:info::STRING AS info,
    cf.value:label::STRING AS label,
    cf.value:unit::STRING AS unit,
    cf.value:value::NUMBER AS value
FROM SEC_JSON_2024_Q1 t, 
     LATERAL FLATTEN(input => t.json_data:data.cf) cf;

SELECT * FROM JSON_CF_2024_Q1;

CREATE OR REPLACE VIEW JSON_IS_2024_Q1 AS
SELECT 
    t.json_data:year::NUMBER AS year,
    t.json_data:quarter::STRING AS quarter,
    t.json_data:country::STRING AS country,
    t.json_data:city::STRING AS city,
    t.json_data:name::STRING AS company_name,
    t.json_data:symbol::STRING AS symbol,

    ic.value:concept::STRING AS concept,
    ic.value:info::STRING AS info,
    ic.value:label::STRING AS label,
    ic.value:unit::STRING AS unit,
    ic.value:value::NUMBER AS value
FROM SEC_JSON_2024_Q1 t, 
     LATERAL FLATTEN(input => t.json_data:data.ic) ic;

SELECT * FROM JSON_IS_2024_Q1;

CREATE OR REPLACE VIEW JSON_FV_2024_Q1 AS
SELECT *, 'Balance Sheet' AS section_type FROM JSON_BS_2024_Q1
UNION ALL
SELECT *, 'Cash Flow' AS section_type FROM JSON_CF_2024_Q1
UNION ALL
SELECT *, 'Income Statement' AS section_type FROM JSON_IS_2024_Q1;

SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='DBT_SCHEMA' AND TABLE_NAME = 'SEC_JSON_2024_Q1';

SELECT section_type,COUNT(*) FROM JSON_FV_2024_Q1 GROUP BY section_type;