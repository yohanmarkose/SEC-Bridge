USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE COMPUTE_JSON_WH with WAREHOUSE_SIZE='x-small';
USE WAREHOUSE COMPUTE_JSON_WH;

CREATE OR REPLACE DATABASE JSON_DB;
USE DATABASE JSON_DB;
USE SCHEMA PUBLIC;

// Creating JSON File Format
CREATE OR REPLACE FILE FORMAT sec_json_format
TYPE = 'JSON';
// STRIP_OUTER_ARRAY = TRUE; -- Optional: Use if your JSON files are arrays

// CREATE JSON Stage on Snowflake from S3
CREATE OR REPLACE STAGE sec_json_stage
URL = 's3://bucketcompletebath'
CREDENTIALS = (AWS_KEY_ID = 'AWS_KEY_ID'
                AWS_SECRET_KEY = 'AWS_SECRET_KEY')
FILE_FORMAT = sec_json_format;
//STORAGE_INTEGRATION = your_storage_integration_name -- Optional: Use if private bucket

//CREATE Table for JSON Files
CREATE OR REPLACE TABLE sec_json_table (
    json_data VARIANT
);

//INSERT JSON Data into Table
COPY INTO sec_json_table
FROM @sec_json_stage
FILE_FORMAT = (FORMAT_NAME = sec_json_format)
PATTERN = '.*\.json'; -- Matches all files with .json extension

SELECT * FROM sec_json_table LIMIT 10;

SELECT COUNT(*) FROM sec_json_table;

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
FROM sec_json_table;

SELECT * FROM METADATA_VIEW LIMIT 10;

CREATE OR REPLACE VIEW balance_sheet_view AS
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
FROM sec_json_table t, 
     LATERAL FLATTEN(input => t.json_data:data.bs) bs;

SELECT * FROM balance_sheet_view;

CREATE OR REPLACE VIEW cash_flow_view AS
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
FROM sec_json_table t, 
     LATERAL FLATTEN(input => t.json_data:data.cf) cf;

SELECT * FROM cash_flow_view;

CREATE OR REPLACE VIEW income_statement_view AS
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
FROM sec_json_table t, 
     LATERAL FLATTEN(input => t.json_data:data.ic) ic;

SELECT * FROM income_statement_view;

CREATE OR REPLACE VIEW combined_financial_view AS
SELECT *, 'Balance Sheet' AS section_type FROM balance_sheet_view
UNION ALL
SELECT *, 'Cash Flow' FROM cash_flow_view
UNION ALL
SELECT *, 'Income Statement' FROM income_statement_view;

SELECT * FROM combined_financial_view;

SELECT section_type,COUNT(*) FROM combined_financial_view GROUP BY section_type;

