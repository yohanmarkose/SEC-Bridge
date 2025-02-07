sql_commands = ["""use role accountadmin;""",
                """create warehouse dbt_wh with warehouse_size='x-small';""",
                """create database if not exists dbt_db;""",
                """create role if not exists dbt_role;""",
                """show grants on warehouse dbt_wh;""",
                """grant role dbt_role to user yohanmarkose;""",
                """grant usage on warehouse dbt_wh to role dbt_role;""",
                """grant all on database dbt_db to role dbt_role;""",
                """use role dbt_role;""",
                """create schema if not exists dbt_db.dbt_schema;""",
                """CREATE OR REPLACE STAGE my_s3_stage
                    URL = 's3://secdatafiles/'
                    CREDENTIALS = (AWS_KEY_ID = 'AKIA4SZHNTVUXP4OX3VV' 
                                AWS_SECRET_KEY = 'sE6Bupmmbmxo5boOvSQedcNJVsFdsstQ0FO151mX');""",
                """CREATE OR REPLACE FILE FORMAT my_csv_format
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY='"'  -- Handles strings with commas
                NULL_IF = ('', 'NULL')
                PARSE_HEADER = TRUE;""",

                """CREATE OR REPLACE TABLE raw_sub (
                    adsh VARCHAR PRIMARY KEY, -- Unique submission ID
                    cik VARCHAR, -- Central Index Key (company identifier)
                    name VARCHAR, -- Company name
                    sic VARCHAR, -- Standard Industrial Classification code
                    countryba VARCHAR, -- Country of business address
                    stprba VARCHAR, -- State/province of business address
                    cityba VARCHAR, -- City of business address
                    zipba VARCHAR, -- ZIP code of business address
                    bas1 VARCHAR, -- Business address street 1
                    bas2 VARCHAR, -- Business address street 2
                    baph VARCHAR, -- Business address phone
                    countryma VARCHAR, -- Country of mailing address
                    stprma VARCHAR, -- State/province of mailing address
                    cityma VARCHAR, -- City of mailing address
                    zipma VARCHAR, -- ZIP code of mailing address
                    mas1 VARCHAR, -- Mailing address street 1
                    mas2 VARCHAR, -- Mailing address street 2
                    countryinc VARCHAR, -- Country of incorporation
                    stprinc VARCHAR, -- State/province of incorporation
                    ein VARCHAR, -- Employer Identification Number
                    former VARCHAR, -- Former company name
                    changed VARCHAR, -- Date of name change
                    afs VARCHAR, -- Accounting standards (e.g., 1-LAF, 4-NON)
                    wksi VARCHAR, -- Well-known seasoned issuer (0 or 1)
                    fye VARCHAR, -- Fiscal year end (e.g., 1231 for December 31)
                    form VARCHAR, -- Form type (e.g., 10-Q, 10-K)
                    period VARCHAR, -- Period end date
                    fy VARCHAR, -- Fiscal year
                    fp VARCHAR, -- Fiscal period (e.g., Q1, Q2, Q3, Q4)
                    filed VARCHAR, -- Filing date
                    accepted VARCHAR, -- Time accepted by SEC
                    prevrpt VARCHAR, -- Previous report (0 or 1)
                    detail VARCHAR, -- Detailed submission (0 or 1)
                    instance VARCHAR, -- Instance document name
                    nciks VARCHAR, -- Number of CIKs
                    aciks VARCHAR NULL -- Additional CIKs (comma-separated)
                );""",
                """CREATE OR REPLACE TABLE raw_num (
                    adsh VARCHAR PRIMARY KEY,
                    "tag" VARCHAR,
                    "version" VARCHAR,
                    ddate VARCHAR,
                    qtrs VARCHAR,
                    uom VARCHAR,
                    segments VARCHAR,
                    coreg VARCHAR,
                    value VARCHAR,
                    footnote VARCHAR
                );""",
                """CREATE OR REPLACE TABLE raw_tag (
                    "tag" VARCHAR,
                    "version" VARCHAR,
                    custom VARCHAR,
                    abstract VARCHAR,
                    datatype VARCHAR,
                    iord VARCHAR,
                    crdr VARCHAR,
                    tlabel VARCHAR,
                    doc VARCHAR
                );""",
                """CREATE OR REPLACE TABLE raw_pre (
                    adsh VARCHAR PRIMARY KEY,
                    report VARCHAR,
                    line VARCHAR,
                    stmt VARCHAR,
                    inpth VARCHAR,
                    rfile VARCHAR,
                    "tag" VARCHAR,
                    "version" VARCHAR,
                    plabel VARCHAR,
                    negating VARCHAR
                );""",
                """COPY INTO raw_sub
                FROM @my_s3_stage/DAMG7245_Assignment02/data/2024/3/sub.csv
                FILE_FORMAT = (FORMAT_NAME = my_csv_format) 
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;""",
                """COPY INTO raw_num
                FROM @my_s3_stage/DAMG7245_Assignment02/data/2024/3/num.csv
                FILE_FORMAT = (FORMAT_NAME = my_csv_format) 
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;""",
                """COPY INTO raw_pre
                FROM @my_s3_stage/DAMG7245_Assignment02/data/2024/3/pre.csv
                FILE_FORMAT = (FORMAT_NAME = my_csv_format) 
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;""",
                """COPY INTO raw_tag
                FROM @my_s3_stage/DAMG7245_Assignment02/data/2024/3/tag.csv
                FILE_FORMAT = (FORMAT_NAME = my_csv_format) 
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"""]