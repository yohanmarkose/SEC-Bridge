from dotenv import load_dotenv
import os
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from pydantic import BaseModel

class input_params(BaseModel):
    source: str
#     year: str
#     quarter: str

# Load environment variables (only once at the module level)
load_dotenv()

def connect_to_snowflake():
    # Generate Snowflake connection URL from environment variables
    registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
    engine = create_engine("snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}".format(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSCODE'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),  # e.g., xyz123.us-east-1
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'), 
            role=os.getenv('SNOWFLAKE_ROLE')
        ))
    return(engine)

def check_data_in_snowflake():
    # Generate Snowflake connection URL from environment variables
    registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
    engine = create_engine("snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}".format(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSCODE'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),  # e.g., xyz123.us-east-1
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema="INFORMATION_SCHEMA",
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'), 
            role=os.getenv('SNOWFLAKE_ROLE')
        ))
    return(engine)

# Initialize FastAPI app and database engine
app = FastAPI()

@app.get("/check-availability")
def check_data_availability(query: str = Query(..., description="SQL query to execute")):
    """
    Endpoint to check data availability on Snowflake.
    """
    try:
        # Validate and sanitize the query (use parameterized queries if possible)
        engine = check_data_in_snowflake()

        # Execute query using a context manager for session handling
        with engine.connect() as connection:
            result = connection.execute(query).fetchall()
        
        # Convert result rows into a list of dictionaries
        data = [dict(row) for row in result]
        return {"data": data}

    except Exception as e:
        # Handle errors and return meaningful messages
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.get("/query-data")
def query_data(query: str = Query(..., description="SQL query to execute")):
    """
    Endpoint to execute SQL queries on Snowflake.
    """
    try:
        # Validate and sanitize the query (use parameterized queries if possible)
        engine = connect_to_snowflake()

        if not query.strip().lower().startswith(("select", "show", "desc")):
            raise HTTPException(
                status_code=400,
                detail="Only SELECT, SHOW, or DESCRIBE queries are allowed."
            )

        # Execute query using a context manager for session handling
        with engine.connect() as connection:
            result = connection.execute(query).fetchall()
        
        # Convert result rows into a list of dictionaries
        data = [dict(row) for row in result]
        return {"data": data}

    except Exception as e:
        # Handle errors and return meaningful messages
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
