"""
Snowflake database connection and query execution module.

This module provides:
- Cached connection management to minimize latency
- Safe query execution with security guards
- Schema retrieval for LLM context
"""
import re
import os
import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
import pandas as pd
from typing import Optional

FORBIDDEN_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|MERGE)\b",
    re.IGNORECASE,
)

VIEWS = [
    "CENSUS_APP_DB.LLM_VIEWS.V_STATE_DEMOGRAPHICS",
    "CENSUS_APP_DB.LLM_VIEWS.V_COUNTY_DEMOGRAPHICS",
]


def _get_credentials() -> dict:
    """
    Retrieve Snowflake credentials from Streamlit secrets or environment variables.
    Streamlit Cloud uses st.secrets; local dev uses .env file.
    """
    # Try Streamlit secrets first (for Streamlit Cloud deployment)
    try:
        if hasattr(st, "secrets") and "snowflake" in st.secrets:
            return {
                "account": st.secrets["snowflake"]["account"],
                "user": st.secrets["snowflake"]["user"],
                "password": st.secrets["snowflake"]["password"],
                "role": st.secrets["snowflake"]["role"],
                "warehouse": st.secrets["snowflake"]["warehouse"],
                "database": st.secrets["snowflake"]["database"],
                "schema": st.secrets["snowflake"]["schema"],
            }
    except Exception:
        pass  # Fall through to .env file
    
    # Fall back to .env file for local development
    from dotenv import load_dotenv
    load_dotenv()
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE", "CENSUS_APP_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "CENSUS_APP_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "CENSUS_APP_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "LLM_VIEWS"),
    }


@st.cache_resource(show_spinner=False)
def _create_connection():
    """Create a new Snowflake connection."""
    creds = _get_credentials()
    conn = snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds["role"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )
    cursor = conn.cursor()
    cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30")
    cursor.close()
    return conn


def get_connection():
    """
    Get a valid Snowflake connection, reconnecting if the session expired.
    
    Handles authentication token expiration by clearing the cache
    and creating a fresh connection.
    """
    conn = _create_connection()
    
    # Test if connection is still valid
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return conn
    except (ProgrammingError, DatabaseError) as e:
        error_msg = str(e)
        # Check for auth expiration or connection issues
        if "expired" in error_msg.lower() or "authenticate" in error_msg.lower() or "connection" in error_msg.lower():
            # Clear cached connection and create new one
            _create_connection.clear()
            return _create_connection()
        raise


def _validate_query(query: str) -> Optional[str]:
    """
    Validate the SQL query for safety.
    
    Returns an error string if the query is unsafe, None if safe.
    """
    stripped = query.strip()
    if not stripped.upper().startswith("SELECT"):
        return "ERROR: Only SELECT queries are allowed."
    
    if FORBIDDEN_KEYWORDS.search(stripped):
        return "ERROR: Query contains forbidden keywords."
    
    return None


def _format_results(df: pd.DataFrame) -> str:
    """
    Format a DataFrame as a readable string for LLM consumption.
    
    Uses markdown table format if tabulate is available, 
    otherwise falls back to a simple pipe-delimited format.
    """
    try:
        return df.to_markdown(index=False)
    except ImportError:
        # Fallback if tabulate is not installed
        lines = [" | ".join(str(col) for col in df.columns)]
        lines.append("-" * len(lines[0]))
        for _, row in df.iterrows():
            lines.append(" | ".join(str(val) for val in row))
        return "\n".join(lines)


def execute_query(conn, query: str, max_rows: int = 100) -> str:
    """
    Execute a SQL query and return results as a formatted string.
    
    Args:
        conn: Snowflake connection object
        query: SQL query to execute
        max_rows: Maximum number of rows to return (default 100)
    
    Returns:
        String containing either:
        - Markdown-formatted table of results
        - "ERROR: <message>" if query failed
    """
    validation_error = _validate_query(query)
    if validation_error:
        return validation_error

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(max_rows)
        cursor.close()
        
        if not rows:
            return "Query returned no results."
        
        df = pd.DataFrame(rows, columns=columns)
        return _format_results(df)
    
    except ProgrammingError as e:
        return f"ERROR: {str(e)}"
    except DatabaseError as e:
        return f"ERROR: {str(e)}"
    except Exception as e:
        return f"ERROR: Unexpected error - {str(e)}"


def get_optimized_schema(conn) -> str:
    """
    Retrieve the DDL for all LLM-optimized views.
    
    This provides the LLM with exact column names, data types, and
    importantly, the column comments we added for semantic clarity.
    """
    schema_parts = []
    
    for view_name in VIEWS:
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT GET_DDL('VIEW', '{view_name}')")
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                schema_parts.append(f"-- View: {view_name}\n{result[0]}")
        except Exception as e:
            schema_parts.append(f"-- View: {view_name} (Error retrieving DDL: {e})")
    
    return "\n\n".join(schema_parts)


def execute_multiple_queries(conn, sql_string: str) -> str:
    """
    Execute one or more SQL queries separated by ---SQL---.
    
    Used for multi-part questions that require querying different tables.
    Returns combined results with clear section labels.
    
    Args:
        conn: Snowflake connection object
        sql_string: Single SQL query or multiple queries separated by "---SQL---"
    
    Returns:
        Combined results string, with section labels for multiple queries
    """
    queries = [q.strip() for q in sql_string.split("---SQL---") if q.strip()]
    
    if len(queries) == 1:
        return execute_query(conn, queries[0])
    
    all_results = []
    for i, query in enumerate(queries, 1):
        result = execute_query(conn, query)
        if result.startswith("ERROR:"):
            all_results.append(f"Query {i} Error: {result}")
        else:
            all_results.append(f"--- Result Set {i} ---\n{result}")
    
    return "\n\n".join(all_results)
