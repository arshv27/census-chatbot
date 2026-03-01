"""Database connection and query execution for Snowflake."""
from db.snowflake_client import (
    get_connection,
    execute_query,
    execute_multiple_queries,
    get_optimized_schema,
)

__all__ = ["get_connection", "execute_query", "execute_multiple_queries", "get_optimized_schema"]
