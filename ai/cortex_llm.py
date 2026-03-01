"""
Snowflake Cortex LLM interaction module.

Provides a clean interface to call LLMs via SNOWFLAKE.CORTEX.COMPLETE,
keeping all data and inference within Snowflake.
"""
from snowflake.connector.errors import ProgrammingError


MODEL_FAST = "llama3.1-70b"
MODEL_CONVERSATIONAL = "claude-3-5-sonnet"
MODEL_SQL = "mistral-large2"


def call_cortex(conn, prompt: str, model: str = MODEL_FAST) -> str:
    """
    Call Snowflake Cortex LLM with the given prompt.
    
    Args:
        conn: Snowflake connection object
        prompt: The prompt to send to the LLM
        model: Model identifier (default: MODEL_FAST)
    
    Returns:
        The LLM's response text, or an error message if the call failed.
    """
    escaped_prompt = prompt.replace("'", "''")
    
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{escaped_prompt}')"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            return result[0].strip()
        return "ERROR: Empty response from Cortex"
    
    except ProgrammingError as e:
        return f"ERROR: Cortex call failed - {str(e)}"
    except Exception as e:
        return f"ERROR: Unexpected error in Cortex call - {str(e)}"
