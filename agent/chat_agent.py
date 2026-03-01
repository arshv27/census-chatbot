"""
Agent orchestration layer for the US Census AI chatbot.

2-CALL ARCHITECTURE:
1. Unified Agent Call: guardrails + context resolution + SQL generation
2. SQL Execution (database call)  
3. Synthesis Call: natural language response generation

Optional: Self-correction step if SQL fails (adds 1 LLM call when needed)
"""
import json
import re
from dataclasses import dataclass, field
from typing import List, Dict, Any

from db.snowflake_client import execute_query, get_optimized_schema
from ai.cortex_llm import call_cortex, MODEL_CONVERSATIONAL, MODEL_SQL
from ai.prompts import (
    UNIFIED_AGENT_PROMPT,
    SELF_CORRECTION_PROMPT,
    SYNTHESIS_PROMPT,
)


MAX_RETRY_COUNT = 1
MAX_HISTORY_TURNS = 10


@dataclass
class AgentState:
    """
    Typed state object for a single conversation turn.
    
    Using a dataclass ensures type safety, immutability of structure,
    and clear tracking of the agent's execution flow.
    """
    user_message: str
    chat_history: List[Dict[str, str]] = field(default_factory=list)
    is_safe: bool = True
    rejection_reason: str = ""
    standalone_query: str = ""
    generated_sql: str = ""
    sql_execution_error: str = ""
    sql_results: str = ""
    final_answer: str = ""
    retry_count: int = 0


def _format_chat_history(history: List[Dict[str, str]], max_turns: int = MAX_HISTORY_TURNS) -> str:
    """Format recent chat history for the LLM prompt."""
    if not history:
        return "No previous conversation."
    
    recent = history[-max_turns * 2:]
    formatted = []
    for msg in recent:
        role = msg.get("role", "unknown").upper()
        content = msg.get("content", "")
        formatted.append(f"{role}: {content}")
    
    return "\n".join(formatted)


def _parse_unified_response(response: str) -> Dict[str, Any]:
    """
    Parse the JSON response from the unified agent prompt.
    
    Expected format:
    {"action": "REJECT"|"QUERY", "reason": "...", "resolved_question": "...", "sql": "..."}
    """
    # Try to find JSON in the response
    json_match = re.search(r'\{[^{}]*"action"[^{}]*\}', response, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # Fallback: try to parse the entire response
    try:
        return json.loads(response.strip())
    except json.JSONDecodeError:
        pass
    
    # Last resort: assume it's a SQL query (backward compatibility)
    return {
        "action": "QUERY",
        "reason": "",
        "resolved_question": "",
        "sql": response.strip()
    }


def _clean_sql(sql: str) -> str:
    """Remove markdown formatting from SQL if present."""
    sql = sql.strip()
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip()


def _step_unified_agent(conn, state: AgentState, schema: str) -> AgentState:
    """
    UNIFIED STEP: Combined guardrails, context resolution, and SQL generation.
    
    This replaces the separate router and SQL generation steps, reducing
    from 2 LLM calls to 1 LLM call.
    """
    history_str = _format_chat_history(state.chat_history)
    
    prompt = UNIFIED_AGENT_PROMPT.format(
        chat_history=history_str,
        user_question=state.user_message,
        schema=schema
    )
    
    # Use the SQL model since it needs to generate SQL
    response = call_cortex(conn, prompt, model=MODEL_SQL)
    
    if response.startswith("ERROR:"):
        state.is_safe = False
        state.rejection_reason = "I encountered a technical issue. Please try again."
        return state
    
    parsed = _parse_unified_response(response)
    
    action = parsed.get("action", "QUERY").upper()
    reason = parsed.get("reason", "")
    
    if action == "REJECT":
        state.is_safe = False
        state.rejection_reason = "OFF_TOPIC"
        state.standalone_query = parsed.get("resolved_question", state.user_message)
    elif action == "REASONING":
        # Question is related but asks for analysis/reasoning we can't provide
        state.is_safe = False
        state.rejection_reason = "REASONING"
        state.standalone_query = parsed.get("resolved_question", state.user_message)
    else:
        state.is_safe = True
        state.standalone_query = parsed.get("resolved_question", state.user_message)
        state.generated_sql = _clean_sql(parsed.get("sql", ""))
    
    return state


def _step_sql_execution(conn, state: AgentState) -> AgentState:
    """Execute the generated SQL."""
    result = execute_query(conn, state.generated_sql)
    
    if result.startswith("ERROR:"):
        state.sql_execution_error = result
    else:
        state.sql_results = result
        state.sql_execution_error = ""
    
    return state


def _step_self_correction(conn, state: AgentState, schema: str) -> AgentState:
    """Attempt to fix failed SQL using LLM self-correction."""
    prompt = SELF_CORRECTION_PROMPT.format(
        failed_sql=state.generated_sql,
        error_message=state.sql_execution_error,
        schema=schema
    )
    
    response = call_cortex(conn, prompt, model=MODEL_SQL)
    
    if response.startswith("ERROR:"):
        return state
    
    state.generated_sql = _clean_sql(response)
    state.retry_count += 1
    
    return state


def _step_synthesis(conn, state: AgentState) -> AgentState:
    """Synthesize a natural language answer from the SQL results."""
    prompt = SYNTHESIS_PROMPT.format(
        question=state.standalone_query,
        data=state.sql_results
    )
    
    response = call_cortex(conn, prompt, model=MODEL_CONVERSATIONAL)
    
    if response.startswith("ERROR:"):
        state.final_answer = "I found the data but encountered an issue formatting the response. Here's the raw data:\n\n" + state.sql_results
    else:
        state.final_answer = response
    
    return state


def process_user_query(conn, user_message: str, chat_history: List[Dict[str, str]]) -> AgentState:
    """
    Main orchestrator function for processing a user query.
    
    OPTIMIZED 2-CALL PIPELINE:
    1. Unified agent (guardrails + context + SQL) → 1 LLM call
    2. SQL execution → DB call
    3. Self-correction if needed → 1 LLM call (optional)
    4. Synthesis → 1 LLM call
    
    Total: 2 LLM calls (down from 3)
    
    Args:
        conn: Snowflake connection object
        user_message: The user's input message
        chat_history: List of previous messages [{"role": "user"|"assistant", "content": "..."}]
    
    Returns:
        AgentState with all fields populated, including final_answer
    """
    state = AgentState(
        user_message=user_message,
        chat_history=chat_history
    )
    
    # Get schema first (needed for unified prompt)
    schema = get_optimized_schema(conn)
    
    if not schema or "Error" in schema:
        state.final_answer = "I'm having trouble accessing the census database schema. Please try again in a moment."
        return state
    
    # UNIFIED STEP: guardrails + context + SQL generation (1 LLM call)
    state = _step_unified_agent(conn, state, schema)
    
    if not state.is_safe:
        if state.rejection_reason == "REASONING":
            # Question is related but asks for analysis/reasoning
            state.final_answer = (
                "That's a great question! While I can see it's related to the census data we were discussing, "
                "I'm only able to look up and report statistics - I can't analyze causes or make predictions. "
                "\n\nHowever, I could help you explore related data that might offer insights. For example, I could show you:\n"
                "• Education levels and employment rates\n"
                "• Poverty statistics and income data\n"
                "• How these metrics compare across different states\n\n"
                "Would you like me to pull any of that data?"
            )
        else:
            # Truly off-topic
            state.final_answer = (
                "I'm designed to answer questions about US Census and demographic data only. "
                "I can help with topics like population, income, housing, education, employment, and poverty statistics "
                "across US states and counties. Could you try asking something in that area?"
            )
        return state
    
    if not state.generated_sql:
        state.final_answer = "I couldn't generate a query for your question. Could you try rephrasing it?"
        return state
    
    # SQL EXECUTION (DB call)
    state = _step_sql_execution(conn, state)
    
    # SELF-CORRECTION if needed (optional LLM call)
    if state.sql_execution_error and state.retry_count < MAX_RETRY_COUNT:
        state = _step_self_correction(conn, state, schema)
        state = _step_sql_execution(conn, state)
    
    if state.sql_execution_error:
        state.final_answer = "I encountered a technical issue querying the census data. Please try rephrasing your question or ask something different."
        return state
    
    # SYNTHESIS (1 LLM call)
    state = _step_synthesis(conn, state)
    
    return state
