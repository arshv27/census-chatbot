"""
DataPulse - US Census Intelligence Platform

An AI-powered chat interface for exploring US Census data through
natural conversation, supporting multi-part queries and data reasoning.
"""
import streamlit as st
from db.snowflake_client import get_connection, get_optimized_schema, execute_multiple_queries
from ai.cortex_llm import call_cortex, MODEL_SQL, MODEL_CONVERSATIONAL
from ai.prompts import UNIFIED_AGENT_PROMPT, SYNTHESIS_PROMPT, REASONING_PROMPT
from agent.chat_agent import (
    _format_chat_history, 
    _parse_unified_response, 
    _clean_sql,
    MAX_HISTORY_TURNS
)


def escape_markdown(text: str) -> str:
    """Escape $ signs to prevent LaTeX rendering issues."""
    if not text:
        return text
    return text.replace("$", "\\$")


st.set_page_config(
    page_title="DataPulse - Census Intelligence",
    page_icon="🔮",
    layout="centered",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
    .main > div { padding-top: 1rem; }
    [data-testid="stChatMessage"] {
        padding: 0.75rem 1rem;
        border-radius: 12px;
        margin-bottom: 0.5rem;
    }
    h1 { color: #2E4057; font-weight: 600; }
    .subtitle { color: #666; font-size: 1.1rem; margin-bottom: 1.5rem; }
    .welcome-box {
        background: linear-gradient(135deg, #f5f7fa 0%, #e4e8ec 100%);
        border-left: 4px solid #4A90D9;
        padding: 1.25rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .example-q {
        background: #f8f9fa;
        padding: 0.5rem 0.75rem;
        border-radius: 6px;
        margin: 0.25rem 0;
        font-size: 0.9rem;
        color: #555;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    dev_mode = st.toggle("🔧 Developer Mode", value=False)
    st.caption("_View generated SQL and raw data_")
    
    st.divider()
    st.markdown("### 📊 What I Know")
    st.markdown("""
    **Demographics**
    - Population by state/county
    - Age distribution
    
    **Economics**  
    - Household income
    - Poverty rates
    - Employment stats
    
    **Housing**
    - Home values
    - Rent prices
    
    **Education**
    - Degree holders (state level)
    """)
    
    st.divider()
    st.markdown("**📅 Data:** 2019 & 2020")
    st.markdown("**🗺️ Coverage:** States & Counties")
    st.caption("_No city-level data available_")
    
    st.divider()
    if st.button("🗑️ Clear Chat", use_container_width=True, type="secondary"):
        st.session_state.messages = []
        st.rerun()
    
    st.caption("🔮 DataPulse · Powered by Snowflake Cortex")

st.session_state.dev_mode = dev_mode

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Header - always show
st.markdown("# 🔮 DataPulse")
st.markdown('<p class="subtitle">Your intelligent guide to US Census data</p>', unsafe_allow_html=True)

# Welcome section placeholder - can be cleared dynamically
welcome_container = st.empty()

# Show welcome only if no messages yet
if not st.session_state.messages:
    with welcome_container.container():
        st.markdown("""
        <div class="welcome-box">
            <strong>👋 Welcome!</strong><br><br>
            I'm an AI-powered assistant that helps you explore US Census data through natural conversation.
            <br><br>
            <strong>✨ What I can do:</strong>
            <ul style="margin: 0.5rem 0; padding-left: 1.2rem;">
                <li><strong>Answer complex questions</strong> — Ask multi-part questions and I'll run multiple queries to get you complete answers</li>
                <li><strong>Reason about data</strong> — I can analyze patterns and offer insights, not just raw numbers</li>
                <li><strong>Remember context</strong> — Follow-up questions work naturally in our conversation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("**💡 Try these questions:**")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="example-q">Which state has the highest income?</div>', unsafe_allow_html=True)
            st.markdown('<div class="example-q">Compare Texas and Florida</div>', unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="example-q">What\'s the poorest state and county? Is the poorest county in the poorest state?</div>', unsafe_allow_html=True)
            st.markdown('<div class="example-q">Why does Mississippi have low income?</div>', unsafe_allow_html=True)

# Display chat history
for message in st.session_state.messages:
    avatar = "🧑" if message["role"] == "user" else "🤖"
    with st.chat_message(message["role"], avatar=avatar):
        display_content = escape_markdown(message["content"]) if message["role"] == "assistant" else message["content"]
        st.markdown(display_content)
        
        if message["role"] == "assistant" and st.session_state.dev_mode and "debug" in message:
            debug = message["debug"]
            with st.expander("🔧 Technical Details", expanded=False):
                if debug.get("standalone_query"):
                    st.caption("**Interpreted as:**")
                    st.info(debug["standalone_query"])
                if debug.get("sql"):
                    sql_queries = [q.strip() for q in debug["sql"].split("---SQL---") if q.strip()]
                    if len(sql_queries) > 1:
                        st.caption(f"**SQL Queries ({len(sql_queries)} queries):**")
                        for i, q in enumerate(sql_queries, 1):
                            st.text(f"Query {i}:")
                            st.code(q, language="sql")
                    else:
                        st.caption("**SQL Query:**")
                        st.code(debug["sql"], language="sql")
                if debug.get("sql_results"):
                    st.caption("**Raw Results:**")
                    results = debug["sql_results"]
                    st.text(results[:600] + "..." if len(results) > 600 else results)

# Chat input
if prompt := st.chat_input("Ask about US Census data..."):
    # Validate input
    if not prompt.strip():
        st.warning("Please enter a question about census data.")
        st.stop()
    
    # Clear welcome section immediately
    welcome_container.empty()
    
    # Add user message to state and display immediately
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user", avatar="🧑"):
        st.markdown(prompt)
    
    with st.chat_message("assistant", avatar="🤖"):
        # Status placeholder for live updates
        status_placeholder = st.empty()
        
        debug_info = {}
        response = ""
        
        try:
            # Step 1: Understanding
            status_placeholder.markdown("🧠 *Understanding your question...*")
            
            conn = get_connection()
            schema = get_optimized_schema(conn)
            
            history_for_agent = [
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages[:-1]
            ]
            history_str = _format_chat_history(history_for_agent, MAX_HISTORY_TURNS)
            
            # Step 2: Generating query
            status_placeholder.markdown("📝 *Generating database query...*")
            
            unified_prompt = UNIFIED_AGENT_PROMPT.format(
                chat_history=history_str,
                user_question=prompt,
                schema=schema
            )
            llm_response = call_cortex(conn, unified_prompt, model=MODEL_SQL)
            parsed = _parse_unified_response(llm_response)
            
            action = parsed.get("action", "QUERY").upper()
            standalone_query = parsed.get("resolved_question", prompt)
            generated_sql = _clean_sql(parsed.get("sql", ""))
            
            debug_info["standalone_query"] = standalone_query
            debug_info["sql"] = generated_sql
            
            if action == "REJECT":
                reason = parsed.get("reason", "").upper()
                user_q_lower = prompt.lower()
                
                # Check if it's a city-related question
                if "UNANSWERABLE" in reason and ("city" in user_q_lower or "cities" in user_q_lower):
                    response = (
                        "I don't have city-level data - my data covers **states and counties** only. "
                        "I can help you find information about specific counties or states instead. "
                        "For example, try asking about a specific county like 'Los Angeles County' or a state like 'California'."
                    )
                elif "UNANSWERABLE" in reason:
                    response = (
                        "That's an interesting question, but it requires information beyond what census data can tell us. "
                        "Census data covers demographics like population, income, housing, education, and employment - "
                        "but not things like geography, politics, or historical events. "
                        "Is there something specific about the demographics I could help you explore instead?"
                    )
                else:
                    response = (
                        "I'm designed to answer questions about US Census and demographic data only. "
                        "I can help with topics like population, income, housing, education, employment, and poverty statistics "
                        "across US states and counties. Could you try asking something in that area?"
                    )
            elif action == "REASONING":
                status_placeholder.markdown("✨ *Analyzing the data...*")
                
                if generated_sql:
                    sql_results = execute_multiple_queries(conn, generated_sql)
                    debug_info["sql_results"] = sql_results
                    
                    if "ERROR:" in sql_results and "Result Set" not in sql_results:
                        response = (
                            "I'd love to help analyze that, but I had trouble fetching the relevant data. "
                            "Could you try rephrasing your question?"
                        )
                    else:
                        reasoning_prompt = REASONING_PROMPT.format(
                            question=standalone_query,
                            data=sql_results
                        )
                        response = call_cortex(conn, reasoning_prompt, model=MODEL_CONVERSATIONAL)
                        
                        if response.startswith("ERROR:"):
                            response = (
                                "I found some relevant data about this topic but had trouble completing the analysis. "
                                "Would you like me to try a more specific question, like comparing specific metrics or regions?"
                            )
                else:
                    response = (
                        "That's a thoughtful question! To help analyze this, could you be more specific about "
                        "which state or region you'd like me to look at?"
                    )
            else:
                status_placeholder.markdown("✨ *Crafting your answer...*")
                
                sql_results = execute_multiple_queries(conn, generated_sql)
                debug_info["sql_results"] = sql_results
                
                if sql_results.startswith("ERROR:") and "Result Set" not in sql_results:
                    response = "I encountered a technical issue querying the census data. Please try rephrasing your question."
                else:
                    synthesis_prompt = SYNTHESIS_PROMPT.format(
                        question=standalone_query,
                        data=sql_results
                    )
                    response = call_cortex(conn, synthesis_prompt, model=MODEL_CONVERSATIONAL)
                    
                    if response.startswith("ERROR:"):
                        response = (
                            "I found relevant census data but had trouble summarizing it. "
                            "Would you like me to try answering a more specific question about this topic?"
                        )
        
        except Exception as e:
            response = "I'm having trouble connecting to the database. Please try again in a moment."
            debug_info["error"] = str(e)
        
        # Replace status with final response (using the same placeholder)
        status_placeholder.markdown(escape_markdown(response))
        
        # Dev mode panel
        if st.session_state.dev_mode and debug_info.get("sql"):
            with st.expander("🔧 Technical Details", expanded=False):
                if debug_info.get("standalone_query"):
                    st.caption("**Interpreted as:**")
                    st.info(debug_info["standalone_query"])
                if debug_info.get("sql"):
                    sql_queries = [q.strip() for q in debug_info["sql"].split("---SQL---") if q.strip()]
                    if len(sql_queries) > 1:
                        st.caption(f"**SQL Queries ({len(sql_queries)} queries):**")
                        for i, q in enumerate(sql_queries, 1):
                            st.text(f"Query {i}:")
                            st.code(q, language="sql")
                    else:
                        st.caption("**SQL Query:**")
                        st.code(debug_info["sql"], language="sql")
                if debug_info.get("sql_results"):
                    st.caption("**Raw Results:**")
                    results = debug_info["sql_results"]
                    st.text(results[:600] + "..." if len(results) > 600 else results)
    
    # Save to history and rerun to clear welcome cards + ensure clean state
    st.session_state.messages.append({
        "role": "assistant",
        "content": response,
        "debug": debug_info
    })
    st.rerun()
