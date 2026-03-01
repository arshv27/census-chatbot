"""
Stress Test Suite for DataPulse Census AI Assistant

5 multi-turn conversations testing:
1. Multi-part queries and follow-ups
2. Context retention across turns
3. Reasoning/analytical questions
4. Guardrails (off-topic, unanswerable)
5. Edge cases and complex comparisons
"""

import sys
sys.path.insert(0, '.')

from db.snowflake_client import get_connection, get_optimized_schema, execute_multiple_queries
from ai.cortex_llm import call_cortex, MODEL_SQL, MODEL_CONVERSATIONAL
from ai.prompts import UNIFIED_AGENT_PROMPT, SYNTHESIS_PROMPT, REASONING_PROMPT
from agent.chat_agent import _format_chat_history, _parse_unified_response, _clean_sql


def process_message(conn, schema, messages, user_input):
    """Process a single message and return response + debug info."""
    messages.append({"role": "user", "content": user_input})
    
    history_str = _format_chat_history(messages[:-1], max_turns=10)
    
    unified_prompt = UNIFIED_AGENT_PROMPT.format(
        chat_history=history_str,
        user_question=user_input,
        schema=schema
    )
    llm_response = call_cortex(conn, unified_prompt, model=MODEL_SQL)
    parsed = _parse_unified_response(llm_response)
    
    action = parsed.get("action", "QUERY").upper()
    standalone_query = parsed.get("resolved_question", user_input)
    generated_sql = _clean_sql(parsed.get("sql", ""))
    reason = parsed.get("reason", "")
    
    debug = {
        "action": action,
        "reason": reason,
        "resolved_question": standalone_query,
        "sql": generated_sql,
    }
    
    if action == "REJECT":
        if "UNANSWERABLE" in reason.upper():
            response = (
                "That's an interesting question, but it requires information beyond what census data can tell us. "
                "Census data covers demographics like population, income, housing, education, and employment - "
                "but not things like geography, politics, or historical events."
            )
        else:
            response = (
                "I'm designed to answer questions about US Census and demographic data only. "
                "I can help with topics like population, income, housing, education, employment, and poverty statistics."
            )
    elif action == "REASONING":
        if generated_sql:
            sql_results = execute_multiple_queries(conn, generated_sql)
            debug["sql_results"] = sql_results[:500]
            
            if "ERROR:" in sql_results and "Result Set" not in sql_results:
                response = "I had trouble fetching the relevant data. Could you try rephrasing?"
            else:
                reasoning_prompt = REASONING_PROMPT.format(
                    question=standalone_query,
                    data=sql_results
                )
                response = call_cortex(conn, reasoning_prompt, model=MODEL_CONVERSATIONAL)
                if response.startswith("ERROR:"):
                    response = "I found data but had trouble analyzing it."
        else:
            response = "Could you be more specific about which region you'd like me to analyze?"
    else:  # QUERY
        if generated_sql:
            sql_results = execute_multiple_queries(conn, generated_sql)
            debug["sql_results"] = sql_results[:500]
            
            if sql_results.startswith("ERROR:") and "Result Set" not in sql_results:
                response = f"SQL Error: {sql_results}"
            else:
                synthesis_prompt = SYNTHESIS_PROMPT.format(
                    question=standalone_query,
                    data=sql_results
                )
                response = call_cortex(conn, synthesis_prompt, model=MODEL_CONVERSATIONAL)
                if response.startswith("ERROR:"):
                    response = "I found data but had trouble summarizing it."
        else:
            response = "I couldn't generate a query for that question."
    
    messages.append({"role": "assistant", "content": response})
    return response, debug


def check_response(response, debug, expected_checks):
    """Check if response meets expected criteria."""
    issues = []
    
    for check_name, check_fn in expected_checks.items():
        if not check_fn(response, debug):
            issues.append(f"❌ FAILED: {check_name}")
        else:
            issues.append(f"✅ PASSED: {check_name}")
    
    return issues


def run_conversation(conn, schema, conversation_name, turns):
    """Run a multi-turn conversation and report results."""
    print(f"\n{'='*70}")
    print(f"CONVERSATION: {conversation_name}")
    print('='*70)
    
    messages = []
    all_passed = True
    
    for i, turn in enumerate(turns, 1):
        user_input = turn["user"]
        expected_checks = turn.get("checks", {})
        
        print(f"\n--- Turn {i} ---")
        print(f"👤 USER: {user_input}")
        
        response, debug = process_message(conn, schema, messages, user_input)
        
        print(f"🤖 ACTION: {debug['action']}")
        if debug.get('reason'):
            print(f"   REASON: {debug['reason']}")
        print(f"   RESOLVED: {debug['resolved_question'][:100]}...")
        if debug.get('sql'):
            sql_preview = debug['sql'].replace('\n', ' ')[:150]
            print(f"   SQL: {sql_preview}...")
        print(f"🤖 RESPONSE: {response[:300]}...")
        
        if expected_checks:
            check_results = check_response(response, debug, expected_checks)
            for result in check_results:
                print(f"   {result}")
                if "FAILED" in result:
                    all_passed = False
    
    return all_passed


def main():
    print("="*70)
    print("DATAPULSE STRESS TEST SUITE")
    print("="*70)
    
    conn = get_connection()
    schema = get_optimized_schema(conn)
    
    all_results = []
    
    # =========================================================================
    # CONVERSATION 1: Multi-part queries with follow-ups
    # =========================================================================
    conv1 = [
        {
            "user": "What's the richest and poorest state? How do they compare on education?",
            "checks": {
                "contains_multiple_states": lambda r, d: "District of Columbia" in r or "DC" in r or "Mississippi" in r,
                "mentions_income": lambda r, d: "income" in r.lower() or "$" in r,
                "used_multiple_queries": lambda r, d: "---SQL---" in d.get("sql", "") or "education" in r.lower(),
            }
        },
        {
            "user": "What about their unemployment rates?",
            "checks": {
                "context_retained": lambda r, d: "Mississippi" in d.get("resolved_question", "") or "DC" in d.get("resolved_question", "") or "richest" in d.get("resolved_question", "").lower(),
                "mentions_unemployment": lambda r, d: "unemploy" in r.lower() or "%" in r,
            }
        },
        {
            "user": "Which one has more poverty?",
            "checks": {
                "context_retained": lambda r, d: "Mississippi" in r or "DC" in r or "Columbia" in r,
                "gives_conclusion": lambda r, d: "higher" in r.lower() or "more" in r.lower() or "lower" in r.lower(),
            }
        },
    ]
    all_results.append(("Multi-part + Follow-ups", run_conversation(conn, schema, "Multi-part Queries + Follow-ups", conv1)))
    
    # =========================================================================
    # CONVERSATION 2: Complex comparisons with context
    # =========================================================================
    conv2 = [
        {
            "user": "Compare California, Texas, and Florida on population and income",
            "checks": {
                "mentions_all_three": lambda r, d: "California" in r and "Texas" in r and "Florida" in r,
                "has_numbers": lambda r, d: any(c.isdigit() for c in r),
            }
        },
        {
            "user": "Which of these has the best housing affordability?",
            "checks": {
                "context_retained": lambda r, d: any(s in d.get("resolved_question", "") for s in ["California", "Texas", "Florida"]),
                "mentions_housing": lambda r, d: "home" in r.lower() or "housing" in r.lower() or "rent" in r.lower(),
            }
        },
        {
            "user": "Add New York to this comparison",
            "checks": {
                "includes_ny": lambda r, d: "New York" in r or "NY" in r,
                "still_has_others": lambda r, d: any(s in r for s in ["California", "Texas", "Florida"]),
            }
        },
    ]
    all_results.append(("Complex Comparisons", run_conversation(conn, schema, "Complex Comparisons with Context", conv2)))
    
    # =========================================================================
    # CONVERSATION 3: Reasoning questions
    # =========================================================================
    conv3 = [
        {
            "user": "Why does West Virginia have such low income compared to other states?",
            "checks": {
                "action_is_reasoning": lambda r, d: d.get("action") == "REASONING",
                "provides_analysis": lambda r, d: len(r) > 200,
                "not_raw_data": lambda r, d: "|" not in r[:200],  # No table format at start
            }
        },
        {
            "user": "Is there a correlation between education and income across states?",
            "checks": {
                "action_is_reasoning": lambda r, d: d.get("action") == "REASONING",
                "mentions_correlation": lambda r, d: "correlat" in r.lower() or "relationship" in r.lower() or "pattern" in r.lower(),
            }
        },
    ]
    all_results.append(("Reasoning Questions", run_conversation(conn, schema, "Reasoning/Analytical Questions", conv3)))
    
    # =========================================================================
    # CONVERSATION 4: Guardrails testing
    # =========================================================================
    conv4 = [
        {
            "user": "What's the population of Texas?",
            "checks": {
                "action_is_query": lambda r, d: d.get("action") == "QUERY",
                "has_population": lambda r, d: any(c.isdigit() for c in r),
            }
        },
        {
            "user": "Do you think the governor is doing a good job?",
            "checks": {
                "rejected": lambda r, d: d.get("action") == "REJECT",
                "polite_rejection": lambda r, d: "census" in r.lower() or "demographic" in r.lower(),
            }
        },
        {
            "user": "What geographical features make Texas so big?",
            "checks": {
                "rejected_unanswerable": lambda r, d: d.get("action") == "REJECT" and "UNANSWERABLE" in d.get("reason", "").upper(),
                "explains_limitation": lambda r, d: "geography" in r.lower() or "beyond" in r.lower(),
            }
        },
        {
            "user": "Okay, what's the median rent in Texas then?",
            "checks": {
                "back_to_query": lambda r, d: d.get("action") == "QUERY",
                "has_rent_data": lambda r, d: "$" in r or "rent" in r.lower(),
            }
        },
    ]
    all_results.append(("Guardrails", run_conversation(conn, schema, "Guardrails Testing", conv4)))
    
    # =========================================================================
    # CONVERSATION 5: Edge cases and county-level queries
    # =========================================================================
    conv5 = [
        {
            "user": "What are the 5 richest counties in the US?",
            "checks": {
                "uses_county_table": lambda r, d: "COUNTY" in d.get("sql", "").upper(),
                "lists_counties": lambda r, d: "County" in r or "county" in r.lower(),
            }
        },
        {
            "user": "Are any of them in California?",
            "checks": {
                "context_retained": lambda r, d: "rich" in d.get("resolved_question", "").lower() or "county" in d.get("resolved_question", "").lower(),
                "answers_question": lambda r, d: "California" in r or "yes" in r.lower() or "no" in r.lower(),
            }
        },
        {
            "user": "Compare Puerto Rico to Mississippi - which is poorer overall?",
            "checks": {
                "compares_both": lambda r, d: "Puerto Rico" in r and "Mississippi" in r,
                "gives_conclusion": lambda r, d: "poorer" in r.lower() or "lower" in r.lower() or "less" in r.lower(),
            }
        },
        {
            "user": "Show me year-over-year change for Puerto Rico",
            "checks": {
                "includes_both_years": lambda r, d: "2019" in r or "2020" in r or "year" in r.lower(),
            }
        },
    ]
    all_results.append(("Edge Cases", run_conversation(conn, schema, "Edge Cases & County Queries", conv5)))
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "="*70)
    print("STRESS TEST SUMMARY")
    print("="*70)
    
    for conv_name, passed in all_results:
        status = "✅ PASSED" if passed else "❌ ISSUES FOUND"
        print(f"{status}: {conv_name}")
    
    total_passed = sum(1 for _, p in all_results if p)
    print(f"\nTotal: {total_passed}/{len(all_results)} conversations passed all checks")
    
    if total_passed == len(all_results):
        print("\n🎉 All stress tests passed!")
    else:
        print("\n⚠️  Some tests had issues - review output above")


if __name__ == "__main__":
    main()
