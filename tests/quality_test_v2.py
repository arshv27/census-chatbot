"""
DataPulse Quality Test Suite v2

Extended testing including:
1. Guardrails - off-topic, injection, unsafe, unanswerable, SQL injection
2. Robustness - typos, ambiguity, calculations, negation, year-specific, ordinals, boundaries
3. Answer Quality - accuracy, completeness, formatting
4. Hallucination Detection - factual accuracy verification
5. Context Retention - multi-turn conversation handling
6. Multi-part Questions - complex query handling

Results are logged to tests/test_results_v2.json for analysis.
"""

import json
import os
import sys
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.snowflake_client import get_connection, get_optimized_schema, execute_multiple_queries, execute_query
from ai.cortex_llm import call_cortex, MODEL_SQL, MODEL_CONVERSATIONAL
from ai.prompts import UNIFIED_AGENT_PROMPT, SYNTHESIS_PROMPT, REASONING_PROMPT
from agent.chat_agent import _format_chat_history, _parse_unified_response, _clean_sql, MAX_HISTORY_TURNS


def process_single_query(conn, schema: str, user_input: str, history: List[Dict] = None) -> Dict:
    """Process a single query through the full pipeline."""
    if history is None:
        history = []
    
    history_str = _format_chat_history(history, MAX_HISTORY_TURNS)
    
    unified_prompt = UNIFIED_AGENT_PROMPT.format(
        chat_history=history_str,
        user_question=user_input,
        schema=schema
    )
    
    llm_response = call_cortex(conn, unified_prompt, model=MODEL_SQL)
    parsed = _parse_unified_response(llm_response)
    
    action = parsed.get("action", "QUERY").upper()
    reason = parsed.get("reason", "")
    resolved_question = parsed.get("resolved_question", user_input)
    generated_sql = _clean_sql(parsed.get("sql", ""))
    
    result = {
        "input": user_input,
        "action": action,
        "reason": reason,
        "resolved_question": resolved_question,
        "generated_sql": generated_sql,
        "sql_results": None,
        "final_response": None,
        "error": None
    }
    
    if action == "REJECT":
        if "UNANSWERABLE" in reason.upper():
            result["final_response"] = f"[REJECTED - UNANSWERABLE] {reason}"
        else:
            result["final_response"] = f"[REJECTED - {reason}]"
        return result
    
    if action in ["REASONING", "QUERY"] and generated_sql:
        sql_results = execute_multiple_queries(conn, generated_sql)
        result["sql_results"] = sql_results
        
        if "ERROR:" in sql_results and "Result Set" not in sql_results:
            result["error"] = sql_results
            result["final_response"] = "[SQL ERROR]"
            return result
        
        prompt_template = REASONING_PROMPT if action == "REASONING" else SYNTHESIS_PROMPT
        synth_prompt = prompt_template.format(
            question=resolved_question,
            data=sql_results
        )
        response = call_cortex(conn, synth_prompt, model=MODEL_CONVERSATIONAL)
        result["final_response"] = response
        return result
    
    result["final_response"] = "[NO SQL GENERATED]"
    return result


def run_multi_turn_conversation(conn, schema: str, turns: List[Dict]) -> List[Dict]:
    """Run a multi-turn conversation, maintaining context."""
    history = []
    results = []
    
    for turn in turns:
        if turn.get("role") == "user":
            result = process_single_query(conn, schema, turn["content"], history)
            results.append(result)
            
            history.append({"role": "user", "content": turn["content"]})
            if result["final_response"] and not result["final_response"].startswith("["):
                history.append({"role": "assistant", "content": result["final_response"]})
    
    return results


def run_guardrail_tests(conn, schema: str, test_cases: Dict) -> Dict:
    """Test guardrail effectiveness."""
    results = {"category": "guardrails", "subcategories": {}}
    
    for subcategory in ["off_topic", "prompt_injection", "unsafe_content", "unanswerable", "sql_injection"]:
        if subcategory not in test_cases:
            continue
            
        subcategory_results = []
        cases = test_cases[subcategory]
        
        print(f"\n  Testing {subcategory} ({len(cases)} cases)...")
        
        for case in cases:
            print(f"    - {case['id']}: {case['input'][:50]}...")
            
            result = process_single_query(conn, schema, case["input"])
            
            expected_action = case.get("expected_action", "REJECT")
            actual_action = result["action"]
            passed = actual_action == expected_action
            
            if subcategory == "unanswerable" and passed:
                expected_reason = case.get("expected_reason", "UNANSWERABLE")
                passed = expected_reason.upper() in result.get("reason", "").upper()
            
            # For SQL injection, check that no dangerous SQL was generated
            if subcategory == "sql_injection" and result.get("generated_sql"):
                sql = result["generated_sql"].upper()
                has_dangerous = any(kw in sql for kw in ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE"])
                if has_dangerous:
                    passed = False
            
            subcategory_results.append({
                "id": case["id"],
                "input": case["input"],
                "expected_action": expected_action,
                "actual_action": actual_action,
                "reason": result.get("reason", ""),
                "passed": passed,
                "notes": case.get("notes", ""),
                "response": (result.get("final_response") or "")[:200],
                "sql": (result.get("generated_sql") or "")[:200]
            })
            
            status = "✅" if passed else "❌"
            print(f"      {status} Expected: {expected_action}, Got: {actual_action}")
        
        passed_count = sum(1 for r in subcategory_results if r["passed"])
        results["subcategories"][subcategory] = {
            "results": subcategory_results,
            "passed": passed_count,
            "total": len(subcategory_results),
            "pass_rate": f"{passed_count}/{len(subcategory_results)}"
        }
    
    return results


def run_robustness_tests(conn, schema: str, test_cases: Dict) -> Dict:
    """Test system robustness against edge cases."""
    results = {"category": "robustness", "subcategories": {}}
    
    for subcategory in ["typos_misspellings", "ambiguity", "calculations", "negation", "year_specific", "ordinal", "boundary"]:
        if subcategory not in test_cases:
            continue
            
        subcategory_results = []
        cases = test_cases[subcategory]
        
        print(f"\n  Testing {subcategory} ({len(cases)} cases)...")
        
        for case in cases:
            print(f"    - {case['id']}: {case['input'][:50]}...")
            
            result = process_single_query(conn, schema, case["input"])
            response = result.get("final_response", "") or ""
            
            # Check expected action if specified
            expected_action = case.get("expected_action")
            action_passed = True
            if expected_action:
                action_passed = result["action"] == expected_action
            
            # Check for errors
            has_error = result.get("error") is not None or response.startswith("[")
            
            subcategory_results.append({
                "id": case["id"],
                "input": case["input"],
                "expected_behavior": case.get("expected_behavior", ""),
                "action": result["action"],
                "response": response[:400],
                "sql": (result.get("generated_sql") or "")[:300],
                "action_passed": action_passed,
                "has_error": has_error,
                "notes": case.get("notes", ""),
                "needs_manual_review": True
            })
            
            status = "✅" if action_passed and not has_error else "⚠️"
            print(f"      {status} Action: {result['action']}, Error: {has_error}")
        
        results["subcategories"][subcategory] = {
            "results": subcategory_results,
            "total": len(subcategory_results)
        }
    
    return results


def run_answer_quality_tests(conn, schema: str, test_cases: Dict) -> Dict:
    """Test answer quality and completeness."""
    results = {"category": "answer_quality", "subcategories": {}}
    
    for subcategory in ["basic_lookups", "comparisons", "rankings"]:
        if subcategory not in test_cases:
            continue
            
        subcategory_results = []
        cases = test_cases[subcategory]
        
        print(f"\n  Testing {subcategory} ({len(cases)} cases)...")
        
        for case in cases:
            print(f"    - {case['id']}: {case['input'][:50]}...")
            
            result = process_single_query(conn, schema, case["input"])
            response = result.get("final_response", "") or ""
            
            expected_contains = case.get("expected_contains", [])
            contains_checks = {
                term: term.lower() in response.lower()
                for term in expected_contains
            }
            contains_passed = all(contains_checks.values()) if contains_checks else True
            
            has_error = result.get("error") is not None or response.startswith("[")
            passed = contains_passed and not has_error and result["action"] == "QUERY"
            
            subcategory_results.append({
                "id": case["id"],
                "input": case["input"],
                "response": response[:500],
                "sql": (result.get("generated_sql") or "")[:300],
                "contains_checks": contains_checks,
                "passed": passed,
                "needs_manual_review": True
            })
            
            status = "✅" if passed else "⚠️"
            print(f"      {status} Contains: {contains_checks}")
        
        passed_count = sum(1 for r in subcategory_results if r["passed"])
        results["subcategories"][subcategory] = {
            "results": subcategory_results,
            "passed": passed_count,
            "total": len(subcategory_results),
            "pass_rate": f"{passed_count}/{len(subcategory_results)}"
        }
    
    return results


def run_hallucination_tests(conn, schema: str, test_cases: Dict) -> Dict:
    """Test for hallucinations by verifying against actual DB data."""
    results = {"category": "hallucination", "subcategories": {}}
    
    # Fact checks
    if "fact_checks" in test_cases:
        print(f"\n  Testing fact_checks ({len(test_cases['fact_checks'])} cases)...")
        fact_results = []
        
        for case in test_cases["fact_checks"]:
            print(f"    - {case['id']}: {case['input'][:50]}...")
            
            setup_query = case.get("setup_query", "")
            ground_truth = None
            ground_truth_raw = None
            
            if setup_query:
                try:
                    cursor = conn.cursor()
                    cursor.execute(setup_query)
                    ground_truth_raw = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    cursor.close()
                    ground_truth = {"columns": columns, "data": ground_truth_raw}
                except Exception as e:
                    ground_truth = {"error": str(e)}
            
            result = process_single_query(conn, schema, case["input"])
            response = result.get("final_response", "") or ""
            
            verification = {
                "ground_truth": ground_truth,
                "response_excerpt": response[:400],
                "needs_manual_verification": True
            }
            
            if "verify_field" in case and ground_truth and ground_truth.get("data"):
                expected_value = case.get("expected_value")
                if expected_value:
                    verification["expected"] = expected_value
                    verification["found_in_response"] = expected_value.lower() in response.lower()
            
            fact_results.append({
                "id": case["id"],
                "input": case["input"],
                "setup_query": setup_query,
                "verification": verification,
                "notes": case.get("notes", "")
            })
            
            print(f"      Ground truth: {str(ground_truth_raw)[:100]}...")
        
        results["subcategories"]["fact_checks"] = {"results": fact_results, "total": len(fact_results)}
    
    # Made up data
    if "made_up_data" in test_cases:
        print(f"\n  Testing made_up_data ({len(test_cases['made_up_data'])} cases)...")
        made_up_results = []
        
        for case in test_cases["made_up_data"]:
            print(f"    - {case['id']}: {case['input'][:50]}...")
            
            result = process_single_query(conn, schema, case["input"])
            response = result.get("final_response", "") or ""
            action = result.get("action", "")
            
            appropriately_handled = (
                action == "REJECT" or
                "don't have" in response.lower() or
                "no city" in response.lower() or
                "only have" in response.lower() or
                ("2019" in response.lower() and "2020" in response.lower())
            )
            
            made_up_results.append({
                "id": case["id"],
                "input": case["input"],
                "action": action,
                "response": response[:300],
                "expected_behavior": case.get("expected_behavior", ""),
                "appropriately_handled": appropriately_handled,
                "notes": case.get("notes", "")
            })
            
            status = "✅" if appropriately_handled else "❌"
            print(f"      {status} Action: {action}")
        
        handled_count = sum(1 for r in made_up_results if r["appropriately_handled"])
        results["subcategories"]["made_up_data"] = {
            "results": made_up_results,
            "passed": handled_count,
            "total": len(made_up_results),
            "pass_rate": f"{handled_count}/{len(made_up_results)}"
        }
    
    # Data not present tests
    if "data_not_present" in test_cases:
        print(f"\n  Testing data_not_present ({len(test_cases['data_not_present'])} cases)...")
        nodata_results = []
        
        for case in test_cases["data_not_present"]:
            print(f"    - {case['id']}: {case['input'][:50]}...")
            
            result = process_single_query(conn, schema, case["input"])
            response = result.get("final_response", "") or ""
            action = result.get("action", "")
            
            # Should NOT claim data is missing for valid states
            claims_missing = (
                "don't have" in response.lower() or
                "no data" in response.lower() or
                "not available" in response.lower() or
                "missing" in response.lower()
            )
            
            nodata_results.append({
                "id": case["id"],
                "input": case["input"],
                "action": action,
                "response": response[:400],
                "claims_missing": claims_missing,
                "expected_behavior": case.get("expected_behavior", ""),
                "notes": case.get("notes", "")
            })
            
            status = "✅" if action == "QUERY" and not claims_missing else "⚠️"
            print(f"      {status} Action: {action}, Claims missing: {claims_missing}")
        
        results["subcategories"]["data_not_present"] = {
            "results": nodata_results,
            "total": len(nodata_results)
        }
    
    return results


def run_context_tests(conn, schema: str, test_cases: Dict) -> Dict:
    """Test multi-turn conversation context retention."""
    results = {"category": "context_retention", "conversations": []}
    
    if "conversations" not in test_cases:
        return results
    
    print(f"\n  Testing conversations ({len(test_cases['conversations'])} conversations)...")
    
    for conv in test_cases["conversations"]:
        print(f"\n    Conversation: {conv['name']}")
        
        turns = conv.get("turns", [])
        conv_results = run_multi_turn_conversation(conn, schema, turns)
        
        conversation_data = {
            "id": conv["id"],
            "name": conv["name"],
            "quality_criteria": conv.get("quality_criteria", []),
            "turns": []
        }
        
        for i, (turn, result) in enumerate(zip(turns, conv_results)):
            if turn.get("role") == "user":
                conversation_data["turns"].append({
                    "turn": i + 1,
                    "user_input": turn["content"],
                    "resolved_question": result.get("resolved_question", ""),
                    "response": (result.get("final_response", "") or "")[:300],
                    "action": result.get("action", "")
                })
                print(f"      Turn {i+1}: {turn['content'][:40]}...")
                print(f"        → Resolved: {result.get('resolved_question', '')[:60]}...")
        
        results["conversations"].append(conversation_data)
    
    return results


def run_multi_part_tests(conn, schema: str, test_cases: Dict) -> Dict:
    """Test multi-part question handling."""
    results = {"category": "multi_part", "questions": []}
    
    if "questions" not in test_cases:
        return results
    
    print(f"\n  Testing multi-part questions ({len(test_cases['questions'])} cases)...")
    
    for case in test_cases["questions"]:
        print(f"    - {case['id']}: {case['input'][:50]}...")
        
        result = process_single_query(conn, schema, case["input"])
        response = result.get("final_response", "") or ""
        sql = result.get("generated_sql", "") or ""
        
        has_multiple_queries = "---SQL---" in sql
        query_count = len([q for q in sql.split("---SQL---") if q.strip()]) if sql else 0
        
        results["questions"].append({
            "id": case["id"],
            "input": case["input"],
            "expected_behavior": case.get("expected_behavior", ""),
            "sql": sql[:600],
            "query_count": query_count,
            "has_multiple_queries": has_multiple_queries,
            "response": response[:500],
            "action": result.get("action", ""),
            "needs_manual_review": True
        })
        
        print(f"      Queries: {query_count}, Response length: {len(response)}")
    
    return results


def run_all_tests():
    """Run the complete test suite and save results."""
    
    print("=" * 70)
    print("  DATAPULSE QUALITY TEST SUITE v2")
    print("=" * 70)
    
    test_file = os.path.join(os.path.dirname(__file__), "test_cases_v2.json")
    with open(test_file, "r") as f:
        test_cases = json.load(f)
    
    print(f"\nLoaded test cases from {test_file}")
    
    print("\nConnecting to Snowflake...")
    conn = get_connection()
    schema = get_optimized_schema(conn)
    print("Connected successfully!")
    
    all_results = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "test_file": "test_cases_v2.json",
            "version": "2.0"
        },
        "summary": {},
        "detailed_results": {}
    }
    
    print("\n" + "=" * 70)
    print("  RUNNING TESTS")
    print("=" * 70)
    
    # 1. Guardrails
    print("\n[1/6] GUARDRAILS TESTS")
    guardrail_results = run_guardrail_tests(conn, schema, test_cases.get("guardrails", {}))
    all_results["detailed_results"]["guardrails"] = guardrail_results
    
    # 2. Robustness (NEW)
    print("\n[2/6] ROBUSTNESS TESTS")
    robustness_results = run_robustness_tests(conn, schema, test_cases.get("robustness", {}))
    all_results["detailed_results"]["robustness"] = robustness_results
    
    # 3. Answer Quality
    print("\n[3/6] ANSWER QUALITY TESTS")
    quality_results = run_answer_quality_tests(conn, schema, test_cases.get("answer_quality", {}))
    all_results["detailed_results"]["answer_quality"] = quality_results
    
    # 4. Hallucination
    print("\n[4/6] HALLUCINATION TESTS")
    hall_results = run_hallucination_tests(conn, schema, test_cases.get("hallucination", {}))
    all_results["detailed_results"]["hallucination"] = hall_results
    
    # 5. Context Retention
    print("\n[5/6] CONTEXT RETENTION TESTS")
    ctx_results = run_context_tests(conn, schema, test_cases.get("context_retention", {}))
    all_results["detailed_results"]["context_retention"] = ctx_results
    
    # 6. Multi-part
    print("\n[6/6] MULTI-PART QUESTION TESTS")
    multi_results = run_multi_part_tests(conn, schema, test_cases.get("multi_part", {}))
    all_results["detailed_results"]["multi_part"] = multi_results
    
    # Generate summary
    print("\n" + "=" * 70)
    print("  GENERATING SUMMARY")
    print("=" * 70)
    
    summary = {
        "guardrails": {},
        "answer_quality": {},
        "hallucination": {},
        "total_tests": 0,
        "automated_passes": 0
    }
    
    for subcat, data in guardrail_results.get("subcategories", {}).items():
        summary["guardrails"][subcat] = data.get("pass_rate", "N/A")
        summary["total_tests"] += data.get("total", 0)
        summary["automated_passes"] += data.get("passed", 0)
    
    for subcat, data in quality_results.get("subcategories", {}).items():
        summary["answer_quality"][subcat] = data.get("pass_rate", "N/A")
        summary["total_tests"] += data.get("total", 0)
        summary["automated_passes"] += data.get("passed", 0)
    
    if "made_up_data" in hall_results.get("subcategories", {}):
        data = hall_results["subcategories"]["made_up_data"]
        summary["hallucination"]["made_up_data"] = data.get("pass_rate", "N/A")
        summary["total_tests"] += data.get("total", 0)
        summary["automated_passes"] += data.get("passed", 0)
    
    all_results["summary"] = summary
    
    # Save results
    results_file = os.path.join(os.path.dirname(__file__), "test_results_v2.json")
    with open(results_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\nResults saved to {results_file}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("  TEST SUMMARY")
    print("=" * 70)
    
    print("\n  GUARDRAILS:")
    for subcat, rate in summary["guardrails"].items():
        print(f"    {subcat}: {rate}")
    
    print("\n  ANSWER QUALITY:")
    for subcat, rate in summary["answer_quality"].items():
        print(f"    {subcat}: {rate}")
    
    print("\n  HALLUCINATION:")
    for subcat, rate in summary["hallucination"].items():
        print(f"    {subcat}: {rate}")
    
    print(f"\n  AUTOMATED CHECKS: {summary['automated_passes']}/{summary['total_tests']}")
    print("\n  Note: Robustness tests require manual review. See test_results_v2.json for details.")
    
    return all_results


if __name__ == "__main__":
    run_all_tests()
