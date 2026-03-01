# Development Plan

A step-by-step guide to building a conversational AI agent over structured data.

---

## Phase 1: Data Foundation

**Goal:** Set up the data layer with LLM-friendly views.

### Steps

1. **Acquire Data**
   - Create Snowflake trial account
   - Get [US Open Census](https://app.snowflake.com/marketplace/listing/GZSNZ2UNN0/safegraph-us-open-census-data-neighborhood-insights-free-dataset) dataset from Snowflake Marketplace (SafeGraph)

2. **Create LLM-Optimized Views**
   - Aggregate raw Census Block Group data to State and County levels
   - Select relevant columns (population, income, housing, education, employment)
   - Compute derived metrics (e.g., `Unemployment_Rate_Percent`)
   
3. **Add Semantic Metadata**
   - Add `COMMENT ON COLUMN` for each field
   - Comments help the LLM understand what each column represents
   
   ```sql
   COMMENT ON COLUMN V_STATE_DEMOGRAPHICS."Median_Household_Income" IS 
   'Median household income in dollars for the state';
   ```

**Output:** Two views (`V_STATE_DEMOGRAPHICS`, `V_COUNTY_DEMOGRAPHICS`) ready for querying.

---

## Phase 2: LLM Integration

**Goal:** Connect to Snowflake Cortex AI for text generation.

### Steps

1. **Create Cortex Wrapper**
   - Simple function to call `SNOWFLAKE.CORTEX.COMPLETE`
   - Handle errors gracefully
   - Support multiple models (fast vs quality)

2. **Select Models**
   - **SQL Generation:** Mistral Large 2 (strong at structured output)
   - **Synthesis:** Claude 3.5 Sonnet (natural, conversational responses)

3. **Design Prompt Templates**
   - **Unified Agent Prompt:** Classification + context resolution + SQL generation
   - **Synthesis Prompt:** Convert raw data to natural language
   - **Reasoning Prompt:** For analytical "why" questions

**Output:** `call_cortex()` function and prompt templates.

---

## Phase 3: Agent Orchestration

**Goal:** Build the core agent logic that processes user queries.

### Steps

1. **Define Agent State**
   ```python
   @dataclass
   class AgentState:
       user_message: str
       chat_history: List[Dict]
       standalone_query: str
       generated_sql: str
       sql_results: str
       final_answer: str
   ```

2. **Implement Query Flow**
   ```
   User Input → Classify & Generate SQL → Execute → Synthesize → Response
   ```

3. **Add Guardrails**
   - **OFF_TOPIC:** Unrelated to census data
   - **UNANSWERABLE:** Related but impossible to answer (e.g., city-level, predictions)
   - **UNSAFE:** NSFW or prompt injection attempts

4. **Handle Context**
   - Keep last 10 conversation turns
   - Resolve follow-up questions using history
   - Example: "Compare their income" → infer "their" from previous turn

5. **Support Multi-Part Questions**
   - Detect questions needing multiple queries
   - Split with `---SQL---` delimiter
   - Execute each, combine results

**Output:** `process_user_query()` function that handles end-to-end flow.

---

## Phase 4: Web Interface

**Goal:** Create a user-friendly chat UI with Streamlit.

### Steps

1. **Chat Interface** — Use `st.chat_message` with session state for history
2. **Progress Indicators** — Dynamic status updates during processing
3. **Developer Mode** — Toggle to show SQL and raw results for debugging

**Output:** Fully functional `app.py`.

---

## Phase 5: Testing & Deployment

**Goal:** Validate and ship.

### Steps

1. **Create Test Suite**
   - Multi-turn conversations
   - Edge cases (guardrails, complex queries)
   - Context retention tests

2. **Run Stress Tests**
   - 5+ conversation scenarios
   - Verify all responses are natural (no raw data dumps)

3. **Deploy**
   - Configure Streamlit Cloud secrets
   - Push to GitHub
   - Deploy via Streamlit Community Cloud

**Output:** Live application accessible on the internet.

---

## Design Iterations

Key decisions made during development to improve quality and performance.

### 2-Call Architecture

Initially considered a 3-step flow:
1. Router (classify + guardrails)
2. SQL Generator (text-to-SQL)
3. Synthesizer (data-to-text)

**Optimization:** Combined steps 1 and 2 into a single "Unified Agent" call.

- **Before:** 3 LLM calls per query
- **After:** 2 LLM calls per query
- **Benefit:** ~33% reduction in latency and cost

The unified prompt outputs structured JSON with classification, resolved question, and SQL in one response.

### Reasoning Support

Simple data lookups are straightforward, but users often ask analytical questions like "Why does Mississippi have low income?"

**Solution:** Added a `REASONING` action type.

- Detects "why/how/what causes" questions
- Fetches related contextual data (education, poverty, employment for that region)
- Uses a specialized reasoning prompt that analyzes correlations
- Presents insights with appropriate caveats ("correlation, not causation")

### Multi-Part Query Handling

Questions like "What's the richest state and county?" require two separate queries (state table + county table).

**Solution:** 
- Prompt instructs LLM to generate multiple queries separated by `---SQL---`
- Execution layer splits and runs each query
- Results combined before synthesis

### Context Resolution

Follow-up questions like "What about California?" need context from previous turns.

**Solution:**
- Pass last 10 conversation turns to the LLM
- Prompt explicitly instructs to resolve references using history
- Example: If discussing income, "Compare NY vs CA" becomes "Compare median household income of New York vs California"

### Guardrail Granularity

Initially had binary safe/unsafe classification. Users asked questions that were *related* but impossible to answer (e.g., city-level data, predictions).

**Solution:** Added `UNANSWERABLE` category distinct from `OFF_TOPIC`.

- **OFF_TOPIC:** "What's the weather?" → Generic rejection
- **UNANSWERABLE:** "Best city to live in?" → Explains data limitation, suggests alternatives

### Model Selection

Tested different models for different tasks (limited to [models available in Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability)):

| Task | Model | Rationale |
|------|-------|-----------|
| SQL Generation | Mistral Large 2 | Strong at structured output, cost-effective |
| Synthesis | Claude 3.5 Sonnet | Best conversational quality available in Cortex |

Using the best model for each task optimizes both quality and cost.
