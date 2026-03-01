# Future Improvements

Practical enhancements to make DataPulse production-ready.

---

## Authentication & User Management

### User Login
Add authentication to enable personalized experiences and usage tracking.

**Implementation:**
- Integrate Streamlit's `st.experimental_user` or OAuth providers (Google, GitHub)
- Store user sessions in Snowflake or a lightweight auth service
- Enable role-based access (admin vs regular user)

### User Personalization & Memory
Remember user preferences and past interactions across sessions.

**Implementation:**
- Store user preferences (preferred metrics, regions of interest)
- Save frequently asked question patterns
- Learn from user corrections ("Actually, I meant median income, not per capita")
- Personalized welcome based on usage history

---

## Multi-Session Support

### Multiple Chat Sessions
Allow users to maintain separate conversation threads.

**Implementation:**
- Session selector in sidebar (like ChatGPT)
- Persist conversations to database with timestamps
- Search across past conversations
- Export/share conversation threads

**Schema:**
```sql
CREATE TABLE user_sessions (
    session_id UUID PRIMARY KEY,
    user_id VARCHAR,
    title VARCHAR,
    created_at TIMESTAMP,
    messages VARIANT  -- JSON array of messages
);
```

---

## Agentic Capabilities

### External Tool Integration
Extend the agent to fetch real-time data from external sources.

**Potential Tools:**
- **Bureau of Labor Statistics API** — Latest unemployment figures
- **Census Bureau API** — Most recent ACS data
- **Federal Reserve (FRED)** — Economic indicators
- **News API** — Recent articles about demographic trends

**Implementation:**
- Define tool schemas the LLM can invoke
- Route questions requiring current data to appropriate APIs
- Combine census historical data with live data for comprehensive answers
- Example: "How has California's unemployment changed since 2020?" → Census (historical) + BLS (current)

---

## Data Visualization

### Interactive Charts
Present analysis visually instead of text-only responses.

**Visualizations to Add:**
- **Bar charts** — State/county comparisons
- **Line charts** — Year-over-year trends (2019 vs 2020, extensible to more years)
- **Choropleth maps** — Geographic distribution using Plotly
- **Scatter plots** — Correlation analysis (education vs income)

**Implementation:**
```python
import plotly.express as px

# Example: Income comparison
fig = px.bar(df, x='State_Name', y='Median_Household_Income', 
             title='Median Income by State')
st.plotly_chart(fig)
```

**Trigger:** Detect comparison/trend questions and auto-generate appropriate chart alongside text response.

---

## Reliability & Error Handling

### Retry Logic with Backoff
Handle transient failures gracefully.

**Implementation:**
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((ConnectionError, TimeoutError))
)
def call_cortex_with_retry(conn, prompt, model):
    return call_cortex(conn, prompt, model)
```

**Scenarios:**
- LLM timeout → retry with same prompt
- SQL error → trigger self-correction flow
- Connection drop → reconnect and retry
- Rate limiting → exponential backoff

### Circuit Breaker Pattern
Prevent cascade failures when Snowflake or Cortex is degraded.

---

## Performance Optimizations

### Async Processing
Non-blocking execution for better responsiveness.

**Implementation:**
- Use `asyncio` for parallel query execution in multi-part questions
- Stream LLM responses token-by-token using Server-Sent Events
- Background processing for expensive analytical queries

```python
async def process_multi_part_query(queries):
    tasks = [execute_query_async(q) for q in queries]
    results = await asyncio.gather(*tasks)
    return results
```

### Query Caching
Cache frequent queries to reduce latency and cost.

**Implementation:**
- Hash question → check cache before LLM call
- Cache SQL results for identical queries (TTL: 1 hour)
- Use Snowflake result caching or Redis

---

## Voice Support (Ambitious)

### Speech-to-Text Input
Allow users to ask questions verbally.

**Implementation:**
- Use Web Speech API (browser-native) or Whisper API
- Stream audio → transcribe → process as text
- Indicate "listening" state in UI

```javascript
// Browser Speech API
const recognition = new webkitSpeechRecognition();
recognition.onresult = (event) => {
    const transcript = event.results[0][0].transcript;
    sendToBackend(transcript);
};
```

### Text-to-Speech Output (Optional)
Read responses aloud for accessibility.

---

## Additional Enhancements

### Response Streaming
Show response as it generates instead of waiting for completion.

**Implementation:**
- Cortex doesn't natively support streaming, but can simulate with chunked synthesis
- Or use a streaming-capable model via external API for synthesis step

### Feedback Loop
Let users rate responses to improve prompts over time.

**Implementation:**
- 👍/👎 buttons on each response
- Log feedback with query/response pairs
- Periodic prompt refinement based on negative feedback patterns

### Admin Dashboard
Monitor system health and usage patterns.

**Metrics to Track:**
- Queries per day/hour
- Average response time
- Error rates by type
- Most common question categories
- LLM token usage and costs

---

## Summary Table

| Improvement | Effort | Impact |
|-------------|--------|--------|
| User login | Medium | High |
| Multi-session support | Medium | High |
| Data visualizations | Low | High |
| Retry logic | Low | Medium |
| External tool integration | High | High |
| Async processing | Medium | Medium |
| Query caching | Low | Medium |
| Voice support | High | Medium |
| Response streaming | Medium | High |
