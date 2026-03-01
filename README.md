# 🔮 DataPulse

**An AI-powered conversational interface for exploring US Census data**

Built with Snowflake Cortex AI and Streamlit.

## Live Demo

🌐 **URL:** [https://census-chatbot.streamlit.app](https://census-chatbot.streamlit.app)

## Features

- **Natural Language Queries** — Ask questions in plain English about US demographics
- **Multi-Part Questions** — Complex questions automatically split into multiple queries
- **Conversation Context** — Follow-up questions work naturally
- **Data Reasoning** — Analytical "why" questions get thoughtful analysis
- **Smart Guardrails** — Off-topic and unanswerable questions handled gracefully

## Dataset

**Source:** [SafeGraph US Open Census](https://www.safegraph.com/open-census-data) via Snowflake Marketplace

The raw data contains Census Block Group level statistics from the American Community Survey. For this application, we aggregate it into two LLM-optimized views:

| View | Granularity | Records |
|------|-------------|---------|
| `V_STATE_DEMOGRAPHICS` | State/Territory | 52 × 2 years |
| `V_COUNTY_DEMOGRAPHICS` | County | ~1,955 × 2 years |

**Available Metrics:**
- **Demographics:** Population, median age
- **Economics:** Household income, per capita income, poverty rates, unemployment
- **Housing:** Home values, rent prices, housing units
- **Education:** Bachelor's, Master's, Doctorate holders (state level only)

**Time Period:** 2019 and 2020

## Architecture

DataPulse uses a **2-call LLM architecture** optimized for cost and latency:

```
User Question
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  CALL 1: Unified Agent (Mistral Large 2)                │
│  ┌─────────────┬─────────────────┬──────────────────┐   │
│  │ Guardrails  │ Context         │ SQL Generation   │   │
│  │ (safe/topic)│ Resolution      │ (Text-to-SQL)    │   │
│  └─────────────┴─────────────────┴──────────────────┘   │
│  Output: {action, reason, resolved_question, sql}       │
└─────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  Snowflake Query Execution                              │
│  Execute SQL against V_STATE_DEMOGRAPHICS /             │
│  V_COUNTY_DEMOGRAPHICS views                            │
└─────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  CALL 2: Synthesis (Claude 3.5 Sonnet)                  │
│  Transform raw data → natural, conversational response  │
└─────────────────────────────────────────────────────────┘
     │
     ▼
  Response
```

**Data Flow:**
1. **Classify** → QUERY / REASONING / REJECT
2. **Generate SQL** → One or multiple queries (complex questions auto-split)
3. **Execute** → Against pre-aggregated Census views in Snowflake
4. **Synthesize** → Natural language answer

## Quality Assurance

The system includes a comprehensive test suite covering:

- **Guardrails** — Off-topic detection, prompt injection, unsafe content, unanswerable queries
- **Robustness** — Typos, ambiguity resolution, calculations, negation handling
- **Answer Quality** — Factual accuracy, response formatting, hallucination prevention
- **Context Retention** — Multi-turn conversation handling

See [Development Process](docs/DEVELOPMENT.md#testing-methodology) for detailed methodology and results.

## **Local setup guide:** 
[docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md)

## Documentation

| Document | Description |
|----------|-------------|
| [Development Process](docs/DEVELOPMENT.md) | How this project was built |
| [Local Setup](docs/LOCAL_SETUP.md) | Installation and configuration guide |
| [Future Improvements](docs/IMPROVEMENTS.md) | Ideas for enhancing the project |

## Project Structure

```
├── app.py                 # Streamlit frontend
├── ai/
│   ├── cortex_llm.py      # Snowflake Cortex wrapper
│   └── prompts.py         # LLM prompt templates
├── agent/
│   └── chat_agent.py      # Agent orchestration
├── db/
│   ├── snowflake_client.py # Database connection
│   └── setup_views.sql    # LLM-optimized views
├── tests/
│   ├── test_cases_v2.json # Test suite definitions
│   └── quality_test_v2.py # Test runner
├── scripts/
│   └── 01_snowflake_setup.sql
├── docs/                  # Documentation
└── requirements.txt
```

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Author

[**Arsh Verma**](https://arshv27.github.io)
