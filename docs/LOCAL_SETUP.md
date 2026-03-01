# Local Development Setup

## Prerequisites

- Python 3.9+
- Snowflake account with Cortex AI access
- US Open Census data from Snowflake Marketplace

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/[username]/datapulse-census.git
cd datapulse-census
```

### 2. Create Virtual Environment

```bash
# Using venv
python -m venv venv
source venv/bin/activate  # macOS/Linux
# or
venv\Scripts\activate     # Windows

# Or using conda
conda create -n snowflake python=3.10
conda activate snowflake
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CENSUS_APP_DB
SNOWFLAKE_SCHEMA=LLM_VIEWS
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 5. Snowflake Setup

#### Get the Data
1. Log into Snowflake
2. Go to Marketplace → Search "US Open Census"
3. Get the SafeGraph US Open Census dataset

#### Run Setup Scripts

Execute in Snowflake Worksheets:

```sql
-- First, run the initial setup
-- scripts/01_snowflake_setup.sql

-- Then, create the LLM-optimized views
-- db/setup_views.sql
```

### 6. Start the Application

```bash
streamlit run app.py
```

The app will be available at `http://localhost:8501`

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
├── scripts/
│   └── 01_snowflake_setup.sql # Initial Snowflake setup
├── docs/
│   ├── DEVELOPMENT.md     # Development process
│   ├── LOCAL_SETUP.md     # This file
│   └── IMPROVEMENTS.md    # Future improvements
├── requirements.txt
└── README.md
```

## Running Tests

```bash
# Integration tests
python integration_test.py

# Stress tests (5 multi-turn conversations)
python stress_test.py
```

## Troubleshooting

### Connection Issues

- Verify your Snowflake account identifier format (e.g., `abc12345.us-east-1`)
- Check that your user has access to the warehouse and database
- Ensure Cortex AI is enabled for your account

### Missing Data

- Confirm the US Open Census dataset is properly imported from Marketplace
- Verify the views were created in `CENSUS_APP_DB.LLM_VIEWS`

### LLM Errors

- Check Snowflake Cortex AI availability in your region
- Verify you have sufficient credits for Cortex usage
