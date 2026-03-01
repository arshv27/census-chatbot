-- ============================================================================
-- SNOWFLAKE INITIAL SETUP SCRIPT
-- ============================================================================
-- RUN THIS IN A SNOWFLAKE WORKSHEET (AS ACCOUNTADMIN OR SYSADMIN)
-- 
-- This script sets up the infrastructure needed for the Census AI Agent:
-- 1. A cost-optimized warehouse
-- 2. A least-privilege role for the app
-- 3. Access to Cortex AI functions
-- ============================================================================

-- 1. Create a dedicated, cost-optimized warehouse
-- Using an X-Small warehouse and setting AUTO_SUSPEND to 60 seconds ensures 
-- you don't burn through your $400 credits when the app is idle.
CREATE OR REPLACE WAREHOUSE CENSUS_APP_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- 2. Create a dedicated role for the app (Principle of Least Privilege)
CREATE OR REPLACE ROLE CENSUS_APP_ROLE;

-- 3. Grant access to the warehouse
GRANT USAGE ON WAREHOUSE CENSUS_APP_WH TO ROLE CENSUS_APP_ROLE;
GRANT OPERATE ON WAREHOUSE CENSUS_APP_WH TO ROLE CENSUS_APP_ROLE;

-- 4. Grant access to Cortex AI functions
-- This allows the role to run LLMs using SNOWFLAKE.CORTEX.COMPLETE
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CENSUS_APP_ROLE;

-- 5. Grant the role to your specific user 
-- ⚠️ REPLACE 'YOUR_USER_NAME' WITH YOUR ACTUAL SNOWFLAKE USERNAME
GRANT ROLE CENSUS_APP_ROLE TO USER "YOUR_USER_NAME";

-- ============================================================================
-- AFTER MARKETPLACE DATA ACQUISITION
-- ============================================================================
-- After you "Get" the US Open Census Data from the Snowflake Marketplace
-- and name the database 'CENSUS_DATA', run these commands:

-- 6. Grant the app role access to the marketplace data
-- GRANT IMPORTED PRIVILEGES ON DATABASE CENSUS_DATA TO ROLE CENSUS_APP_ROLE;

-- 7. Then run the view setup script: db/setup_views.sql
-- This creates the LLM-optimized views with human-readable column names

-- ============================================================================
-- VERIFICATION (Run after all setup is complete)
-- ============================================================================
-- USE ROLE CENSUS_APP_ROLE;
-- USE WAREHOUSE CENSUS_APP_WH;
-- SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', 'Say hello');
-- SELECT * FROM CENSUS_DATA.LLM_VIEWS.V_STATE_DEMOGRAPHICS LIMIT 5;
