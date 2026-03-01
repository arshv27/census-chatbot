-- ============================================================================
-- LLM-OPTIMIZED VIEWS FOR SAFEGRAPH US CENSUS DATA
-- ============================================================================
-- 
-- RUN THIS SCRIPT AS ACCOUNTADMIN (it creates a new database)
--
-- IMPORTANT: Since CENSUS_DATA is a shared database from the Marketplace,
-- we cannot create schemas inside it. Instead, we create our own application
-- database (CENSUS_APP_DB) to hold our optimized views.
--
-- NOTE: All column names must be quoted because SafeGraph uses mixed-case
-- identifiers (e.g., "B01001e1" not B01001E1).
--
-- ============================================================================

-- Setup: Use admin role and warehouse
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE CENSUS_APP_WH;

-- Ensure access to the shared marketplace data
GRANT IMPORTED PRIVILEGES ON DATABASE CENSUS_DATA TO ROLE CENSUS_APP_ROLE;

-- Create our own application database
CREATE DATABASE IF NOT EXISTS CENSUS_APP_DB;
GRANT ALL PRIVILEGES ON DATABASE CENSUS_APP_DB TO ROLE CENSUS_APP_ROLE;

-- Create schema for LLM-optimized views
CREATE SCHEMA IF NOT EXISTS CENSUS_APP_DB.LLM_VIEWS;
GRANT ALL PRIVILEGES ON SCHEMA CENSUS_APP_DB.LLM_VIEWS TO ROLE CENSUS_APP_ROLE;


-- ============================================================================
-- HELPER: STATE FIPS TO NAME MAPPING
-- ============================================================================
CREATE OR REPLACE VIEW CENSUS_APP_DB.LLM_VIEWS.V_STATE_FIPS_MAPPING AS
SELECT column1 as state_fips, column2 as state_name FROM (VALUES
    ('01', 'Alabama'), ('02', 'Alaska'), ('04', 'Arizona'), ('05', 'Arkansas'),
    ('06', 'California'), ('08', 'Colorado'), ('09', 'Connecticut'), ('10', 'Delaware'),
    ('11', 'District of Columbia'), ('12', 'Florida'), ('13', 'Georgia'), ('15', 'Hawaii'),
    ('16', 'Idaho'), ('17', 'Illinois'), ('18', 'Indiana'), ('19', 'Iowa'),
    ('20', 'Kansas'), ('21', 'Kentucky'), ('22', 'Louisiana'), ('23', 'Maine'),
    ('24', 'Maryland'), ('25', 'Massachusetts'), ('26', 'Michigan'), ('27', 'Minnesota'),
    ('28', 'Mississippi'), ('29', 'Missouri'), ('30', 'Montana'), ('31', 'Nebraska'),
    ('32', 'Nevada'), ('33', 'New Hampshire'), ('34', 'New Jersey'), ('35', 'New Mexico'),
    ('36', 'New York'), ('37', 'North Carolina'), ('38', 'North Dakota'), ('39', 'Ohio'),
    ('40', 'Oklahoma'), ('41', 'Oregon'), ('42', 'Pennsylvania'), ('44', 'Rhode Island'),
    ('45', 'South Carolina'), ('46', 'South Dakota'), ('47', 'Tennessee'), ('48', 'Texas'),
    ('49', 'Utah'), ('50', 'Vermont'), ('51', 'Virginia'), ('53', 'Washington'),
    ('54', 'West Virginia'), ('55', 'Wisconsin'), ('56', 'Wyoming'), ('72', 'Puerto Rico')
);

GRANT SELECT ON CENSUS_APP_DB.LLM_VIEWS.V_STATE_FIPS_MAPPING TO ROLE CENSUS_APP_ROLE;


-- ============================================================================
-- VIEW 1: STATE-LEVEL DEMOGRAPHICS (Multi-Year)
-- ============================================================================
CREATE OR REPLACE VIEW CENSUS_APP_DB.LLM_VIEWS.V_STATE_DEMOGRAPHICS AS

WITH state_2019 AS (
    SELECT 
        2019 AS data_year,
        SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2) AS state_fips,
        SUM(b01."B01001e1") AS total_population,
        AVG(NULLIF(b01."B01002e1", 0)) AS avg_median_age,
        AVG(NULLIF(b19."B19013e1", 0)) AS avg_median_household_income,
        AVG(NULLIF(b19."B19301e1", 0)) AS avg_per_capita_income,
        SUM(b25."B25001e1") AS total_housing_units,
        AVG(NULLIF(b25."B25077e1", 0)) AS avg_median_home_value,
        AVG(NULLIF(b25."B25064e1", 0)) AS avg_median_rent,
        SUM(b23."B23025e2") AS labor_force_population,
        SUM(b23."B23025e5") AS unemployed_population,
        SUM(b17."B17010e2") AS families_below_poverty,
        SUM(b15."B15003e22") AS bachelors_degree_holders,
        SUM(b15."B15003e23") AS masters_degree_holders,
        SUM(b15."B15003e25") AS doctorate_degree_holders
    FROM CENSUS_DATA.PUBLIC."2019_CBG_B01" b01
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B19" b19 ON b01."CENSUS_BLOCK_GROUP" = b19."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B25" b25 ON b01."CENSUS_BLOCK_GROUP" = b25."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B23" b23 ON b01."CENSUS_BLOCK_GROUP" = b23."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B17" b17 ON b01."CENSUS_BLOCK_GROUP" = b17."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B15" b15 ON b01."CENSUS_BLOCK_GROUP" = b15."CENSUS_BLOCK_GROUP"
    GROUP BY SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2)
),

state_2020 AS (
    SELECT 
        2020 AS data_year,
        SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2) AS state_fips,
        SUM(b01."B01001e1") AS total_population,
        AVG(NULLIF(b01."B01002e1", 0)) AS avg_median_age,
        AVG(NULLIF(b19."B19013e1", 0)) AS avg_median_household_income,
        AVG(NULLIF(b19."B19301e1", 0)) AS avg_per_capita_income,
        SUM(b25."B25001e1") AS total_housing_units,
        AVG(NULLIF(b25."B25077e1", 0)) AS avg_median_home_value,
        AVG(NULLIF(b25."B25064e1", 0)) AS avg_median_rent,
        SUM(b23."B23025e2") AS labor_force_population,
        SUM(b23."B23025e5") AS unemployed_population,
        SUM(b17."B17010e2") AS families_below_poverty,
        SUM(b15."B15003e22") AS bachelors_degree_holders,
        SUM(b15."B15003e23") AS masters_degree_holders,
        SUM(b15."B15003e25") AS doctorate_degree_holders
    FROM CENSUS_DATA.PUBLIC."2020_CBG_B01" b01
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B19" b19 ON b01."CENSUS_BLOCK_GROUP" = b19."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B25" b25 ON b01."CENSUS_BLOCK_GROUP" = b25."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B23" b23 ON b01."CENSUS_BLOCK_GROUP" = b23."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B17" b17 ON b01."CENSUS_BLOCK_GROUP" = b17."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B15" b15 ON b01."CENSUS_BLOCK_GROUP" = b15."CENSUS_BLOCK_GROUP"
    GROUP BY SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2)
),

combined AS (
    SELECT * FROM state_2019
    UNION ALL
    SELECT * FROM state_2020
)

SELECT 
    c.data_year AS "Year",
    s.state_name AS "State_Name",
    c.state_fips AS "State_Code",
    ROUND(c.total_population) AS "Total_Population",
    ROUND(c.avg_median_age, 1) AS "Median_Age",
    ROUND(c.avg_median_household_income) AS "Median_Household_Income",
    ROUND(c.avg_per_capita_income) AS "Per_Capita_Income",
    ROUND(c.total_housing_units) AS "Total_Housing_Units",
    ROUND(c.avg_median_home_value) AS "Median_Home_Value",
    ROUND(c.avg_median_rent) AS "Median_Rent",
    ROUND(c.labor_force_population) AS "Labor_Force_Population",
    ROUND(c.unemployed_population) AS "Unemployed_Population",
    ROUND(c.families_below_poverty) AS "Families_Below_Poverty",
    ROUND(c.bachelors_degree_holders) AS "Bachelors_Degree_Holders",
    ROUND(c.masters_degree_holders) AS "Masters_Degree_Holders",
    ROUND(c.doctorate_degree_holders) AS "Doctorate_Degree_Holders",
    ROUND(c.unemployed_population * 100.0 / NULLIF(c.labor_force_population, 0), 2) AS "Unemployment_Rate_Percent"
FROM combined c
JOIN CENSUS_APP_DB.LLM_VIEWS.V_STATE_FIPS_MAPPING s ON c.state_fips = s.state_fips;

COMMENT ON VIEW CENSUS_APP_DB.LLM_VIEWS.V_STATE_DEMOGRAPHICS IS 
'State-level demographic data from US Census. Contains 2019 and 2020 data. Use Year=2020 for most recent.';

GRANT SELECT ON CENSUS_APP_DB.LLM_VIEWS.V_STATE_DEMOGRAPHICS TO ROLE CENSUS_APP_ROLE;


-- ============================================================================
-- VIEW 2: COUNTY-LEVEL DEMOGRAPHICS (Multi-Year)
-- ============================================================================
CREATE OR REPLACE VIEW CENSUS_APP_DB.LLM_VIEWS.V_COUNTY_DEMOGRAPHICS AS

WITH county_2019 AS (
    SELECT 
        2019 AS data_year,
        SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 5) AS county_fips,
        SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2) AS state_fips,
        SUM(b01."B01001e1") AS total_population,
        AVG(NULLIF(b01."B01002e1", 0)) AS avg_median_age,
        AVG(NULLIF(b19."B19013e1", 0)) AS avg_median_household_income,
        AVG(NULLIF(b19."B19301e1", 0)) AS avg_per_capita_income,
        SUM(b25."B25001e1") AS total_housing_units,
        AVG(NULLIF(b25."B25077e1", 0)) AS avg_median_home_value,
        AVG(NULLIF(b25."B25064e1", 0)) AS avg_median_rent,
        SUM(b23."B23025e2") AS labor_force_population,
        SUM(b23."B23025e5") AS unemployed_population,
        SUM(b17."B17010e2") AS families_below_poverty
    FROM CENSUS_DATA.PUBLIC."2019_CBG_B01" b01
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B19" b19 ON b01."CENSUS_BLOCK_GROUP" = b19."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B25" b25 ON b01."CENSUS_BLOCK_GROUP" = b25."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B23" b23 ON b01."CENSUS_BLOCK_GROUP" = b23."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2019_CBG_B17" b17 ON b01."CENSUS_BLOCK_GROUP" = b17."CENSUS_BLOCK_GROUP"
    GROUP BY SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 5), SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2)
),

county_2020 AS (
    SELECT 
        2020 AS data_year,
        SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 5) AS county_fips,
        SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2) AS state_fips,
        SUM(b01."B01001e1") AS total_population,
        AVG(NULLIF(b01."B01002e1", 0)) AS avg_median_age,
        AVG(NULLIF(b19."B19013e1", 0)) AS avg_median_household_income,
        AVG(NULLIF(b19."B19301e1", 0)) AS avg_per_capita_income,
        SUM(b25."B25001e1") AS total_housing_units,
        AVG(NULLIF(b25."B25077e1", 0)) AS avg_median_home_value,
        AVG(NULLIF(b25."B25064e1", 0)) AS avg_median_rent,
        SUM(b23."B23025e2") AS labor_force_population,
        SUM(b23."B23025e5") AS unemployed_population,
        SUM(b17."B17010e2") AS families_below_poverty
    FROM CENSUS_DATA.PUBLIC."2020_CBG_B01" b01
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B19" b19 ON b01."CENSUS_BLOCK_GROUP" = b19."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B25" b25 ON b01."CENSUS_BLOCK_GROUP" = b25."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B23" b23 ON b01."CENSUS_BLOCK_GROUP" = b23."CENSUS_BLOCK_GROUP"
    LEFT JOIN CENSUS_DATA.PUBLIC."2020_CBG_B17" b17 ON b01."CENSUS_BLOCK_GROUP" = b17."CENSUS_BLOCK_GROUP"
    GROUP BY SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 5), SUBSTR(b01."CENSUS_BLOCK_GROUP", 1, 2)
),

combined AS (
    SELECT * FROM county_2019
    UNION ALL
    SELECT * FROM county_2020
)

SELECT 
    c.data_year AS "Year",
    m."COUNTY" AS "County_Name",
    s.state_name AS "State_Name",
    c.county_fips AS "County_Code",
    c.state_fips AS "State_Code",
    ROUND(c.total_population) AS "Total_Population",
    ROUND(c.avg_median_age, 1) AS "Median_Age",
    ROUND(c.avg_median_household_income) AS "Median_Household_Income",
    ROUND(c.avg_per_capita_income) AS "Per_Capita_Income",
    ROUND(c.total_housing_units) AS "Total_Housing_Units",
    ROUND(c.avg_median_home_value) AS "Median_Home_Value",
    ROUND(c.avg_median_rent) AS "Median_Rent",
    ROUND(c.labor_force_population) AS "Labor_Force_Population",
    ROUND(c.unemployed_population) AS "Unemployed_Population",
    ROUND(c.families_below_poverty) AS "Families_Below_Poverty",
    ROUND(c.unemployed_population * 100.0 / NULLIF(c.labor_force_population, 0), 2) AS "Unemployment_Rate_Percent"
FROM combined c
JOIN (
    SELECT DISTINCT 
        CONCAT("STATE_FIPS", "COUNTY_FIPS") as county_fips, 
        "COUNTY"
    FROM CENSUS_DATA.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
) m ON c.county_fips = m.county_fips
JOIN CENSUS_APP_DB.LLM_VIEWS.V_STATE_FIPS_MAPPING s ON c.state_fips = s.state_fips;

COMMENT ON VIEW CENSUS_APP_DB.LLM_VIEWS.V_COUNTY_DEMOGRAPHICS IS 
'County-level demographic data from US Census. Contains 2019 and 2020 data. Use for specific county questions.';

GRANT SELECT ON CENSUS_APP_DB.LLM_VIEWS.V_COUNTY_DEMOGRAPHICS TO ROLE CENSUS_APP_ROLE;


-- ============================================================================
-- VERIFICATION QUERIES (run these to test)
-- ============================================================================
-- SELECT "Year", "State_Name", "Total_Population" 
-- FROM CENSUS_APP_DB.LLM_VIEWS.V_STATE_DEMOGRAPHICS 
-- WHERE "State_Name" = 'California' 
-- ORDER BY "Year";
--
-- SELECT COUNT(*) FROM CENSUS_APP_DB.LLM_VIEWS.V_COUNTY_DEMOGRAPHICS;
-- ============================================================================
