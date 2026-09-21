-- ==============================================================================
-- DATA QUALITY FRAMEWORK: SETUP, INGESTION & AUDIT QUERIES
-- Database: DATA_QUALITY_DB | Schema: PUBLIC
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- PHASE 1: ENVIRONMENT & OBJECT INITIALIZATION
-- ------------------------------------------------------------------------------
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS DATA_QUALITY_DB;
USE DATABASE DATA_QUALITY_DB;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Create Central Audit Log Table
CREATE TABLE IF NOT EXISTS DQ_AUDIT_LOG (
    LOG_ID STRING DEFAULT UUID_STRING(),
    PIPELINE_RUN_ID STRING,
    ENVIRONMENT STRING,
    TARGET_TABLE STRING,
    OVERALL_DQ_SCORE NUMBER(5,2),
    CHECKS_DETAIL VARIANT,
    EXECUTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create File Format for Kaggle Online Retail Dataset
CREATE OR REPLACE FILE FORMAT KAGGLE_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', 'NaN')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create Internal Stage
CREATE OR REPLACE STAGE KAGGLE_STAGE
    FILE_FORMAT = KAGGLE_CSV_FORMAT;

-- Create Target Table Shell
CREATE TABLE IF NOT EXISTS KAGGLE_RETAIL_REAL (
    ORDER_ID STRING,
    PRODUCT_ID STRING,
    DESCRIPTION STRING,
    QUANTITY INTEGER,
    TRANSACTION_TIMESTAMP VARCHAR,
    UNIT_PRICE FLOAT,
    CUSTOMER_ID STRING,
    COUNTRY STRING
);

-- ------------------------------------------------------------------------------
-- PHASE 2: DATA INGESTION & TRANSFORMATION
-- Note: Ensure file is uploaded to @KAGGLE_STAGE via UI before running COPY INTO
-- ------------------------------------------------------------------------------

-- Ingest Raw Data from Stage
COPY INTO KAGGLE_RETAIL_REAL
FROM @KAGGLE_STAGE
FILE_FORMAT = (FORMAT_NAME = 'KAGGLE_CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Safe In-Place Timestamp Conversion
ALTER TABLE KAGGLE_RETAIL_REAL 
ALTER COLUMN TRANSACTION_TIMESTAMP SET DATA TYPE TIMESTAMP_NTZ 
USING TRY_TO_TIMESTAMP_NTZ(TRANSACTION_TIMESTAMP, 'MM/DD/YYYY HH24:MI');

-- Verify Total Record Count
SELECT COUNT(*) AS TOTAL_LOADED_ROWS FROM KAGGLE_RETAIL_REAL;


-- ==============================================================================
-- PHASE 3: AUDIT VERIFICATION (Run after executing run_dq_assessment.py)
-- ==============================================================================

-- 1. Simple Audit Fetch
SELECT 
    PIPELINE_RUN_ID,
    ENVIRONMENT,
    TARGET_TABLE,
    OVERALL_DQ_SCORE,
    CHECKS_DETAIL,
    EXECUTION_TIMESTAMP
FROM DATA_QUALITY_DB.PUBLIC.DQ_AUDIT_LOG
ORDER BY EXECUTION_TIMESTAMP DESC;

-- 2. Unpack JSON Metrics & Dimension Scores
SELECT 
    PIPELINE_RUN_ID,
    TARGET_TABLE,
    OVERALL_DQ_SCORE,
    CHECKS_DETAIL:scores.completeness_score::FLOAT AS COMPLETENESS_SCORE,
    CHECKS_DETAIL:scores.validity_score::FLOAT AS VALIDITY_SCORE,
    CHECKS_DETAIL:metrics.total_records::INT AS TOTAL_RECORDS,
    CHECKS_DETAIL:metrics.missing_customer_ids::INT AS MISSING_CUSTOMER_IDS,
    CHECKS_DETAIL:metrics.missing_descriptions::INT AS MISSING_DESCRIPTIONS,
    CHECKS_DETAIL:metrics.invalid_quantities::INT AS INVALID_QUANTITIES,
    CHECKS_DETAIL:metrics.invalid_unit_prices::INT AS INVALID_UNIT_PRICES,
    CHECKS_DETAIL:metrics.missing_timestamps::INT AS MISSING_TIMESTAMPS,
    EXECUTION_TIMESTAMP
FROM DATA_QUALITY_DB.PUBLIC.DQ_AUDIT_LOG
ORDER BY EXECUTION_TIMESTAMP DESC;
