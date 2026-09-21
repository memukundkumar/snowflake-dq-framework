# snowflake-dq-framework
An automated Data Quality assessment and audit logging engine built natively inside Snowflake using Snowpark Python and SQL.
# ❄️ Snowflake Data Quality & Observability Framework

An enterprise-grade, automated Data Quality (DQ) assessment and telemetry logging pipeline built natively inside **Snowflake** using **Snowpark Python** and **Snowflake SQL**. 

This solution evaluates incoming retail records, executes single-pass aggregate quality checks, calculates weighted Data Quality Index (DQI) scores (Completeness vs. Validity), and persists structured audit telemetry into a JSON-enabled logging table using Snowflake's `VARIANT` column type.

---

## 🏗️ Architecture & Data Flow

```text
┌─────────────────────────┐      ┌──────────────────────────┐      ┌────────────────────────────┐
│  Kaggle Retail Dataset  │ ───► │  KAGGLE_RETAIL_REAL      │ ───► │  run_dq_assessment.py      │
│  (CSV File on Stage)    │      │  (Snowflake Table)       │      │  (Snowpark Python Engine)  │
└─────────────────────────┘      └──────────────────────────┘      └─────────────┬──────────────┘
                                                                                 │
                                                                                 ▼
┌─────────────────────────┐                                        ┌────────────────────────────┐
│ Flattened Audit Views   │ ◄───────────────────────────────────── │  DQ_AUDIT_LOG              │
│ (JSON Variant Parsing)  │                                        │  (Audit Telemetry Table)   │
└─────────────────────────┘                                        └────────────────────────────┘

Deployment & Quick Start
Prerequisites
Snowflake account with warehouse usage privileges (COMPUTE_WH).

Snowflake Notebook or Python Worksheet environment.

Step 1: Database & Table Setup
Execute the initialization script inside your Snowflake SQL Worksheet (scripts/DataQuality.sql):

USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS DATA_QUALITY_DB;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA PUBLIC;

-- Create Audit Log Table
CREATE TABLE IF NOT EXISTS DQ_AUDIT_LOG (
    LOG_ID STRING DEFAULT UUID_STRING(),
    PIPELINE_RUN_ID STRING,
    ENVIRONMENT STRING,
    TARGET_TABLE STRING,
    OVERALL_DQ_SCORE NUMBER(5,2),
    CHECKS_DETAIL VARIANT,
    EXECUTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

Step 2: Data Ingestion & Transformation
Upload the Kaggle retail dataset to @KAGGLE_STAGE and execute:
Download from : https://www.kaggle.com/datasets/jyotikushwaha545/onlineretail

Step 3: Run the Data Quality Pipeline
Run python/run_dq_assessment.py inside a Snowflake Notebook or Python execution environment:

from snowflake.snowpark.context import get_active_session

session = get_active_session()
run_dq_framework(session)

Querying Audit & Observability Telemetry
Unpack and query the JSON metrics directly using Snowflake's native semi-structured SQL syntax:

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
    EXECUTION_TIMESTAMP
FROM DATA_QUALITY_DB.PUBLIC.DQ_AUDIT_LOG
ORDER BY EXECUTION_TIMESTAMP DESC;


