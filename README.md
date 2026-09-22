# ❄️ Snowflake Data Quality & Observability Framework (Medallion Architecture)

An enterprise-grade, automated Data Quality (DQ) assessment and telemetry logging pipeline built natively inside **Snowflake** using **Snowpark Python** and **Snowflake SQL**.

This framework acts as an automated **pipeline circuit breaker** across a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), executing single-pass quality checks across incoming raw records, computing a 3-dimensional weighted Data Quality Index (DQI), promoting valid records to Silver, and routing anomalies into a JSON-enabled Quarantine table with full run lineage.

---

## 🏗️ Architecture & Data Flow

```text
┌─────────────────────────────────┐
│  Kaggle Online Retail Dataset   │
│  (OnlineRetail.csv on Stage)    │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  BRONZE_KAGGLE_RETAIL           │
│  (Raw Ingestion Landing Table)  │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  run_dq_assessment.py           │ ───► Logs Audit Telemetry to DQ_AUDIT_LOG
│  (Snowpark Pushdown Engine)     │      (JSON Stored in VARIANT Column)
└────────────────┬────────────────┘
                 │
         ┌───────┴───────┐
         │ DQI >= 85.0%  │
         ▼               ▼
┌──────────────────┐   ┌───────────────────────────────┐
│  SILVER_KAGGLE   │   │  QUARANTINE_KAGGLE_RETAIL     │
│  (Curated Layer) │   │  (Anomalies + Run ID Lineage) │
└────────┬─────────┘   └───────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  GOLD_DAILY_REVENUE_BY_COUNTRY  │
│  (Business Analytical View)     │
└─────────────────────────────────┘
🌐 Cross-Cloud Parity: Microsoft Fabric vs. SnowflakeThis framework establishes functional and architectural parity with lakehouse data quality pipelines built on Microsoft Fabric:Architectural DimensionMicrosoft Fabric ImplementationSnowflake / Snowpark ImplementationStorage & PlatformDelta Tables on Fabric OneLakeManaged Tables on Snowflake EngineCompute EnginePySpark DataFramesSnowpark Python DataFramesExecution PatternSingle-pass Spark DataFrame AggregationsSingle-pass SQL Engine Pushdown ScansQuality DimensionsCompleteness, Uniqueness, ValidityCompleteness, Uniqueness, ValidityGatekeeping LogicConditional Delta Table WritesSilver Promotion + VARIANT Quarantine RoutingAudit TelemetryDelta Audit Log TablesStructured JSON via Snowflake VARIANT Column

 Scoring Engine & Mathematical FormulationThe framework evaluates three core quality dimensions and calculates a weighted Overall DQI Score:1. Completeness Score (30% Weight)$$\text{Completeness (\%)} = \left( \frac{\text{Total Records} - (\text{Null Customers} + \text{Null Descriptions} + \text{Null Timestamps})}{\text{Total Records} \times 3} \right) \times 100$$2. Uniqueness Score (20% Weight)Evaluates composite primary key integrity (ORDER_ID + PRODUCT_ID):$$\text{Uniqueness (\%)} = \left( \frac{\text{Total Records} - \text{Duplicate Composite Keys}}{\text{Total Records}} \right) \times 100$$3. Validity Score (50% Weight)Validates numerical domain constraints (QUANTITY > 0 and UNIT_PRICE >= 0):$$\text{Validity (\%)} = \left( \frac{\text{Total Records} - (\text{Invalid Quantities} + \text{Invalid Prices})}{\text{Total Records} \times 2} \right) \times 100$$4. Overall DQI Score Formula$$\text{Overall DQI} = (0.30 \times \text{Completeness}) + (0.20 \times \text{Uniqueness}) + (0.50 \times \text{Validity})$$🛠️ Step-by-Step Testing & Execution GuideFollow these exact steps to deploy and test the entire framework end-to-end in your Snowflake account.Prerequisites & Dataset SetupDownload Dataset: Download the Online Retail Dataset directly from Kaggle:🔗 Kaggle Online Retail Dataset Download LinkUnzip the downloaded file locally to obtain OnlineRetail.csv.Access Snowflake: Log into your Snowflake Web Interface (Snowsight).Step 1: Initialize Database & Medallion DDLs (Snowflake SQL)In Snowsight, click Worksheets $\rightarrow$ + (New SQL Worksheet).Ensure your active role has privileges to create databases (e.g., ACCOUNTADMIN or SYSADMIN) and select active warehouse COMPUTE_WH.Copy and execute Phase 1 & Phase 2 from scripts/DataQuality.sql:

USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS DATA_QUALITY_DB;
USE DATABASE DATA_QUALITY_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Create Audit Log Table
CREATE TABLE IF NOT EXISTS DQ_AUDIT_LOG (
    LOG_ID STRING DEFAULT UUID_STRING(),
    PIPELINE_RUN_ID STRING,
    ENVIRONMENT STRING,
    TARGET_TABLE STRING,
    OVERALL_DQ_SCORE NUMBER(5,2),
    PROMOTION_STATUS STRING,
    CHECKS_DETAIL VARIANT,
    EXECUTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- File Format & Stage Setup
CREATE OR REPLACE FILE FORMAT KAGGLE_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', 'NaN')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE KAGGLE_STAGE FILE_FORMAT = KAGGLE_CSV_FORMAT;

-- Medallion Architecture Tables
CREATE TABLE IF NOT EXISTS BRONZE_KAGGLE_RETAIL (
    ORDER_ID STRING, PRODUCT_ID STRING, DESCRIPTION STRING, 
    QUANTITY INTEGER, TRANSACTION_TIMESTAMP VARCHAR, 
    UNIT_PRICE FLOAT, CUSTOMER_ID STRING, COUNTRY STRING,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXIS

TS SILVER_KAGGLE_RETAIL (
    ORDER_ID STRING, PRODUCT_ID STRING, DESCRIPTION STRING, 
    QUANTITY INTEGER, TRANSACTION_TIMESTAMP TIMESTAMP_NTZ, 
    UNIT_PRICE FLOAT, CUSTOMER_ID STRING, COUNTRY STRING,
    PROMOTED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QUARANTINE_KAGGLE_RETAIL (
    QUARANTINE_ID STRING DEFAULT UUID_STRING(),
    PIPELINE_RUN_ID STRING, ORDER_ID STRING, REJECT_REASON STRING, 
    RAW_RECORD VARIANT, QUARANTINED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE VIEW GOLD_DAILY_REVENUE_BY_COUNTRY AS
SELECT 
    DATE(TRANSACTION_TIMESTAMP) AS SALES_DATE, COUNTRY,
    COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS,
    ROUND(SUM(QUANTITY * UNIT_PRICE), 2) AS TOTAL_REVENUE
FROM SILVER_KAGGLE_RETAIL
GROUP BY 1, 2;

Step 2: Upload Raw CSV Data to Internal StageIn Snowsight, go to Data $\rightarrow$ Databases $\rightarrow$ DATA_QUALITY_DB $\rightarrow$ PUBLIC $\rightarrow$ Stages.Click KAGGLE_STAGE.Click + Files in the top right corner.Browse and select your unzipped OnlineRetail.csv file, then click Upload.Step 3: Populate Bronze Layer (Raw Ingestion)Return to your SQL Worksheet and execute Phase 3:


COPY INTO BRONZE_KAGGLE_RETAIL (
    ORDER_ID, PRODUCT_ID, DESCRIPTION, QUANTITY, 
    TRANSACTION_TIMESTAMP, UNIT_PRICE, CUSTOMER_ID, COUNTRY
)
FROM @KAGGLE_STAGE
FILE_FORMAT = (FORMAT_NAME = 'KAGGLE_CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Verify raw ingestion
SELECT COUNT(*) AS BRONZE_TOTAL_ROWS FROM BRONZE_KAGGLE_RETAIL;

Step 4: Execute Snowpark Python Assessment & Gatekeeper EngineIn Snowsight, click Worksheets $\rightarrow$ + $\rightarrow$ Python Worksheet (or create a Python Notebook).Set context to DATA_QUALITY_DB and PUBLIC.Paste the contents of python/run_dq_assessment.py into the editor and click Run.The engine will:Perform single-pass pushdown aggregations on BRONZE_KAGGLE_RETAIL.Calculate 3D DQI scores (Completeness, Uniqueness, Validity).Promote valid rows to SILVER_KAGGLE_RETAIL.Route invalid rows with rule violation masks to QUARANTINE_KAGGLE_RETAIL.Save structured JSON telemetry into DQ_AUDIT_LOG.Step 5: Verify Pipeline Results & Audit Logs (Phase 4 SQL)Switch back to your SQL Worksheet and run Phase 4 queries:

-- 1. Query JSON Audit Log
SELECT 
    PIPELINE_RUN_ID, OVERALL_DQ_SCORE, PROMOTION_STATUS,
    CHECKS_DETAIL:scores.completeness_score::FLOAT AS COMPLETENESS,
    CHECKS_DETAIL:scores.uniqueness_score::FLOAT AS UNIQUENESS,
    CHECKS_DETAIL:scores.validity_score::FLOAT AS VALIDITY,
    CHECKS_DETAIL:metrics.missing_c
ustomer_ids::INT AS MISSING_CUSTOMERS,
    CHECKS_DETAIL:metrics.invalid_quantities::INT AS INVALID_QUANTITIES,
    EXECUTION_TIMESTAMP
FROM DQ_AUDIT_LOG
ORDER BY EXECUTION_TIMESTAMP DESC;

-- 2. Verify Medallion Layer Distribution
SELECT 'BRONZE' AS LAYER, COUNT(*) AS REsnowflake-dq-framework/
├── .gitignore               # Excludes temporary cache, OS, and IDE configs
├── README.md                # Detailed project documentation & deployment guide
├── python/
│   └── run_dq_assessment.py # Snowpark Python DQ assessment & gatekeeper script
└── scripts/
    └── DataQuality.sql      # Database DDL, Medallion tables, & audit queriesCORDS FROM BRONZE_KAGGLE_RETAIL
UNION ALL
SELECT 'SILVER' AS LAYER, COUNT(*) AS RECORDS FROM SILVER_KAGGLE_RETAIL
UNION ALL
SELECT 'QUARANTINE' AS LAYER, COUNT(*) AS RECORDS FROM QUARANTINE_KAGGLE_RETAIL;

-- 3. Query Quarantine Rejection Summary
SELECT REJECT_REASON, COUNT(*) AS REJECTED_ROWS
FROM QUARANTINE_KAGGLE_RETAIL
GROUP BY REJECT_REASON
ORDER BY REJECTED_ROWS DESC;

⚠️ Schema & Data Quality ConsiderationsHeader Remapping: Source headers are standardized during COPY INTO:InvoiceNo $\rightarrow$ ORDER_IDStockCode $\rightarrow$ PRODUCT_IDInvoiceDate $\rightarrow$ TRANSACTION_TIMESTAMPUnitPrice $\rightarrow$ UNIT_PRICECustomerID $\rightarrow$ CUSTOMER_IDSafe Type Casting: TRANSACTION_TIMESTAMP is staged as VARCHAR in Bronze to avoid parsing crashes, and subsequently converted to TIMESTAMP_NTZ during Silver promotion using TRY_TO_TIMESTAMP_NTZ(TRANSACTION_TIMESTAMP, 'MM/DD/YYYY HH24:MI').Null Handling: NULL_IF = ('', 'NULL', 'null', 'NaN') ensures missing values resolve to true SQL NULLs for accurate Completeness scoring.📂 Repository Structure

