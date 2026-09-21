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
