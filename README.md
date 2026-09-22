# ❄️ Snowflake Data Quality & Observability Framework (Medallion Architecture)

An enterprise-grade Data Quality (DQ) assessment and telemetry logging pipeline built natively inside **Snowflake** using **Snowpark Python** and **Snowflake SQL**.

This framework acts as an automated **pipeline circuit breaker** across a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), executing single-pass quality checks across incoming raw records, computing a 3-dimensional weighted Data Quality Index (DQI), promoting valid records to Silver, and routing anomalies into a JSON-enabled Quarantine table with full run lineage.

---

## 🏗️ Architecture & Data Flow

```text
┌─────────────────────────────────┐
│  Kaggle Online Retail Dataset   │
│  (CSV File on Internal Stage)   │
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
