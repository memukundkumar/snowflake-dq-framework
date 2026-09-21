import json
from datetime import datetime
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.context import get_active_session

def run_dq_framework(
    session: Session,
    target_table: str = "DATA_QUALITY_DB.PUBLIC.KAGGLE_RETAIL_REAL",
    audit_table: str = "DATA_QUALITY_DB.PUBLIC.DQ_AUDIT_LOG",
    environment: str = "DEV"
):
    pipeline_run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # 1. Load Target Dataset
    df = session.table(target_table)
    total_records = df.count()
    
    if total_records == 0:
        print(f"FAILED: Target table {target_table} is empty.")
        return session.sql(f"SELECT '{pipeline_run_id}' AS PIPELINE_RUN_ID, 'FAILED: Empty Table' AS EXECUTION_STATUS")

    # 2. Rule Validation Aggregations (Single-pass pushdown)
    dq_checks = df.select(
        F.sum(F.when(F.col("CUSTOMER_ID").is_null(), 1).otherwise(0)).alias("MISSING_CUST"),
        F.sum(F.when(F.col("DESCRIPTION").is_null(), 1).otherwise(0)).alias("MISSING_DESC"),
        F.sum(F.when(F.col("QUANTITY") <= 0, 1).otherwise(0)).alias("INVALID_QTY"),
        F.sum(F.when(F.col("UNIT_PRICE") < 0, 1).otherwise(0)).alias("INVALID_PRICE"),
        F.sum(F.when(F.col("TRANSACTION_TIMESTAMP").is_null(), 1).otherwise(0)).alias("MISSING_TIME")
    ).collect()[0]

    missing_cust = int(dq_checks["MISSING_CUST"] or 0)
    missing_desc = int(dq_checks["MISSING_DESC"] or 0)
    invalid_qty = int(dq_checks["INVALID_QTY"] or 0)
    invalid_price = int(dq_checks["INVALID_PRICE"] or 0)
    missing_time = int(dq_checks["MISSING_TIME"] or 0)

    # 3. Calculate Dimension Scores
    completeness = ((total_records - (missing_cust + missing_desc + missing_time)) / float(total_records * 3)) * 100
    validity = ((total_records - (invalid_qty + invalid_price)) / float(total_records * 2)) * 100
    overall_dqi = float(round((0.40 * completeness) + (0.60 * validity), 2))

    # 4. Build Audit Payload Dictionary
    checks_detail_dict = {
        "metrics": {
            "total_records": total_records,
            "missing_customer_ids": missing_cust,
            "missing_descriptions": missing_desc,
            "invalid_quantities": invalid_qty,
            "invalid_unit_prices": invalid_price,
            "missing_timestamps": missing_time
        },
        "scores": {
            "completeness_score": round(completeness, 2),
            "validity_score": round(validity, 2),
            "overall_dqi": overall_dqi
        }
    }

    # 5. Insert Log Record into Audit Table via SELECT PARSE_JSON
    json_str = json.dumps(checks_detail_dict).replace("'", "''")
    insert_sql = f"""
    INSERT INTO {audit_table} 
        (PIPELINE_RUN_ID, ENVIRONMENT, TARGET_TABLE, OVERALL_DQ_SCORE, CHECKS_DETAIL)
    SELECT 
        '{pipeline_run_id}', 
        '{environment}', 
        '{target_table}', 
        {overall_dqi}, 
        PARSE_JSON('{json_str}')
    """
    
    session.sql(insert_sql).collect()
    
    # 6. Render confirmation output DataFrame in UI
    return session.sql(f"""
        SELECT 
            '{pipeline_run_id}' AS PIPELINE_RUN_ID,
            '{target_table}' AS TARGET_TABLE,
            {overall_dqi} AS OVERALL_DQI_SCORE,
            'SUCCESS' AS EXECUTION_STATUS
    """)

# --- EXPLICIT EXECUTION ---
session = get_active_session()
run_dq_framework(session)
