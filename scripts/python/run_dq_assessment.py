import json
from datetime import datetime
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.context import get_active_session

def run_dq_framework(
    session: Session,
    bronze_table: str = "DATA_QUALITY_DB.PUBLIC.BRONZE_KAGGLE_RETAIL",
    silver_table: str = "DATA_QUALITY_DB.PUBLIC.SILVER_KAGGLE_RETAIL",
    quarantine_table: str = "DATA_QUALITY_DB.PUBLIC.QUARANTINE_KAGGLE_RETAIL",
    audit_table: str = "DATA_QUALITY_DB.PUBLIC.DQ_AUDIT_LOG",
    environment: str = "DEV",
    dqi_threshold: float = 85.0
):
    pipeline_run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # 1. Single-Pass Engine Scan (Combines count, completeness, and validity checks into 1 query)
    df_bronze = session.table(bronze_table)
    
    dq_checks = df_bronze.select(
        F.count("*").alias("TOTAL_COUNT"),
        F.sum(F.when(F.col("CUSTOMER_ID").is_null(), 1).otherwise(0)).alias("MISSING_CUST"),
        F.sum(F.when(F.col("DESCRIPTION").is_null(), 1).otherwise(0)).alias("MISSING_DESC"),
        F.sum(F.when(F.col("QUANTITY") <= 0, 1).otherwise(0)).alias("INVALID_QTY"),
        F.sum(F.when(F.col("UNIT_PRICE") < 0, 1).otherwise(0)).alias("INVALID_PRICE"),
        F.sum(F.when(F.col("TRANSACTION_TIMESTAMP").is_null(), 1).otherwise(0)).alias("MISSING_TIME")
    ).collect()[0]

    total_records = int(dq_checks["TOTAL_COUNT"] or 0)
    
    if total_records == 0:
        print(f"HALTED: Bronze table {bronze_table} is empty.")
        return session.sql(f"SELECT '{pipeline_run_id}' AS PIPELINE_RUN_ID, 'HALTED: Empty Bronze' AS STATUS")

    missing_cust = int(dq_checks["MISSING_CUST"] or 0)
    missing_desc = int(dq_checks["MISSING_DESC"] or 0)
    invalid_qty = int(dq_checks["INVALID_QTY"] or 0)
    invalid_price = int(dq_checks["INVALID_PRICE"] or 0)
    missing_time = int(dq_checks["MISSING_TIME"] or 0)

    # 2. Compute Uniqueness Dimension (Composite key check: ORDER_ID + PRODUCT_ID)
    distinct_item_count = df_bronze.select("ORDER_ID", "PRODUCT_ID").distinct().count()
    duplicate_records = max(0, total_records - distinct_item_count)

    # 3. Calculate 3-Dimension Weighted Scores (30% Completeness / 20% Uniqueness / 50% Validity)
    completeness = ((total_records - (missing_cust + missing_desc + missing_time)) / float(total_records * 3)) * 100
    uniqueness = ((total_records - duplicate_records) / float(total_records)) * 100
    validity = ((total_records - (invalid_qty + invalid_price)) / float(total_records * 2)) * 100
    
    overall_dqi = float(round((0.30 * completeness) + (0.20 * uniqueness) + (0.50 * validity), 2))

    # 4. Gatekeeping & Medallion Data Promotion
    promotion_status = "HALTED"
    
    if overall_dqi >= dqi_threshold:
        promotion_status = "PROMOTED_WITH_QUARANTINE"
        
        # A. Promote Valid Records to Silver Layer (Casting Timestamp & Standardizing Schema)
        insert_silver_sql = f"""
        INSERT INTO {silver_table} (
            ORDER_ID, PRODUCT_ID, DESCRIPTION, QUANTITY, 
            TRANSACTION_TIMESTAMP, UNIT_PRICE, CUSTOMER_ID, COUNTRY
        )
        SELECT 
            ORDER_ID, 
            PRODUCT_ID, 
            DESCRIPTION, 
            QUANTITY, 
            TRY_TO_TIMESTAMP_NTZ(TRANSACTION_TIMESTAMP, 'MM/DD/YYYY HH24:MI'), 
            UNIT_PRICE, 
            CUSTOMER_ID, 
            COUNTRY
        FROM {bronze_table}
        WHERE CUSTOMER_ID IS NOT NULL 
          AND DESCRIPTION IS NOT NULL
          AND QUANTITY > 0 
          AND UNIT_PRICE >= 0;
        """
        session.sql(insert_silver_sql).collect()
        
        # B. Route Anomaly Records to Quarantine Table (Including PIPELINE_RUN_ID Lineage)
        insert_quarantine_sql = f"""
        INSERT INTO {quarantine_table} (PIPELINE_RUN_ID, ORDER_ID, REJECT_REASON, RAW_RECORD)
        SELECT 
            '{pipeline_run_id}' AS PIPELINE_RUN_ID,
            ORDER_ID,
            ARRAY_TO_STRING(
                ARRAY_CONSTRUCT_COMPACT(
                    CASE WHEN CUSTOMER_ID IS NULL THEN 'Missing Customer ID' END,
                    CASE WHEN DESCRIPTION IS NULL THEN 'Missing Description' END,
                    CASE WHEN QUANTITY <= 0 THEN 'Invalid Quantity (<= 0)' END,
                    CASE WHEN UNIT_PRICE < 0 THEN 'Invalid Unit Price (< 0)' END
                ), 
                ' | '
            ) AS REJECT_REASON,
            OBJECT_CONSTRUCT(*) AS RAW_RECORD
        FROM {bronze_table}
        WHERE CUSTOMER_ID IS NULL 
           OR DESCRIPTION IS NULL
           OR QUANTITY <= 0 
           OR UNIT_PRICE < 0;
        """
        session.sql(insert_quarantine_sql).collect()
    else:
        print(f"PIPELINE HALTED: DQI score ({overall_dqi}%) is below required threshold ({dqi_threshold}%).")

    # 5. Build Audit Payload Dictionary
    checks_detail_dict = {
        "metrics": {
            "total_records": total_records,
            "duplicate_item_records": duplicate_records,
            "missing_customer_ids": missing_cust,
            "missing_descriptions": missing_desc,
            "invalid_quantities": invalid_qty,
            "invalid_unit_prices": invalid_price,
            "missing_timestamps": missing_time
        },
        "scores": {
            "completeness_score": round(completeness, 2),
            "uniqueness_score": round(uniqueness, 2),
            "validity_score": round(validity, 2),
            "overall_dqi": overall_dqi,
            "threshold_applied": dqi_threshold
        }
    }

    # 6. Insert Log Record into Central Audit Table via SELECT PARSE_JSON
    json_str = json.dumps(checks_detail_dict).replace("'", "''")
    insert_audit_sql = f"""
    INSERT INTO {audit_table} 
        (PIPELINE_RUN_ID, ENVIRONMENT, TARGET_TABLE, OVERALL_DQ_SCORE, PROMOTION_STATUS, CHECKS_DETAIL)
    SELECT 
        '{pipeline_run_id}', 
        '{environment}', 
        '{bronze_table}', 
        {overall_dqi}, 
        '{promotion_status}',
        PARSE_JSON('{json_str}')
    """
    session.sql(insert_audit_sql).collect()
    
    # 7. Render Execution Summary DataFrame
    return session.sql(f"""
        SELECT 
            '{pipeline_run_id}' AS PIPELINE_RUN_ID,
            '{bronze_table}' AS SOURCE_BRONZE_TABLE,
            {overall_dqi} AS OVERALL_DQI_SCORE,
            '{promotion_status}' AS PIPELINE_STATUS
    """)

# --- EXPLICIT EXECUTION ---
session = get_active_session()
run_dq_framework(session)
