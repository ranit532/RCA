-- =====================================================
-- Data Metric Functions (DMFs) for Reusable DQ Metrics
-- =====================================================
-- DMFs provide declarative metrics that can be referenced
-- in queries and reused across the organization

USE DATABASE RCA_POC_DB;
CREATE SCHEMA IF NOT EXISTS CONTROLS;

-- =====================================================
-- Data Metric Logging
-- =====================================================
-- The previous SQL UDF definitions used dynamic IDENTIFIER() references,
-- which are not supported in this Snowflake SQL UDF context.
-- Instead, this script creates a logging table and a procedure that
-- computes DQ/reconciliation metrics directly.

-- =====================================================
-- Example: Create a DQ Dashboard Table from DMFs
-- =====================================================

CREATE OR REPLACE TABLE CONTROLS.DQ_METRICS_LOG (
  EXECUTION_ID STRING,
  METRIC_NAME STRING,
  METRIC_VALUE NUMBER,
  THRESHOLD_VALUE NUMBER,
  PASSED BOOLEAN,
  CALCULATED_AT TIMESTAMP_NTZ,
  SOURCE_TABLE STRING,
  SOURCE_COLUMN STRING
);

-- Procedure to populate DQ metrics log
CREATE OR REPLACE PROCEDURE CONTROLS.SP_LOG_DQ_METRICS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_execution_id STRING := UUID_STRING();
BEGIN
  -- Log completeness metrics for RAW.FCT_TRADE_RAW.TRADE_ID
  INSERT INTO CONTROLS.DQ_METRICS_LOG
  SELECT
    v_execution_id,
    'COMPLETENESS_TRADE_ID' AS metric_name,
    ROUND(((total_count - null_count) / total_count * 100), 2) AS metric_value,
    95.0 AS threshold_value,
    IFF(ROUND(((total_count - null_count) / total_count * 100), 2) >= 95.0, TRUE, FALSE),
    CURRENT_TIMESTAMP(),
    'RAW.FCT_TRADE_RAW',
    'TRADE_ID'
  FROM (
    SELECT
      COUNT(*) AS total_count,
      COUNT(CASE WHEN TRADE_ID IS NULL THEN 1 END) AS null_count
    FROM RAW.FCT_TRADE_RAW
  );

  -- Log reconciliation metrics for row counts
  INSERT INTO CONTROLS.DQ_METRICS_LOG
  SELECT
    v_execution_id,
    'ROW_COUNT_RAW.FCT_TRADE_RAW_vs_CURATED.DT_FCT_TRADE_CURATED' AS metric_name,
    ROUND((ABS(src_count - tgt_count) / src_count) * 100, 2) AS metric_value,
    1.0 AS threshold_value,
    IFF(ROUND((ABS(src_count - tgt_count) / src_count) * 100, 2) <= 1.0, TRUE, FALSE),
    CURRENT_TIMESTAMP(),
    'RAW.FCT_TRADE_RAW',
    'COUNT'
  FROM (
    SELECT
      (SELECT COUNT(*) FROM RAW.FCT_TRADE_RAW) AS src_count,
      (SELECT COUNT(*) FROM CURATED.DT_FCT_TRADE_CURATED) AS tgt_count
  );

  -- Log referential integrity for RAW.FCT_TRADE_RAW.ACCOUNT_ID
  INSERT INTO CONTROLS.DQ_METRICS_LOG
  SELECT
    v_execution_id,
    'REFERENTIAL_INTEGRITY_RAW.FCT_TRADE_RAW' AS metric_name,
    ROUND(((total_rows - orphaned) / total_rows) * 100, 2) AS metric_value,
    100.0 AS threshold_value,
    IFF(ROUND(((total_rows - orphaned) / total_rows) * 100, 2) = 100.0, TRUE, FALSE),
    CURRENT_TIMESTAMP(),
    'RAW.FCT_TRADE_RAW',
    'ACCOUNT_ID'
  FROM (
    SELECT
      COUNT(*) AS total_rows,
      COUNT(CASE WHEN ACCOUNT_ID NOT IN (SELECT ACCOUNT_ID FROM RAW.DIM_ACCOUNT_RAW) THEN 1 END) AS orphaned
    FROM RAW.FCT_TRADE_RAW
  );

  RETURN 'DQ Metrics logged successfully';
END;
$$;

-- Schedule the metric logging (if Snowflake Tasks are enabled)
-- CREATE TASK CONTROLS.TASK_LOG_DQ_METRICS
-- WAREHOUSE = RCA_INGEST_WH
-- SCHEDULE = '1 HOUR'
-- AS
-- CALL CONTROLS.SP_LOG_DQ_METRICS();
