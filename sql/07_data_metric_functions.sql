-- =====================================================
-- Data Metric Functions (DMFs) for Reusable DQ Metrics
-- =====================================================
-- DMFs provide declarative metrics that can be referenced
-- in queries and reused across the organization

USE DATABASE RCA_POC_DB;
CREATE SCHEMA IF NOT EXISTS CONTROLS;

-- =====================================================
-- Core Data Quality Metrics
-- =====================================================

CREATE OR REPLACE FUNCTION DQ_METRIC_COMPLETENESS(
  table_name VARCHAR,
  column_name VARCHAR
)
RETURNS TABLE (
  METRIC_NAME VARCHAR,
  METRIC_VALUE NUMBER,
  RECORDS_TESTED NUMBER,
  RECORDS_NULL NUMBER
)
LANGUAGE SQL
AS
$$
WITH source_data AS (
  SELECT COUNT(*) as total_count,
         COUNT(CASE WHEN $column_name IS NULL THEN 1 END) as null_count
  FROM IDENTIFIER($table_name)
)
SELECT
  'COMPLETENESS_' || $column_name as metric_name,
  ROUND(((total_count - null_count) / total_count * 100), 2) as metric_value,
  total_count as records_tested,
  null_count as records_null
FROM source_data
$$;

-- Uniqueness metric
CREATE OR REPLACE FUNCTION DQ_METRIC_UNIQUENESS(
  table_name VARCHAR,
  column_name VARCHAR
)
RETURNS TABLE (
  METRIC_NAME VARCHAR,
  METRIC_VALUE NUMBER,
  TOTAL_ROWS NUMBER,
  DUPLICATE_ROWS NUMBER
)
LANGUAGE SQL
AS
$$
WITH uniqueness_check AS (
  SELECT COUNT(*) as total_rows,
         COUNT(DISTINCT $column_name) as distinct_values
  FROM IDENTIFIER($table_name)
),
duplicates AS (
  SELECT COUNT(*) - COUNT(DISTINCT $column_name) as dup_count
  FROM IDENTIFIER($table_name)
)
SELECT
  'UNIQUENESS_' || $column_name as metric_name,
  ROUND((SELECT COUNT(DISTINCT $column_name) FROM IDENTIFIER($table_name)) /
         COUNT(*) * 100, 2) as metric_value,
  total_rows,
  dup_count as duplicate_rows
FROM uniqueness_check, duplicates
$$;

-- Distribution metric (for trend analysis)
CREATE OR REPLACE FUNCTION DQ_METRIC_VALID_VALUES(
  table_name VARCHAR,
  column_name VARCHAR,
  valid_values VARCHAR  -- comma-separated list
)
RETURNS TABLE (
  METRIC_NAME VARCHAR,
  METRIC_VALUE NUMBER,
  VALID_COUNT NUMBER,
  INVALID_COUNT NUMBER,
  TOTAL_COUNT NUMBER
)
LANGUAGE SQL
AS
$$
WITH validation AS (
  SELECT
    COUNT(*) as total,
    COUNT(CASE WHEN $column_name IN (
      SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE($valid_values, ','))
    ) THEN 1 END) as valid_count
  FROM IDENTIFIER($table_name)
)
SELECT
  'VALID_VALUES_' || $column_name as metric_name,
  ROUND((valid_count / total) * 100, 2) as metric_value,
  valid_count,
  total - valid_count as invalid_count,
  total
FROM validation
$$;

-- =====================================================
-- Reconciliation Metrics
-- =====================================================

CREATE OR REPLACE FUNCTION RECON_METRIC_ROW_COUNT(
  source_table VARCHAR,
  target_table VARCHAR
)
RETURNS TABLE (
  METRIC_NAME VARCHAR,
  SOURCE_COUNT NUMBER,
  TARGET_COUNT NUMBER,
  VARIANCE NUMBER,
  VARIANCE_PCT NUMBER
)
LANGUAGE SQL
AS
$$
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM IDENTIFIER($source_table)) as src_count,
    (SELECT COUNT(*) FROM IDENTIFIER($target_table)) as tgt_count
)
SELECT
  'ROW_COUNT_' || $source_table || '_vs_' || $target_table,
  src_count,
  tgt_count,
  ABS(src_count - tgt_count),
  ROUND((ABS(src_count - tgt_count) / src_count) * 100, 2)
FROM counts
$$;

-- Amount reconciliation metric
CREATE OR REPLACE FUNCTION RECON_METRIC_SUM_AMOUNT(
  source_table VARCHAR,
  source_column VARCHAR,
  target_table VARCHAR,
  target_column VARCHAR
)
RETURNS TABLE (
  METRIC_NAME VARCHAR,
  SOURCE_SUM NUMBER,
  TARGET_SUM NUMBER,
  VARIANCE NUMBER,
  VARIANCE_PCT NUMBER
)
LANGUAGE SQL
AS
$$
WITH sums AS (
  SELECT
    (SELECT COALESCE(SUM($source_column), 0) FROM IDENTIFIER($source_table)) as src_sum,
    (SELECT COALESCE(SUM($target_column), 0) FROM IDENTIFIER($target_table)) as tgt_sum
)
SELECT
  'SUM_' || $source_column || '_' || $target_column,
  src_sum,
  tgt_sum,
  ABS(src_sum - tgt_sum),
  ROUND((ABS(src_sum - tgt_sum) / NULLIFZERO(ABS(src_sum))) * 100, 2)
FROM sums
$$;

-- =====================================================
-- Referential Integrity Metrics
-- =====================================================

CREATE OR REPLACE FUNCTION DQ_METRIC_REFERENTIAL_INTEGRITY(
  detail_table VARCHAR,
  detail_fk_column VARCHAR,
  ref_table VARCHAR,
  ref_pk_column VARCHAR
)
RETURNS TABLE (
  METRIC_NAME VARCHAR,
  TOTAL_DETAIL_ROWS NUMBER,
  ORPHANED_ROWS NUMBER,
  INTEGRITY_PCT NUMBER
)
LANGUAGE SQL
AS
$$
WITH detail_analysis AS (
  SELECT
    (SELECT COUNT(*) FROM IDENTIFIER($detail_table)) as total_rows,
    (SELECT COUNT(*) FROM IDENTIFIER($detail_table) d
     WHERE $detail_fk_column NOT IN (
       SELECT $ref_pk_column FROM IDENTIFIER($ref_table)
     )) as orphaned
)
SELECT
  'REFERENTIAL_INTEGRITY_' || $detail_table,
  total_rows,
  orphaned,
  ROUND(((total_rows - orphaned) / total_rows) * 100, 2)
FROM detail_analysis
$$;

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
COMMENT = 'Logs all DQ metrics to dashboard table for trend analysis'
AS
$$
DECLARE
  v_execution_id STRING := UUID_STRING();
BEGIN
  -- Log completeness metrics
  INSERT INTO CONTROLS.DQ_METRICS_LOG
  SELECT
    v_execution_id,
    metric_name,
    metric_value,
    95.0,  -- 95% threshold
    IFF(metric_value >= 95.0, TRUE, FALSE),
    CURRENT_TIMESTAMP(),
    'DIM_TRADE_RAW',
    'TRADE_ID'
  FROM TABLE(DQ_METRIC_COMPLETENESS('RAW.FCT_TRADE_RAW', 'TRADE_ID'));
  
  -- Log reconciliation metrics
  INSERT INTO CONTROLS.DQ_METRICS_LOG
  SELECT
    v_execution_id,
    metric_name,
    variance_pct,
    1.0,  -- 1% variance tolerance
    IFF(variance_pct <= 1.0, TRUE, FALSE),
    CURRENT_TIMESTAMP(),
    'RAW.FCT_TRADE_RAW',
    'COUNT'
  FROM TABLE(RECON_METRIC_ROW_COUNT('RAW.FCT_TRADE_RAW', 'CURATED.DT_FCT_TRADE_CURATED'));
  
  -- Log referential integrity
  INSERT INTO CONTROLS.DQ_METRICS_LOG
  SELECT
    v_execution_id,
    metric_name,
    integrity_pct,
    100.0,  -- No orphaned records allowed
    IFF(integrity_pct = 100.0, TRUE, FALSE),
    CURRENT_TIMESTAMP(),
    'FCT_TRADE_RAW',
    'ACCOUNT_ID'
  FROM TABLE(DQ_METRIC_REFERENTIAL_INTEGRITY(
    'RAW.FCT_TRADE_RAW', 'ACCOUNT_ID',
    'RAW.DIM_ACCOUNT_RAW', 'ACCOUNT_ID'
  ));
  
  RETURN 'DQ Metrics logged successfully';
END;
$$;

-- Schedule the metric logging (if Snowflake Tasks are enabled)
-- CREATE TASK CONTROLS.TASK_LOG_DQ_METRICS
-- WAREHOUSE = RCA_INGEST_WH
-- SCHEDULE = '1 HOUR'
-- AS
-- CALL CONTROLS.SP_LOG_DQ_METRICS();
