# RCA PoC - Testing Guide

## Quick Start Testing (5 minutes)

### 1. Verify SQL Setup
```bash
# Ensure all SQL scripts have been executed (01_setup.sql → 07_data_metric_functions.sql)
# Check Snowflake:
SELECT COUNT(*) FROM RCA_POC_DB.RAW.FCT_TRADE_RAW;
SELECT COUNT(*) FROM RCA_POC_DB.CURATED.DT_FCT_TRADE_CURATED;
SELECT COUNT(*) FROM RCA_POC_DB.CONTROLS.DQ_METRICS_LOG;
```

### 2. Activate Virtual Environment & Run Orchestration
```bash
cd "c:\Users\Ranit.Sinha\Documents\Snowflake\RCA POC\RCA\python"
.\venv\Scripts\activate
python orchestrate.py
```

**Expected Output**:
```
2026-04-09 13:45:00,000 - __main__ - INFO - =====================================
2026-04-09 13:45:00,000 - __main__ - INFO - RCA PoC - Orchestration Starting
2026-04-09 13:45:00,000 - __main__ - INFO - =====================================
2026-04-09 13:45:01,123 - src.snowpark_dq.session_manager - INFO - Creating Snowflake connector session for account: TQWSLTQ-TW60698
2026-04-09 13:45:05,456 - src.snowpark_dq.session_manager - INFO - Snowflake connector session created successfully

2026-04-09 13:45:05,456 - __main__ - INFO - ============================
2026-04-09 13:45:05,456 - __main__ - INFO - PHASE 1: DATA QUALITY VALIDATION
2026-04-09 13:45:05,456 - __main__ - INFO - ============================

2026-04-09 13:45:06,000 - src.snowpark_dq.quality_engine - INFO - Executing 8 quality rules (Run ID: abc-123-def-456)
2026-04-09 13:45:06,100 - src.snowpark_dq.quality_engine - INFO - Rule COMPLETENESS_FCT_TRADE executed: 0/30000 failed (0.00%) - PASSED
2026-04-09 13:45:06,150 - src.snowpark_dq.quality_engine - INFO - Rule UNIQUENESS_TRADE_ID executed: 0/30000 failed (0.00%) - PASSED
...

✅ DQ Results Summary:
  - Total Rules: 8
  - Passed: 8
  - Failed: 0
  - Readiness Status: YES_READY_FOR_REPORTING

2026-04-09 13:45:15,000 - __main__ - INFO - ============================
2026-04-09 13:45:15,000 - __main__ - INFO - PHASE 2: RECONCILIATION CONTROLS
2026-04-09 13:45:15,000 - __main__ - INFO - ============================

2026-04-09 13:45:15,100 - src.controls.reconciliation_engine - INFO - Executing 5 reconciliation controls (Run ID: xyz-789-uvw-123)
2026-04-09 13:45:15,200 - src.controls.reconciliation_engine - INFO - Control ROW_COUNT_FCT executed: PASSED (Variance: 0.00%)
2026-04-09 13:45:15,250 - src.controls.reconciliation_engine - INFO - Control SUM_TRADE_AMOUNT executed: PASSED (Variance: 0.02%)
...

✅ Reconciliation Summary:
  - Total Controls: 5
  - Passed: 5
  - Failed: 0
  - Readiness: YES_READY_FOR_REPORTING

2026-04-09 13:45:25,000 - __main__ - INFO - ✅ Orchestration Completed Successfully
```

### 3. Verify Results in Snowflake
```sql
-- Check DQ Metrics
SELECT * FROM RCA_POC_DB.CONTROLS.DQ_METRICS_LOG 
ORDER BY EXECUTED_AT DESC 
LIMIT 10;

-- Check Reconciliation Results
SELECT * FROM RCA_POC_DB.CONTROLS.RECONCILIATION_RESULTS 
ORDER BY EXECUTED_AT DESC 
LIMIT 10;

-- Verify counts match between RAW and CURATED
SELECT 'RAW' as layer, COUNT(*) as record_count FROM RCA_POC_DB.RAW.FCT_TRADE_RAW
UNION ALL
SELECT 'CURATED' as layer, COUNT(*) as record_count FROM RCA_POC_DB.CURATED.DT_FCT_TRADE_CURATED;
```

---

## Comprehensive Testing Scenarios

### Test 1: Data Integrity Validation
**Objective**: Verify data flows correctly from RAW → CURATED layers

**Steps**:
```bash
# Run orchestration
python orchestrate.py

# Query results
sqlite> SELECT COUNT(*) FROM RCA_POC_DB.RAW.FCT_TRADE_RAW;
sqlite> SELECT COUNT(*) FROM RCA_POC_DB.CURATED.DT_FCT_TRADE_CURATED;
```

**Expected**: Row counts should match (or be within tolerance)

**Pass Criteria**: 
- ✅ No row count variance > 0.5%
- ✅ All foreign keys resolved
- ✅ No data loss in transformation

---

### Test 2: Data Quality Rules Execution
**Objective**: Verify all DQ rules execute and report accurately

**Steps**:
```python
# From Python shell
from src.snowpark_dq.quality_engine import DataQualityEngine
from src.snowpark_dq.quality_rules import ASSET_DATA_QUALITY_RULES
from src.snowpark_dq.session_manager import SnowparkSessionManager
from config.snowflake_config import DEFAULT_CONFIG

# Initialize
session = SnowparkSessionManager.initialize(DEFAULT_CONFIG)
engine = DataQualityEngine(session)

# Execute rules
results = engine.execute_rules(ASSET_DATA_QUALITY_RULES)

# Verify results
for result in results:
    print(f"{result.rule_name}: {result.records_failed}/{result.records_tested} failed")
```

**Expected**:
- ✅ All 8 rules execute
- ✅ Results persisted to CONTROLS.DQ_EXECUTION_RESULTS
- ✅ Execution time < 1 second per rule

**Failure Handling**:
If a rule fails:
1. Check if it's within tolerance threshold
2. Review SQL logic in quality_rules.py
3. Investigate data quality issues in source tables

---

### Test 3: Reconciliation Controls
**Objective**: Verify reconciliation logic and variance detection

**Steps**:
```bash
# Intentionally introduce data variance
INSERT INTO RCA_POC_DB.RAW.FCT_TRADE_RAW 
SELECT TOP 100 * FROM RCA_POC_DB.RAW.FCT_TRADE_RAW;

# Run reconciliation
python orchestrate.py --phase reconciliation-only

# Check variance reported
SELECT CONTROL_NAME, SOURCE_COUNT, TARGET_COUNT, VARIANCE, PASSED 
FROM RCA_POC_DB.CONTROLS.RECONCILIATION_RESULTS 
ORDER BY EXECUTED_AT DESC LIMIT 5;
```

**Expected**:
- ✅ Variance detected correctly
- ✅ Controls passed/failed appropriately
- ✅ Results logged to CONTROLS schema

**Tolerance Testing**:
- Set tolerance_value = 100 (ABSOLUTE) → Should PASS with 100 variance
- Set tolerance_value = 0 (ABSOLUTE) → Should FAIL with any variance

---

### Test 4: Streamlit Dashboard
**Objective**: Verify dashboard displays live metrics and controls

**Steps**:
```bash
# Start Streamlit
cd streamlit_app
streamlit run app.py

# Browser: http://localhost:8501
```

**Dashboard Checks**:
- [ ] Controls Cockpit loads without errors
- [ ] DQ metrics displayed correctly
- [ ] Reconciliation status shows latest results
- [ ] Historical trends render
- [ ] Live metric updates refresh
- [ ] No SQL errors in logs

**Test Interactions**:
- [ ] Filter by date range
- [ ] Select specific rules/controls
- [ ] Export results (if implemented)

---

### Test 5: DYD Integration
**Objective**: Verify integration with DYD backend services

**Steps**:
```bash
# Test DYD discovery
python examples/dyd_backend_integration.py

# Verify API calls
# - GET /discover
# - GET /mappings
# - POST /validate
```

**Expected**:
- ✅ Successfully connects to DYD backend
- ✅ Entity discovery returns mapped entities
- ✅ Validation completes without errors
- ✅ Metrics sent to DYD for analytics

**Error Scenarios**:
- [ ] Backend unavailable → Graceful degradation
- [ ] Timeout > 30s → Retry logic
- [ ] Invalid mapping → Validation error reported

---

### Test 6: Error Handling & Recovery
**Objective**: Verify error handling and logging

**Steps**:
```bash
# Test 1: Invalid credentials
# Update .env with wrong password
python orchestrate.py
# Expected: Clear error message, graceful exit

# Test 2: Database unavailable
# Disconnect from VPN
python orchestrate.py
# Expected: Connection error logged, retry attempted

# Test 3: Missing tables
# Rename a table
python orchestrate.py
# Expected: Table not found error, specific message
```

**Expected**:
- ✅ Errors logged with context
- ✅ No sensitive data in error messages
- ✅ Session cleanup on error
- ✅ Exit code reflects error state

---

### Test 7: Performance Baseline
**Objective**: Establish performance benchmarks

**Baseline Metrics** (with default data):
- Setup time: ~5 seconds
- DQ execution time: 50-100ms per rule
- Reconciliation time: 100-200ms per control
- Dashboard load time: < 3 seconds
- Total orchestration: 30-60 seconds

**Stress Testing**:
```bash
# Increase dataset size
# Update FCT_TRADE_RAW to 1M+ records
# Measure: Execution time, memory usage, query performance
```

---

### Test 8: Idempotency
**Objective**: Verify re-running produces same results

**Steps**:
```bash
# Run 1
python orchestrate.py
# Capture run_id and results

# Run 2 (immediately after)
python orchestrate.py
# Capture run_id and results

# Compare
SELECT * FROM CONTROLS.DQ_EXECUTION_RESULTS 
WHERE RUN_ID IN ('{run_1_id}', '{run_2_id}')
ORDER BY RUN_ID, RULE_ID;
```

**Expected**: 
- ✅ Same rules executed
- ✅ Same pass/fail results (if data unchanged)
- ✅ Timestamp differences only

---

## Automated Test Suite

### Unit Tests (pytest)
```python
# tests/test_quality_engine.py
def test_completeness_rule():
    # Test completeness check
    pass

def test_uniqueness_rule():
    # Test uniqueness check
    pass

# tests/test_reconciliation_engine.py
def test_row_count_recon():
    # Test row count matching
    pass

def test_sum_recon():
    # Test amount reconciliation
    pass
```

### Run Tests
```bash
cd python
python -m pytest tests/ -v
```

---

## Troubleshooting Guide

### Issue 1: Authentication Failure
**Error**: `390190 (08001): SAML Identity Provider account parameter`

**Solutions**:
- [ ] Verify .env credentials
- [ ] Change authenticator to `username_password_mfa`
- [ ] Check Snowflake account status
- [ ] Verify role permissions

### Issue 2: Table Not Found
**Error**: `SQL compilation error: Object does not exist`

**Solutions**:
- [ ] Verify SQL scripts have been executed (01-07)
- [ ] Check database and schema names
- [ ] Ensure current warehouse is set
- [ ] Check table privileges

### Issue 3: Memory Error
**Error**: `MemoryError: Unable to allocate memory`

**Solutions**:
- [ ] Reduce batch size
- [ ] Split query into chunks
- [ ] Increase virtual machine memory
- [ ] Optimize SQL queries

### Issue 4: Dashboard Not Loading
**Error**: `StreamlitAPIException` or blank page

**Solutions**:
- [ ] Clear cache: `streamlit cache clear`
- [ ] Verify Snowflake connection
- [ ] Check network connectivity
- [ ] Review browser console for errors

---

## Regression Testing Checklist

- [ ] SQL setup scripts pass
- [ ] Python dependencies install
- [ ] Orchestration runs without errors
- [ ] All DQ rules execute
- [ ] All reconciliation controls execute
- [ ] Dashboard displays correctly
- [ ] Historical data persists
- [ ] Error scenarios handled gracefully
- [ ] Performance within baselines

---

## Sign-Off

**Tested By**: ___________________  
**Date**: ___________________  
**Status**: ✅ PASSED / ❌ FAILED  

**Test Results Summary**:
- ✅ All phases passed
- ✅ Performance acceptable
- ✅ No critical issues
- ✅ Ready for production (subject to deployment checklist)

