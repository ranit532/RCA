# Quick Start Guide - RCA PoC

Follow these steps to get the RCA PoC up and running in your environment.

## ⚡ 5-Minute Quick Start

### Step 1: Prerequisites Check
- [ ] Python 3.9+ installed
- [ ] Snowflake account access (TQWSLTQ-TW60698)
- [ ] Credentials ready (RANIT532 user)
- [ ] Browser-based authentication enabled

### Step 2: SQL Setup (Snowflake)

1. Connect to your Snowflake account
2. Execute SQL scripts in order:
   ```
   sql/01_setup.sql
   sql/02_synthetic_dimensions.sql
   sql/03_synthetic_facts_and_streaming.sql
   sql/06_dynamic_tables.sql
   sql/07_data_metric_functions.sql
   ```
3. Verify database `RCA_POC_DB` created with schemas: RAW, CURATED, ANALYTICS, CONTROLS

### Step 3: Python Environment Setup

**Windows:**
```bash
cd python
setup.bat
# Select option 1
```

**macOS/Linux:**
```bash
cd python
chmod +x setup.sh
./setup.sh
# Select option 1
```

### Step 4: Run Orchestration

```bash
cd python
python orchestrate.py
```

**Expected Output:**
```
================================================================================
RCA PoC - Orchestration Starting
================================================================================
✅ Connected to Snowflake
✅ Data Quality: 6/6 rules passed
✅ Reconciliation: 3/3 controls passed
✅ Results persisted to CONTROLS schema
================================== ===========================================
✅ RCA PoC Orchestration Complete
================================================================================
```

### Step 5: View Dashboard

```bash
cd python/streamlit_app
streamlit run app.py
```

Browse to: `http://localhost:8501`

## 📊 What You've Accomplished

✅ **Data Quality Validation**
- Executed 6 quality rules on asset data
- Results in `CONTROLS.DQ_EXECUTION_RESULTS`

✅ **Proactive Reconciliation**
- Row count and amount reconciliations
- Results in `CONTROLS.RECONCILIATION_RESULTS`

✅ **DYD Integration**
- Mapping reference tables created
- Metadata registered in CONTROLS schema

✅ **Dynamic Transformations**
- 8 Dynamic Tables created (curated + analytics layers)
- Automatic refresh every 15 minutes - 1 hour

✅ **Interactive Dashboard**
- Real-time controls cockpit
- Audit trail visibility
- Export capabilities

## 🔍 Query Your Results

### Check Data Quality
```sql
SELECT 
  RULE_ID,
  RULE_NAME,
  RECORDS_TESTED,
  FAILURE_RATE,
  PASSED
FROM CONTROLS.DQ_EXECUTION_RESULTS
ORDER BY EXECUTED_AT DESC;
```

### Check Reconciliation Status
```sql
SELECT
  CONTROL_ID,
  CONTROL_NAME,
  VARIANCE,
  VARIANCE_PERCENTAGE,
  PASSED
FROM CONTROLS.RECONCILIATION_RESULTS
ORDER BY EXECUTED_AT DESC;
```

### View Curated Data
```sql
SELECT * FROM CURATED.DT_DIM_PARTY_CURATED LIMIT 10;
SELECT * FROM CURATED.DT_FCT_TRADE_CURATED LIMIT 10;
```

### View Analytics
```sql
SELECT * FROM ANALYTICS.DT_TRADE_DAILY_SUMMARY;
SELECT * FROM ANALYTICS.DT_ACCOUNT_ANALYTICS;
```

## 🚨 Troubleshooting

### Connection Failed
**Issue:** Cannot connect to Snowflake
**Solution:** 
1. Verify credentials in `config/snowflake_config.py`
2. Check browser-based auth is enabled
3. Confirm account URL: `TQWSLTQ-TW60698.snowflakecomputing.com`

### SQL Scripts Failed
**Issue:** Scripts don't execute
**Solution:**
1. Ensure you have ACCOUNTADMIN role
2. Run scripts in exact order (01→02→03→06→07)
3. Check warehouse is created and running

### Orchestration Errors
**Issue:** `orchestrate.py` fails
**Solution:**
```bash
# Enable debug logging
python -c "import logging; logging.basicConfig(level=logging.DEBUG)" orchestrate.py

# Or check specific error in logs
# Look for CONTROLS.DQ_EXECUTION_RESULTS for details
```

### Dashboard Not Loading
**Issue:** Streamlit app shows blank
**Solution:**
1. Run orchestration first (creates data)
2. Access http://localhost:8501
3. Check browser console for errors
4. Ensure Python dependencies installed: `pip install streamlit`

## 📚 Next Steps

### Customize Quality Rules
Edit `src/snowpark_dq/quality_rules.py`:
```python
ASSET_DATA_QUALITY_RULES = [
    DQRule(
        rule_id="CUSTOM_001",
        rule_name="My Custom Rule",
        table_name="RAW.MY_TABLE",
        sql_logic="YOUR_CONDITION"
    ),
]
```

### Add Reconciliation Controls
Edit orchestration logic:
```python
controls = [
    ReconciliationControl(
        control_id="CUSTOM_ROW_001",
        control_name="My Row Count Control",
        source_table="RAW.TABLE_A",
        target_table="CURATED.TABLE_B",
    ),
]
```

### Integrate DYD Exports
Place DYD mappings in JSON format:
```python
dyd = DYDIntegration(session)
dyd.load_mappings_from_json("dyd_mappings.json")
dyd.load_metadata_from_json("dyd_metadata.json")
```

### Schedule Regular Execution
Create Snowflake Task:
```sql
CREATE TASK ORCHESTRATE_RCA
  WAREHOUSE = RCA_INGEST_WH
  SCHEDULE = '1 HOUR'
  AS
  CALL CONTROLS.SP_LOG_DQ_METRICS();
```

## 📞 Support

- Review logs: Check Python console output with `-v` flag
- Check CONTROLS tables: Query `DQ_EXECUTION_RESULTS` and `RECONCILIATION_RESULTS`
- Review case study: See attached PDF for context

## 🎓 Learning Resources

- [Snowpark Python Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/)
- [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables)
- [Streamlit Documentation](https://docs.streamlit.io/)

---

**Status:** ✅ Ready to Deploy
**Version:** 1.0.0
**Last Updated:** January 9, 2025
