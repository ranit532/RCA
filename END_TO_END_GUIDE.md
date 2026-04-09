# 🚀 End-to-End Guide - RCA PoC Complete Walkthrough

This guide walks you through running the entire RCA Proof of Concept from start to finish, including database setup, backend orchestration, and interactive dashboard.

---

## 📋 Prerequisites (15 minutes)

### ✅ System Requirements
- **OS:** Windows 10+, macOS 12+, or Linux (Ubuntu 20+)
- **Python:** 3.9+ (verify with `python --version`)
- **Git:** Installed (verify with `git --version`)
- **Snowflake Account:** Access to TQWSLTQ-TW60698
- **Snowflake User:** RANIT532 with ACCOUNTADMIN role
- **Browser:** Chrome, Firefox, or Edge (for Streamlit dashboard)

### ✅ Verify Python Installation

**Windows (PowerShell):**
```powershell
python --version
# Expected: Python 3.9.x or higher

python -m pip --version
# Expected: pip 21.x or higher
```

**macOS/Linux (Terminal):**
```bash
python3 --version
pip3 --version
```

---

## 🗄️ Phase 1: Snowflake Database Setup (20 minutes)

### Step 1.1: Connect to Snowflake

1. Open Snowflake Web UI: [https://TQWSLTQ-TW60698.snowflakecomputing.com](https://TQWSLTQ-TW60698.snowflakecomputing.com)
2. Login with user: `RANIT532`
3. Verify you're in the **ACCOUNTADMIN** role (top-right corner)

### Step 1.2: Create Worksheet for SQL Execution

1. Click **Projects** → **Worksheets** (top-left)
2. Click **+ → SQL Worksheet**
3. Name it: `RCA_POC_Setup`

### Step 1.3: Execute SQL Scripts (In Order)

**Execute Script 1: Setup (Database & Warehouses)**
```sql
-- Copy content from: sql/01_setup.sql
-- Paste into worksheet and run
```

**Actions:** This creates:
- Database: `RCA_POC_DB`
- Schemas: `RAW`, `CURATED`, `ANALYTICS`, `CONTROLS`
- Warehouse: `COMPUTE_WH` (Medium)

**Wait for completion** → you should see ✅ status for each CREATE statement.

---

**Execute Script 2: Synthetic Dimensions**
```sql
-- Copy content from: sql/02_synthetic_dimensions.sql
-- This creates dimension tables in RAW schema
```

**Tables created:**
- `RAW.DIM_PARTY_RAW` (10,000 rows)
- `RAW.DIM_ACCOUNT_RAW` (30,000 rows)
- `RAW.DIM_INSTRUMENT_RAW` (50,000 rows)

---

**Execute Script 3: Synthetic Facts & Streaming**
```sql
-- Copy content from: sql/03_synthetic_facts_and_streaming.sql
-- This creates transaction/fact tables
```

**Tables created:**
- `RAW.FCT_TRADE_RAW` (100,000 rows)

---

**Execute Script 4: Dynamic Tables**
```sql
-- Copy content from: sql/06_dynamic_tables.sql
-- This creates curated and analytics transformations
```

**Expected Output:**
```
Statement executed successfully.
```

**Dynamic Tables created (8 total):**
- CURATED layer (4 tables: parties, accounts, instruments, trades)
- ANALYTICS layer (2 summary tables)
- CONTROLS layer (control table schema)

---

**Execute Script 5: Data Metric Functions**
```sql
-- Copy content from: sql/07_data_metric_functions.sql
-- This creates reusable quality functions
```

---

### Step 1.4: Verify Database Setup

**Query to verify all schemas and tables exist:**
```sql
SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE DATABASE_NAME = 'RCA_POC_DB' 
ORDER BY SCHEMA_NAME;
```

**Expected result:**
```
ANALYTICS
CONTROLS
CURATED
RAW
```

**Verify table counts:**
```sql
SELECT 
  TABLE_SCHEMA,
  COUNT(*) as TABLE_COUNT
FROM RCA_POC_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('RAW', 'CURATED', 'ANALYTICS')
GROUP BY TABLE_SCHEMA
ORDER BY TABLE_SCHEMA;
```

**Expected result:**
```
ANALYTICS    | 2
CURATED      | 4
RAW          | 4
```

✅ **Phase 1 Complete!** You now have the Snowflake data foundation ready.

---

## 🐍 Phase 2: Python Environment Setup (10 minutes)

### Step 2.1: Open Project Directory

**Windows (PowerShell):**
```powershell
cd "c:\Users\Ranit.Sinha\Documents\Snowflake\RCA POC\RCA"
```

**macOS/Linux (Terminal):**
```bash
cd ~/Documents/Snowflake/RCA\ POC/RCA
```

### Step 2.2: View Project Structure

```
RCA/
├── sql/
│   ├── 01_setup.sql
│   ├── 02_synthetic_dimensions.sql
│   ├── 03_synthetic_facts_and_streaming.sql
│   ├── 06_dynamic_tables.sql
│   └── 07_data_metric_functions.sql
│
└── python/
    ├── requirements.txt          ← Python dependencies
    ├── setup.bat / setup.sh      ← Environment setup
    ├── orchestrate.py            ← Main orchestration script
    │
    ├── config/
    │   └── snowflake_config.py   ← Connection settings
    │
    ├── src/
    │   ├── snowpark_dq/          ← Quality engine
    │   ├── controls/             ← Reconciliation engine
    │   ├── dyd_integration/       ← DYD mapping logic
    │   └── streamlit_backend.py  ← Dashboard backend
    │
    └── streamlit_app/
        ├── app.py                ← Streamlit UI
        └── README.md             ← UI documentation
```

### Step 2.3: Navigate to Python Directory

```powershell
cd python
```

### Step 2.4: Create Virtual Environment & Install Dependencies

**Windows (PowerShell):**
```powershell
# Run the setup script
.\setup.bat
```

**Follow the prompts:**
```
Select option:
1) Create new venv & install dependencies
2) Activate existing venv
3) Exit

: 1
```

**What this does:**
- Creates virtual environment: `RCA_venv/`
- Installs all packages from `requirements.txt`
- Activates the environment

**macOS/Linux (Terminal):**
```bash
chmod +x setup.sh
./setup.sh
# Select option 1
```

### Step 2.5: Verify Installation

**Verify all packages installed:**
```powershell
pip list
```

**You should see:**
```
Package                          Version
------------------------------ ----------
pandas                           2.1.0
snowflake-connector-python       3.4.0
snowflake-snowpark-python        1.10.0
streamlit                         1.28.0
pydantic                          2.4.0
```

✅ **Phase 2 Complete!** Python environment is ready.

---

## 🎯 Phase 3: Run Backend Orchestration (5 minutes)

### Step 3.1: Execute Orchestration Script

**Ensure you're in the python directory:**
```powershell
cd python
```

**Run the orchestration:**
```powershell
python orchestrate.py
```

### Step 3.2: Monitor Execution

**Expected Console Output:**
```
================================================================================
RCA PoC - Orchestration Starting
================================================================================
🔗 Initializing Snowflake connection...
✅ Connected to Snowflake
✅ Using warehouse: COMPUTE_WH
✅ Using database: RCA_POC_DB

📊 DATA QUALITY ENGINE
---
Executing 6 data quality rules...
  ✅ COMP_001 - Party ID Completeness: PASSED (5 failures)
  ✅ COMP_002 - Account Code Completeness: PASSED (15 failures)
  ✅ UNIQ_001 - ISIN Uniqueness: PASSED (0 failures)
  ✅ VALID_001 - Asset Class Validity: PASSED (250 failures)
  ✅ THRESH_001 - Trade Amount Threshold: PASSED (500 failures)
  ✅ RI_001 - Referential Integrity: PASSED (0 failures)

Results persisted to: CONTROLS.DQ_EXECUTION_RESULTS

⚖️ RECONCILIATION ENGINE
---
Executing 3 reconciliation controls...
  ✅ ROW_001 - Raw vs Curated Row Count: PASSED (0 variance)
  ✅ SUM_001 - Raw vs Curated Amount Sum: PASSED ($0 variance)
  ✅ RI_001 - Account Referential Integrity: PASSED (0 orphaned)

Results persisted to: CONTROLS.RECONCILIATION_RESULTS

📝 AUDIT TRAIL
---
RUN_ID: 550e8400-e29b-41d4-a716-446655440000
Execution Time: 2025-01-09 10:45:23.456
Duration: 45.2 seconds
    DQ Rules: 6/6 passed
    Recon Controls: 3/3 passed

================================================================================
✅ RCA PoC Orchestration Complete
================================================================================
```

### Step 3.3: What Just Happened

The orchestration script:

1. **Connected to Snowflake** using credentials in `config/snowflake_config.py`
2. **Fetched raw data** from `RAW` schema
3. **Executed 6 DQ rules:**
   - Completeness checks (nulls, blanks)
   - Uniqueness validation
   - Validity checks (valid values)
   - Threshold checks (amount ranges)
   - Referential integrity
4. **Executed 3 reconciliation controls:**
   - Row count validation (source vs target)
   - Amount sum validation
   - Referential integrity between tables
5. **Persisted results** to `CONTROLS` schema tables:
   - `CONTROLS.DQ_EXECUTION_RESULTS`
   - `CONTROLS.RECONCILIATION_RESULTS`
6. **Logged audit trail** for traceability

✅ **Phase 3 Complete!** Backend has executed and results are in Snowflake.

---

## 📊 Phase 4: Verify Backend Results in Snowflake (5 minutes)

### Step 4.1: Query DQ Results

**In Snowflake Worksheet, run:**
```sql
SELECT 
  RULE_ID,
  RULE_NAME,
  TABLE_NAME,
  RECORDS_TESTED,
  RECORDS_FAILED,
  PASSED,
  EXECUTED_AT
FROM RCA_POC_DB.CONTROLS.DQ_EXECUTION_RESULTS
ORDER BY EXECUTED_AT DESC
LIMIT 10;
```

**Expected result:** 6 rows with DQ rule execution details.

### Step 4.2: Query Reconciliation Results

```sql
SELECT 
  CONTROL_ID,
  CONTROL_NAME,
  SOURCE_COUNT,
  TARGET_COUNT,
  VARIANCE,
  PASSED,
  EXECUTED_AT
FROM RCA_POC_DB.CONTROLS.RECONCILIATION_RESULTS
ORDER BY EXECUTED_AT DESC
LIMIT 10;
```

**Expected result:** 3 rows showing reconciliation control results.

### Step 4.3: Query DYD Metadata

```sql
SELECT * FROM RCA_POC_DB.CONTROLS.DYD_MAPPINGS LIMIT 5;
SELECT * FROM RCA_POC_DB.CONTROLS.DYD_METADATA LIMIT 5;
```

✅ **Phase 4 Complete!** Backend results are persisted in Snowflake.

---

## 🎨 Phase 5: Start Streamlit Dashboard (5 minutes)

### Step 5.1: Navigate to Streamlit App

**Ensure you're in the python/streamlit_app directory:**
```powershell
cd python/streamlit_app
```

### Step 5.2: Start the Streamlit Server

```powershell
streamlit run app.py
```

### Step 5.3: Monitor Startup

**Expected Output:**
```
  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.x.x:8501
  Debugger: http://localhost:8501?logger.level=debug
```

### Step 5.4: Open Dashboard in Browser

1. A browser window should open automatically at `http://localhost:8501`
2. If not, manually navigate to that URL
3. You should see the **RCA PoC Controls Cockpit** dashboard

### Step 5.5: Dashboard Overview

**Sidebar Navigation Options:**
- **Overview** - Status summary + quality/reconciliation metrics
- **Data Quality** - Detailed DQ rule results
- **Reconciliation** - Control results and tolerance analysis
- **Audit Trail** - Execution history and logs
- **DYD Integration** - Discover Your Data status
- **Settings** - Configuration options

**Live Metrics on Overview:**
```
┌─────────────────────────────────────────────────┐
│ Data Quality Rules: 6/6                         │
│ Reconciliation Controls: 3/3                    │
│ Data Ready for Reporting: ✅ YES                │
│ Last Run: Live (April 9, 2026 14:35:22)       │
└─────────────────────────────────────────────────┘
```

✅ **Phase 5 Complete!** Dashboard is live and displaying data.

---

## 🧪 Phase 6: Interactive Testing (10 minutes)

### Test 6.1: View Data Quality Details

1. Click **Data Quality** in sidebar
2. Expander opens showing all 6 DQ rules
3. Columns show: Rule ID, Rule Name, Table, Records Tested, Failed, Failure %, Status, Execution Time
4. Scroll to see trend chart (Failure Rate Trend - Last 7 Days)

**Try This:**
- Hover over table cells
- Download data using... (coming in next step)

### Test 6.2: View Reconciliation Controls

1. Click **Reconciliation** in sidebar
2. View all 3 reconciliation controls
3. Columns show: Control ID, Control Name, Type, Source Count, Target Count, Variance, Variance %, Status, Execution Time

**Statuses:**
- ✅ PASS = Variance within tolerance
- ⚠️ REVIEW = Manually review before promoting
- ❌ FAIL = Data quality issue - do not promote

### Test 6.3: Export Audit Trail

1. Click **Audit Trail** in sidebar
2. View execution history (DQ + reconciliation runs)
3. Click **Download Audit Trail (CSV)** button
4. CSV file downloads with filename: `audit_trail_20250109_143522.csv`

### Test 6.4: View DYD Integration Status

1. Click **DYD Integration** in sidebar
2. View live metrics:
   - DYD Mappings: X items loaded
   - DYD Metadata: Y items registered
   - Dynamic Tables: Z tables created
   - DQ Rules Generated: N rules auto-built from metadata

### Test 6.5: Test Refresh Controls

1. On **Overview** tab, click **🔄 Refresh All Metrics** button
2. Dashboard re-queries Snowflake
3. Metrics update in real-time

---

## 🔄 Phase 7: Run End-to-End Flow Automation (Optional, 15 minutes)

### Scenario: Simulate Daily Schedule

**Step 1: Inject new data into RAW**

In Snowflake Worksheet:
```sql
-- Add 100 new trades
INSERT INTO RCA_POC_DB.RAW.FCT_TRADE_RAW
SELECT 
  NOW() as TRADE_DATE,
  UNIFORM(1, 10000, RANDOM()) as PARTY_ID,
  UNIFORM(1, 30000, RANDOM()) as ACCOUNT_ID,
  UNIFORM(1, 50000, RANDOM()) as INSTRUMENT_ID,
  UNIFORM(100, 1000000, RANDOM()) as TRADE_AMOUNT,
  UNIFORM(1, 100, RANDOM()) as QUANTITY,
  CURRENT_TIMESTAMP() as CREATED_AT,
  CURRENT_TIMESTAMP() as UPDATED_AT
FROM TABLE(GENERATOR(ROWCOUNT => 100));
```

**Step 2: Re-run orchestration**

Back in PowerShell:
```powershell
python orchestrate.py
```

**Step 3: Observe dashboard updates**

Refresh browser at `http://localhost:8501` and watch:
- New execution timestamp
- Updated failure counts
- New audit trail entry

---

## ✅ Verification Checklist

Use this checklist to confirm everything is working:

- [ ] Snowflake database `RCA_POC_DB` created
- [ ] All 4 schemas exist (RAW, CURATED, ANALYTICS, CONTROLS)
- [ ] All 12 tables created (4 RAW + 4 CURATED + 2 ANALYTICS + 2 CONTROLS)
- [ ] Python virtual environment activated
- [ ] All dependencies installed (pip list shows 20+ packages)
- [ ] Orchestration ran successfully (no error messages)
- [ ] DQ results persisted in CONTROLS.DQ_EXECUTION_RESULTS (6 rows)
- [ ] Reconciliation results persisted in CONTROLS.RECONCILIATION_RESULTS (3 rows)
- [ ] Streamlit dashboard opens at http://localhost:8501
- [ ] Dashboard shows live metrics (counts > 0)
- [ ] All sidebar navigation options work
- [ ] Data Quality tab displays 6 rules with trend chart
- [ ] Reconciliation tab shows 3 controls with status badges
- [ ] Audit Trail tab shows execution history
- [ ] DYD Integration tab displays mapping/metadata counts
- [ ] Download CSV button works on Audit Trail
- [ ] Refresh button responds and updates timestamps

---

## 🐛 Troubleshooting

### Issue: "Cannot connect to Snowflake"

**Solution:**
1. Verify credentials in `python/config/snowflake_config.py`
2. Check account: `TQWSLTQ-TW60698`
3. Verify username: `RANIT532`
4. Ensure you have ACCOUNTADMIN role
5. Test manually in Snowflake Web UI first

---

### Issue: "Module not found: streamlit / pandas / snowpark"

**Solution:**
```powershell
# Ensure venv is activated
cd python
.\RCA_venv\Scripts\Activate.ps1

# Reinstall all packages
pip install -r requirements.txt --force-reinstall
```

---

### Issue: "Streamlit app does not show live data"

**Solution:**
1. Check that orchestration ran successfully (Step 3)
2. Verify CONTROLS tables have data:
   ```sql
   SELECT COUNT(*) FROM RCA_POC_DB.CONTROLS.DQ_EXECUTION_RESULTS;
   ```
3. If count = 0, run orchestration again
4. Refresh browser page

---

### Issue: "Port 8501 already in use"

**Solution:**
```powershell
# Kill existing Streamlit process
Get-Process streamlit | Stop-Process -Force

# Or use alternate port
streamlit run app.py --server.port 8502
```

---

## 🎓 What Each Component Does

### Orchestrate.py
- **Purpose:** Main orchestration script
- **Runs:** DQ engine + Reconciliation engine
- **Output:** Persists results to CONTROLS schema
- **Frequency:** Manual (can be scheduled in production)

### Session Manager
- **Purpose:** Manages Snowflake connection lifecycle
- **Handles:** Authentication, session creation/cleanup
- **Used by:** All backend modules

### Quality Engine
- **Purpose:** Executes data quality rules
- **Rule Types:** Completeness, Uniqueness, Validity, Threshold, Referential Integrity
- **Output:** DQ_EXECUTION_RESULTS table

### Reconciliation Engine
- **Purpose:** Compares source vs target datasets
- **Controls:** Row count, amount sum, referential integrity
- **Output:** RECONCILIATION_RESULTS table

### DYD Integration
- **Purpose:** Consumes DYD mapping exports
- **Maps:** Legacy system attributes → Snowflake columns
- **Auto-generates:** Quality rules from metadata

### Streamlit Dashboard
- **Purpose:** Real-time controls cockpit
- **Backend:** Queries live CONTROLS tables
- **Fallback:** Shows sample data if live data unavailable

---

## 🎉 You're Done!

Congratulations! You have successfully:

✅ Set up a Snowflake database with 12 tables
✅ Ran a Python-based data quality & reconciliation engine
✅ Persisted results to control tables
✅ Launched an interactive Streamlit dashboard
✅ Viewed live metrics and audit trails
✅ Tested end-to-end data flow

---

## 📚 Next Steps

1. **Schedule orchestration:** Set up Snowflake Task or external scheduler (cron/Windows Task)
2. **Customize DQ rules:** Edit `python/src/snowpark_dq/quality_rules.py` for your assets
3. **Add more reconciliation controls:** Update `python/src/controls/reconciliation_rules.py`
4. **Integrate with DYD:** Import mappings via DYD_INTEGRATION_GUIDE.md
5. **Extend dashboard:** Add charts, filters, exports per requirements
6. **Deploy to Production:** Use secure credential management and CI/CD pipeline

---

## 📞 Support & Documentation

- **README.md** - Project overview and capabilities
- **QUICK_START.md** - 5-minute abbreviated guide
- **DYD_INTEGRATION_GUIDE.md** - DYD-specific integration steps
- **python/README.md** - Technical architecture deep-dive
- **Snowflake Docs:** [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about.html), [Data Metric Functions](https://docs.snowflake.com/en/user-guide/data-metric-functions.html)
- **Streamlit Docs:** [streamlit.io](https://docs.streamlit.io)

---

**Last Updated:** April 9, 2026  
**Version:** 1.0.0  
**Status:** ✅ Production Ready
