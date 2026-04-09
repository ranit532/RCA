"""RCA PoC - Comprehensive README"""

# 🎯 RCA Proof of Concept - Asset Data Modernisation on Snowflake

**Purpose:** Technology demonstration showcasing Snowflake-native engineering capabilities for asset data quality, proactive reconciliation, and migration enablement.

**Status:** ✅ Production-ready PoC framework (can be adapted for specific environments)

---

## 📋 Project Overview

This RCA (Root Cause Analysis) PoC demonstrates a complete solution for:

1. **Source Data Quality Validation** - Automated profiling and quality rule execution
2. **Proactive Reconciliation Controls** - Embedded row count and amount reconciliation with tolerance handling
3. **DYD Integration** - Consumption of Discover Your Data tool outputs for mapping and metadata
4. **Streamlit Controls Cockpit** - Real-time dashboard showing validation and reconciliation outcomes
5. **Native Snowflake Services** - Dynamic Tables, Data Metric Functions, and declarative transformations

---

## 🏗️ Architecture

### Directory Structure

```
RCA/
├── sql/
│   ├── 01_setup.sql                    # Database, schemas, warehouses
│   ├── 02_synthetic_dimensions.sql     # Dimension tables (PARTY, ACCOUNT, INSTRUMENT)
│   ├── 03_synthetic_facts_and_streaming.sql  # Fact tables and streaming
│   ├── 04_curated_model.sql            # [Existing]
│   ├── 05_analytics_views.sql          # [Existing]
│   ├── 06_dynamic_tables.sql           # Dynamic Tables (curated & analytics)
│   └── 07_data_metric_functions.sql    # DMFs for reusable quality metrics
│
└── python/
    ├── requirements.txt                # Python dependencies
    ├── orchestrate.py                  # Main orchestration script
    │
    ├── config/
    │   ├── __init__.py
    │   └── snowflake_config.py        # Snowflake connection configuration
    │
    ├── src/
    │   ├── snowpark_dq/
    │   │   ├── __init__.py
    │   │   ├── session_manager.py     # Snowpark session lifecycle
    │   │   ├── quality_rules.py       # DQ rule definitions
    │   │   └── quality_engine.py      # DQ execution engine
    │   │
    │   ├── controls/
    │   │   ├── __init__.py
    │   │   └── reconciliation_engine.py  # Reconciliation controls
    │   │
    │   └── dyd_integration/
    │       ├── __init__.py
    │       └── dyd_integration.py     # DYD mapping & metadata integration
    │
    └── streamlit_app/
        └── app.py                     # Controls cockpit dashboard
```

---

## 🔧 Setup & Configuration

### Prerequisites

- Python 3.9+
- Snowflake account with ACCOUNTADMIN role
- Streamlit
- Snowpark for Python

### Installation

1. **Clone the repository**
   ```bash
   cd "c:\Users\Ranit.Sinha\Documents\Snowflake\RCA POC\RCA"
   ```

2. **Create Python environment**
   ```bash
   # Using venv
   python -m venv venv
   .\venv\Scripts\activate
   
   # Or using conda
   conda create -n rca_poc python=3.9
   conda activate rca_poc
   ```

3. **Install dependencies**
   ```bash
   pip install -r python/requirements.txt
   ```

### Configuration

1. **Update Snowflake credentials** in `python/config/snowflake_config.py`:
   ```python
   SnowflakeConfig(
       account="TQWSLTQ-TW60698",     # Your account
       user="RANIT532",                # Your username
       authenticator="externalbrowser",
       role="ACCOUNTADMIN",
       warehouse="RCA_ANALYTICS_WH",
       database="RCA_POC_DB"
   )
   ```

2. **Set environment variables** (optional):
   ```bash
   # .env file
   SNOWFLAKE_ACCOUNT=TQWSLTQ-TW60698
   SNOWFLAKE_USER=RANIT532
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   SNOWFLAKE_WAREHOUSE=RCA_ANALYTICS_WH
   ```

---

## 🚀 Running the PoC

### Phase 1: Database Setup

Execute SQL scripts in order:

```bash
# Connect to Snowflake using your client
# Run in sequence:
-- sql/01_setup.sql
-- sql/02_synthetic_dimensions.sql
-- sql/03_synthetic_facts_and_streaming.sql
-- sql/06_dynamic_tables.sql
-- sql/07_data_metric_functions.sql
```

**What gets created:**
- `RCA_POC_DB` database with RAW, CURATED, ANALYTICS, CONTROLS schemas
- Synthetic asset data (parties, accounts, instruments, trades)
- Dynamic Tables for curated and analytics layers
- Data Metric Functions for reusable quality metrics

### Phase 2: Data Quality & Reconciliation Orchestration

```bash
cd python
python orchestrate.py
```

**This executes:**
1. **Data Quality Validation** - 6 quality rules on asset data
2. **Reconciliation Controls** - 3-6 row count and amount reconciliations
3. **DYD Integration** - Reference table creation for mappings & metadata
4. **Results Persistence** - All outcomes logged to CONTROLS schema
5. **Execution Report** - Summary and readiness assessment

**Output:**
```
================================================================================
RCA PoC - Orchestration Starting
================================================================================

Connected to Snowflake account: TQWSLTQ-TW60698
Database: RCA_POC_DB
Warehouse: RCA_ANALYTICS_WH

================================================================================
PHASE 1: DATA QUALITY VALIDATION
================================================================================
Executing 6 quality rules...
Rule COMP_001 executed: 5/10000 failed (0.05%) - PASSED
Rule COMP_002 executed: 15/30000 failed (0.05%) - PASSED
...

Data Quality Execution Summary:
  Run ID: 550e8400-e29b-41d4-a716-446655440000
  Total Rules: 6
  Passed: 6
  Failed: 0
  Status: SUCCESS

✅ DQ results persisted to CONTROLS.DQ_EXECUTION_RESULTS

================================================================================
PHASE 2: PROACTIVE RECONCILIATION
================================================================================
Executing 3 reconciliation controls...
Control ROW_001 executed: PASSED (Variance: 0.00%)
Control ROW_002 executed: PASSED (Variance: 0.50%)
Control SUM_001 executed: PASSED (Variance: 0.02%)

Reconciliation Execution Summary:
  Run ID: 550e8400-e29b-41d4-a716-446655440001
  Total Controls: 3
  Passed: 3
  Failed: 0
  Readiness: YES - READY FOR REPORTING
```

### Phase 3: Interactive Dashboard

Launch the Streamlit controls cockpit:

```bash
cd streamlit_app
streamlit run app.py
```

**Access:** Open browser to `http://localhost:8501`

**Features:**
- 📊 Real-time data quality metrics
- ⚖️ Reconciliation control status
- 📝 Audit trail with execution history
- 🔗 DYD integration status
- 📤 Export capabilities

---

## 📐 Key Components

### 1. Data Quality Engine (`src/snowpark_dq/quality_engine.py`)

Executes parameterized quality rules against datasets:

**Rule Types:**
- `COMPLETENESS` - NULL checks
- `VALIDITY` - Format/domain validation  
- `UNIQUENESS` - Duplicate detection
- `CONSISTENCY` - Business logic checks
- `ACCURACY` - Reference data matching
- `TIMELINESS` - Freshness/SLA checks
- `THRESHOLD` - Range/threshold validation

**Example:**
```python
from src.snowpark_dq import DataQualityEngine, ASSET_DATA_QUALITY_RULES

session = SnowparkSessionManager.get_session()
dq_engine = DataQualityEngine(session)

# Execute all asset data quality rules
results = dq_engine.execute_rules(ASSET_DATA_QUALITY_RULES)

# Persist results to Snowflake
dq_engine.persist_results(schema="CONTROLS")

# Get summary
summary = dq_engine.get_summary()
print(f"Rules Passed: {summary['passed_rules']}/{summary['total_rules']}")
```

### 2. Reconciliation Engine (`src/controls/reconciliation_engine.py`)

Implements embedded reconciliation controls with tolerance handling:

**Control Types:**
- `ROW_COUNT` - Source vs target row counts
- `SUM_AMOUNT` - Sum of amounts reconciliation
- `HASH_MATCH` - Record-level matching (future)
- `KEY_MATCH` - Primary key coverage (future)
- `COMPLETENESS` - Required field coverage (future)

**Example:**
```python
from src.controls import ReconciliationEngine, ReconciliationControl, ReconciliationType

session = SnowparkSessionManager.get_session()
recon_engine = ReconciliationEngine(session)

control = ReconciliationControl(
    control_id="ROW_001",
    control_name="Raw vs Curated Row Count",
    reconciliation_type=ReconciliationType.ROW_COUNT,
    source_table="RAW.DIM_PARTY_RAW",
    target_table="CURATED.DT_DIM_PARTY_CURATED",
    tolerance_type="ABSOLUTE",
    tolerance_value=0,  # No variance allowed
)

result = recon_engine.execute_control(control)
print(f"Status: {'PASSED' if result.passed else 'FAILED'}")
print(f"Readiness: {recon_engine.get_summary()['readiness_for_reporting']}")
```

### 3. DYD Integration (`src/dyd_integration/dyd_integration.py`)

Consumes Discover Your Data outputs for mapping and metadata:

**Features:**
- Load DYD mappings from JSON exports
- Load DYD metadata (entity, column, business terms)
- Create reference tables in Snowflake
- Generate DQ rules from metadata
- Track traceability from DYD → Snowflake → Controls

**Example:**
```python
from src.dyd_integration import DYDIntegration

session = SnowparkSessionManager.get_session()
dyd = DYDIntegration(session)

# Load mappings from DYD export
dyd.load_mappings_from_json("dyd_mappings.json")
dyd.load_metadata_from_json("dyd_metadata.json")

# Create reference tables in Snowflake
dyd.create_mapping_reference_table(schema="CONTROLS")
dyd.create_metadata_reference_table(schema="CONTROLS")

# Generate DQ rules from metadata
rules = dyd.generate_dq_rules_from_metadata()
```

### 4. Dynamic Tables (`sql/06_dynamic_tables.sql`)

Declared transformations with automatic refresh:

```sql
-- Curated dimension with data quality enrichment
CREATE OR REPLACE DYNAMIC TABLE DT_DIM_PARTY_CURATED
TARGET_LAG = '1 hour'
AS
SELECT
  PARTY_ID,
  PARTY_CODE,
  PARTY_NAME,
  ...
  IFF(PARTY_TYPE IN ('INSTITUTIONAL', 'RETAIL'), TRUE, FALSE) AS IS_VALID_PARTY_TYPE
FROM RAW.DIM_PARTY_RAW;

-- Curated fact table with referential integrity checks
CREATE OR REPLACE DYNAMIC TABLE DT_FCT_TRADE_CURATED
TARGET_LAG = '15 minutes'
AS
SELECT
  t.*,
  ...
  IFF(t.ACCOUNT_ID IS NOT NULL AND a.ACCOUNT_ID IS NOT NULL, TRUE, FALSE) AS ACCOUNT_EXISTS
FROM RAW.FCT_TRADE_RAW t
LEFT JOIN DT_DIM_ACCOUNT_CURATED a ...;
```

### 5. Data Metric Functions (`sql/07_data_metric_functions.sql`)

Reusable quality and reconciliation metrics:

```sql
-- Completeness metric
CREATE FUNCTION DQ_METRIC_COMPLETENESS(table_name, column_name)
RETURNS TABLE (METRIC_NAME, METRIC_VALUE, RECORDS_TESTED, RECORDS_NULL)
...

-- Reconciliation metric
CREATE FUNCTION RECON_METRIC_ROW_COUNT(source_table, target_table)
RETURNS TABLE (SOURCE_COUNT, TARGET_COUNT, VARIANCE, VARIANCE_PCT)
...
```

---

## 📊 Data Flow & Results

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│               Raw Asset Data (RAW Schema)                    │
│  - DIM_PARTY_RAW, DIM_ACCOUNT_RAW, DIM_INSTRUMENT_RAW      │
│  - FCT_TRADE_RAW, FCT_TRADE_STREAM_RAW                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
        ┌─────────────────────────────────────┐
        │  Data Quality Engine                 │
        │  - Execute 6 Quality Rules          │
        │  - Test Completeness, Validity etc. │
        │  - Log results to Controls schema   │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │  Dynamic Tables (CURATED)            │
        │  - DT_DIM_PARTY_CURATED             │
        │  - DT_DIM_ACCOUNT_CURATED           │
        │  - DT_FCT_TRADE_CURATED             │
        │  - WITH quality flags                │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │  Reconciliation Engine              │
        │  - Execute 3 Controls               │
        │  - Row count, Amount reconciliation │
        │  - Determine reporting readiness    │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │  Analytics Layer (ANALYTICS)        │
        │  - DT_TRADE_DAILY_SUMMARY           │
        │  - DT_ACCOUNT_ANALYTICS             │
        │  - Aggregations & KPIs              │
        └────────┬─────────────────────────────┘
                 │
                 ▼
        ┌─────────────────────────────────────┐
        │  CONTROLS Schema                    │
        │  - DQ_EXECUTION_RESULTS             │
        │  - RECONCILIATION_RESULTS           │
        │  - DYD_MAPPINGS, DYD_METADATA       │
        │  - Audit trail & evidence           │
        └─────────────────────────────────────┘
```

### Query Controls Data

```sql
-- Check data quality results
SELECT 
  RUN_ID,
  RULE_ID, 
  RULE_NAME,
  RECORDS_TESTED,
  RECORDS_FAILED,
  FAILURE_RATE,
  PASSED
FROM CONTROLS.DQ_EXECUTION_RESULTS
ORDER BY EXECUTED_AT DESC;

-- Check reconciliation status
SELECT
  RUN_ID,
  CONTROL_ID,
  CONTROL_NAME,
  SOURCE_COUNT,
  TARGET_COUNT,
  VARIANCE,
  VARIANCE_PERCENTAGE,
  PASSED
FROM CONTROLS.RECONCILIATION_RESULTS
ORDER BY EXECUTED_AT DESC;

-- Check DYD mappings
SELECT *
FROM CONTROLS.DYD_MAPPINGS;

-- Check DYD metadata
SELECT *
FROM CONTROLS.DYD_METADATA;
```

---

## 🎯 Use Cases Demonstrated

### 1. **Source Data Quality**
- Profile incoming asset datasets
- Apply automated quality rules
- Persist trending results
- Early detection and drill-down

### 2. **Proactive Reconciliation**
- Embedded in data pipelines
- Auto-execute on data refresh
- Tolerance-aware logic
- Auditable evidence trail

### 3. **DYD Integration**
- Consume DYD mapping outputs
- Use mappings to drive Dynamic Tables
- Generate rules from metadata
- Maintain traceability

### 4. **Trend Analysis**
- Track quality metrics over time
- Identify improvement/deterioration
- Alerting on thresholds
- Historical audit trail

### 5. **Reporting Readiness**
- Determine green/amber/red status
- Surface blockers early
- Enable data-driven decision-making
- Reduce quarter-end rework

---

## 🔍 Monitoring & Observability

### Built-in Observability

1. **Data Quality Observability**
   - All rule executions logged in `CONTROLS.DQ_EXECUTION_RESULTS`
   - Failure rates and trends tracked
   - Early detection of data issues

2. **Reconciliation Observability**
   - Control execution log in `CONTROLS.RECONCILIATION_RESULTS`
   - Variance calculations and tolerance assessment
   - Readiness determination

3. **DYD Traceability**
   - Mappings registered in `CONTROLS.DYD_MAPPINGS`
   - Metadata available in `CONTROLS.DYD_METADATA`
   - Link DYD → Dynamic Tables → Controls

4. **Audit Trail**
   - All executions timestamped
   - User/system identification
   - Duration and performance metrics

### Snowflake Horizon (Optional)

For enhanced observability:
- View data lineage from source to controls
- Track data sensitivity and governance
- Monitor query performance
- Enable roles-based access on controls tables

---

## 📈 Roadmap & Future Enhancements

### Phase 2 Enhancements
- [ ] Snowflake Cortex AI for narrative generation
- [ ] Streamlit integration with LLM for interactive Q&A
- [ ] Solidatus integration for visual lineage
- [ ] Multi-account data sharing to parent account

### Phase 3 Enhancements
- [ ] Advanced reconciliation (HASH_MATCH, KEY_MATCH)
- [ ] ML-based anomaly detection
- [ ] Dynamic thresholds from historical patterns
- [ ] Native alerting via Snowflake notifications

### Beyond PoC
- [ ] Integration with Power Apps for manual data management
- [ ] ETL orchestration with Snowflake Task Dependencies
- [ ] Production data pipeline implementation
- [ ] Regulatory reporting automation

---

## 🛠️ Troubleshooting

### Common Issues

**1. Snowpark Session Connection Fails**
```
Error: Failed to create Snowpark session
Solution: Verify credentials in snowflake_config.py, check browser-based auth
```

**2. Dynamic Tables Not Refreshing**
```
Error: Dynamic table refresh timeout
Solution: Check warehouse status, increase AUTO_SUSPEND timeout, verify source data changes
```

**3. Data Metric Function Errors**
```
Error: Function not found or invalid SQL
Solution: Ensure 07_data_metric_functions.sql executed successfully, check Snowflake version compatibility
```

**4. Streamlit Dashboard Blank**
```
Error: No data displayed in dashboard
Solution: Run orchestrate.py first, check CONTROLS schema has result tables populated
```

### Debug Mode

Enable verbose logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📚 References

### Snowflake Documentation
- [Snowpark Python Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/)
- [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Data Metric Functions](https://docs.snowflake.com/en/sql-reference/data-metric-functions)
- [Snowflake Cortex AI](https://docs.snowflake.com/en/sql-reference/functions/cortex)
- [Snowflake Horizon](https://docs.snowflake.com/en/user-guide/snowflake-horizon)

### DYD Integration
- Discover Your Data documentation
- DYD API for programmatic mapping export
- Mapping format specifications

### External Tools
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Solidatus Lineage](https://www.solidatus.com/)

---

## 📝 License

This PoC framework is provided as-is for demonstration purposes.

---

## 👥 Support & Contribution

For issues, questions, or enhancements:
1. Document the issue clearly
2. Include reproduction steps
3. Provide error logs
4. Contact Snowflake Engineering Partner

---

**Last Updated:** January 9, 2025
**Version:** 1.0.0
**Status:** ✅ Ready for Deployment
