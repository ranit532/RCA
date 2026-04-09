# RCA PoC - Architecture & API Documentation

## Overview
Complete technical architecture for **Asset Data Modernisation on Snowflake** including data flow, API calls, entity relationships, and swim lanes.

---

## Architecture Components

### SWIM LANE 1: DATA INGESTION & RAW LAYER

#### Components:
1. **External Data Sources** → Synthetic data generation
2. **Synthetic Data Generator** (FCT_TRADE_RAW)
   - Generates 30,000 account IDs and 50,000 instrument IDs
   - Creates trade facts with random combinations
   
3. **Dimension Tables**
   - `DIM_ACCOUNT_RAW`: Account master data
   - `DIM_PARTY_RAW`: Party/counterparty information
   - `DIM_INSTRUMENT_RAW`: Product/instrument catalog

4. **Streaming Ingestion** (SP_GENERATE_STREAMING_TRADES)
   - Scheduled procedure for continuous data updates
   - Real-time trade data simulation

#### Database Objects:
```sql
-- Raw Schema Tables
RCA_POC_DB.RAW.FCT_TRADE_RAW
RCA_POC_DB.RAW.DIM_ACCOUNT_RAW
RCA_POC_DB.RAW.DIM_PARTY_RAW
RCA_POC_DB.RAW.DIM_INSTRUMENT_RAW
```

---

### SWIM LANE 2: DATA QUALITY, TRANSFORMATIONS & CURATED LAYER

#### Components:

##### 2.1 Data Quality Engine
**Module**: `src/snowpark_dq/quality_engine.py`

**Class**: `DataQualityEngine`

**Methods**:
```python
def execute_rule(rule: DQRule) -> DQResult
    - Executes single DQ rule
    - API: Direct SQL COUNT(*) queries
    - Returns: DQResult with pass/fail status

def execute_rules(rules: List[DQRule]) -> List[DQResult]
    - Batch rule execution
    - Iterates through quality rule set

def persist_results(schema: str = "CONTROLS") -> bool
    - Saves DQ results to Snowflake
    - Table: CONTROLS.DQ_EXECUTION_RESULTS
```

**Quality Rules Executed**:
- `COMPLETENESS`: Non-null field validation
- `UNIQUENESS`: Primary key uniqueness checks
- `VALIDITY`: Data format and range validation
- `REFERENTIAL_INTEGRITY`: Foreign key checks

**SQL API Calls**:
```sql
-- Rule Execution
SELECT COUNT(*) as cnt FROM {table_name}
SELECT COUNT(*) as cnt FROM {table_name} WHERE NOT ({rule_logic})

-- Persistence
INSERT INTO CONTROLS.DQ_EXECUTION_RESULTS 
(RUN_ID, RULE_ID, RULE_NAME, TABLE_NAME, RULE_TYPE, ...)
VALUES (?, ?, ?, ?, ?, ...)
```

##### 2.2 Dynamic Tables (Curated Layer)
**Module**: `sql/06_dynamic_tables.sql`

**Tables Created**:
```sql
-- Dimension Tables (Curated)
RCA_POC_DB.CURATED.DT_DIM_PARTY_CURATED
RCA_POC_DB.CURATED.DT_DIM_ACCOUNT_CURATED
RCA_POC_DB.CURATED.DT_DIM_INSTRUMENT_CURATED

-- Fact Tables (Curated)
RCA_POC_DB.CURATED.DT_FCT_TRADE_CURATED
```

**Refresh Strategy**: 15-minute TARGET_LAG (declarative refresh)

##### 2.3 Data Quality Metrics Logging
**Module**: `sql/07_data_metric_functions.sql`

**Procedure**: `SP_LOG_DQ_METRICS`

**SQL API Calls**:
```sql
-- Log Completeness Metric
INSERT INTO CONTROLS.DQ_METRICS_LOG 
(METRIC_ID, METRIC_NAME, TABLE_NAME, COMPLETENESS_PERCENTAGE, ...)
SELECT 
  UUID_STRING() as METRIC_ID,
  'COMPLETENESS' as METRIC_NAME,
  'RAW.FCT_TRADE_RAW' as TABLE_NAME,
  COUNT(1) - COUNT(NULL_COLUMN) / COUNT(1) * 100 as COMPLETENESS_PERCENTAGE

-- Reconciliation Metrics
INSERT INTO CONTROLS.DQ_METRICS_LOG 
(METRIC_ID, METRIC_NAME, SOURCE_COUNT, TARGET_COUNT, VARIANCE, ...)
SELECT 
  (SELECT COUNT(*) FROM RAW.FCT_TRADE_RAW) as SOURCE_COUNT,
  (SELECT COUNT(*) FROM CURATED.DT_FCT_TRADE_CURATED) as TARGET_COUNT
```

**Table**: `CONTROLS.DQ_METRICS_LOG`

---

### SWIM LANE 3: RECONCILIATION & CONTROLS

#### Components:

##### 3.1 Reconciliation Engine
**Module**: `src/controls/reconciliation_engine.py`

**Class**: `ReconciliationEngine`

**Methods**:
```python
def execute_row_count_recon(control: ReconciliationControl) -> ReconciliationResult
    - Compares row counts between source and target
    - API: SELECT COUNT(*) queries from dual tables
    - Tolerance: ABSOLUTE or PERCENTAGE
    - Returns: Variance details

def execute_sum_recon(control: ReconciliationControl) -> ReconciliationResult
    - Reconciles sum of numeric columns
    - API: SELECT COALESCE(SUM(...), 0) queries
    - Supports amount reconciliation

def execute_controls(controls: List[ReconciliationControl]) -> List[ReconciliationResult]
    - Batch reconciliation execution

def persist_results(schema: str = "CONTROLS") -> bool
    - Saves reconciliation results
    - Table: CONTROLS.RECONCILIATION_RESULTS
```

**Reconciliation Types**:
- `ROW_COUNT`: Source vs target record counts
- `SUM_AMOUNT`: Amount reconciliation
- `HASH_MATCH`: Record-level matching (extensible)
- `KEY_MATCH`: Primary key coverage validation

**SQL API Calls**:
```sql
-- Row Count Reconciliation
SELECT COUNT(*) as cnt FROM RAW.FCT_TRADE_RAW
SELECT COUNT(*) as cnt FROM CURATED.DT_FCT_TRADE_CURATED

VARIANCE = ABS(source_count - target_count)
status = PASS if (variance <= tolerance) else FAIL

-- Sum Reconciliation
SELECT COALESCE(SUM(TRADE_AMOUNT), 0) FROM RAW.FCT_TRADE_RAW
SELECT COALESCE(SUM(TRADE_AMOUNT), 0) FROM CURATED.DT_FCT_TRADE_CURATED

-- Persistence
INSERT INTO CONTROLS.RECONCILIATION_RESULTS 
(RUN_ID, CONTROL_ID, CONTROL_NAME, SOURCE_COUNT, TARGET_COUNT, VARIANCE, ...)
VALUES (?, ?, ?, ?, ?, ?, ...)
```

**Controls Table**: `CONTROLS.RECONCILIATION_RESULTS`

---

### SWIM LANE 4: ANALYTICS LAYER & REPORTING

#### Components:

##### 4.1 Analytics Schema
**Tables**: Derived fact and dimension views in ANALYTICS schema
- Optimized for reporting queries
- Aggregated metrics and KPIs

##### 4.2 Streamlit Backend
**Module**: `src/streamlit_backend.py`

**Functions**:
```python
def get_dq_results() -> pd.DataFrame
    - Retrieves latest DQ execution results
    - API: SELECT from CONTROLS.DQ_EXECUTION_RESULTS
    - Filters: Last 24 hours, by rule type, pass/fail status

def get_reconciliation_status() -> pd.DataFrame
    - Retrieves reconciliation control results
    - API: SELECT from CONTROLS.RECONCILIATION_RESULTS
    - Shows: Passed/failed controls, variance details

def get_live_metrics() -> Dict[str, Any]
    - Real-time metric dashboard data
    - API: Multiple SELECT queries for aggregates
```

**SQL API Calls**:
```sql
-- DQ Results Retrieval
SELECT * FROM CONTROLS.DQ_EXECUTION_RESULTS 
WHERE EXECUTED_AT > NOW() - INTERVAL '24 HOURS'
ORDER BY EXECUTED_AT DESC

-- Reconciliation Status
SELECT * FROM CONTROLS.RECONCILIATION_RESULTS 
WHERE EXECUTED_AT > NOW() - INTERVAL '24 HOURS'
AND PASSED = FALSE

-- Live Metrics Aggregation
SELECT 
  COUNT(*) as total_rules,
  SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) as passed,
  SUM(CASE WHEN PASSED THEN 0 ELSE 1 END) as failed,
  AVG(FAILURE_RATE) as avg_failure_rate
FROM CONTROLS.DQ_EXECUTION_RESULTS
```

##### 4.3 Streamlit Dashboard (Web UI)
**Module**: `streamlit_app/app.py`

**Pages/Components**:
- **Controls Cockpit**: Live DQ and reconciliation status
- **Historical Metrics**: Trend analysis over time
- **Data Quality Dashboard**: Detailed rule results
- **Reconciliation Report**: Control variance analysis
- **System Health**: Pipeline execution logs

##### 4.4 DYD Integration Module
**Module**: `src/dyd_integration/dyd_integration.py`

**Class**: `DYDIntegration`

**Methods**:
```python
def discover_entities() -> List[DYDMapping]
    - Discover data entities from Snowflake
    - API: REST GET to DYD backend
    - Returns: Entity mappings and metadata

def validate_mappings(mappings: List[DYDMapping]) -> bool
    - Validates discovered mappings
    - API: REST POST to DYD backend for validation

def push_metrics(metrics: Dict) -> bool
    - Sends captured metrics to DYD backend
```

**DYD Backend API Calls**:
```
GET /discover
  - Query params: account, database, schema
  - Response: { entities: [...], metadata: {...} }

GET /mappings
  - Query params: entity_id, discovery_run_id
  - Response: { mappings: [...], confidence: [...] }

POST /validate
  - Body: { mapping_id, source_table, target_table, rules: [...] }
  - Response: { valid: bool, issues: [...] }

POST /metrics
  - Body: { run_id, dq_results, reconciliation_results }
  - Response: { status: "accepted" }
```

---

### SWIM LANE 5: ORCHESTRATION & SCHEDULING

#### Components:

##### 5.1 Main Orchestrator
**Module**: `orchestrate.py`

**Execution Flow**:
```
Phase 1: Setup Environment
  └─ Initialize Snowflake session
  └─ Load configuration
  └─ Log session details

Phase 2: Data Quality Validation
  └─ Execute quality engine
  └─ Run all DQ rules
  └─ Persist DQ results to CONTROLS schema

Phase 3: Reconciliation Controls
  └─ Execute reconciliation engine
  └─ Run row count, sum, and match controls
  └─ Persist reconciliation results

Phase 4: Reporting & Insights
  └─ Generate execution summary
  └─ Log metrics to analytics schema
  └─ Report readiness for business consumption

Phase 5: Error Handling & Cleanup
  └─ Capture any exceptions
  └─ Log errors with context
  └─ Close Snowflake session
```

**Main Function Calls**:
```python
def setup_environment() -> (Session, SnowflakeConfig)
    - Initializes Snowflake session
    - API: snowflake.connector.connect()

def run_data_quality_checks(session) -> List[DQResult]
    - Executes quality engine
    - Calls: DataQualityEngine.execute_rules()

def run_reconciliation_controls(session) -> List[ReconciliationResult]
    - Executes reconciliation engine
    - Calls: ReconciliationEngine.execute_controls()

def report_summary() -> Dict
    - Generates execution summary
    - Returns: Pass/fail stats and readiness status
```

---

## Entity-Relationship Diagram (ER)

### Core Tables

```
RAW Schema:
├─ FCT_TRADE_RAW (Fact)
│  ├─ TRADE_ID (PK)
│  ├─ ACCOUNT_ID (FK → DIM_ACCOUNT_RAW)
│  ├─ PARTY_ID (FK → DIM_PARTY_RAW)
│  ├─ INSTRUMENT_ID (FK → DIM_INSTRUMENT_RAW)
│  ├─ TRADE_AMOUNT
│  ├─ TRADE_DATE
│  └─ CREATED_AT
├─ DIM_ACCOUNT_RAW (Dimension)
├─ DIM_PARTY_RAW (Dimension)
└─ DIM_INSTRUMENT_RAW (Dimension)

CURATED Schema (Dynamic Tables):
├─ DT_DIM_PARTY_CURATED
├─ DT_DIM_ACCOUNT_CURATED
├─ DT_DIM_INSTRUMENT_CURATED
└─ DT_FCT_TRADE_CURATED

CONTROLS Schema:
├─ DQ_METRICS_LOG
│  ├─ METRIC_ID (PK)
│  ├─ RUN_ID (FK)
│  ├─ METRIC_NAME
│  ├─ TABLE_NAME
│  ├─ RECORDS_TESTED
│  ├─ COMPLETENESS_PERCENTAGE
│  └─ EXECUTED_AT
└─ RECONCILIATION_RESULTS
   ├─ RUN_ID (PK)
   ├─ CONTROL_ID (PK)
   ├─ SOURCE_COUNT
   ├─ TARGET_COUNT
   ├─ VARIANCE
   ├─ PASSED (BOOLEAN)
   └─ EXECUTED_AT

ANALYTICS Schema:
└─ [Aggregated views for reporting]
```

---

## Configuration & Environment

### Environment Variables (.env)
```
SNOWFLAKE_ACCOUNT=TQWSLTQ-TW60698
SNOWFLAKE_USER=RANIT532
SNOWFLAKE_PASSWORD=***
SNOWFLAKE_AUTHENTICATOR=username_password_mfa
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=RCA_ANALYTICS_WH
SNOWFLAKE_DATABASE=RCA_POC_DB
```

### Default Configuration
**Location**: `config/snowflake_config.py`
- Loads from .env or uses defaults
- Falls back to hardcoded values if not set

---

## Testing Strategy

### Phase 1: Unit Testing
```bash
# Test individual components
python -m pytest tests/test_quality_engine.py
python -m pytest tests/test_reconciliation_engine.py
```

### Phase 2: Integration Testing
```bash
# Test full orchestration
cd python && .\venv\Scripts\python orchestrate.py
```

### Phase 3: Dashboard Testing
```bash
# Launch Streamlit dashboard
cd streamlit_app && streamlit run app.py
# Access at http://localhost:8501
```

### Phase 4: DYD Integration Testing
```bash
# Test DYD backend integration
python examples/dyd_backend_integration.py
```

---

## Monitoring & Observability

### Logging
- All logs output to console with timestamps
- Log level: INFO (configurable to DEBUG)
- Componennts: Session manager, Quality engine, Reconciliation engine

### Metrics Collection
- DQ execution metrics → CONTROLS.DQ_METRICS_LOG
- Reconciliation variance → CONTROLS.RECONCILIATION_RESULTS
- Execution time tracking in all result records

### Alerting (Extensible)
- Threshold-based alerts (e.g., failure rate > 5%)
- Failed control notifications
- Variance alerts for reconciliation

---

## Performance Considerations

1. **Dynamic Tables**: 15-minute refresh interval
2. **Batch DQ Execution**: ~30-50ms per rule
3. **Reconciliation**: ~100-200ms per control
4. **Result Persistence**: Bulk INSERT operations
5. **Dashboard Queries**: Cached for 5-minute TTL

---

## Security & Best Practices

✅ Credentials in .env (not in version control)  
✅ Role-based access in Snowflake (ACCOUNTADMIN)  
✅ Session cleanup on completion  
✅ Error logging without sensitive data exposure  
✅ Parameterized queries to prevent SQL injection  

---

## Deployment Checklist

- [ ] SQL scripts executed (01-07)
- [ ] Python environment set up
- [ ] .env file configured with credentials
- [ ] Dependencies installed
- [ ] Orchestration tested (--dry-run)
- [ ] Dashboard verified
- [ ] DYD integration tested (optional)
- [ ] Monitoring configured
- [ ] Documentation reviewed

