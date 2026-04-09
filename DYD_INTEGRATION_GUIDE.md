# Implementation Guide - DYD Backend System Integration

**Purpose:** Guide for integrating RCA PoC with DYD (Discover Your Data) backend services listed in the requirements.

## 🏗️ DYD Toolkit Architecture Overview

```
Frontend (Vue.js/React)
    ↓
API Gateway / Platform
    ├─ Auth Service
    ├─ Lineage Service
    ├─ STTM Service
    ├─ CoPilot Service
    ├─ Quality Service
    └─ Metadata Service
    ↓
Snowflake RCA PoC
```

## 🔗 Integration Points

### 1. **Lineage Service** (IC-DataDiscovery-Backend-Lineage)

**Purpose:** Track data lineage from legacy systems to Snowflake

**Integration:**
```python
from examples.dyd_backend_integration import DYDBackendServices

dyd = DYDBackendServices(base_url="http://dyd-api:8080")

# Get complete lineage tree
lineage = dyd.get_lineage_tree("FCT_TRADE_RAW")

# Perform impact analysis
impact = dyd.get_impact_analysis("DIM_PARTY_RAW", "ADD_COLUMN")

# Store in Snowflake for audit
dyd.trace_lineage_to_controls("FCT_TRADE_RAW", snowflake_session)
```

**Output Tables:**
- `CONTROLS.LINEAGE_EVIDENCE` - Traceability from DYD to Snowflake

### 2. **STTM Service** (IC-DataDiscovery-Backend-Sttm)

**Purpose:** Generate SQL transformations from mappings

**Integration:**
```python
# Generate transformation SQL from mapping
sttm = dyd.generate_sttm(mapping_id="MAP_TRADE_001")

# Create Dynamic Table from STTM
dynamic_table_sql = dyd.create_dynamic_table_from_mapping(
    mapping=mapping_dict,
    snowflake_session=session
)
```

**Generated Artifacts:**
- Dynamic Tables with transformation logic
- Stored in `CURATED` and `ANALYTICS` schemas

### 3. **CoPilot Service** (IC-DataDicsovery-Backend-Copilot)

**Purpose:** AI-powered recommendations for mappings and transformations

**Integration:**
```python
# Get mapping recommendations
recommendations = dyd.get_mapping_recommendations("LEGACY_SYSTEM.TRADES")

# Ask natural language questions
answer = dyd.ask_copilot("What are the key reconciliation points between legacy and Snowflake?")

# Get transformation suggestions
suggestion = dyd.get_transformation_suggestion(
    source_schema=legacy_schema,
    target_schema=snowflake_schema
)
```

**Use Cases:**
- Auto-suggest quality rules from data patterns
- Recommend reconciliation controls based on schema analysis
- Generate transformation logic for complex mappings

### 4. **Quality Service** (IC-DataDicsovery-Backend-Dataquality)

**Purpose:** Profile data and detect quality anomalies

**Integration:**
```python
# Profile dataset
profile = dyd.profile_dataset("FCT_TRADE_RAW")

# Get quality metrics
metrics = dyd.get_quality_metrics("FCT_TRADE_RAW")

# Detect anomalies
anomalies = dyd.detect_anomalies("DIM_PARTY_RAW")
```

**Connection to RCA PoC:**
- Feed `metrics` to data quality rules
- Add anomalies as additional quality checks
- Track metric trends in `CONTROLS.DQ_METRICS_LOG`

### 5. **Platform Service** (IC-DataDsicovery-Platform)

**Purpose:** Core API gateway for all DYD services

**Integration:**
```python
# Authenticate
dyd.authenticate("user@company.com", "password")

# Export all mappings
mappings = dyd.export_mapping_json()

# Export all metadata
metadata = dyd.export_metadata_json()

# Save to files for loading into Snowflake
with open("dyd_mappings.json", "w") as f:
    json.dump(mappings, f)
```

**Deployment Pattern:**
- Platform runs on Kubernetes (see K8S deployment configs)
- RCA PoC calls Platform REST APIs
- Results flow back to Snowflake

### 6. **Auth Service** (IC-DataDiscovery-Auth)

**Purpose:** Centralized authentication and authorization

**Integration:**
```python
# OAuth/OIDC authentication
dyd.authenticate(
    oauth_provider="azure",  # or 'okta', 'keycloak', etc.
    token=user_oauth_token
)

# Or basic auth for development
dyd.authenticate("username", "password")
```

**Security Considerations:**
- Use OAuth/OIDC for production
- Store credentials in secure vaults (Azure KeyVault, etc.)
- Implement role-based access to DYD entities

### 7. **Test Data Service** (IC-DataDiscovery-Toolkit-Test-Data)

**Purpose:** Generate and manage synthetic test datasets

**Integration:**
```python
# Use for testing transformations and validations
test_data = dyd.get_test_dataset("ASSET_TRADES_SAMPLE")

# Load into RAW schema for testing
session.create_dataframe(test_data).write.mode("overwrite").save_as_table(
    f"{database}.RAW.TEST_FCT_TRADE"
)
```

### 8. **Frontend** (IC-DataDiscovery-Frontend-v2)

**Purpose:** Web UI for discovery and visualization

**Integration:**
- Access at: `http://dyd-fronted:3000`
- Browse discovered mappings and metadata
- Manual refinement of auto-generated mappings
- Export refined versions back to Snowflake

### 9. **Kubernetes Deployment** (xpsdatadiscovery-kubernetes-deploy)

**Purpose:** Container orchestration for DYD services

**Deployment:**
```yaml
# Example Kubernetes deployment
apiVersion: v1
kind: Namespace
metadata:
  name: dyd-toolkit

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dyd-platform
  namespace: dyd-toolkit
spec:
  replicas: 3
  selector:
    matchLabels:
      app: dyd-platform
  template:
    metadata:
      labels:
        app: dyd-platform
    spec:
      containers:
      - name: platform
        image: dyd-toolkit:platform-latest
        ports:
        - containerPort: 8080
        env:
        - name: SNOWFLAKE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: snowflake-creds
              key: account
```

## 🔄 End-to-End Integration Workflow

```
1. DISCOVERY PHASE
   ↓
   DYD Frontend scans legacy systems
   → Discovers entities, columns, business terms
   → Creates mappings with confidence scores
   
2. EXPORT PHASE
   ↓
   DYD Platform exports mappings/metadata
   → Generate dyd_mappings.json
   → Generate dyd_metadata.json
   
3. TRANSFORMATION PHASE
   ↓
   STTM Service generates SQL
   → Create Dynamic Tables in CURATED schema
   → Link back to DYD sources
   
4. VALIDATION PHASE
   ↓
   RCA PoC executes quality rules
   → Quality Service detects anomalies
   → CoPilot suggests additional checks
   → Log results to CONTROLS schema
   
5. RECONCILIATION PHASE
   ↓
   RCA PoC runs reconciliation controls
   → CoPilot suggests optimal tolerances
   → Lineage Service tracks impact
   → Determine readiness for reporting
   
6. GOVERNANCE PHASE
   ↓
   CONTROLS schema maintains audit trail
   → All DYD artifacts traced
   → Lineage evidence recorded
   → Compliance evidence captured
```

## 📝 Implementation Checklist

- [ ] **Network Connectivity**
  - [ ] RCA PoC can reach DYD Platform API
  - [ ] Firewall rules allow HTTP/HTTPS to DYD services
  - [ ] DNS resolves DYD service hostnames

- [ ] **Authentication Setup**
  - [ ] OAuth/OIDC configured
  - [ ] Service accounts created
  - [ ] Credentials securely stored

- [ ] **Data Integration**
  - [ ] DYD exports accessible as JSON files
  - [ ] RCA PoC loads mappings into CONTROLS.DYD_MAPPINGS
  - [ ] RCA PoC loads metadata into CONTROLS.DYD_METADATA

- [ ] **Transformation Pipeline**
  - [ ] STTM service generates SQL without errors
  - [ ] Dynamic Tables created successfully
  - [ ] Transformation logic tested on sample data

- [ ] **Quality Integration**
  - [ ] Quality metrics from DYD Service integrated
  - [ ] Anomaly detection results stored
  - [ ] DQ rules enhanced by CoPilot suggestions

- [ ] **Reconciliation Integration**
  - [ ] Reconciliation controls reference DYD mappings
  - [ ] Tolerances validated against quality baseline
  - [ ] Readiness determination includes DYD confidence

- [ ] **Monitoring & Alerting**
  - [ ] DYD service health monitored
  - [ ] API failures logged and alerted
  - [ ] Lineage evidence regularly captured

- [ ] **Documentation**
  - [ ] Mappings documented in CONTROLS.DYD_MAPPINGS
  - [ ] Transformation logic traceable to DYD STTM
  - [ ] Quality rules linked to DYD metadata

## 🚀 Production Deployment Pattern

```
Development Environment
├─ DYD Services (Docker Compose)
├─ Snowflake DEV account
├─ RCA PoC components
└─ Sample data

↓

Staging Environment
├─ DYD Services (Kubernetes)
├─ Snowflake STG account
├─ RCA PoC production-like
└─ Realistic data volumes

↓

Production Environment
├─ DYD Services (HA Kubernetes)
├─ Snowflake PROD account
├─ RCA PoC hardened
└─ Full data, compliance controls
```

## 🔐 Security Best Practices

1. **API Keys & Credentials**
   - Store in Azure KeyVault or equivalent
   - Rotate credentials regularly
   - Use service principals for automation

2. **Network Security**
   - Use private endpoints to DYD services
   - Encrypt data in transit (HTTPS/TLS)
   - Restrict API access by IP/network

3. **Data Access Control**
   - Role-based access to CONTROLS schema
   - Audit all data lineage access
   - Log all API calls

4. **Compliance & Governance**
   - Maintain lineage evidence for audits
   - Archive execution results per retention policy
   - Tag sensitive mappings/metadata

## 📊 Monitoring Queries

```sql
-- Check DYD integration status
SELECT 
  COUNT(*) as total_mappings,
  AVG(MAPPING_CONFIDENCE) as avg_confidence,
  MIN(CREATED_AT) as first_mapping,
  MAX(CREATED_AT) as last_updated
FROM CONTROLS.DYD_MAPPINGS;

-- Track lineage evidence
SELECT 
  ENTITY_NAME,
  LENGTH(LINEAGE_JSON) as lineage_size,
  CREATED_AT
FROM CONTROLS.LINEAGE_EVIDENCE
ORDER BY CREATED_AT DESC;

-- Monitor quality metrics from DYD
SELECT
  SOURCE_TABLE,
  AVG(METRIC_VALUE) as avg_quality,
  COUNT(*) as executions
FROM CONTROLS.DQ_METRICS_LOG
WHERE CALCULATED_AT > CURRENT_DATE - 7
GROUP BY SOURCE_TABLE;
```

## 🆘 Troubleshooting DYD Integration

### DYD API Connectivity Issues
```bash
# Test connectivity
curl -X GET http://dyd-api:8080/health

# Check logs
kubectl logs -n dyd-toolkit deployment/dyd-platform -f
```

### Missing Mappings
```sql
-- Verify mappings loaded
SELECT COUNT(*) FROM CONTROLS.DYD_MAPPINGS;
-- Should be > 0

-- Check for failed loads
SELECT * FROM CONTROLS.DYD_MAPPINGS WHERE MAPPING_CONFIDENCE < 75;
```

### Transformation Errors
```python
# Debug STTM generation
sttm = dyd.generate_sttm(mapping_id="MAP_001")
print("Generated SQL:", sttm)
# Validate syntax
validation_result = dyd.validate_sttm(sttm)
```

---

**Integration Status:** ✅ Ready for Implementation
**Last Updated:** January 9, 2025
