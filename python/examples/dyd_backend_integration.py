"""
Advanced DYD Integration - Backend System Integration Guide
Demonstrates integration with DYD backend services for advanced use cases
"""

import logging
import requests
import json
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# =====================================================================
# DYD Backend Services Configuration
# =====================================================================

class DYDBackendServices:
    """
    Integrates with DYD Toolkit backend services:
    
    1. Frontend: IC-DataDiscovery-Frontend-v2
       - Web UI for data discovery, profiling, visualization
       
    2. Lineage: IC-DataDiscovery-Backend-Lineage
       - Manages data lineage and impact analysis
       - Tracks source-to-target mappings
       
    3. STTM: IC-DataDiscovery-Backend-Sttm
       - SQL Template Transformation Mapping generator
       - Generates SQL for transformations from mappings
       
    4. Platform: IC-DataDsicovery-Platform
       - Core orchestration and API gateway
       - Handles auth, data management
       
    5. CoPilot: IC-DataDicsovery-Backend-Copilot
       - AI-powered recommendations and suggestions
       - Natural language querying of metadata
       
    6. Quality: IC-DataDicsovery-Backend-Dataquality
       - Data profiling and quality metrics
       - Anomaly detection
       
    7. Auth: IC-DataDiscovery-Auth
       - Authentication and authorization
       - OAuth, OIDC support
       
    8. Test Data: IC-DataDiscovery Toolkit-Test Data
       - Sample datasets for testing
       
    9. K8S Deployment: xpsdatadiscovery-kubernetes-deploy
       - Container orchestration configs
    """
    
    def __init__(self, base_url: str = "http://dyd-platform-api"):
        self.base_url = base_url
        self.session = requests.Session()
        self.auth_token = None
    
    # ====================================================================
    # 1. LINEAGE SERVICE - Data Lineage Management
    # ====================================================================
    
    def get_lineage_tree(self, entity_name: str) -> Dict:
        """Get complete lineage tree for an entity"""
        response = self.session.get(
            f"{self.base_url}/lineage/entities/{entity_name}/tree"
        )
        return response.json()
    
    def get_upstream_lineage(self, entity_name: str) -> List[str]:
        """Get all upstream dependencies (sources)"""
        response = self.session.get(
            f"{self.base_url}/lineage/entities/{entity_name}/upstream"
        )
        return response.json()["dependencies"]
    
    def get_downstream_lineage(self, entity_name: str) -> List[str]:
        """Get all downstream consumers"""
        response = self.session.get(
            f"{self.base_url}/lineage/entities/{entity_name}/downstream"
        )
        return response.json()["consumers"]
    
    def get_impact_analysis(self, entity_name: str, change_scope: str) -> Dict:
        """Perform impact analysis for proposed changes"""
        payload = {"entity": entity_name, "scope": change_scope}
        response = self.session.post(
            f"{self.base_url}/lineage/impact-analysis",
            json=payload
        )
        return response.json()
    
    # ====================================================================
    # 2. STTM SERVICE - SQL Transformation Generation
    # ====================================================================
    
    def generate_sttm(self, mapping_id: str) -> str:
        """Generate SQL Template Transformation Mapping"""
        response = self.session.get(
            f"{self.base_url}/sttm/generate/{mapping_id}"
        )
        return response.json()["sql_logic"]
    
    def get_sttm_template(self, source_system: str, target_system: str) -> str:
        """Get STTM template for system-to-system mapping"""
        response = self.session.get(
            f"{self.base_url}/sttm/templates/{source_system}/{target_system}"
        )
        return response.json()["template"]
    
    def validate_sttm(self, sql_logic: str) -> Dict:
        """Validate generated STTM logic"""
        payload = {"sql": sql_logic}
        response = self.session.post(
            f"{self.base_url}/sttm/validate",
            json=payload
        )
        return response.json()
    
    # ====================================================================
    # 3. COPILOT SERVICE - AI-Powered Recommendations
    # ====================================================================
    
    def get_mapping_recommendations(self, source_table: str) -> List[Dict]:
        """Get AI-recommended target mappings for source table"""
        response = self.session.get(
            f"{self.base_url}/copilot/mappings/recommend/{source_table}"
        )
        return response.json()["recommendations"]
    
    def ask_copilot(self, question: str) -> str:
        """Natural language query to DYD Copilot"""
        payload = {"question": question}
        response = self.session.post(
            f"{self.base_url}/copilot/ask",
            json=payload
        )
        return response.json()["answer"]
    
    def get_transformation_suggestion(self, source_schema: Dict, target_schema: Dict) -> Dict:
        """Get AI suggestion for transformation logic"""
        payload = {
            "source": source_schema,
            "target": target_schema
        }
        response = self.session.post(
            f"{self.base_url}/copilot/transform/suggest",
            json=payload
        )
        return response.json()["suggestion"]
    
    # ====================================================================
    # 4. QUALITY SERVICE - Data Profiling & Quality Metrics
    # ====================================================================
    
    def profile_dataset(self, table_name: str) -> Dict:
        """Profile dataset for quality metrics"""
        response = self.session.get(
            f"{self.base_url}/quality/profile/{table_name}"
        )
        return response.json()
    
    def get_quality_metrics(self, table_name: str) -> Dict:
        """Get data quality metrics for a table"""
        response = self.session.get(
            f"{self.base_url}/quality/metrics/{table_name}"
        )
        metrics = response.json()
        return {
            "completeness": metrics.get("completeness"),
            "uniqueness": metrics.get("uniqueness"),
            "validity": metrics.get("validity"),
            "consistency": metrics.get("consistency"),
            "accuracy": metrics.get("accuracy"),
            "timeliness": metrics.get("timeliness"),
        }
    
    def detect_anomalies(self, table_name: str) -> List[Dict]:
        """Detect data quality anomalies"""
        response = self.session.get(
            f"{self.base_url}/quality/anomalies/{table_name}"
        )
        return response.json()["anomalies"]
    
    # ====================================================================
    # 5. PLATFORM SERVICE - Core Integration
    # ====================================================================
    
    def authenticate(self, username: str, password: str) -> str:
        """Authenticate with DYD Platform"""
        payload = {"username": username, "password": password}
        response = self.session.post(
            f"{self.base_url}/auth/login",
            json=payload
        )
        self.auth_token = response.json()["token"]
        self.session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
        return self.auth_token
    
    def get_all_mappings(self) -> List[Dict]:
        """Retrieve all discovered mappings"""
        response = self.session.get(f"{self.base_url}/mappings")
        return response.json()["mappings"]
    
    def get_all_metadata(self, entity_type: str = None) -> List[Dict]:
        """Retrieve all metadata items"""
        url = f"{self.base_url}/metadata"
        if entity_type:
            url += f"?type={entity_type}"
        response = self.session.get(url)
        return response.json()["metadata"]
    
    def export_mapping_json(self) -> Dict:
        """Export all mappings as JSON"""
        response = self.session.get(f"{self.base_url}/export/mappings/json")
        return response.json()
    
    def export_metadata_json(self) -> Dict:
        """Export all metadata as JSON"""
        response = self.session.get(f"{self.base_url}/export/metadata/json")
        return response.json()
    
    # ====================================================================
    # 6. Integration with Snowpark - End-to-End Workflow
    # ====================================================================
    
    def create_dynamic_table_from_mapping(self, mapping: Dict, snowflake_session) -> str:
        """Create Snowflake Dynamic Table from DYD mapping"""
        
        # Get STTM for this mapping
        sttm = self.generate_sttm(mapping["mapping_id"])
        
        # Create Dynamic Table SQL
        table_name = f"DT_{mapping['target_table'].upper()}"
        
        create_dt_sql = f"""
        CREATE OR REPLACE DYNAMIC TABLE {table_name}
        TARGET_LAG = '1 hour'
        AS
        {sttm}
        """
        
        # Execute in Snowflake
        snowflake_session.sql(create_dt_sql).collect()
        
        logger.info(f"✅ Created Dynamic Table: {table_name}")
        return create_dt_sql
    
    def create_dq_rules_from_metadata(self, metadata_items: List[Dict]) -> List[Dict]:
        """Generate DQ rules from DYD metadata"""
        
        rules = []
        for meta in metadata_items:
            if meta.get("entity_type") == "FIELD" and meta.get("is_required"):
                # Generate completeness rule
                rule = {
                    "rule_id": f"COMP_{meta['entity_name']}_{meta['column_name']}",
                    "rule_name": f"Required: {meta.get('business_term', meta['column_name'])}",
                    "rule_type": "COMPLETENESS",
                    "table_name": meta["entity_name"],
                    "columns": [meta["column_name"]],
                    "sql_logic": f"{meta['column_name']} IS NOT NULL",
                    "severity": "HIGH",
                }
                rules.append(rule)
        
        logger.info(f"Generated {len(rules)} DQ rules from metadata")
        return rules
    
    def trace_lineage_to_controls(self, entity_name: str, snowflake_session) -> None:
        """Trace complete lineage and create lineage evidence table"""
        
        lineage = self.get_lineage_tree(entity_name)
        
        # Create lineage evidence table
        lineage_sql = f"""
        CREATE OR REPLACE TABLE CONTROLS.LINEAGE_EVIDENCE (
            ENTITY_NAME VARCHAR,
            LINEAGE_JSON VARCHAR,
            CREATED_AT TIMESTAMP_NTZ,
            PRIMARY KEY (ENTITY_NAME)
        );
        """
        snowflake_session.sql(lineage_sql).collect()
        
        # Insert lineage data
        insert_sql = f"""
        INSERT INTO CONTROLS.LINEAGE_EVIDENCE
        VALUES ('{entity_name}', '{json.dumps(lineage)}', CURRENT_TIMESTAMP());
        """
        snowflake_session.sql(insert_sql).collect()
        
        logger.info(f"✅ Lineage evidence recorded for {entity_name}")

# =====================================================================
# Example Usage
# =====================================================================

def example_end_to_end_dyd_integration():
    """
    Example: Complete end-to-end DYD integration workflow
    """
    
    # Initialize DYD Backend Services
    dyd_services = DYDBackendServices(base_url="http://localhost:8080")
    
    # 1. Authenticate
    dyd_services.authenticate("user@company.com", "password")
    logger.info("✅ Authenticated with DYD Platform")
    
    # 2. Get all mappings and metadata
    mappings = dyd_services.get_all_mappings()
    metadata = dyd_services.get_all_metadata()
    logger.info(f"✅ Loaded {len(mappings)} mappings and {len(metadata)} metadata items")
    
    # 3. Export for Snowflake consumption
    mappings_export = dyd_services.export_mapping_json()
    metadata_export = dyd_services.export_metadata_json()
    
    with open("dyd_mappings_export.json", "w") as f:
        json.dump(mappings_export, f, indent=2)
    
    with open("dyd_metadata_export.json", "w") as f:
        json.dump(metadata_export, f, indent=2)
    
    logger.info("✅ DYD exports saved")
    
    # 4. Ask Copilot for recommendations
    # recommendations = dyd_services.get_mapping_recommendations("LEGACY_ASSET_SYSTEM.TRADES")
    # logger.info(f"✅ Got {len(recommendations)} transformation recommendations from Copilot")
    
    # 5. Perform impact analysis
    # impact = dyd_services.get_impact_analysis("DIM_PARTY_RAW", "ADD_COLUMN")
    # logger.info(f"✅ Impact analysis: {impact}")
    
    # 6. Get lineage
    lineage = dyd_services.get_upstream_lineage("FCT_TRADE_RAW")
    logger.info(f"✅ Upstream dependencies: {lineage}")
    
    downstream = dyd_services.get_downstream_lineage("FCT_TRADE_RAW")
    logger.info(f"✅ Downstream consumers: {downstream}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    example_end_to_end_dyd_integration()
