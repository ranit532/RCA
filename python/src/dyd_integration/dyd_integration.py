"""DYD (Discover Your Data) Integration Module"""
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class DYDMapping:
    """Represents a DYD discovered mapping"""
    source_system: str
    source_entity: str
    source_columns: List[str]
    target_table: str
    target_columns: List[str]
    mapping_confidence: float  # 0-100
    transformation_logic: Optional[str] = None
    join_keys: List[str] = None
    created_date: str = None
    
    def to_dict(self):
        return asdict(self)

@dataclass
class DYDMetadata:
    """Represents DYD discovered metadata"""
    entity_name: str
    entity_type: str  # TABLE, FIELD, MEASURE
    column_name: Optional[str] = None
    data_type: Optional[str] = None
    description: Optional[str] = None
    business_term: Optional[str] = None
    sample_values: Optional[List[str]] = None
    
    def to_dict(self):
        return asdict(self)

class DYDIntegration:
    """Integrates DYD outputs with Snowflake implementation"""
    
    def __init__(self, session: Any):
        self.session = session
        self.mappings: Dict[str, DYDMapping] = {}
        self.metadata: Dict[str, DYDMetadata] = {}
    
    def load_mappings_from_json(self, json_file: str) -> bool:
        """Load mappings from DYD JSON export"""
        try:
            with open(json_file, 'r') as f:
                mappings_data = json.load(f)
            
            for mapping_dict in mappings_data.get('mappings', []):
                mapping = DYDMapping(
                    source_system=mapping_dict.get('source_system'),
                    source_entity=mapping_dict.get('source_entity'),
                    source_columns=mapping_dict.get('source_columns', []),
                    target_table=mapping_dict.get('target_table'),
                    target_columns=mapping_dict.get('target_columns', []),
                    mapping_confidence=mapping_dict.get('confidence', 0),
                    transformation_logic=mapping_dict.get('transformation'),
                    join_keys=mapping_dict.get('join_keys', []),
                )
                self.mappings[mapping.target_table] = mapping
            
            logger.info(f"Loaded {len(self.mappings)} mappings from {json_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading mappings: {str(e)}")
            return False
    
    def load_metadata_from_json(self, json_file: str) -> bool:
        """Load metadata from DYD JSON export"""
        try:
            with open(json_file, 'r') as f:
                metadata_data = json.load(f)
            
            for meta_dict in metadata_data.get('metadata', []):
                meta_key = f"{meta_dict.get('entity_name')}.{meta_dict.get('column_name', '')}"
                metadata = DYDMetadata(
                    entity_name=meta_dict.get('entity_name'),
                    entity_type=meta_dict.get('entity_type'),
                    column_name=meta_dict.get('column_name'),
                    data_type=meta_dict.get('data_type'),
                    description=meta_dict.get('description'),
                    business_term=meta_dict.get('business_term'),
                    sample_values=meta_dict.get('sample_values', []),
                )
                self.metadata[meta_key] = metadata
            
            logger.info(f"Loaded {len(self.metadata)} metadata items from {json_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading metadata: {str(e)}")
            return False
    
    def create_mapping_reference_table(self, schema: str = "CONTROLS") -> bool:
        """Create and populate mapping reference table in Snowflake"""
        cursor = None
        try:
            cursor = self.session.cursor()

            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.DYD_MAPPINGS (
                MAPPING_ID NUMBER AUTOINCREMENT,
                SOURCE_SYSTEM VARCHAR,
                SOURCE_ENTITY VARCHAR,
                SOURCE_COLUMNS VARCHAR,
                TARGET_TABLE VARCHAR,
                TARGET_COLUMNS VARCHAR,
                MAPPING_CONFIDENCE NUMBER(5,2),
                TRANSFORMATION_LOGIC VARCHAR,
                JOIN_KEYS VARCHAR,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (MAPPING_ID)
            )
            """
            cursor.execute(create_table_sql)

            # Insert mappings
            if self.mappings:
                for mapping in self.mappings.values():
                    insert_sql = f"""
                    INSERT INTO {schema}.DYD_MAPPINGS
                    (SOURCE_SYSTEM, SOURCE_ENTITY, SOURCE_COLUMNS, TARGET_TABLE, TARGET_COLUMNS,
                     MAPPING_CONFIDENCE, TRANSFORMATION_LOGIC, JOIN_KEYS)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        mapping.source_system,
                        mapping.source_entity,
                        ",".join(mapping.source_columns),
                        mapping.target_table,
                        ",".join(mapping.target_columns),
                        mapping.mapping_confidence,
                        mapping.transformation_logic,
                        ",".join(mapping.join_keys) if mapping.join_keys else None,
                    ))

                logger.info(f"Created mapping reference table with {len(self.mappings)} records")

            return True

        except Exception as e:
            logger.error(f"Error creating mapping table: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def create_metadata_reference_table(self, schema: str = "CONTROLS") -> bool:
        """Create and populate metadata reference table in Snowflake"""
        cursor = None
        try:
            cursor = self.session.cursor()

            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.DYD_METADATA (
                METADATA_ID NUMBER AUTOINCREMENT,
                ENTITY_NAME VARCHAR,
                ENTITY_TYPE VARCHAR,
                COLUMN_NAME VARCHAR,
                DATA_TYPE VARCHAR,
                DESCRIPTION VARCHAR,
                BUSINESS_TERM VARCHAR,
                SAMPLE_VALUES VARCHAR,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (METADATA_ID)
            )
            """
            cursor.execute(create_table_sql)

            # Insert metadata
            if self.metadata:
                for meta in self.metadata.values():
                    insert_sql = f"""
                    INSERT INTO {schema}.DYD_METADATA
                    (ENTITY_NAME, ENTITY_TYPE, COLUMN_NAME, DATA_TYPE, DESCRIPTION, BUSINESS_TERM, SAMPLE_VALUES)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        meta.entity_name,
                        meta.entity_type,
                        meta.column_name,
                        meta.data_type,
                        meta.description,
                        meta.business_term,
                        ",".join(meta.sample_values) if meta.sample_values else None,
                    ))

                logger.info(f"Created metadata table with {len(self.metadata)} records")

            return True

        except Exception as e:
            logger.error(f"Error creating metadata table: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def get_mapping_for_target(self, target_table: str) -> Optional[DYDMapping]:
        """Get mapping details for a target table"""
        return self.mappings.get(target_table)
    
    def get_metadata_for_entity(self, entity_name: str) -> List[DYDMetadata]:
        """Get all metadata for an entity"""
        return [m for m in self.metadata.values() if m.entity_name == entity_name]
    
    def generate_dq_rules_from_metadata(self) -> List[Dict]:
        """Generate DQ rule definitions from DYD metadata"""
        rules = []
        
        for meta in self.metadata.values():
            if meta.entity_type == "FIELD" and meta.column_name:
                # Generate completeness rule
                rule = {
                    "rule_id": f"COMP_{meta.entity_name}_{meta.column_name}".upper(),
                    "rule_name": f"Completeness: {meta.business_term or meta.column_name}",
                    "rule_type": "COMPLETENESS",
                    "table_name": meta.entity_name,
                    "columns": [meta.column_name],
                    "sql_logic": f"{meta.column_name} IS NOT NULL",
                    "threshold": 0.01,
                    "severity": "HIGH",
                }
                rules.append(rule)
        
        logger.info(f"Generated {len(rules)} DQ rules from metadata")
        return rules
