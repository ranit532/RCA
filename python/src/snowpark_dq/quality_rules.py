"""Data Quality Rules Engine for Snowpark"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class RuleType(Enum):
    """Data quality rule types"""
    COMPLETENESS = "COMPLETENESS"  # NULL checks
    VALIDITY = "VALIDITY"  # Format/domain validation
    UNIQUENESS = "UNIQUENESS"  # Duplicate detection
    CONSISTENCY = "CONSISTENCY"  # Business logic consistency
    ACCURACY = "ACCURACY"  # Reference data matching
    TIMELINESS = "TIMELINESS"  # Freshness/SLA checks
    THRESHOLD = "THRESHOLD"  # Range/threshold violations

@dataclass
class DQRule:
    """Data Quality Rule definition"""
    rule_id: str
    rule_name: str
    rule_type: RuleType
    table_name: str
    columns: List[str]
    sql_logic: str  # DQM SQL expression
    threshold: Optional[float] = None  # Acceptable failure %
    severity: str = "HIGH"  # HIGH, MEDIUM, LOW
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "RULE_ID": self.rule_id,
            "RULE_NAME": self.rule_name,
            "RULE_TYPE": self.rule_type.value,
            "TABLE_NAME": self.table_name,
            "COLUMNS": ",".join(self.columns),
            "SQL_LOGIC": self.sql_logic,
            "THRESHOLD": self.threshold,
            "SEVERITY": self.severity,
            "ENABLED": self.enabled,
            "CREATED_AT": self.created_at,
        }

@dataclass
class DQResult:
    """Data Quality Rule execution result"""
    run_id: str
    rule_id: str
    rule_name: str
    table_name: str
    rule_type: RuleType
    records_tested: int
    records_failed: int
    failure_rate: float  # 0-100
    passed: bool
    error_message: Optional[str] = None
    execution_time_sec: float = 0.0
    executed_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Snowflake insert"""
        return {
            "RUN_ID": self.run_id,
            "RULE_ID": self.rule_id,
            "RULE_NAME": self.rule_name,
            "TABLE_NAME": self.table_name,
            "RULE_TYPE": self.rule_type.value,
            "RECORDS_TESTED": self.records_tested,
            "RECORDS_FAILED": self.records_failed,
            "FAILURE_RATE": self.failure_rate,
            "PASSED": self.passed,
            "ERROR_MESSAGE": self.error_message,
            "EXECUTION_TIME_SEC": self.execution_time_sec,
            "EXECUTED_AT": self.executed_at,
        }

# Pre-defined quality rules for asset data
ASSET_DATA_QUALITY_RULES = [
    DQRule(
        rule_id="COMP_001",
        rule_name="Party ID Completeness",
        rule_type=RuleType.COMPLETENESS,
        table_name="DIM_PARTY_RAW",
        columns=["PARTY_ID"],
        sql_logic="PARTY_ID IS NOT NULL",
        threshold=0.01,  # Allow 1% nulls
        severity="HIGH",
    ),
    DQRule(
        rule_id="COMP_002",
        rule_name="Account Code Completeness",
        rule_type=RuleType.COMPLETENESS,
        table_name="DIM_ACCOUNT_RAW",
        columns=["ACCOUNT_CODE"],
        sql_logic="ACCOUNT_CODE IS NOT NULL",
        threshold=0.01,
        severity="HIGH",
    ),
    DQRule(
        rule_id="UNIQ_001",
        rule_name="ISIN Uniqueness",
        rule_type=RuleType.UNIQUENESS,
        table_name="DIM_INSTRUMENT_RAW",
        columns=["ISIN"],
        sql_logic="COUNT(*) = 1 OVER (PARTITION BY ISIN)",
        threshold=0.0,
        severity="HIGH",
    ),
    DQRule(
        rule_id="VALID_001",
        rule_name="Asset Class Validity",
        rule_type=RuleType.VALIDITY,
        table_name="DIM_INSTRUMENT_RAW",
        columns=["ASSET_CLASS"],
        sql_logic="ASSET_CLASS IN ('EQUITY', 'BOND', 'FUND')",
        threshold=0.05,
        severity="MEDIUM",
    ),
    DQRule(
        rule_id="THRESH_001",
        rule_name="Trade Amount Threshold",
        rule_type=RuleType.THRESHOLD,
        table_name="FCT_TRADE_RAW",
        columns=["GROSS_AMOUNT"],
        sql_logic="GROSS_AMOUNT > 0 AND GROSS_AMOUNT < 1000000",
        threshold=0.1,
        severity="MEDIUM",
    ),
]
