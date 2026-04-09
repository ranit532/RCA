"""Proactive Reconciliation Controls Engine"""
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import time

logger = logging.getLogger(__name__)

class ReconciliationType(Enum):
    """Reconciliation control types"""
    ROW_COUNT = "ROW_COUNT"  # Source vs target row counts
    SUM_AMOUNT = "SUM_AMOUNT"  # Sum of amounts
    HASH_MATCH = "HASH_MATCH"  # Record-level matching
    KEY_MATCH = "KEY_MATCH"  # Primary key coverage
    COMPLETENESS = "COMPLETENESS"  # Required field coverage

@dataclass
class ReconciliationControl:
    """Reconciliation control definition"""
    control_id: str
    control_name: str
    reconciliation_type: ReconciliationType
    source_table: str
    target_table: str
    source_column: Optional[str] = None
    target_column: Optional[str] = None
    tolerance_type: str = "ABSOLUTE"  # ABSOLUTE, PERCENTAGE
    tolerance_value: float = 0.0
    join_keys: List[str] = field(default_factory=list)  # For HASH_MATCH
    enabled: bool = True

@dataclass
class ReconciliationResult:
    """Reconciliation execution result"""
    run_id: str
    control_id: str
    control_name: str
    reconciliation_type: ReconciliationType
    source_count: int
    target_count: int
    variance: int
    variance_percentage: float
    passed: bool
    tolerance_breached: Optional[Dict] = None  # Details if tolerance exceeded
    error_message: Optional[str] = None
    execution_time_sec: float = 0.0
    executed_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for persistence"""
        return {
            "RUN_ID": self.run_id,
            "CONTROL_ID": self.control_id,
            "CONTROL_NAME": self.control_name,
            "RECONCILIATION_TYPE": self.reconciliation_type.value,
            "SOURCE_COUNT": self.source_count,
            "TARGET_COUNT": self.target_count,
            "VARIANCE": self.variance,
            "VARIANCE_PERCENTAGE": self.variance_percentage,
            "PASSED": self.passed,
            "TOLERANCE_DETAILS": str(self.tolerance_breached) if self.tolerance_breached else None,
            "ERROR_MESSAGE": self.error_message,
            "EXECUTION_TIME_SEC": self.execution_time_sec,
            "EXECUTED_AT": self.executed_at,
        }

class ReconciliationEngine:
    """Executes reconciliation controls"""
    
    def __init__(self, session: Any):
        self.session = session
        self.run_id = str(uuid.uuid4())
        self.results: List[ReconciliationResult] = []
    
    def execute_row_count_recon(
        self, control: ReconciliationControl
    ) -> ReconciliationResult:
        """Execute row count reconciliation"""
        start_time = time.time()
        cursor = None
        
        try:
            cursor = self.session.cursor()
            
            # Get source count
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {control.source_table}")
            source_count = cursor.fetchone()[0]
            
            # Get target count
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {control.target_table}")
            target_count = cursor.fetchone()[0]
            
            variance = abs(source_count - target_count)
            variance_pct = (variance / source_count * 100) if source_count > 0 else 0
            
            # Check tolerance
            tolerance_breached = None
            if control.tolerance_type == "ABSOLUTE":
                passed = variance <= control.tolerance_value
                if not passed:
                    tolerance_breached = {
                        "tolerance_type": control.tolerance_type,
                        "tolerance_value": control.tolerance_value,
                        "actual_variance": variance,
                    }
            else:  # PERCENTAGE
                passed = variance_pct <= control.tolerance_value
                if not passed:
                    tolerance_breached = {
                        "tolerance_type": control.tolerance_type,
                        "tolerance_value": control.tolerance_value,
                        "actual_variance_pct": variance_pct,
                    }
            
            result = ReconciliationResult(
                run_id=self.run_id,
                control_id=control.control_id,
                control_name=control.control_name,
                reconciliation_type=ReconciliationType.ROW_COUNT,
                source_count=source_count,
                target_count=target_count,
                variance=variance,
                variance_percentage=variance_pct,
                passed=passed,
                tolerance_breached=tolerance_breached,
                execution_time_sec=time.time() - start_time,
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error in row count reconciliation: {str(e)}")
            return ReconciliationResult(
                run_id=self.run_id,
                control_id=control.control_id,
                control_name=control.control_name,
                reconciliation_type=ReconciliationType.ROW_COUNT,
                source_count=0,
                target_count=0,
                variance=0,
                variance_percentage=0,
                passed=False,
                error_message=str(e),
                execution_time_sec=time.time() - start_time,
            )
        finally:
            if cursor:
                cursor.close()
    
    def execute_sum_recon(self, control: ReconciliationControl) -> ReconciliationResult:
        """Execute sum reconciliation on specified column"""
        start_time = time.time()
        cursor = None
        
        try:
            cursor = self.session.cursor()
            
            # Get source sum
            cursor.execute(
                f"SELECT COALESCE(SUM({control.source_column}), 0) as sum_val "
                f"FROM {control.source_table}"
            )
            source_sum = cursor.fetchone()[0]
            
            # Get target sum
            cursor.execute(
                f"SELECT COALESCE(SUM({control.target_column}), 0) as sum_val "
                f"FROM {control.target_table}"
            )
            target_sum = cursor.fetchone()[0]
            
            variance = abs(float(source_sum) - float(target_sum))
            variance_pct = (variance / float(source_sum) * 100) if float(source_sum) > 0 else 0
            
            # Check tolerance
            tolerance_breached = None
            if control.tolerance_type == "ABSOLUTE":
                passed = variance <= control.tolerance_value
                if not passed:
                    tolerance_breached = {
                        "tolerance_type": control.tolerance_type,
                        "tolerance_value": control.tolerance_value,
                        "actual_variance": variance,
                    }
            else:
                passed = variance_pct <= control.tolerance_value
                if not passed:
                    tolerance_breached = {
                        "tolerance_type": control.tolerance_type,
                        "tolerance_value": control.tolerance_value,
                        "actual_variance_pct": variance_pct,
                    }
            
            result = ReconciliationResult(
                run_id=self.run_id,
                control_id=control.control_id,
                control_name=control.control_name,
                reconciliation_type=ReconciliationType.SUM_AMOUNT,
                source_count=1,  # Not applicable for summing
                target_count=1,
                variance=int(variance),
                variance_percentage=variance_pct,
                passed=passed,
                tolerance_breached=tolerance_breached,
                execution_time_sec=time.time() - start_time,
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error in sum reconciliation: {str(e)}")
            return ReconciliationResult(
                run_id=self.run_id,
                control_id=control.control_id,
                control_name=control.control_name,
                reconciliation_type=ReconciliationType.SUM_AMOUNT,
                source_count=0,
                target_count=0,
                variance=0,
                variance_percentage=0,
                passed=False,
                error_message=str(e),
                execution_time_sec=time.time() - start_time,
            )
        finally:
            if cursor:
                cursor.close()
    
    def execute_control(self, control: ReconciliationControl) -> ReconciliationResult:
        """Execute a reconciliation control based on type"""
        if not control.enabled:
            logger.debug(f"Control {control.control_id} is disabled")
            return None
        
        if control.reconciliation_type == ReconciliationType.ROW_COUNT:
            result = self.execute_row_count_recon(control)
        elif control.reconciliation_type == ReconciliationType.SUM_AMOUNT:
            result = self.execute_sum_recon(control)
        else:
            logger.warning(f"Reconciliation type {control.reconciliation_type} not yet implemented")
            return None
        
        self.results.append(result)
        logger.info(
            f"Control {control.control_id} executed: "
            f"{'PASSED' if result.passed else 'FAILED'} "
            f"(Variance: {result.variance_percentage:.2f}%)"
        )
        
        return result
    
    def execute_controls(self, controls: List[ReconciliationControl]) -> List[ReconciliationResult]:
        """Execute multiple reconciliation controls"""
        logger.info(f"Executing {len(controls)} reconciliation controls (Run ID: {self.run_id})")
        
        for control in controls:
            self.execute_control(control)
        
        return self.results
    
    def persist_results(self, schema: str = "CONTROLS") -> bool:
        """Persist reconciliation results to Snowflake"""
        if not self.results:
            logger.warning("No results to persist")
            return False
        
        cursor = None
        try:
            cursor = self.session.cursor()
            
            # Ensure schema exists
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Create table if doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.RECONCILIATION_RESULTS (
                RUN_ID VARCHAR,
                CONTROL_ID VARCHAR,
                CONTROL_NAME VARCHAR,
                RECONCILIATION_TYPE VARCHAR,
                SOURCE_COUNT NUMBER,
                TARGET_COUNT NUMBER,
                VARIANCE NUMBER,
                VARIANCE_PERCENTAGE NUMBER(6,2),
                PASSED BOOLEAN,
                TOLERANCE_DETAILS VARCHAR,
                ERROR_MESSAGE VARCHAR,
                EXECUTION_TIME_SEC NUMBER(10,2),
                EXECUTED_AT TIMESTAMP_NTZ
            )
            """
            cursor.execute(create_table_sql)
            
            # Insert results
            for result in self.results:
                result_dict = result.to_dict()
                insert_sql = f"""
                INSERT INTO {schema}.RECONCILIATION_RESULTS 
                (RUN_ID, CONTROL_ID, CONTROL_NAME, RECONCILIATION_TYPE, SOURCE_COUNT, TARGET_COUNT, 
                 VARIANCE, VARIANCE_PERCENTAGE, PASSED, TOLERANCE_DETAILS, ERROR_MESSAGE, 
                 EXECUTION_TIME_SEC, EXECUTED_AT)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_sql, (
                    result_dict["RUN_ID"],
                    result_dict["CONTROL_ID"],
                    result_dict["CONTROL_NAME"],
                    result_dict["RECONCILIATION_TYPE"],
                    result_dict["SOURCE_COUNT"],
                    result_dict["TARGET_COUNT"],
                    result_dict["VARIANCE"],
                    result_dict["VARIANCE_PERCENTAGE"],
                    result_dict["PASSED"],
                    result_dict["TOLERANCE_DETAILS"],
                    result_dict["ERROR_MESSAGE"],
                    result_dict["EXECUTION_TIME_SEC"],
                    result_dict["EXECUTED_AT"],
                ))
            
            logger.info(f"Persisted {len(self.results)} results")
            return True
            
        except Exception as e:
            logger.error(f"Error persisting results: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def get_summary(self) -> Dict:
        """Get execution summary"""
        total_controls = len(self.results)
        passed_controls = sum(1 for r in self.results if r.passed)
        failed_controls = total_controls - passed_controls
        
        return {
            "run_id": self.run_id,
            "total_controls": total_controls,
            "passed_controls": passed_controls,
            "failed_controls": failed_controls,
            "readiness_for_reporting": "YES" if failed_controls == 0 else "NO_REVIEW_REQUIRED",
            "timestamp": datetime.utcnow().isoformat(),
        }
