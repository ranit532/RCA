"""Data Quality Execution Engine"""
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional
import time
from snowpark.session import Session
from snowpark.dataframe import DataFrame
from .quality_rules import DQRule, DQResult, RuleType

logger = logging.getLogger(__name__)

class DataQualityEngine:
    """Executes data quality rules against datasets"""
    
    def __init__(self, session: Session):
        self.session = session
        self.run_id = str(uuid.uuid4())
        self.results: List[DQResult] = []
    
    def execute_rule(self, rule: DQRule, table_name: str = None) -> DQResult:
        """Execute a single data quality rule"""
        if not rule.enabled:
            logger.debug(f"Rule {rule.rule_id} is disabled, skipping")
            return None
        
        source_table = table_name or rule.table_name
        start_time = time.time()
        
        try:
            # Get total record count
            total_df = self.session.sql(
                f"SELECT COUNT(*) as cnt FROM {source_table}"
            )
            total_count = total_df.collect()[0][0]
            
            # Count records that FAIL the rule (NOT matching the logic)
            failed_df = self.session.sql(
                f"""SELECT COUNT(*) as cnt 
                   FROM {source_table} 
                   WHERE NOT ({rule.sql_logic})"""
            )
            failed_count = failed_df.collect()[0][0]
            
            execution_time = time.time() - start_time
            failure_rate = (failed_count / total_count * 100) if total_count > 0 else 0
            
            # Determine if rule passed (within threshold)
            threshold_pct = (rule.threshold or 0) * 100
            passed = failure_rate <= threshold_pct
            
            result = DQResult(
                run_id=self.run_id,
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_name=source_table,
                rule_type=rule.rule_type,
                records_tested=total_count,
                records_failed=failed_count,
                failure_rate=failure_rate,
                passed=passed,
                execution_time_sec=execution_time,
            )
            
            self.results.append(result)
            logger.info(
                f"Rule {rule.rule_id} executed: {failed_count}/{total_count} failed "
                f"({failure_rate:.2f}%) - {'PASSED' if passed else 'FAILED'}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing rule {rule.rule_id}: {str(e)}")
            result = DQResult(
                run_id=self.run_id,
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_name=source_table,
                rule_type=rule.rule_type,
                records_tested=0,
                records_failed=0,
                failure_rate=0,
                passed=False,
                error_message=str(e),
                execution_time_sec=time.time() - start_time,
            )
            self.results.append(result)
            return result
    
    def execute_rules(self, rules: List[DQRule]) -> List[DQResult]:
        """Execute multiple data quality rules"""
        logger.info(f"Executing {len(rules)} quality rules (Run ID: {self.run_id})")
        
        for rule in rules:
            self.execute_rule(rule)
        
        return self.results
    
    def persist_results(self, schema: str = "CONTROLS") -> bool:
        """Persist DQ results to Snowflake"""
        if not self.results:
            logger.warning("No results to persist")
            return False
        
        try:
            # Ensure controls schema and table exist
            self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.DQ_EXECUTION_RESULTS (
                RUN_ID VARCHAR,
                RULE_ID VARCHAR,
                RULE_NAME VARCHAR,
                TABLE_NAME VARCHAR,
                RULE_TYPE VARCHAR,
                RECORDS_TESTED NUMBER,
                RECORDS_FAILED NUMBER,
                FAILURE_RATE NUMBER(6,2),
                PASSED BOOLEAN,
                ERROR_MESSAGE VARCHAR,
                EXECUTION_TIME_SEC NUMBER(10,2),
                EXECUTED_AT TIMESTAMP_NTZ,
                PRIMARY KEY (RUN_ID, RULE_ID)
            )
            """
            self.session.sql(create_table_sql).collect()
            
            # Convert results to dataframe and insert
            results_data = [r.to_dict() for r in self.results]
            df = self.session.create_dataframe(results_data)
            df.write.mode("append").save_as_table(
                f"{schema}.DQ_EXECUTION_RESULTS"
            )
            
            logger.info(f"Persisted {len(self.results)} results to {schema}.DQ_EXECUTION_RESULTS")
            return True
            
        except Exception as e:
            logger.error(f"Error persisting results: {str(e)}")
            return False
    
    def get_summary(self) -> Dict:
        """Get summary statistics for execution"""
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        failed_rules = total_rules - passed_rules
        total_records = sum(r.records_tested for r in self.results)
        total_failures = sum(r.records_failed for r in self.results)
        
        return {
            "run_id": self.run_id,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "total_records_tested": total_records,
            "total_failures": total_failures,
            "execution_status": "SUCCESS" if failed_rules == 0 else "ISSUES_DETECTED",
            "timestamp": datetime.utcnow().isoformat(),
        }
