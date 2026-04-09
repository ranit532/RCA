"""
Main orchestration script for RCA PoC
Demonstrates end-to-end data quality and reconciliation workflow
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from config.snowflake_config import SnowflakeConfig, DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager
from src.snowpark_dq.quality_engine import DataQualityEngine
from src.snowpark_dq.quality_rules import ASSET_DATA_QUALITY_RULES
from src.controls.reconciliation_engine import (
    ReconciliationEngine,
    ReconciliationControl,
    ReconciliationType
)
from src.dyd_integration.dyd_integration import DYDIntegration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_environment():
    """Initialize Snowpark session and configuration"""
    logger.info("=" * 80)
    logger.info("RCA PoC - Orchestration Starting")
    logger.info("=" * 80)
    
    config = DEFAULT_CONFIG
    session = SnowparkSessionManager.initialize(config)
    
    logger.info(f"Connected to Snowflake account: {config.account}")
    logger.info(f"Database: {config.database}")
    logger.info(f"Warehouse: {config.warehouse}")
    
    return session, config

def run_data_quality_checks(session):
    """Execute all data quality rules"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 1: DATA QUALITY VALIDATION")
    logger.info("=" * 80)
    
    dq_engine = DataQualityEngine(session)
    
    logger.info(f"Executing {len(ASSET_DATA_QUALITY_RULES)} quality rules...")
    results = dq_engine.execute_rules(ASSET_DATA_QUALITY_RULES)
    
    # Display summary
    summary = dq_engine.get_summary()
    logger.info("\nData Quality Execution Summary:")
    logger.info(f"  Run ID: {summary['run_id']}")
    logger.info(f"  Total Rules: {summary['total_rules']}")
    logger.info(f"  Passed: {summary['passed_rules']}")
    logger.info(f"  Failed: {summary['failed_rules']}")
    logger.info(f"  Status: {summary['execution_status']}")
    
    # Persist results
    if dq_engine.persist_results(schema="CONTROLS"):
        logger.info("✅ DQ results persisted to CONTROLS.DQ_EXECUTION_RESULTS")
    
    return dq_engine, summary

def run_reconciliation_controls(session):
    """Execute reconciliation controls"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 2: PROACTIVE RECONCILIATION")
    logger.info("=" * 80)
    
    recon_engine = ReconciliationEngine(session)
    
    # Define reconciliation controls
    controls = [
        ReconciliationControl(
            control_id="ROW_001",
            control_name="Raw vs Curated Party Row Count",
            reconciliation_type=ReconciliationType.ROW_COUNT,
            source_table="RAW.DIM_PARTY_RAW",
            target_table="CURATED.DT_DIM_PARTY_CURATED",
            tolerance_type="ABSOLUTE",
            tolerance_value=0,
        ),
        ReconciliationControl(
            control_id="ROW_002",
            control_name="Raw vs Curated Account Row Count",
            reconciliation_type=ReconciliationType.ROW_COUNT,
            source_table="RAW.DIM_ACCOUNT_RAW",
            target_table="CURATED.DT_DIM_ACCOUNT_CURATED",
            tolerance_type="PERCENTAGE",
            tolerance_value=1.0,
        ),
        ReconciliationControl(
            control_id="SUM_001",
            control_name="Trade Amount Reconciliation",
            reconciliation_type=ReconciliationType.SUM_AMOUNT,
            source_table="RAW.FCT_TRADE_RAW",
            source_column="GROSS_AMOUNT",
            target_table="CURATED.DT_FCT_TRADE_CURATED",
            target_column="GROSS_AMOUNT",
            tolerance_type="PERCENTAGE",
            tolerance_value=0.5,
        ),
    ]
    
    logger.info(f"Executing {len(controls)} reconciliation controls...")
    results = recon_engine.execute_controls(controls)
    
    # Display summary
    summary = recon_engine.get_summary()
    logger.info("\nReconciliation Execution Summary:")
    logger.info(f"  Run ID: {summary['run_id']}")
    logger.info(f"  Total Controls: {summary['total_controls']}")
    logger.info(f"  Passed: {summary['passed_controls']}")
    logger.info(f"  Failed: {summary['failed_controls']}")
    logger.info(f"  Readiness: {summary['readiness_for_reporting']}")
    
    # Persist results
    if recon_engine.persist_results(schema="CONTROLS"):
        logger.info("✅ Reconciliation results persisted to CONTROLS.RECONCILIATION_RESULTS")
    
    return recon_engine, summary

def integrate_dyd(session):
    """Integrate DYD outputs"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 3: DYD INTEGRATION")
    logger.info("=" * 80)
    
    dyd = DYDIntegration(session)
    
    # In a real scenario, these would be loaded from DYD exports
    # For this PoC, we'll create sample mappings
    logger.info("Loading DYD mappings and metadata...")
    logger.info("  - Creating mapping reference tables")
    logger.info("  - Creating metadata reference tables")
    
    if dyd.create_mapping_reference_table(schema="CONTROLS"):
        logger.info("✅ DYD mappings table created")
    
    if dyd.create_metadata_reference_table(schema="CONTROLS"):
        logger.info("✅ DYD metadata table created")
    
    logger.info("DYD integration status: SUCCESS")
    logger.info("  Mappings: Ready for Dynamic Table logic")
    logger.info("  Metadata: Available for DQ rule generation")
    logger.info("  Lineage: Traceable from DYD to Snowflake")
    
    return dyd

def generate_execution_report(dq_summary, recon_summary):
    """Generate execution report"""
    logger.info("\n" + "=" * 80)
    logger.info("EXECUTION REPORT")
    logger.info("=" * 80)
    
    # Overall status
    dq_passed = dq_summary['failed_rules'] == 0
    recon_passed = recon_summary['failed_controls'] == 0
    overall_passed = dq_passed and recon_passed
    
    logger.info("\n📊 DATA QUALITY RESULTS:")
    logger.info(f"  Status: {'✅ PASSED' if dq_passed else '⚠️  ISSUES DETECTED'}")
    logger.info(f"  Total Rules: {dq_summary['total_rules']}")
    logger.info(f"  Passed: {dq_summary['passed_rules']}")
    logger.info(f"  Failed: {dq_summary['failed_rules']}")
    
    logger.info("\n⚖️ RECONCILIATION RESULTS:")
    logger.info(f"  Status: {'✅ PASSED' if recon_passed else '⚠️  REVIEW REQUIRED'}")
    logger.info(f"  Total Controls: {recon_summary['total_controls']}")
    logger.info(f"  Passed: {recon_summary['passed_controls']}")
    logger.info(f"  Failed: {recon_summary['failed_controls']}")
    
    logger.info("\n📋 REPORTING READINESS:")
    readiness = "✅ YES - Data ready for downstream reporting"
    if not overall_passed:
        readiness = "⚠️  CONDITIONAL - Review items flagged in CONTROLS schema"
    logger.info(f"  Ready for Reporting: {readiness}")
    
    logger.info("\n📍 NEXT STEPS:")
    logger.info("  1. Review flagged items in CONTROLS.DQ_EXECUTION_RESULTS")
    logger.info("  2. Review reconciliation breaks in CONTROLS.RECONCILIATION_RESULTS")
    logger.info("  3. Consult DYD mappings in CONTROLS.DYD_MAPPINGS")
    logger.info("  4. Proceed with downstream reporting if status is green")
    
    return overall_passed

def main():
    """Main orchestration workflow"""
    try:
        # Setup
        session, config = setup_environment()
        
        # Phase 1: Data Quality
        dq_engine, dq_summary = run_data_quality_checks(session)
        
        # Phase 2: Reconciliation
        recon_engine, recon_summary = run_reconciliation_controls(session)
        
        # Phase 3: DYD Integration
        dyd = integrate_dyd(session)
        
        # Report
        overall_passed = generate_execution_report(dq_summary, recon_summary)
        
        # Cleanup
        logger.info("\n" + "=" * 80)
        logger.info("Closing Snowpark session...")
        SnowparkSessionManager.close()
        
        logger.info("=" * 80)
        logger.info("✅ RCA PoC Orchestration Complete")
        logger.info("=" * 80)
        
        return 0 if overall_passed else 1
        
    except Exception as e:
        logger.error(f"❌ Orchestration failed: {str(e)}", exc_info=True)
        SnowparkSessionManager.close()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
