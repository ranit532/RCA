"""Backend support module for Streamlit RCA PoC."""
import logging
from typing import Dict, Optional, Any
import pandas as pd
from config.snowflake_config import DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

logger = logging.getLogger(__name__)

DEFAULT_DB = DEFAULT_CONFIG.database
DEFAULT_CONTROLS_SCHEMA = DEFAULT_CONFIG.schema_controls

# Snowflake session helpers

def initialize_snowflake_session() -> Optional[Any]:
    """Initialize and return a Snowflake connection."""
    try:
        return SnowparkSessionManager.initialize(DEFAULT_CONFIG)
    except Exception as exc:
        logger.error(f"Unable to initialize Snowflake session: {exc}")
        return None


def close_snowflake_session() -> None:
    """Close the active Snowpark session."""
    SnowparkSessionManager.close()


def _execute_sql(session: Any, sql: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    if session is None:
        return pd.DataFrame()
    try:
        cursor = session.cursor()
        cursor.execute(sql)
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        # Fetch all rows
        rows = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as exc:
        logger.error(f"SQL execution failed: {exc} -- SQL: {sql}")
        return pd.DataFrame()


def _table_exists(session: Any, schema: str, table: str) -> bool:
    """Check whether a table exists in the current database."""
    if session is None:
        return False
    sql = (
        f"SELECT COUNT(*) AS CNT FROM {DEFAULT_DB}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table.upper()}'"
    )
    df = _execute_sql(session, sql)
    return not df.empty and int(df.iloc[0]['CNT']) > 0


def get_overview_metrics(session: Any) -> Dict[str, int]:
    """Get overview counts for the dashboard."""
    metrics = {
        "dq_total": 0,
        "dq_passed": 0,
        "recon_total": 0,
        "recon_passed": 0,
        "dyd_mappings": 0,
        "dyd_metadata": 0,
        "dynamic_tables": 0,
        "dq_rule_count": 0,
        "recon_control_count": 0,
        "live": False,
    }

    if session is None:
        return metrics

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DQ_EXECUTION_RESULTS"):
        dq_sql = (
            f"SELECT COUNT(*) AS CNT, SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) AS PASSED, "
            f"COUNT(DISTINCT RULE_ID) AS RULE_COUNT "
            f"FROM {DEFAULT_CONTROLS_SCHEMA}.DQ_EXECUTION_RESULTS"
        )
        df = _execute_sql(session, dq_sql)
        if not df.empty:
            metrics["dq_total"] = int(df.iloc[0]['CNT'] or 0)
            metrics["dq_passed"] = int(df.iloc[0]['PASSED'] or 0)
            metrics["dq_rule_count"] = int(df.iloc[0]['RULE_COUNT'] or 0)

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "RECONCILIATION_RESULTS"):
        recon_sql = (
            f"SELECT COUNT(*) AS CNT, SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) AS PASSED, "
            f"COUNT(DISTINCT CONTROL_ID) AS CONTROL_COUNT "
            f"FROM {DEFAULT_CONTROLS_SCHEMA}.RECONCILIATION_RESULTS"
        )
        df = _execute_sql(session, recon_sql)
        if not df.empty:
            metrics["recon_total"] = int(df.iloc[0]['CNT'] or 0)
            metrics["recon_passed"] = int(df.iloc[0]['PASSED'] or 0)
            metrics["recon_control_count"] = int(df.iloc[0]['CONTROL_COUNT'] or 0)

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DYD_MAPPINGS"):
        df = _execute_sql(session, f"SELECT COUNT(*) AS CNT FROM {DEFAULT_CONTROLS_SCHEMA}.DYD_MAPPINGS")
        metrics["dyd_mappings"] = int(df.iloc[0]['CNT'] or 0) if not df.empty else 0

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DYD_METADATA"):
        df = _execute_sql(session, f"SELECT COUNT(*) AS CNT FROM {DEFAULT_CONTROLS_SCHEMA}.DYD_METADATA")
        metrics["dyd_metadata"] = int(df.iloc[0]['CNT'] or 0) if not df.empty else 0

    sql = (
        f"SELECT COUNT(*) AS CNT FROM {DEFAULT_DB}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA IN ('{DEFAULT_CONFIG.schema_curated.upper()}', '{DEFAULT_CONFIG.schema_analytics.upper()}')"
    )
    df = _execute_sql(session, sql)
    metrics["dynamic_tables"] = int(df.iloc[0]['CNT'] or 0) if not df.empty else 0

    metrics["live"] = True
    return metrics


def get_dq_results(session: Any, limit: int = 50) -> pd.DataFrame:
    """Fetch the latest data quality results."""
    if session is None or not _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DQ_EXECUTION_RESULTS"):
        return pd.DataFrame()

    sql = (
        f"SELECT RULE_ID, RULE_NAME, TABLE_NAME, RECORDS_TESTED, RECORDS_FAILED, FAILURE_RATE, PASSED, EXECUTED_AT "
        f"FROM {DEFAULT_CONTROLS_SCHEMA}.DQ_EXECUTION_RESULTS "
        f"ORDER BY EXECUTED_AT DESC LIMIT {limit}"
    )
    return _execute_sql(session, sql)


def get_recon_results(session: Any, limit: int = 50) -> pd.DataFrame:
    """Fetch the latest reconciliation results."""
    if session is None or not _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "RECONCILIATION_RESULTS"):
        return pd.DataFrame()

    sql = (
        f"SELECT CONTROL_ID, CONTROL_NAME, RECONCILIATION_TYPE, SOURCE_COUNT, TARGET_COUNT, VARIANCE, "
        f"VARIANCE_PERCENTAGE, PASSED, EXECUTED_AT "
        f"FROM {DEFAULT_CONTROLS_SCHEMA}.RECONCILIATION_RESULTS "
        f"ORDER BY EXECUTED_AT DESC LIMIT {limit}"
    )
    return _execute_sql(session, sql)


def get_audit_trail(session: Any, limit: int = 20) -> pd.DataFrame:
    """Fetch an audit trail summary for DQ and reconciliation executions."""
    if session is None:
        return pd.DataFrame()

    dfs = []
    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DQ_EXECUTION_RESULTS"):
        dq_sql = (
            f"SELECT RUN_ID, 'DQ Rules' AS TYPE, 'SYSTEM' AS EXECUTED_BY, EXECUTED_AT, "
            f"AVG(EXECUTION_TIME_SEC) AS DURATION_SEC, COUNT(*) AS RULES_CONTROLS, "
            f"SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) AS PASSED, "
            f"SUM(CASE WHEN PASSED THEN 0 ELSE 1 END) AS FAILED, "
            f"CASE WHEN SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) = COUNT(*) THEN 'SUCCESS' ELSE 'PARTIAL' END AS STATUS "
            f"FROM {DEFAULT_CONTROLS_SCHEMA}.DQ_EXECUTION_RESULTS "
            f"GROUP BY RUN_ID, EXECUTED_AT "
            f"ORDER BY EXECUTED_AT DESC LIMIT {limit}"
        )
        dfs.append(_execute_sql(session, dq_sql))

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "RECONCILIATION_RESULTS"):
        recon_sql = (
            f"SELECT RUN_ID, 'Reconciliation' AS TYPE, 'SYSTEM' AS EXECUTED_BY, EXECUTED_AT, "
            f"SUM(EXECUTION_TIME_SEC) AS DURATION_SEC, COUNT(*) AS RULES_CONTROLS, "
            f"SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) AS PASSED, "
            f"SUM(CASE WHEN PASSED THEN 0 ELSE 1 END) AS FAILED, "
            f"CASE WHEN SUM(CASE WHEN PASSED THEN 1 ELSE 0 END) = COUNT(*) THEN 'SUCCESS' ELSE 'PARTIAL' END AS STATUS "
            f"FROM {DEFAULT_CONTROLS_SCHEMA}.RECONCILIATION_RESULTS "
            f"GROUP BY RUN_ID, EXECUTED_AT "
            f"ORDER BY EXECUTED_AT DESC LIMIT {limit}"
        )
        dfs.append(_execute_sql(session, recon_sql))

    if not dfs:
        return pd.DataFrame()

    non_empty_dfs = [df for df in dfs if not df.empty]
    if not non_empty_dfs:
        return pd.DataFrame()

    result = pd.concat(non_empty_dfs, ignore_index=True)
    if result.empty:
        return result
    return result.sort_values(by="EXECUTED_AT", ascending=False).head(limit)


def get_dyd_status(session: Any) -> Dict[str, int]:
    """Fetch DYD integration counts."""
    status = {
        "mappings": 0,
        "metadata": 0,
        "dynamic_tables": 0,
        "dq_rules_generated": 0,
        "live": False,
    }
    if session is None:
        return status

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DYD_MAPPINGS"):
        df = _execute_sql(session, f"SELECT COUNT(*) AS CNT FROM {DEFAULT_CONTROLS_SCHEMA}.DYD_MAPPINGS")
        if not df.empty and "CNT" in df.columns:
            status["mappings"] = int(df.iloc[0, 0] or 0)

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DYD_METADATA"):
        df = _execute_sql(session, f"SELECT COUNT(*) AS CNT FROM {DEFAULT_CONTROLS_SCHEMA}.DYD_METADATA")
        if not df.empty and "CNT" in df.columns:
            status["metadata"] = int(df.iloc[0, 0] or 0)

    sql = (
        f"SELECT COUNT(*) AS CNT FROM {DEFAULT_DB}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA IN ('{DEFAULT_CONFIG.schema_curated.upper()}', '{DEFAULT_CONFIG.schema_analytics.upper()}')"
    )
    df = _execute_sql(session, sql)
    if not df.empty and "CNT" in df.columns:
        status["dynamic_tables"] = int(df.iloc[0, 0] or 0)

    if _table_exists(session, DEFAULT_CONTROLS_SCHEMA, "DQ_EXECUTION_RESULTS"):
        df = _execute_sql(session, f"SELECT COUNT(DISTINCT RULE_ID) AS CNT FROM {DEFAULT_CONTROLS_SCHEMA}.DQ_EXECUTION_RESULTS")
        if not df.empty and "CNT" in df.columns:
            status["dq_rules_generated"] = int(df.iloc[0, 0] or 0)

    status["live"] = True
    return status


# ---------------------------------------------------------------------------
# Snowflake Cortex AI
# ---------------------------------------------------------------------------

CORTEX_DEFAULT_MODEL = "mistral-large2"


def get_cortex_insight(session: Any, prompt: str, model: str = CORTEX_DEFAULT_MODEL) -> str:
    """Call Snowflake Cortex COMPLETE and return the generated text."""
    if session is None:
        return "Cortex AI is unavailable: no active Snowflake session."
    try:
        cursor = session.cursor()
        cursor.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS RESPONSE",
            (model, prompt),
        )
        row = cursor.fetchone()
        cursor.close()
        if row and row[0]:
            return str(row[0]).strip()
        return "Cortex AI returned an empty response."
    except Exception as exc:
        logger.error(f"Cortex AI call failed: {exc}")
        return f"Cortex AI is not available in this Snowflake account or region. Error: {exc}"


def build_dq_summary_prompt(metrics: Dict[str, int], dq_df: pd.DataFrame, recon_df: pd.DataFrame) -> str:
    """Build a structured prompt summarising current DQ and reconciliation metrics."""
    dq_total = metrics.get("dq_total", 0)
    dq_passed = metrics.get("dq_passed", 0)
    recon_total = metrics.get("recon_total", 0)
    recon_passed = metrics.get("recon_passed", 0)

    failed_rules = ""
    if not dq_df.empty and "Rule Name" in dq_df.columns and "Status" in dq_df.columns:
        failed = dq_df[dq_df["Status"] == "FAIL"]
        if not failed.empty:
            failed_rules = ", ".join(failed["Rule Name"].tolist())

    failed_controls = ""
    if not recon_df.empty and "Control Name" in recon_df.columns and "Status" in recon_df.columns:
        failed = recon_df[recon_df["Status"] != "PASS"]
        if not failed.empty:
            failed_controls = ", ".join(failed["Control Name"].tolist())

    prompt = (
        "You are a financial data quality analyst. Based on the following RCA (Risk Controls & Analytics) "
        "data quality metrics for an asset management platform, provide a concise executive summary "
        "(3-5 sentences) covering: overall data health, key risks, and one actionable recommendation.\n\n"
        f"Data Quality Rules: {dq_passed}/{dq_total} passed.\n"
        f"Reconciliation Controls: {recon_passed}/{recon_total} passed.\n"
    )
    if failed_rules:
        prompt += f"Failed DQ rules: {failed_rules}.\n"
    if failed_controls:
        prompt += f"Failed/review reconciliation controls: {failed_controls}.\n"
    prompt += "\nExecutive Summary:"
    return prompt
