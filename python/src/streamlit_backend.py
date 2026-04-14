"""Backend support module for Streamlit RCA PoC."""
import logging
import re
from typing import Dict, Optional, Any
import pandas as pd
from config.snowflake_config import DEFAULT_CONFIG
from src.snowpark_dq.session_manager import SnowparkSessionManager

logger = logging.getLogger(__name__)

DEFAULT_DB = DEFAULT_CONFIG.database
DEFAULT_CONTROLS_SCHEMA = DEFAULT_CONFIG.schema_controls
LAST_CONNECTION_ERROR = ""

# Snowflake session helpers

def initialize_snowflake_session() -> Optional[Any]:
    """Initialize and return a Snowflake connection."""
    global LAST_CONNECTION_ERROR
    try:
        session = SnowparkSessionManager.initialize(DEFAULT_CONFIG)
        LAST_CONNECTION_ERROR = ""
        return session
    except Exception as exc:
        LAST_CONNECTION_ERROR = str(exc)
        logger.error(f"Unable to initialize Snowflake session: {exc}")
        return None


def get_last_connection_error() -> str:
    """Return the most recent Snowflake connection error, if any."""
    return LAST_CONNECTION_ERROR


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

# Whitelisted tables for safe Cortex table-grounded Q&A.
ALLOWED_CORTEX_TABLES = {
    "DIM_PARTY": "CURATED.DIM_PARTY",
    "DIM_ACCOUNT": "CURATED.DIM_ACCOUNT",
    "DIM_INSTRUMENT": "CURATED.DIM_INSTRUMENT",
    "FCT_TRADE": "CURATED.FCT_TRADE",
    "DQ_EXECUTION_RESULTS": "CONTROLS.DQ_EXECUTION_RESULTS",
    "RECONCILIATION_RESULTS": "CONTROLS.RECONCILIATION_RESULTS",
    "DYD_MAPPINGS": "CONTROLS.DYD_MAPPINGS",
    "DYD_METADATA": "CONTROLS.DYD_METADATA",
}


def _detect_allowed_table(question: str) -> Optional[str]:
    """Detect a whitelisted table name from a natural language question."""
    q = question.upper()
    if not q.strip():
        return None

    # Direct fully qualified references.
    for table_name in ALLOWED_CORTEX_TABLES.values():
        if table_name in q:
            return table_name

    # Alias/simple name references.
    for alias, table_name in ALLOWED_CORTEX_TABLES.items():
        if re.search(rf"\b{re.escape(alias)}\b", q):
            return table_name

    return None


def get_table_preview_for_question(session: Any, question: str, limit: int = 20) -> Dict[str, Any]:
    """Return a safe table preview for table-aware Cortex questions."""
    response = {
        "table": None,
        "data": pd.DataFrame(),
        "error": "",
    }
    if session is None:
        response["error"] = "No active Snowflake session."
        return response

    table_name = _detect_allowed_table(question)
    if table_name is None:
        return response

    try:
        schema, table = table_name.split(".", 1)
        if not _table_exists(session, schema, table):
            response["table"] = table_name
            response["error"] = f"Table {table_name} was not found in the current database."
            return response

        safe_limit = min(max(int(limit), 1), 100)
        df = _execute_sql(session, f"SELECT * FROM {table_name} LIMIT {safe_limit}")
        response["table"] = table_name
        response["data"] = df
        return response
    except Exception as exc:
        logger.error(f"Table preview fetch failed: {exc}")
        response["table"] = table_name
        response["error"] = f"Unable to fetch table preview for {table_name}: {exc}"
        return response


def build_table_grounded_prompt(question: str, table_name: str, df: pd.DataFrame) -> str:
    """Build a Cortex prompt grounded on fetched table rows."""
    if df.empty:
        return (
            "You are a financial data analyst. The user asked a table-specific question, "
            f"but no rows were returned for {table_name}. Explain what this means and provide "
            "one safe SQL query to validate the table content.\n\n"
            f"User question: {question}\n"
        )

    preview_rows = df.head(10).to_dict(orient="records")
    prompt = (
        "You are a financial data governance analyst. Answer using only the table preview below. "
        "If the preview is insufficient, explicitly say what additional data is needed.\n\n"
        f"Table: {table_name}\n"
        f"Columns: {', '.join(df.columns.tolist())}\n"
        f"Row count in preview: {len(df)}\n"
        f"Preview rows: {preview_rows}\n\n"
        f"User question: {question}\n\n"
        "Provide a concise response with: findings, caveats, and next SQL check."
    )
    return prompt


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
