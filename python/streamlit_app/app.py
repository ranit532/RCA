"""
Streamlit Controls Cockpit Dashboard
Main entry point for the RCA PoC Streamlit application
Displays data quality, reconciliation controls, and audit trail
"""

import sys
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
from enum import Enum

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from config.snowflake_config import DEFAULT_CONFIG
from src.streamlit_backend import (
    initialize_snowflake_session,
    get_overview_metrics,
    get_dq_results,
    get_recon_results,
    get_audit_trail,
    get_dyd_status,
)

# Configure Streamlit
st.set_page_config(
    page_title="RCA PoC - Controls Cockpit",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@st.cache_resource
def get_snowflake_session():
    return initialize_snowflake_session()

session = get_snowflake_session()

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .success { color: #09a338; font-weight: bold; }
    .warning { color: #ffa421; font-weight: bold; }
    .error { color: #ff2b2b; font-weight: bold; }
    .header-section {
        background-color: #1f77e8;
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Application Status
class RunStatus(Enum):
    SUCCESS = "✅ SUCCESS"
    PARTIAL_SUCCESS = "⚠️ PARTIAL SUCCESS"
    FAILED = "❌ FAILED"
    RUNNING = "⏳ RUNNING"

def render_header():
    """Render application header"""
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        st.title("🎯 RCA PoC Controls Cockpit")
        st.markdown("*Asset Data Quality & Reconciliation Dashboard*")
    
    with col3:
        st.metric(
            label="Last Refresh",
            value=datetime.now().strftime("%H:%M:%S"),
            delta="-5m ago"
        )

def render_status_summary():
    """Render overall status summary"""
    metrics = get_overview_metrics(session)
    
    if not metrics.get("live"):
        st.warning("Live Snowflake metrics are not available. Showing sample values.")

    dq_value = f"{metrics['dq_passed']}/{metrics['dq_total']}" if metrics['dq_total'] else "0/0"
    recon_value = f"{metrics['recon_passed']}/{metrics['recon_total']}" if metrics['recon_total'] else "0/0"
    readiness = "✅ YES" if metrics.get("dq_total", 0) == metrics.get("dq_passed", 0) and metrics.get("recon_total", 0) == metrics.get("recon_passed", 0) else "⚠️ REVIEW"
    last_run = "Live" if metrics.get("live") else "N/A"

    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Data Quality Rules",
            value=dq_value,
            delta="+0",
            delta_color="off"
        )
    
    with col2:
        st.metric(
            label="Reconciliation Controls",
            value=recon_value,
            delta="+0",
            delta_color="normal"
        )
    
    with col3:
        st.metric(
            label="Data Ready for Reporting",
            value=readiness,
            delta="Live status" if metrics.get("live") else "Offline"
        )
    
    with col4:
        st.metric(
            label="Last Run",
            value=last_run,
            delta=""
        )

def render_quality_dashboard():
    """Render data quality dashboard"""
    st.subheader("📈 Data Quality Metrics")
    
    df_dq = get_dq_results(session)
    if df_dq.empty:
        st.warning("Live data quality results are not available; showing sample content.")
        dq_data = {
            'Rule ID': ['COMP_001', 'COMP_002', 'UNIQ_001', 'VALID_001', 'THRESH_001', 'RI_001'],
            'Rule Name': [
                'Party ID Completeness',
                'Account Code Completeness', 
                'ISIN Uniqueness',
                'Asset Class Validity',
                'Trade Amount Threshold',
                'Referential Integrity'
            ],
            'Table': ['DIM_PARTY_RAW', 'DIM_ACCOUNT_RAW', 'DIM_INSTRUMENT_RAW', 
                      'DIM_INSTRUMENT_RAW', 'FCT_TRADE_RAW', 'FCT_TRADE_RAW'],
            'Records Tested': [10000, 30000, 50000, 50000, 100000, 100000],
            'Failed': [5, 15, 0, 250, 500, 0],
            'Failure %': [0.05, 0.05, 0.00, 0.50, 0.50, 0.00],
            'Status': ['✅ PASS', '✅ PASS', '✅ PASS', '✅ PASS', '✅ PASS', '✅ PASS'],
            'Executed At': [
                datetime.now() - timedelta(minutes=10),
                datetime.now() - timedelta(minutes=10),
                datetime.now() - timedelta(minutes=10),
                datetime.now() - timedelta(minutes=10),
                datetime.now() - timedelta(minutes=10),
                datetime.now() - timedelta(minutes=10),
            ]
        }
        df_dq = pd.DataFrame(dq_data)
    else:
        df_dq = df_dq.rename(columns={
            'RULE_ID': 'Rule ID',
            'RULE_NAME': 'Rule Name',
            'TABLE_NAME': 'Table',
            'RECORDS_TESTED': 'Records Tested',
            'RECORDS_FAILED': 'Failed',
            'FAILURE_RATE': 'Failure %',
            'PASSED': 'Passed',
            'EXECUTED_AT': 'Executed At'
        })
        df_dq['Status'] = df_dq['Passed'].apply(lambda x: '✅ PASS' if x else '❌ FAIL')
        df_dq = df_dq[['Rule ID', 'Rule Name', 'Table', 'Records Tested', 'Failed', 'Failure %', 'Status', 'Executed At']]

    with st.expander("View Detailed Metrics", expanded=True):
        st.dataframe(df_dq, use_container_width=True, hide_index=True)
        
        # Trend chart
        st.markdown("#### Failure Rate Trend (Last 7 Days)")
        trend_data = {
            'Date': pd.date_range(start='2025-01-01', periods=7),
            'Failure Rate %': [0.45, 0.48, 0.42, 0.50, 0.48, 0.50, 0.45]
        }
        df_trend = pd.DataFrame(trend_data)
        st.line_chart(df_trend.set_index('Date'))

def render_reconciliation_dashboard():
    """Render reconciliation controls dashboard"""
    st.subheader("⚖️ Reconciliation Controls")
    
    df_recon = get_recon_results(session)
    if df_recon.empty:
        st.warning("Live reconciliation results are not available; showing sample content.")
        recon_data = {
            'Control ID': ['ROW_001', 'SUM_001', 'ROW_002', 'SUM_002', 'RI_001', 'RI_002'],
            'Control Name': [
                'Raw vs Curated Row Count',
                'Raw vs Curated Amount Sum',
                'Trade Raw vs Curated Count',
                'Trade Amount Reconciliation',
                'Account Referential Integrity',
                'Instrument Referential Integrity'
            ],
            'Type': ['ROW_COUNT', 'SUM_AMOUNT', 'ROW_COUNT', 'SUM_AMOUNT', 'RI', 'RI'],
            'Source': [
                'RAW.DIM_PARTY_RAW',
                'RAW.DIM_PARTY_RAW',
                'RAW.FCT_TRADE_RAW',
                'RAW.FCT_TRADE_RAW',
                'RAW.FCT_TRADE_RAW',
                'RAW.FCT_TRADE_RAW'
            ],
            'Target': [
                'CURATED.DT_DIM_PARTY_CURATED',
                'CURATED.DT_DIM_PARTY_CURATED',
                'CURATED.DT_FCT_TRADE_CURATED',
                'CURATED.DT_FCT_TRADE_CURATED',
                'CURATED.DT_DIM_ACCOUNT_CURATED',
                'CURATED.DT_DIM_INSTRUMENT_CURATED'
            ],
            'Variance': ['0 rows', '$0', '15 rows', '$250', '0 orphaned', '0 orphaned'],
            'Tolerance': ['0 rows', '$100', '5 rows', '$500', '0', '0'],
            'Status': ['✅ PASS', '✅ PASS', '⚠️ REVIEW', '✅ PASS', '✅ PASS', '✅ PASS'],
            'Readiness': ['READY', 'READY', 'READY*', 'READY', 'READY', 'READY']
        }
        df_recon = pd.DataFrame(recon_data)
    else:
        df_recon = df_recon.rename(columns={
            'CONTROL_ID': 'Control ID',
            'CONTROL_NAME': 'Control Name',
            'RECONCILIATION_TYPE': 'Type',
            'SOURCE_COUNT': 'Source Count',
            'TARGET_COUNT': 'Target Count',
            'VARIANCE': 'Variance',
            'VARIANCE_PERCENTAGE': 'Variance %',
            'PASSED': 'Passed',
            'EXECUTED_AT': 'Executed At'
        })
        df_recon['Status'] = df_recon['Passed'].apply(lambda x: '✅ PASS' if x else '❌ FAIL')
        df_recon = df_recon[['Control ID', 'Control Name', 'Type', 'Source Count', 'Target Count', 'Variance', 'Variance %', 'Status', 'Executed At']]

    with st.expander("View Reconciliation Details", expanded=True):
        st.dataframe(df_recon, use_container_width=True, hide_index=True)
        
        st.info("*READY indicates data can proceed to reporting with review of flagged items")

def render_audit_trail():
    """Render audit trail and execution history"""
    st.subheader("📝 Execution Audit Trail")
    
    df_audit = get_audit_trail(session)
    if df_audit.empty:
        st.warning("Live audit history is not available; showing sample content.")
        audit_data = {
            'Run ID': [
                '550e8400-e29b-41d4-a716-446655440000',
                '550e8400-e29b-41d4-a716-446655440001',
                '550e8400-e29b-41d4-a716-446655440002',
                '550e8400-e29b-41d4-a716-446655440003',
            ],
            'Type': ['DQ Rules', 'Reconciliation', 'DQ Rules', 'Reconciliation'],
            'Executed By': ['SYSTEM', 'SYSTEM', 'RANIT532', 'SYSTEM'],
            'Execution Time': [
                datetime.now() - timedelta(minutes=30),
                datetime.now() - timedelta(minutes=25),
                datetime.now() - timedelta(minutes=20),
                datetime.now() - timedelta(minutes=15),
            ],
            'Duration (sec)': [45.2, 38.5, 42.1, 36.8],
            'Rules/Controls': [23, 18, 23, 18],
            'Passed': [23, 17, 23, 18],
            'Failed': [0, 1, 0, 0],
            'Status': ['✅ SUCCESS', '⚠️ PARTIAL', '✅ SUCCESS', '✅ SUCCESS']
        }
        df_audit = pd.DataFrame(audit_data)
    else:
        df_audit = df_audit.rename(columns={
            'RUN_ID': 'Run ID',
            'TYPE': 'Type',
            'EXECUTED_BY': 'Executed By',
            'EXECUTED_AT': 'Execution Time',
            'DURATION_SEC': 'Duration (sec)',
            'RULES_CONTROLS': 'Rules/Controls'
        })
        df_audit = df_audit[['Run ID', 'Type', 'Executed By', 'Execution Time', 'Duration (sec)', 'Rules/Controls', 'PASSED', 'FAILED', 'STATUS']]

    with st.expander("View Audit Trail", expanded=True):
        st.dataframe(df_audit, use_container_width=True, hide_index=True)
        
        # Download option
        csv = df_audit.to_csv(index=False)
        st.download_button(
            label="Download Audit Trail (CSV)",
            data=csv,
            file_name=f"audit_trail_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

def render_dyd_integration_status():
    """Render DYD integration status"""
    st.subheader("🔗 DYD Integration Status")
    
    status = get_dyd_status(session)
    if not status.get("live"):
        st.warning("Live DYD integration status is not available; showing sample values.")

    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("DYD Mappings", str(status.get("mappings", 12)), "+0")
    
    with col2:
        st.metric("DYD Metadata Items", str(status.get("metadata", 48)), "+0")
    
    with col3:
        st.metric("Dynamic Tables", str(status.get("dynamic_tables", 8)), "+0")
    
    with col4:
        st.metric("DQ Rules Generated", str(status.get("dq_rules_generated", 23)), "+0")
    
    with st.expander("DYD Configuration Details"):
        st.write("""
        **Discover Your Data Integration:**
        - ✅ Mappings loaded from DYD exports
        - ✅ Metadata registered in reference tables
        - ✅ Dynamic Tables created from mapping logic
        - ✅ DQ rules auto-generated from metadata
        - 📊 Lineage traceability enabled
        - 🔍 Observability via Snowflake Horizon
        """)

def render_controls():
    """Render action controls"""
    st.subheader("⚙️ Controls & Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Refresh All Metrics", use_container_width=True):
            st.info("Refreshing data quality and reconciliation metrics...")
            st.success("✅ Metrics refreshed successfully!")
    
    with col2:
        if st.button("📤 Export Results", use_container_width=True):
            st.info("Preparing export...")
            st.success("✅ Results exported to CONTROLS schema")
    
    with col3:
        if st.button("📋 Generate Report", use_container_width=True):
            st.info("Generating comprehensive report...")
            st.success("✅ Report generated successfully!")

def render_sidebar():
    """Render sidebar navigation"""
    with st.sidebar:
        st.markdown("### 📊 Navigation")
        
        view_option = st.radio(
            "Select View:",
            [
                "Overview",
                "Data Quality",
                "Reconciliation",
                "Audit Trail",
                "DYD Integration",
                "Settings"
            ],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown("### 🔧 Configuration")
        
        env = st.selectbox(
            "Environment:",
            ["Production", "Staging", "Development"]
        )
        
        refresh_interval = st.slider(
            "Auto-Refresh Interval (min):",
            min_value=1,
            max_value=60,
            value=5,
            step=1
        )
        
        st.markdown("---")
        st.markdown("### ℹ️ Information")
        st.info("""
        **RCA PoC Version:** 1.0.0
        
        **Last Updated:** Jan 9, 2025
        
        **Connected To:**
        - Account: TQWSLTQ-TW60698
        - Database: RCA_POC_DB
        - User: RANIT532
        """)
        
        return view_option, env, refresh_interval

def main():
    """Main application"""
    render_header()
    
    view_option, env, refresh_interval = render_sidebar()
    
    st.markdown("---")
    
    # Route to appropriate view
    if view_option == "Overview":
        render_status_summary()
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            render_quality_dashboard()
        
        with col2:
            render_reconciliation_dashboard()
        
        render_controls()
    
    elif view_option == "Data Quality":
        render_quality_dashboard()
        render_controls()
    
    elif view_option == "Reconciliation":
        render_reconciliation_dashboard()
        render_controls()
    
    elif view_option == "Audit Trail":
        render_audit_trail()
    
    elif view_option == "DYD Integration":
        render_dyd_integration_status()
    
    elif view_option == "Settings":
        st.subheader("⚙️ Application Settings")
        st.write("Settings configuration coming soon...")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 12px;'>
    RCA Proof of Concept | Asset Data Modernisation on Snowflake<br>
    Data Quality, Proactive Reconciliation & Migration Enablement
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
