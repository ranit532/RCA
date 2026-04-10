import { useEffect, useState } from 'react';
import {
  getOverview,
  getDQResults,
  getReconciliationResults,
  getAuditTrail,
  getDYDStatus,
} from './api.js';

function Card({ title, value, subtitle }) {
  return (
    <div className="card">
      <div className="card-title">{title}</div>
      <div className="card-value">{value}</div>
      {subtitle && <div className="card-subtitle">{subtitle}</div>}
    </div>
  );
}

function Table({ title, columns, data }) {
  return (
    <section className="table-section">
      <h2>{title}</h2>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, index) => (
              <tr key={index}>
                {columns.map((col) => (
                  <td key={col}>{row[col] ?? ''}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function App() {
  const [overview, setOverview] = useState(null);
  const [dq, setDQ] = useState([]);
  const [recon, setRecon] = useState([]);
  const [audit, setAudit] = useState([]);
  const [dyd, setDyd] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      setLoading(true);
      try {
        const [overviewData, dqData, reconData, auditData, dydData] = await Promise.all([
          getOverview(),
          getDQResults(),
          getReconciliationResults(),
          getAuditTrail(),
          getDYDStatus(),
        ]);
        setOverview(overviewData);
        setDQ(dqData);
        setRecon(reconData);
        setAudit(auditData);
        setDyd(dydData);
      } catch (err) {
        console.error('Dashboard load error:', err);
        setError(err.message || String(err));
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  return (
    <div className="app-shell">
      <header className="app-header">
        <div>
          <h1>RCA PoC Dashboard</h1>
          <p>React frontend with Python/Snowflake backend</p>
        </div>
      </header>

      <main>
        {loading && <div className="status-message">Loading dashboard data...</div>}
        {error && <div className="status-message error">{error}</div>}

        {!loading && !error && (
          <>
            <section className="overview-grid">
              <Card
                title="Data Quality Rules"
                value={overview ? `${overview.dq_passed} / ${overview.dq_total}` : 'N/A'}
                subtitle="Passed / Total"
              />
              <Card
                title="Reconciliation Controls"
                value={overview ? `${overview.recon_passed} / ${overview.recon_total}` : 'N/A'}
                subtitle="Passed / Total"
              />
              <Card
                title="DYD Mappings"
                value={dyd ? dyd.mappings : 'N/A'}
                subtitle="Total mappings loaded"
              />
              <Card
                title="Curated + Analytics Tables"
                value={dyd ? dyd.dynamic_tables : 'N/A'}
                subtitle="Tables detected"
              />
            </section>

            <Table
              title="Data Quality Results"
              columns={['RULE_ID', 'RULE_NAME', 'TABLE_NAME', 'RECORDS_TESTED', 'RECORDS_FAILED', 'FAILURE_RATE', 'PASSED', 'EXECUTED_AT']}
              data={dq}
            />

            <Table
              title="Reconciliation Results"
              columns={['CONTROL_ID', 'CONTROL_NAME', 'RECONCILIATION_TYPE', 'SOURCE_COUNT', 'TARGET_COUNT', 'VARIANCE', 'VARIANCE_PERCENTAGE', 'PASSED', 'EXECUTED_AT']}
              data={recon}
            />

            <Table
              title="Audit Trail"
              columns={['RUN_ID', 'TYPE', 'EXECUTED_BY', 'EXECUTION_TIME', 'DURATION_SEC', 'RULES_CONTROLS', 'PASSED', 'FAILED', 'STATUS']}
              data={audit}
            />
          </>
        )}
      </main>
    </div>
  );
}

export default App;
