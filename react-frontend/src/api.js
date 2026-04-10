const API_BASE = import.meta.env.VITE_API_URL || '/api';

async function fetchJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  return await response.json();
}

export async function getOverview() {
  return fetchJson('/overview');
}

export async function getDQResults() {
  return fetchJson('/dq');
}

export async function getReconciliationResults() {
  return fetchJson('/reconciliation');
}

export async function getAuditTrail() {
  return fetchJson('/audit');
}

export async function getDYDStatus() {
  return fetchJson('/dyd');
}
