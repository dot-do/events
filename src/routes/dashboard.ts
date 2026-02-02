/**
 * Admin Dashboard Route Handler
 *
 * Serves a minimal admin dashboard UI at /dashboard showing:
 * - Recent events with real-time updates
 * - Subscription status and health
 * - CDC/Catalog state
 * - Webhook activity by provider
 */

import type { Env } from '../env'
import { authCorsHeaders } from '../utils'

/**
 * Generate dashboard HTML with inline styles and JavaScript.
 * Uses existing API endpoints for data fetching.
 */
function generateDashboardHTML(origin: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>events.do - Admin Dashboard</title>
  <style>
    :root {
      --bg: #0a0a0a;
      --bg-card: #111;
      --border: #222;
      --text: #e5e5e5;
      --text-muted: #888;
      --accent: #3b82f6;
      --success: #22c55e;
      --warning: #f59e0b;
      --error: #ef4444;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      line-height: 1.5;
    }
    .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 16px 0;
      border-bottom: 1px solid var(--border);
      margin-bottom: 24px;
    }
    h1 { font-size: 1.5rem; font-weight: 600; }
    h1 span { color: var(--accent); }
    .user-info { color: var(--text-muted); font-size: 0.875rem; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      gap: 20px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px;
    }
    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--border);
    }
    .card-title { font-size: 0.875rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); }
    .card-value { font-size: 2rem; font-weight: 700; }
    .stat-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--border); }
    .stat-row:last-child { border-bottom: none; }
    .stat-label { color: var(--text-muted); }
    .stat-value { font-weight: 500; font-variant-numeric: tabular-nums; }
    .stat-value.success { color: var(--success); }
    .stat-value.warning { color: var(--warning); }
    .stat-value.error { color: var(--error); }
    .events-table { width: 100%; border-collapse: collapse; font-size: 0.8125rem; }
    .events-table th, .events-table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
    .events-table th { color: var(--text-muted); font-weight: 500; text-transform: uppercase; font-size: 0.75rem; }
    .events-table tr:hover { background: rgba(59, 130, 246, 0.05); }
    .event-type { font-family: monospace; color: var(--accent); }
    .event-time { color: var(--text-muted); font-variant-numeric: tabular-nums; }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 500;
    }
    .badge-success { background: rgba(34, 197, 94, 0.2); color: var(--success); }
    .badge-warning { background: rgba(245, 158, 11, 0.2); color: var(--warning); }
    .badge-error { background: rgba(239, 68, 68, 0.2); color: var(--error); }
    .badge-info { background: rgba(59, 130, 246, 0.2); color: var(--accent); }
    .loading { color: var(--text-muted); font-style: italic; }
    .error-msg { color: var(--error); }
    .refresh-btn {
      background: transparent;
      border: 1px solid var(--border);
      color: var(--text);
      padding: 6px 12px;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.8125rem;
    }
    .refresh-btn:hover { background: var(--bg-card); border-color: var(--accent); }
    .refresh-btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .tabs { display: flex; gap: 8px; margin-bottom: 16px; }
    .tab {
      background: transparent;
      border: 1px solid var(--border);
      color: var(--text-muted);
      padding: 8px 16px;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.8125rem;
    }
    .tab:hover { border-color: var(--accent); color: var(--text); }
    .tab.active { background: var(--accent); border-color: var(--accent); color: white; }
    .full-width { grid-column: 1 / -1; }
    .scroll-container { max-height: 400px; overflow-y: auto; }
    pre { font-family: monospace; font-size: 0.75rem; white-space: pre-wrap; word-break: break-all; background: rgba(0,0,0,0.3); padding: 8px; border-radius: 4px; max-height: 100px; overflow-y: auto; }
    .empty-state { text-align: center; padding: 40px; color: var(--text-muted); }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1><span>events</span>.do</h1>
      <div style="display: flex; gap: 16px; align-items: center;">
        <span class="user-info" id="user-info">Loading...</span>
        <button class="refresh-btn" onclick="refreshAll()" id="refresh-btn">Refresh</button>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <div class="card-header">
          <span class="card-title">Events (24h)</span>
        </div>
        <div id="events-stats" class="loading">Loading...</div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">Subscriptions</span>
        </div>
        <div id="subscriptions-stats" class="loading">Loading...</div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">CDC / Catalog</span>
        </div>
        <div id="catalog-stats" class="loading">Loading...</div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">Webhooks</span>
        </div>
        <div id="webhooks-stats" class="loading">Loading...</div>
      </div>
    </div>

    <div class="card full-width">
      <div class="card-header">
        <span class="card-title">Recent Events</span>
        <div class="tabs">
          <button class="tab active" data-source="all" onclick="setSource('all')">All</button>
          <button class="tab" data-source="events" onclick="setSource('events')">Ingest</button>
          <button class="tab" data-source="tail" onclick="setSource('tail')">Tail</button>
        </div>
      </div>
      <div class="scroll-container">
        <table class="events-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Type</th>
              <th>Source</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody id="events-body">
            <tr><td colspan="4" class="loading">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="grid" style="margin-top: 20px;">
      <div class="card">
        <div class="card-header">
          <span class="card-title">Active Subscriptions</span>
        </div>
        <div class="scroll-container" id="subscriptions-list" style="max-height: 300px;">
          <div class="loading">Loading...</div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">CDC Tables</span>
        </div>
        <div class="scroll-container" id="cdc-tables" style="max-height: 300px;">
          <div class="loading">Loading...</div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const API_BASE = '${origin}';
    let currentSource = 'all';
    let refreshing = false;

    async function fetchJSON(path) {
      const res = await fetch(API_BASE + path, { credentials: 'include' });
      if (!res.ok) throw new Error(\`HTTP \${res.status}\`);
      return res.json();
    }

    function formatTime(ts) {
      const d = new Date(ts);
      return d.toLocaleTimeString('en-US', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
    }

    function formatRelativeTime(ts) {
      const diff = Date.now() - new Date(ts).getTime();
      if (diff < 60000) return Math.floor(diff / 1000) + 's ago';
      if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
      if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
      return Math.floor(diff / 86400000) + 'd ago';
    }

    function escapeHtml(str) {
      if (typeof str !== 'string') return str;
      return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function getEventDetails(event) {
      if (event.webhook?.provider) return \`<span class="badge badge-info">\${escapeHtml(event.webhook.provider)}</span>\`;
      if (event.provider) return \`<span class="badge badge-info">\${escapeHtml(event.provider)}</span>\`;
      if (event.do?.class) return \`<span class="badge badge-info">\${escapeHtml(event.do.class)}</span>\`;
      if (event.collection) return \`<span class="badge badge-info">\${escapeHtml(event.collection)}</span>\`;
      if (event.method) return \`<span class="badge badge-info">\${escapeHtml(event.method)}</span>\`;
      return '';
    }

    function getEventSource(event) {
      if (event.webhook?.provider || event.provider) return 'webhook';
      if (event.type?.startsWith('collection.')) return 'cdc';
      if (event.type?.startsWith('rpc.')) return 'rpc';
      if (event.type?.startsWith('worker.')) return 'tail';
      return 'ingest';
    }

    async function loadEventsStats() {
      const el = document.getElementById('events-stats');
      try {
        const data = await fetchJSON('/events?limit=1&source=all');
        const meta = data.meta || {};
        el.innerHTML = \`
          <div class="stat-row">
            <span class="stat-label">Events scanned</span>
            <span class="stat-value">\${(meta.scan?.eventsScanned || 0).toLocaleString()}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Files read</span>
            <span class="stat-value">\${meta.scan?.filesRead || 0}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Bytes read</span>
            <span class="stat-value">\${((meta.scan?.bytesRead || 0) / 1024).toFixed(1)} KB</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Query time</span>
            <span class="stat-value">\${meta.query?.durationMs || 0}ms</span>
          </div>
        \`;
      } catch (e) {
        el.innerHTML = '<span class="error-msg">Failed to load</span>';
      }
    }

    async function loadSubscriptionsStats() {
      const el = document.getElementById('subscriptions-stats');
      try {
        const data = await fetchJSON('/subscriptions/list');
        const subs = data.subscriptions || [];
        const active = subs.filter(s => s.active).length;
        const inactive = subs.length - active;
        el.innerHTML = \`
          <div class="stat-row">
            <span class="stat-label">Total</span>
            <span class="stat-value">\${subs.length}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Active</span>
            <span class="stat-value success">\${active}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Inactive</span>
            <span class="stat-value \${inactive > 0 ? 'warning' : ''}">\${inactive}</span>
          </div>
        \`;
        loadSubscriptionsList(subs);
      } catch (e) {
        el.innerHTML = '<span class="error-msg">Failed to load</span>';
      }
    }

    function loadSubscriptionsList(subs) {
      const el = document.getElementById('subscriptions-list');
      if (!subs.length) {
        el.innerHTML = '<div class="empty-state">No subscriptions</div>';
        return;
      }
      el.innerHTML = subs.slice(0, 20).map(s => \`
        <div class="stat-row">
          <div>
            <span class="event-type">\${escapeHtml(s.pattern)}</span>
            <br><span style="color: var(--text-muted); font-size: 0.75rem;">\${escapeHtml(s.workerId)} / \${escapeHtml(s.rpcMethod)}</span>
          </div>
          <span class="badge \${s.active ? 'badge-success' : 'badge-warning'}">\${s.active ? 'active' : 'inactive'}</span>
        </div>
      \`).join('');
    }

    async function loadCatalogStats() {
      const el = document.getElementById('catalog-stats');
      try {
        const nsData = await fetchJSON('/catalog/namespaces');
        const namespaces = nsData.namespaces || [];
        let totalTables = 0;
        for (const ns of namespaces) {
          try {
            const tablesData = await fetchJSON(\`/catalog/tables?namespace=\${encodeURIComponent(ns)}\`);
            totalTables += (tablesData.tables || []).length;
          } catch (e) {
            console.warn(\`[dashboard] Failed to fetch tables for namespace \${ns}:\`, e);
          }
        }
        el.innerHTML = \`
          <div class="stat-row">
            <span class="stat-label">Namespaces</span>
            <span class="stat-value">\${namespaces.length}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Tables</span>
            <span class="stat-value">\${totalTables}</span>
          </div>
        \`;
        loadCDCTables(namespaces);
      } catch (e) {
        el.innerHTML = '<span class="error-msg">Failed to load</span>';
      }
    }

    async function loadCDCTables(namespaces) {
      const el = document.getElementById('cdc-tables');
      const tables = [];
      for (const ns of namespaces) {
        try {
          const data = await fetchJSON(\`/catalog/tables?namespace=\${encodeURIComponent(ns)}\`);
          for (const t of (data.tables || [])) {
            tables.push({ namespace: ns, name: t });
          }
        } catch (e) {
          console.warn(\`[dashboard] Failed to fetch CDC tables for namespace \${ns}:\`, e);
        }
      }
      if (!tables.length) {
        el.innerHTML = '<div class="empty-state">No CDC tables</div>';
        return;
      }
      el.innerHTML = tables.slice(0, 20).map(t => \`
        <div class="stat-row">
          <span class="event-type">\${escapeHtml(t.namespace)}/\${escapeHtml(t.name)}</span>
        </div>
      \`).join('');
    }

    async function loadWebhooksStats() {
      const el = document.getElementById('webhooks-stats');
      try {
        const data = await fetchJSON('/events?limit=100&source=events');
        const events = data.data || [];
        const providers = {};
        for (const e of events) {
          const p = e.webhook?.provider || e.provider;
          if (p) providers[p] = (providers[p] || 0) + 1;
        }
        const entries = Object.entries(providers).sort((a, b) => b[1] - a[1]);
        if (!entries.length) {
          el.innerHTML = '<div class="stat-row"><span class="stat-label">No webhook events in last 24h</span></div>';
          return;
        }
        el.innerHTML = entries.map(([p, c]) => \`
          <div class="stat-row">
            <span class="stat-label">\${escapeHtml(p)}</span>
            <span class="stat-value">\${c}</span>
          </div>
        \`).join('');
      } catch (e) {
        el.innerHTML = '<span class="error-msg">Failed to load</span>';
      }
    }

    async function loadRecentEvents() {
      const el = document.getElementById('events-body');
      try {
        const data = await fetchJSON(\`/events?limit=50&source=\${currentSource}\`);
        const events = data.data || [];
        if (!events.length) {
          el.innerHTML = '<tr><td colspan="4" class="empty-state">No events found</td></tr>';
          return;
        }
        el.innerHTML = events.map(e => \`
          <tr>
            <td class="event-time">\${formatTime(e.ts)}</td>
            <td class="event-type">\${escapeHtml(e.type || 'unknown')}</td>
            <td>\${getEventSource(e)}</td>
            <td>\${getEventDetails(e)}</td>
          </tr>
        \`).join('');
      } catch (e) {
        el.innerHTML = '<tr><td colspan="4" class="error-msg">Failed to load events</td></tr>';
      }
    }

    function setSource(source) {
      currentSource = source;
      document.querySelectorAll('.tab').forEach(t => {
        t.classList.toggle('active', t.dataset.source === source);
      });
      loadRecentEvents();
    }

    async function loadUserInfo() {
      try {
        const data = await fetchJSON('/me');
        const user = data.user || {};
        document.getElementById('user-info').textContent = user.email || 'Unknown user';
      } catch (e) {
        // User info is optional - display default text on error
        console.debug('[dashboard] Could not load user info:', e);
        document.getElementById('user-info').textContent = 'Admin';
      }
    }

    async function refreshAll() {
      if (refreshing) return;
      refreshing = true;
      const btn = document.getElementById('refresh-btn');
      btn.disabled = true;
      btn.textContent = 'Refreshing...';
      try {
        await Promise.all([
          loadEventsStats(),
          loadSubscriptionsStats(),
          loadCatalogStats(),
          loadWebhooksStats(),
          loadRecentEvents(),
        ]);
      } finally {
        refreshing = false;
        btn.disabled = false;
        btn.textContent = 'Refresh';
      }
    }

    // Initial load
    loadUserInfo();
    refreshAll();

    // Auto-refresh every 30 seconds
    setInterval(() => {
      if (!document.hidden && !refreshing) {
        refreshAll();
      }
    }, 30000);
  </script>
</body>
</html>`;
}

/**
 * Handle dashboard route - /dashboard
 */
export async function handleDashboard(request: Request, env: Env, url: URL): Promise<Response | null> {
  if (url.pathname !== '/dashboard') {
    return null
  }

  // Only allow GET
  if (request.method !== 'GET') {
    return new Response('Method not allowed', { status: 405 })
  }

  // Dashboard requires authentication - handled by main router
  // Generate and return the dashboard HTML
  const html = generateDashboardHTML(url.origin)

  return new Response(html, {
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache, no-store, must-revalidate',
    },
  })
}
