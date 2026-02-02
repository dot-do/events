/**
 * Fetch handler - Main fetch handler and routing
 *
 * Routes:
 * - POST /ingest, /e - Receive batched events
 * - POST /webhooks?provider=xxx - Receive and verify webhooks
 * - GET /health - Health check
 * - POST /query - Generate DuckDB query (admin)
 * - GET /recent - List recent events (admin)
 * - GET /events - Events query (admin)
 * - GET /pipeline - Check pipeline bucket (admin)
 * - GET /catalog/* - Catalog API (admin)
 * - /subscriptions/* - Subscription management API (admin)
 * - GET /benchmark - Benchmark results (admin)
 * - GET /dashboard - Admin dashboard (admin)
 */

import type { Env, AuthRequest } from '../env'
import type { TenantContext } from '../middleware/tenant'

// Route handlers
import { handleHealth, handlePipelineCheck, handleBenchmark } from '../routes/health'
import { handleAuth } from '../routes/auth'
import { handleIngest } from '../routes/ingest'
import { handleWebhooks } from '../routes/webhooks'
import { handleEventsQuery, handleRecent } from '../routes/events'
import { handleQuery } from '../routes/query'
import { handleCatalog } from '../routes/catalog'
import { handleDashboard } from '../routes/dashboard'
import { handleSubscriptionRoutes } from '../subscription-routes'
import {
  handleRegisterSchema,
  handleListSchemas,
  handleGetSchema,
  handleGetSchemaHistory,
  handleDeleteSchema,
  handleConfigureNamespace,
  handleGetNamespaceConfig,
  handleValidateEvents,
} from '../routes/schema'
import { handleShardRoutes } from '../routes/shards'
import { getAllowedOrigin } from '../utils'
import { createRequestLogger, getRequestId } from '../logger'

/**
 * Handle CORS preflight requests
 */
function handleCORS(request?: Request, env?: { ALLOWED_ORIGINS?: string }, authenticated = false): Response {
  const requestOrigin = request?.headers.get('Origin') ?? null
  const origin = authenticated
    ? (getAllowedOrigin(requestOrigin, env) ?? 'null')
    : '*'

  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': origin,
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400',
      ...(authenticated && origin !== '*' ? { 'Vary': 'Origin' } : {}),
    },
  })
}

/**
 * Main fetch handler
 */
export async function handleFetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const startTime = performance.now()
  const url = new URL(request.url)
  const requestId = getRequestId(request)
  const log = createRequestLogger({ requestId })

  // CORS preflight - use restricted origins for authenticated routes
  if (request.method === 'OPTIONS') {
    return handleCORS()
  }

  // Detect domain for service name
  const isWebhooksDomain = url.hostname === 'webhooks.do' || url.hostname.endsWith('.webhooks.do')

  // Health check
  const healthResponse = handleHealth(request, env, url, isWebhooksDomain, startTime)
  if (healthResponse) return healthResponse

  // Auth routes (/login, /callback, /logout, /me)
  const authResponse = await handleAuth(request, env, url)
  if (authResponse) return authResponse

  // Ingest endpoint
  if ((url.pathname === '/ingest' || url.pathname === '/e') && request.method === 'POST') {
    const response = await handleIngest(request, env, ctx)
    const cpuTime = performance.now() - startTime
    log.info('POST /ingest', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/ingest' })
    return response
  }

  // Webhook endpoints
  const webhookResponse = await handleWebhooks(request, env, ctx, url, isWebhooksDomain, startTime)
  if (webhookResponse) return webhookResponse

  // ================================================================
  // Protected routes - require admin auth
  // ================================================================
  const protectedRoutes = ['/query', '/recent', '/events', '/pipeline', '/catalog', '/subscriptions', '/schemas', '/shards', '/benchmark', '/dashboard']
  if (protectedRoutes.some(r => url.pathname === r || url.pathname.startsWith(r + '/'))) {
    // Authenticate using AUTH RPC binding directly
    const auth = request.headers.get('Authorization')
    const cookie = request.headers.get('Cookie')
    const result = await env.AUTH.authenticate(auth, cookie)

    if (!result.ok) {
      const accept = request.headers.get('Accept') || ''
      if (accept.includes('text/html')) {
        const loginUrl = `${url.origin}/login?redirect_uri=${encodeURIComponent(url.pathname + url.search)}`
        return Response.redirect(loginUrl, 302)
      }
      return Response.json({ error: result.error || 'Authentication required' }, { status: result.status || 401 })
    }

    if (!result.user?.roles?.includes('admin')) {
      return Response.json({
        error: 'Admin access required',
        authenticated: true,
        message: 'You are logged in but do not have admin privileges'
      }, { status: 403 })
    }

    // Attach user to request for downstream handlers
    const authReq = request as AuthRequest
    authReq.auth = { user: result.user, isAuth: true }

    const user = authReq.auth?.user?.email || 'unknown'

    // Create admin tenant context for authenticated admin users
    // This allows them to access all namespaces or specify a namespace in requests
    const adminTenant: TenantContext = {
      namespace: env.DEFAULT_NAMESPACE || 'default',
      isAdmin: true,
      keyId: `oauth:${user}`,
    }

    // Query builder endpoint
    if (url.pathname === '/query' && request.method === 'POST') {
      const response = await handleQuery(request, env, adminTenant)
      const cpuTime = performance.now() - startTime
      log.info('POST /query', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/query', user })
      return response
    }

    // Recent events
    if (url.pathname === '/recent' && request.method === 'GET') {
      const response = await handleRecent(request, env)
      const cpuTime = performance.now() - startTime
      log.info('GET /recent', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/recent', user })
      return response
    }

    // Events query (parquet)
    if (url.pathname === '/events' && request.method === 'GET') {
      const response = await handleEventsQuery(request, env)
      const cpuTime = performance.now() - startTime
      log.info('GET /events', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/events', user })
      return response
    }

    // Pipeline bucket check
    if (url.pathname === '/pipeline' && request.method === 'GET') {
      const response = await handlePipelineCheck(request, env)
      const cpuTime = performance.now() - startTime
      log.info('GET /pipeline', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/pipeline', user })
      return response
    }

    // Catalog API
    if (url.pathname.startsWith('/catalog')) {
      const response = await handleCatalog(request, env, url)
      const cpuTime = performance.now() - startTime
      log.info(`${request.method} ${url.pathname}`, { cpuMs: parseFloat(cpuTime.toFixed(2)), path: url.pathname, user })
      return response
    }

    // Subscription API (with admin tenant context for namespace isolation)
    if (url.pathname.startsWith('/subscriptions')) {
      const response = await handleSubscriptionRoutes(request, env, url, adminTenant)
      const cpuTime = performance.now() - startTime
      log.info(`${request.method} ${url.pathname}`, { cpuMs: parseFloat(cpuTime.toFixed(2)), path: url.pathname, user })
      if (response) return response
    }

    // Schema Registry API
    if (url.pathname.startsWith('/schemas')) {
      let response: Response | null = null

      // POST /schemas - Register schema
      if (url.pathname === '/schemas' && request.method === 'POST') {
        response = await handleRegisterSchema(request, env)
      }
      // GET /schemas - List schemas
      else if (url.pathname === '/schemas' && request.method === 'GET') {
        response = await handleListSchemas(request, env)
      }
      // POST /schemas/validate - Validate events (testing endpoint)
      else if (url.pathname === '/schemas/validate' && request.method === 'POST') {
        response = await handleValidateEvents(request, env)
      }
      // PUT /schemas/namespace/:namespace/config - Configure namespace
      else if (url.pathname.match(/^\/schemas\/namespace\/[^/]+\/config$/) && request.method === 'PUT') {
        const namespace = url.pathname.split('/')[3]!
        response = await handleConfigureNamespace(request, env, { namespace })
      }
      // GET /schemas/namespace/:namespace/config - Get namespace config
      else if (url.pathname.match(/^\/schemas\/namespace\/[^/]+\/config$/) && request.method === 'GET') {
        const namespace = url.pathname.split('/')[3]!
        response = await handleGetNamespaceConfig(request, env, { namespace })
      }
      // GET /schemas/:namespace/:eventType/history - Get schema history
      else if (url.pathname.match(/^\/schemas\/[^/]+\/[^/]+\/history$/) && request.method === 'GET') {
        const parts = url.pathname.split('/')
        const namespace = parts[2]!
        const eventType = decodeURIComponent(parts[3]!)
        response = await handleGetSchemaHistory(request, env, { namespace, eventType })
      }
      // GET /schemas/:namespace/:eventType - Get specific schema
      else if (url.pathname.match(/^\/schemas\/[^/]+\/[^/]+$/) && request.method === 'GET') {
        const parts = url.pathname.split('/')
        const namespace = parts[2]!
        const eventType = decodeURIComponent(parts[3]!)
        response = await handleGetSchema(request, env, { namespace, eventType })
      }
      // DELETE /schemas/:namespace/:eventType - Delete schema
      else if (url.pathname.match(/^\/schemas\/[^/]+\/[^/]+$/) && request.method === 'DELETE') {
        const parts = url.pathname.split('/')
        const namespace = parts[2]!
        const eventType = decodeURIComponent(parts[3]!)
        response = await handleDeleteSchema(request, env, { namespace, eventType })
      }

      if (response) {
        const cpuTime = performance.now() - startTime
        log.info(`${request.method} ${url.pathname}`, { cpuMs: parseFloat(cpuTime.toFixed(2)), path: url.pathname, user })
        return response
      }
    }

    // Benchmark results
    if (url.pathname === '/benchmark' && request.method === 'GET') {
      const response = await handleBenchmark(request, env)
      const cpuTime = performance.now() - startTime
      log.info('GET /benchmark', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/benchmark', user })
      return response
    }

    // Shard management API
    if (url.pathname.startsWith('/shards')) {
      const response = await handleShardRoutes(request, env, url)
      if (response) {
        const cpuTime = performance.now() - startTime
        log.info(`${request.method} ${url.pathname}`, { cpuMs: parseFloat(cpuTime.toFixed(2)), path: url.pathname, user })
        return response
      }
    }

    // Admin dashboard
    const dashboardResponse = await handleDashboard(request, env, url)
    if (dashboardResponse) {
      const cpuTime = performance.now() - startTime
      log.info('GET /dashboard', { cpuMs: parseFloat(cpuTime.toFixed(2)), path: '/dashboard', user })
      return dashboardResponse
    }
  }

  return new Response('Not found', { status: 404 })
}
