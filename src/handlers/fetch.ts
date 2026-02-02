/**
 * Fetch handler - Main fetch handler and routing
 *
 * Routes:
 * - POST /ingest, /e - Receive batched events
 * - POST /webhooks?provider=xxx - Receive and verify webhooks
 * - GET /health - Health check
 * - GET /metrics - Prometheus metrics endpoint
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
import { handleMetrics } from '../routes/metrics'
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
import {
  createRequestLogger,
  extractRequestContext,
  withCorrelationId,
  type RequestContext,
} from '../logger'

/**
 * Handle CORS preflight requests
 */
function handleCORS(
  request?: Request,
  env?: { ALLOWED_ORIGINS?: string },
  authenticated = false,
  correlationId?: string
): Response {
  const requestOrigin = request?.headers.get('Origin') ?? null
  const origin = authenticated
    ? (getAllowedOrigin(requestOrigin, env) ?? 'null')
    : '*'

  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': origin,
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Correlation-ID, X-Request-ID',
      'Access-Control-Expose-Headers': 'X-Correlation-ID',
      'Access-Control-Max-Age': '86400',
      ...(authenticated && origin !== '*' ? { 'Vary': 'Origin' } : {}),
      ...(correlationId ? { 'x-correlation-id': correlationId } : {}),
    },
  })
}

/**
 * Adds correlation ID to a response
 */
function addCorrelationId(response: Response, correlationId: string): Response {
  const headers = new Headers(response.headers)
  headers.set('x-correlation-id', correlationId)
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

/**
 * Main fetch handler
 */
export async function handleFetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const startTime = performance.now()
  const url = new URL(request.url)

  // Extract request context including correlation ID
  const reqCtx = extractRequestContext(request)
  const { correlationId, requestId, method, path } = reqCtx
  const log = createRequestLogger({ correlationId, requestId, method, path })

  // CORS preflight - use restricted origins for authenticated routes
  if (request.method === 'OPTIONS') {
    const protectedRoutes = ['/subscriptions', '/schemas', '/catalog', '/dashboard']
    const isAuthenticatedRoute = protectedRoutes.some(r =>
      url.pathname === r || url.pathname.startsWith(r + '/')
    )
    return handleCORS(request, env, isAuthenticatedRoute, correlationId)
  }

  // Detect domain for service name
  const isWebhooksDomain = url.hostname === 'webhooks.do' || url.hostname.endsWith('.webhooks.do')

  // Health check
  const healthResponse = handleHealth(request, env, url, isWebhooksDomain, startTime)
  if (healthResponse) return addCorrelationId(healthResponse, correlationId)

  // Prometheus metrics endpoint
  if (url.pathname === '/metrics') {
    if (request.method !== 'GET') {
      return addCorrelationId(
        Response.json({ error: 'Method not allowed' }, {
          status: 405,
          headers: { 'Allow': 'GET' }
        }),
        correlationId
      )
    }
    const response = await handleMetrics(request, env)
    const cpuTime = performance.now() - startTime
    log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), status: response.status })
    return addCorrelationId(response, correlationId)
  }

  // Auth routes (/login, /callback, /logout, /me)
  const authResponse = await handleAuth(request, env, url)
  if (authResponse) return addCorrelationId(authResponse, correlationId)

  // Ingest endpoint
  if (url.pathname === '/ingest' || url.pathname === '/e') {
    if (request.method !== 'POST') {
      return addCorrelationId(
        Response.json({ error: 'Method not allowed' }, {
          status: 405,
          headers: { 'Allow': 'POST' }
        }),
        correlationId
      )
    }
    const response = await handleIngest(request, env, ctx)
    const cpuTime = performance.now() - startTime
    log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), status: response.status })
    return addCorrelationId(response, correlationId)
  }

  // Webhook endpoints
  const webhookResponse = await handleWebhooks(request, env, ctx, url, isWebhooksDomain, startTime)
  if (webhookResponse) return addCorrelationId(webhookResponse, correlationId)

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
        log.info('Redirecting to login', { status: 302 })
        return Response.redirect(loginUrl, 302)
      }
      log.warn('Authentication failed', { status: result.status || 401 })
      return addCorrelationId(
        Response.json({ error: result.error || 'Authentication required' }, { status: result.status || 401 }),
        correlationId
      )
    }

    if (!result.user?.roles?.includes('admin')) {
      log.warn('Admin access denied', { user: result.user?.email, status: 403 })
      return addCorrelationId(
        Response.json({
          error: 'Admin access required',
          authenticated: true,
          message: 'You are logged in but do not have admin privileges'
        }, { status: 403 }),
        correlationId
      )
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
    if (url.pathname === '/query') {
      if (request.method !== 'POST') {
        return addCorrelationId(
          Response.json({ error: 'Method not allowed' }, {
            status: 405,
            headers: { 'Allow': 'POST' }
          }),
          correlationId
        )
      }
      const response = await handleQuery(request, env, adminTenant)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
      return addCorrelationId(response, correlationId)
    }

    // Recent events
    if (url.pathname === '/recent') {
      if (request.method !== 'GET') {
        return addCorrelationId(
          Response.json({ error: 'Method not allowed' }, {
            status: 405,
            headers: { 'Allow': 'GET' }
          }),
          correlationId
        )
      }
      const response = await handleRecent(request, env)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
      return addCorrelationId(response, correlationId)
    }

    // Events query (parquet)
    if (url.pathname === '/events') {
      if (request.method !== 'GET') {
        return addCorrelationId(
          Response.json({ error: 'Method not allowed' }, {
            status: 405,
            headers: { 'Allow': 'GET' }
          }),
          correlationId
        )
      }
      const response = await handleEventsQuery(request, env)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
      return addCorrelationId(response, correlationId)
    }

    // Pipeline bucket check
    if (url.pathname === '/pipeline') {
      if (request.method !== 'GET') {
        return addCorrelationId(
          Response.json({ error: 'Method not allowed' }, {
            status: 405,
            headers: { 'Allow': 'GET' }
          }),
          correlationId
        )
      }
      const response = await handlePipelineCheck(request, env)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
      return addCorrelationId(response, correlationId)
    }

    // Catalog API
    if (url.pathname.startsWith('/catalog')) {
      const response = await handleCatalog(request, env, url)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
      return addCorrelationId(response, correlationId)
    }

    // Subscription API (with admin tenant context for namespace isolation)
    if (url.pathname.startsWith('/subscriptions')) {
      const response = await handleSubscriptionRoutes(request, env, url, adminTenant)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response?.status })
      if (response) return addCorrelationId(response, correlationId)
    }

    // Schema Registry API
    if (url.pathname.startsWith('/schemas')) {
      let response: Response | null = null

      // /schemas - supports GET and POST
      if (url.pathname === '/schemas') {
        if (request.method === 'POST') {
          response = await handleRegisterSchema(request, env)
        } else if (request.method === 'GET') {
          response = await handleListSchemas(request, env)
        } else {
          return addCorrelationId(
            Response.json({ error: 'Method not allowed' }, {
              status: 405,
              headers: { 'Allow': 'GET, POST' }
            }),
            correlationId
          )
        }
      }
      // POST /schemas/validate - Validate events (testing endpoint)
      else if (url.pathname === '/schemas/validate') {
        if (request.method !== 'POST') {
          return addCorrelationId(
            Response.json({ error: 'Method not allowed' }, {
              status: 405,
              headers: { 'Allow': 'POST' }
            }),
            correlationId
          )
        }
        response = await handleValidateEvents(request, env)
      }
      // /schemas/namespace/:namespace/config - supports GET and PUT
      else if (url.pathname.match(/^\/schemas\/namespace\/[^/]+\/config$/)) {
        const configParts = url.pathname.split('/')
        const namespace = configParts[3]
        if (!namespace) {
          return addCorrelationId(
            Response.json({ error: 'Invalid path: missing namespace' }, { status: 400 }),
            correlationId
          )
        }
        if (request.method === 'PUT') {
          response = await handleConfigureNamespace(request, env, { namespace })
        } else if (request.method === 'GET') {
          response = await handleGetNamespaceConfig(request, env, { namespace })
        } else {
          return addCorrelationId(
            Response.json({ error: 'Method not allowed' }, {
              status: 405,
              headers: { 'Allow': 'GET, PUT' }
            }),
            correlationId
          )
        }
      }
      // GET /schemas/:namespace/:eventType/history - Get schema history
      else if (url.pathname.match(/^\/schemas\/[^/]+\/[^/]+\/history$/)) {
        if (request.method !== 'GET') {
          return addCorrelationId(
            Response.json({ error: 'Method not allowed' }, {
              status: 405,
              headers: { 'Allow': 'GET' }
            }),
            correlationId
          )
        }
        const historyParts = url.pathname.split('/')
        const namespace = historyParts[2]
        const eventType = historyParts[3]
        if (!namespace || !eventType) {
          return addCorrelationId(
            Response.json({ error: 'Invalid path: missing namespace or eventType' }, { status: 400 }),
            correlationId
          )
        }
        response = await handleGetSchemaHistory(request, env, { namespace, eventType: decodeURIComponent(eventType) })
      }
      // /schemas/:namespace/:eventType - supports GET and DELETE
      else if (url.pathname.match(/^\/schemas\/[^/]+\/[^/]+$/)) {
        const schemaParts = url.pathname.split('/')
        const namespace = schemaParts[2]
        const eventType = schemaParts[3]
        if (!namespace || !eventType) {
          return addCorrelationId(
            Response.json({ error: 'Invalid path: missing namespace or eventType' }, { status: 400 }),
            correlationId
          )
        }
        if (request.method === 'GET') {
          response = await handleGetSchema(request, env, { namespace, eventType: decodeURIComponent(eventType) })
        } else if (request.method === 'DELETE') {
          response = await handleDeleteSchema(request, env, { namespace, eventType: decodeURIComponent(eventType) })
        } else {
          return addCorrelationId(
            Response.json({ error: 'Method not allowed' }, {
              status: 405,
              headers: { 'Allow': 'GET, DELETE' }
            }),
            correlationId
          )
        }
      }

      if (response) {
        const cpuTime = performance.now() - startTime
        log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
        return addCorrelationId(response, correlationId)
      }
    }

    // Benchmark results
    if (url.pathname === '/benchmark') {
      if (request.method !== 'GET') {
        return addCorrelationId(
          Response.json({ error: 'Method not allowed' }, {
            status: 405,
            headers: { 'Allow': 'GET' }
          }),
          correlationId
        )
      }
      const response = await handleBenchmark(request, env)
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
      return addCorrelationId(response, correlationId)
    }

    // Shard management API
    if (url.pathname.startsWith('/shards')) {
      const response = await handleShardRoutes(request, env, url)
      if (response) {
        const cpuTime = performance.now() - startTime
        log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: response.status })
        return addCorrelationId(response, correlationId)
      }
    }

    // Admin dashboard
    const dashboardResponse = await handleDashboard(request, env, url)
    if (dashboardResponse) {
      const cpuTime = performance.now() - startTime
      log.info('Request completed', { cpuMs: parseFloat(cpuTime.toFixed(2)), user, status: dashboardResponse.status })
      return addCorrelationId(dashboardResponse, correlationId)
    }
  }

  log.info('Not found', { status: 404 })
  return addCorrelationId(new Response('Not found', { status: 404 }), correlationId)
}
