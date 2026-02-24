/**
 * Infrastructure Routes — events.do-specific routes mounted via API() routes callback.
 *
 * These routes handle event ingestion, webhooks, auth, health, metrics,
 * and admin/debug endpoints. They wrap the existing route handlers (which use
 * raw Request/Env/ExecutionContext signatures) into Hono route handlers.
 *
 * The eventsConvention in API() handles the public event browsing surface
 * (/events, /commits, /errors, /traces, etc.) via the EVENTS service binding.
 * This file handles everything else.
 */

import type { Hono, Context } from 'hono'
import type { Env, AuthRequest } from '../env'
import type { TenantContext } from '../middleware/tenant'

// Route handlers (existing, raw Request/Env/Ctx signatures)
import { handleHealth, handlePipelineCheck, handleBenchmark } from '../routes/health'
import { handleMetrics } from '../routes/metrics'
import { handleAuth } from '../routes/auth'
import { handleIngest } from '../routes/ingest'
import { handleWebhooks } from '../routes/webhooks'
import { handleRecent } from '../routes/events'
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
import {
  createRequestLogger,
  extractRequestContext,
  type Logger,
} from '../logger'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Extract the raw Cloudflare Env from Hono context */
function getEnv(c: Context): Env {
  return c.env as unknown as Env
}

/** Get ExecutionContext from Hono (c.executionCtx) */
function getCtx(c: Context): ExecutionContext {
  return c.executionCtx
}

/** Adds correlation ID to a response */
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
 * Authenticate an admin request via AUTH RPC binding.
 * Returns { user, tenant } on success, or a Response on failure.
 */
async function authenticateAdmin(
  request: Request,
  env: Env,
  url: URL,
  correlationId: string,
  log: Logger,
): Promise<{ user: string; tenant: TenantContext; authReq: AuthRequest } | Response> {
  const auth = request.headers.get('Authorization')
  const cookie = request.headers.get('Cookie')
  let result: { ok: true; user: { id: string; email?: string; roles?: string[]; permissions?: string[]; [key: string]: unknown } } | { ok: false; status: number; error: string }
  try {
    result = await env.AUTH.authenticate(auth, cookie) as typeof result
  } catch (err) {
    log.error('AUTH.authenticate RPC failed', { error: err instanceof Error ? err.message : String(err) })
    return addCorrelationId(
      Response.json({ error: 'Authentication service unavailable' }, { status: 503 }),
      correlationId
    )
  }

  if (!result.ok) {
    const accept = request.headers.get('Accept') || ''
    if (accept.includes('text/html')) {
      const loginUrl = `${url.origin}/login?redirect_uri=${encodeURIComponent(url.pathname + url.search)}`
      log.info('Redirecting to login', { status: 302, reason: result.error })
      return Response.redirect(loginUrl, 302)
    }
    log.warn('Authentication failed', { status: result.status || 401, reason: result.error })
    return addCorrelationId(
      Response.json({ error: result.error || 'Authentication required' }, { status: result.status || 401 }),
      correlationId
    )
  }

  const authReq = request as AuthRequest
  ;(authReq as unknown as Record<string, unknown>).auth = { user: result.user, isAuth: true }

  const user = (result.user as Record<string, unknown>).email as string || 'unknown'

  const adminTenant: TenantContext = {
    namespace: env.DEFAULT_NAMESPACE || 'default',
    isAdmin: true,
    keyId: `oauth:${user}`,
  }

  return { user, tenant: adminTenant, authReq }
}

// ---------------------------------------------------------------------------
// Mount function
// ---------------------------------------------------------------------------

/**
 * Mount all infrastructure routes onto a Hono app.
 * Called from the API() `routes` callback.
 *
 * Note: The app parameter uses Hono<any> because @dotdo/api's ApiEnv type
 * is not available at typecheck time (DTS build is broken upstream).
 * This matches the pattern used by apis.do and other consumers.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function mountInfraRoutes(app: Hono<any>): void {

  // ==================== Health Check ====================
  // Note: the API() landing page handles GET / for the JSON discovery surface.
  // /health is a separate, simple health probe.
  app.get('/health', (c: Context) => {
    const env = getEnv(c)
    const url = new URL(c.req.url)
    const isWebhooksDomain = url.hostname === 'webhooks.do' || url.hostname.endsWith('.webhooks.do')
    const response = handleHealth(c.req.raw, env, url, isWebhooksDomain, performance.now())
    if (response) return response
    return c.json({ status: 'ok' })
  })

  // ==================== Metrics ====================
  app.get('/metrics', async (c: Context) => {
    const env = getEnv(c)
    return handleMetrics(c.req.raw, env)
  })

  // ==================== Auth Routes ====================
  // /login, /callback, /logout, /me
  app.all('/login', async (c: Context) => {
    const env = getEnv(c)
    const url = new URL(c.req.url)
    const response = await handleAuth(c.req.raw, env, url)
    return response ?? c.notFound()
  })

  app.all('/callback', async (c: Context) => {
    const env = getEnv(c)
    const url = new URL(c.req.url)
    const response = await handleAuth(c.req.raw, env, url)
    return response ?? c.notFound()
  })

  app.all('/logout', async (c: Context) => {
    const env = getEnv(c)
    const url = new URL(c.req.url)
    const response = await handleAuth(c.req.raw, env, url)
    return response ?? c.notFound()
  })

  app.get('/me', async (c: Context) => {
    const env = getEnv(c)
    const url = new URL(c.req.url)
    const response = await handleAuth(c.req.raw, env, url)
    return response ?? c.notFound()
  })

  // ==================== Debug: Test Cookie ====================
  app.get('/debug/set-test-cookie', async (c: Context) => {
    const env = getEnv(c)
    try {
      const token = await (env.OAUTH as unknown as { signTestJwt(): Promise<string> }).signTestJwt()
      const maxAge = 3600 * 24 * 7
      return new Response(null, {
        status: 302,
        headers: {
          'Location': '/events',
          'Set-Cookie': `auth=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAge}`,
        },
      })
    } catch (err) {
      return c.json({ error: String(err) }, 500)
    }
  })

  // ==================== Event Ingest ====================
  // POST /e (canonical), POST /ingest (deprecated)
  app.post('/e', async (c: Context) => {
    const env = getEnv(c)
    const ctx = getCtx(c)
    return handleIngest(c.req.raw, env, ctx)
  })

  app.post('/ingest', async (c: Context) => {
    const env = getEnv(c)
    const ctx = getCtx(c)
    const response = await handleIngest(c.req.raw, env, ctx)
    response.headers.set('Deprecation', 'true')
    response.headers.set('Link', '</e>; rel="successor-version"')
    return response
  })

  // ==================== Webhooks ====================
  // POST /webhooks?provider=xxx + webhooks.do/:provider
  app.post('/webhooks', async (c: Context) => {
    const env = getEnv(c)
    const ctx = getCtx(c)
    const url = new URL(c.req.url)
    const isWebhooksDomain = url.hostname === 'webhooks.do' || url.hostname.endsWith('.webhooks.do')
    const response = await handleWebhooks(c.req.raw, env, ctx, url, isWebhooksDomain, performance.now())
    return response ?? c.notFound()
  })

  // webhooks.do domain pattern: POST /:provider
  // This catches webhook provider paths on the webhooks.do domain.
  // Note: this must come AFTER more specific POST routes to avoid shadowing.
  app.post('/:provider', async (c: Context) => {
    const env = getEnv(c)
    const ctx = getCtx(c)
    const url = new URL(c.req.url)
    const isWebhooksDomain = url.hostname === 'webhooks.do' || url.hostname.endsWith('.webhooks.do')
    // Only handle on webhooks.do domain — skip otherwise to let other routes handle it
    if (!isWebhooksDomain) return c.notFound()
    const response = await handleWebhooks(c.req.raw, env, ctx, url, isWebhooksDomain, performance.now())
    return response ?? c.notFound()
  })

  // ==================== Protected Admin Routes ====================
  // All routes below require admin authentication

  // Helper: admin-guarded route handler
  const adminRoute = (
    handler: (c: Context, env: Env, user: string, tenant: TenantContext, authReq: AuthRequest) => Promise<Response | undefined>
  ) => {
    return async (c: Context) => {
      const env = getEnv(c)
      const url = new URL(c.req.url)
      const reqCtx = extractRequestContext(c.req.raw)
      const { correlationId, requestId, method, path } = reqCtx
      const log = createRequestLogger({ correlationId, requestId, method, path })

      const authResult = await authenticateAdmin(c.req.raw, env, url, correlationId, log)
      if (authResult instanceof Response) return authResult

      const response = await handler(c, env, authResult.user, authResult.tenant, authResult.authReq)
      if (response) return addCorrelationId(response, correlationId)
      return c.notFound()
    }
  }

  // POST /query — Ad-hoc query builder
  app.post('/query', adminRoute(async (c, env, _user, tenant) => {
    return handleQuery(c.req.raw, env, tenant)
  }))

  // GET /recent — Recent events (debug)
  app.get('/recent', adminRoute(async (c, env) => {
    return handleRecent(c.req.raw, env)
  }))

  // GET /pipeline — Pipeline bucket check
  app.get('/pipeline', adminRoute(async (c, env) => {
    return handlePipelineCheck(c.req.raw, env)
  }))

  // GET /catalog/* — Catalog API
  app.all('/catalog/*', adminRoute(async (c, env) => {
    const url = new URL(c.req.url)
    return handleCatalog(c.req.raw, env, url)
  }))

  app.all('/catalog', adminRoute(async (c, env) => {
    const url = new URL(c.req.url)
    return handleCatalog(c.req.raw, env, url)
  }))

  // /subscriptions/* — Subscription management
  app.all('/subscriptions/*', adminRoute(async (c, env, _user, tenant) => {
    const url = new URL(c.req.url)
    const response = await handleSubscriptionRoutes(c.req.raw, env, url, tenant)
    return response ?? undefined
  }))

  app.all('/subscriptions', adminRoute(async (c, env, _user, tenant) => {
    const url = new URL(c.req.url)
    const response = await handleSubscriptionRoutes(c.req.raw, env, url, tenant)
    return response ?? undefined
  }))

  // /schemas/* — Schema Registry
  app.post('/schemas', adminRoute(async (c, env) => {
    return handleRegisterSchema(c.req.raw, env)
  }))

  app.get('/schemas', adminRoute(async (c, env) => {
    return handleListSchemas(c.req.raw, env)
  }))

  app.post('/schemas/validate', adminRoute(async (c, env) => {
    return handleValidateEvents(c.req.raw, env)
  }))

  app.get('/schemas/namespace/:namespace/config', adminRoute(async (c, env) => {
    const namespace = c.req.param('namespace')
    return handleGetNamespaceConfig(c.req.raw, env, { namespace })
  }))

  app.put('/schemas/namespace/:namespace/config', adminRoute(async (c, env) => {
    const namespace = c.req.param('namespace')
    return handleConfigureNamespace(c.req.raw, env, { namespace })
  }))

  app.get('/schemas/:namespace/:eventType/history', adminRoute(async (c, env) => {
    const namespace = c.req.param('namespace')
    const eventType = c.req.param('eventType')
    return handleGetSchemaHistory(c.req.raw, env, { namespace, eventType: decodeURIComponent(eventType) })
  }))

  app.get('/schemas/:namespace/:eventType', adminRoute(async (c, env) => {
    const namespace = c.req.param('namespace')
    const eventType = c.req.param('eventType')
    return handleGetSchema(c.req.raw, env, { namespace, eventType: decodeURIComponent(eventType) })
  }))

  app.delete('/schemas/:namespace/:eventType', adminRoute(async (c, env) => {
    const namespace = c.req.param('namespace')
    const eventType = c.req.param('eventType')
    return handleDeleteSchema(c.req.raw, env, { namespace, eventType: decodeURIComponent(eventType) })
  }))

  // GET /benchmark — Benchmark results
  app.get('/benchmark', adminRoute(async (c, env) => {
    return handleBenchmark(c.req.raw, env)
  }))

  // /shards/* — Shard management
  app.all('/shards/*', adminRoute(async (c, env) => {
    const url = new URL(c.req.url)
    const response = await handleShardRoutes(c.req.raw, env, url)
    return response ?? undefined
  }))

  app.all('/shards', adminRoute(async (c, env) => {
    const url = new URL(c.req.url)
    const response = await handleShardRoutes(c.req.raw, env, url)
    return response ?? undefined
  }))

  // /dashboard — Admin dashboard
  app.all('/dashboard/*', adminRoute(async (c, env) => {
    const url = new URL(c.req.url)
    const response = await handleDashboard(c.req.raw, env, url)
    return response ?? undefined
  }))

  app.all('/dashboard', adminRoute(async (c, env) => {
    const url = new URL(c.req.url)
    const response = await handleDashboard(c.req.raw, env, url)
    return response ?? undefined
  }))
}
