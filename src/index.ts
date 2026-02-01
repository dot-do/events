/**
 * events.do - Event Ingestion Worker
 *
 * Routes:
 * - events.do (primary)
 * - events.workers.do (alias)
 * - apis.do/events, apis.do/e (client-side with cookies)
 *
 * Endpoints:
 * - POST /ingest - Receive batched events
 * - POST /webhooks?provider=xxx - Receive and verify webhooks (github, stripe, workos, slack, linear, svix)
 * - GET /health - Health check
 * - POST /query - Generate DuckDB query
 * - GET /recent - List recent events (debug)
 * - GET /pipeline - Check pipeline bucket for Parquet files
 * - GET /catalog/* - Catalog API
 * - /subscriptions/* - Subscription management API
 */

import type { Env, AuthRequest } from './env'

// Import DOs from local source (not package) for proper bundling
import { CatalogDO } from '../core/src/catalog'
import { SubscriptionDO } from '../core/src/subscription'
import { CDCProcessorDO } from '../core/src/cdc-processor'
import { EventWriterDO } from './event-writer-do'
import { compactCollection } from '../core/src/cdc-compaction'
import { handleSubscriptionRoutes } from './subscription-routes'
import { corsHeaders, getAllowedOrigin } from './utils'

// Route handlers
import { handleHealth, handlePipelineCheck, handleBenchmark } from './routes/health'
import { handleAuth } from './routes/auth'
import { handleIngest } from './routes/ingest'
import { handleWebhooks } from './routes/webhooks'
import { handleEventsQuery, handleRecent } from './routes/events'
import { handleQuery } from './routes/query'
import { handleCatalog } from './routes/catalog'

// Re-export DOs for wrangler
export { CatalogDO, SubscriptionDO, CDCProcessorDO, EventWriterDO }

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const startTime = performance.now()
    const url = new URL(request.url)

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
      console.log(`[CPU:${cpuTime.toFixed(2)}ms] POST /ingest`)
      return response
    }

    // Webhook endpoints
    const webhookResponse = await handleWebhooks(request, env, ctx, url, isWebhooksDomain, startTime)
    if (webhookResponse) return webhookResponse

    // ================================================================
    // Protected routes - require admin auth
    // ================================================================
    const protectedRoutes = ['/query', '/recent', '/events', '/pipeline', '/catalog', '/subscriptions', '/benchmark']
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
          user: result.user,
          message: 'You are logged in but do not have admin privileges'
        }, { status: 403 })
      }

      // Attach user to request for downstream handlers
      const authReq = request as AuthRequest
      authReq.auth = { user: result.user, isAuth: true }

      const user = authReq.auth?.user?.email || 'unknown'

      // Query builder endpoint
      if (url.pathname === '/query' && request.method === 'POST') {
        const response = await handleQuery(request, env)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] POST /query (${user})`)
        return response
      }

      // Recent events
      if (url.pathname === '/recent' && request.method === 'GET') {
        const response = await handleRecent(request, env)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] GET /recent (${user})`)
        return response
      }

      // Events query (parquet)
      if (url.pathname === '/events' && request.method === 'GET') {
        const response = await handleEventsQuery(request, env)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] GET /events (${user})`)
        return response
      }

      // Pipeline bucket check
      if (url.pathname === '/pipeline' && request.method === 'GET') {
        const response = await handlePipelineCheck(request, env)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] GET /pipeline (${user})`)
        return response
      }

      // Catalog API
      if (url.pathname.startsWith('/catalog')) {
        const response = await handleCatalog(request, env, url)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] ${request.method} ${url.pathname} (${user})`)
        return response
      }

      // Subscription API
      if (url.pathname.startsWith('/subscriptions')) {
        const response = await handleSubscriptionRoutes(request, env, url)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] ${request.method} ${url.pathname} (${user})`)
        if (response) return response
      }

      // Benchmark results
      if (url.pathname === '/benchmark' && request.method === 'GET') {
        const response = await handleBenchmark(request, env)
        const cpuTime = performance.now() - startTime
        console.log(`[CPU:${cpuTime.toFixed(2)}ms] GET /benchmark (${user})`)
        return response
      }
    }

    return new Response('Not found', { status: 404 })
  },

  /**
   * Scheduled handler - runs CDC compaction for collections with pending deltas.
   *
   * Cron: every hour at :30 (30 * * * *)
   *
   * 1. Lists namespaces and tables from CatalogDO
   * 2. For each collection, checks for pending delta files in R2
   * 3. Runs compactCollection() for collections with pending deltas
   * 4. Logs results
   */
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const startTime = Date.now()
    console.log(`[COMPACT-CDC] Scheduled compaction triggered at ${new Date().toISOString()}, cron: ${event.cron}`)

    ctx.waitUntil((async () => {
      try {
        // 1. Get catalog to list namespaces and tables
        const catalogId = env.CATALOG.idFromName('events-catalog')
        const catalog = env.CATALOG.get(catalogId)

        const namespaces = await catalog.listNamespaces()
        console.log(`[COMPACT-CDC] Found ${namespaces.length} namespaces: ${namespaces.join(', ')}`)

        let totalCompacted = 0
        let totalSkipped = 0
        const results: { ns: string; table: string; result: string; recordCount?: number; deltasProcessed?: number }[] = []

        // 2. For each namespace, list tables and check for pending deltas
        for (const ns of namespaces) {
          const tables = await catalog.listTables(ns)

          for (const table of tables) {
            const deltasPrefix = `cdc/${ns}/${table}/deltas/`

            // Check if there are any pending delta files
            const deltasList = await env.EVENTS_BUCKET.list({ prefix: deltasPrefix, limit: 1 })

            if (deltasList.objects.length === 0) {
              totalSkipped++
              continue
            }

            // 3. Run compaction for this collection
            try {
              const compactionResult = await compactCollection(
                env.EVENTS_BUCKET,
                ns,
                table,
                { deleteDeltas: true }
              )

              if (compactionResult.success && compactionResult.deltasProcessed > 0) {
                totalCompacted++
                results.push({
                  ns,
                  table,
                  result: 'compacted',
                  recordCount: compactionResult.recordCount,
                  deltasProcessed: compactionResult.deltasProcessed,
                })
                console.log(
                  `[COMPACT-CDC] Compacted ${ns}/${table}: ${compactionResult.deltasProcessed} deltas -> ${compactionResult.recordCount} records`
                )
              } else {
                totalSkipped++
                results.push({ ns, table, result: 'no-deltas' })
              }
            } catch (err) {
              const errorMsg = err instanceof Error ? err.message : String(err)
              console.error(`[COMPACT-CDC] Error compacting ${ns}/${table}:`, errorMsg)
              results.push({ ns, table, result: `error: ${errorMsg}` })
            }
          }
        }

        const durationMs = Date.now() - startTime
        console.log(
          `[COMPACT-CDC] Completed in ${durationMs}ms: ${totalCompacted} compacted, ${totalSkipped} skipped`
        )
      } catch (err) {
        console.error('[COMPACT-CDC] Fatal error in scheduled compaction:', err)
      }
    })())
  },
}

// ============================================================================
// CORS
// ============================================================================

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
