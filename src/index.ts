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
import type { EventBatch, DurableEvent } from '@dotdo/events'
import type { CDCEvent } from '../core/src/cdc-processor'
import { ulid } from '../core/src/ulid'

// Import DOs from local source (not package) for proper bundling
import { CatalogDO } from '../core/src/catalog'
import { SubscriptionDO } from '../core/src/subscription'
import { CDCProcessorDO } from '../core/src/cdc-processor'
import { EventWriterDO } from './event-writer-do'
import { RateLimiterDO } from './middleware/rate-limiter-do'
import { compactCollection } from '../core/src/cdc-compaction'
import { compactEventStream } from '../core/src/event-compaction'
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
export { CatalogDO, SubscriptionDO, CDCProcessorDO, EventWriterDO, RateLimiterDO }

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
   * Scheduled handler - runs CDC compaction, event compaction, and dedup marker cleanup.
   *
   * Cron: every hour at :30 (30 * * * *)
   *
   * Tasks:
   * 1. Dedup Cleanup: Deletes dedup markers older than 24 hours (hourly)
   * 2. CDC Compaction: Compacts delta files for collections with pending deltas (hourly)
   * 3. Event Stream Compaction: Compacts hourly Parquet files into daily files (daily at 1:30 UTC)
   */
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const startTime = Date.now()
    console.log(`[SCHEDULED] Triggered at ${new Date().toISOString()}, cron: ${event.cron}`)

    ctx.waitUntil((async () => {
      // ================================================================
      // Task 1: Dedup marker cleanup (delete markers older than 24 hours)
      // ================================================================
      try {
        const DEDUP_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours
        const now = Date.now()
        let dedupDeleted = 0
        let dedupChecked = 0
        let cursor: string | undefined

        console.log('[DEDUP-CLEANUP] Starting dedup marker cleanup...')

        // Paginate through all dedup markers
        do {
          const list = await env.EVENTS_BUCKET.list({
            prefix: 'dedup/',
            limit: 1000,
            cursor,
          })

          for (const obj of list.objects) {
            dedupChecked++
            // Check if marker is older than TTL based on uploaded timestamp
            const uploadedAt = obj.uploaded.getTime()
            if (now - uploadedAt > DEDUP_TTL_MS) {
              await env.EVENTS_BUCKET.delete(obj.key)
              dedupDeleted++
            }
          }

          cursor = list.truncated ? list.cursor : undefined
        } while (cursor)

        console.log(`[DEDUP-CLEANUP] Completed: checked ${dedupChecked}, deleted ${dedupDeleted} expired markers`)
      } catch (err) {
        console.error('[DEDUP-CLEANUP] Error during dedup cleanup:', err)
      }

      // ================================================================
      // Task 2: CDC Compaction
      // ================================================================
      try {
        console.log('[COMPACT-CDC] Starting CDC compaction...')

        // Get catalog to list namespaces and tables
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

        console.log(
          `[COMPACT-CDC] Completed: ${totalCompacted} compacted, ${totalSkipped} skipped`
        )
      } catch (err) {
        console.error('[COMPACT-CDC] Error during CDC compaction:', err)
      }

      // ================================================================
      // Task 3: Event Stream Compaction (daily at 1:30 UTC only)
      // Compacts hourly Parquet files from EventWriterDO into daily files.
      // Only runs at 1:30 UTC to avoid compacting files still being written.
      // ================================================================
      const currentHour = new Date().getUTCHours()
      if (currentHour === 1) {
        try {
          console.log('[COMPACT-EVENTS] Starting event stream compaction...')

          const result = await compactEventStream(env.EVENTS_BUCKET, {
            prefixes: ['events', 'webhooks', 'tail'],
            daysBack: 7,
          })

          console.log(
            `[COMPACT-EVENTS] Completed in ${result.durationMs}ms: ` +
            `${result.days.length} days processed, ` +
            `${result.totalSourceFiles} source files -> ${result.totalOutputFiles} output files, ` +
            `${result.totalRecords} total records`
          )

          if (result.errors && result.errors.length > 0) {
            console.warn('[COMPACT-EVENTS] Errors:', result.errors)
          }
        } catch (err) {
          console.error('[COMPACT-EVENTS] Error during event stream compaction:', err)
        }
      }

      const durationMs = Date.now() - startTime
      console.log(`[SCHEDULED] All tasks completed in ${durationMs}ms`)
    })())
  },

  /**
   * Queue consumer handler - processes EventBatch messages for CDC/subscription fanout.
   *
   * This provides an alternative to direct DO calls in the ingest flow, enabling:
   * - Better scalability through queue-based buffering
   * - Automatic retries on failure
   * - Decoupled processing from the ingest response path
   *
   * Queue message format: EventBatch (same as ingest payload)
   */
  async queue(batch: MessageBatch<EventBatch>, env: Env): Promise<void> {
    const startTime = Date.now()
    console.log(`[QUEUE] Processing batch of ${batch.messages.length} messages`)

    let totalEvents = 0
    let cdcEventsProcessed = 0
    let subscriptionFanouts = 0

    for (const message of batch.messages) {
      try {
        const eventBatch = message.body

        if (!eventBatch?.events || !Array.isArray(eventBatch.events)) {
          console.warn(`[QUEUE] Invalid message format, missing events array`)
          message.ack()
          continue
        }

        totalEvents += eventBatch.events.length

        // Process CDC events (type starts with 'collection.')
        const cdcEvents = eventBatch.events.filter((e: DurableEvent) => e.type.startsWith('collection.'))

        if (cdcEvents.length > 0 && env.CDC_PROCESSOR) {
          // Group CDC events by namespace/collection for routing to the correct DO instance
          const cdcByKey = new Map<string, CDCEvent[]>()
          for (const event of cdcEvents) {
            const cdcEvent = event as unknown as CDCEvent
            const ns = cdcEvent.do?.class || cdcEvent.do?.name || 'default'
            const collection = cdcEvent.collection || 'default'
            const key = `${ns}/${collection}`
            const existing = cdcByKey.get(key) ?? []
            existing.push(cdcEvent)
            cdcByKey.set(key, existing)
          }

          // Send each group to the appropriate CDCProcessorDO instance
          for (const [key, events] of cdcByKey) {
            try {
              const processorId = env.CDC_PROCESSOR.idFromName(key)
              const processor = env.CDC_PROCESSOR.get(processorId)
              await processor.process(events)
              cdcEventsProcessed += events.length
            } catch (err) {
              console.error(`[QUEUE] CDC processor error for ${key}:`, err)
              // Don't ack the message yet - will retry
              throw err
            }
          }
        }

        // Fan out all events to subscription matching and delivery
        if (env.SUBSCRIPTIONS) {
          // Group events by shard key (first segment before '.', or 'default')
          const eventsByShard = new Map<string, DurableEvent[]>()
          for (const event of eventBatch.events) {
            const dotIndex = event.type.indexOf('.')
            const shardKey = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
            const existing = eventsByShard.get(shardKey) ?? []
            existing.push(event)
            eventsByShard.set(shardKey, existing)
          }

          // Fan out each group to its corresponding shard
          for (const [shardKey, events] of eventsByShard) {
            try {
              const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
              const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

              for (const event of events) {
                await subscriptionDO.fanout({
                  id: ulid(),
                  type: event.type,
                  ts: event.ts,
                  payload: event,
                })
                subscriptionFanouts++
              }
            } catch (err) {
              console.error(`[QUEUE] Subscription shard error for shard ${shardKey}:`, err)
              // Don't ack the message yet - will retry
              throw err
            }
          }
        }

        // Successfully processed this message
        message.ack()
      } catch (err) {
        console.error(`[QUEUE] Error processing message:`, err)
        // Message will be retried automatically by the queue
        message.retry()
      }
    }

    const durationMs = Date.now() - startTime
    console.log(
      `[QUEUE] Completed in ${durationMs}ms: ${totalEvents} events, ${cdcEventsProcessed} CDC, ${subscriptionFanouts} fanouts`
    )
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
