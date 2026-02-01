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
import { withSchedulerLock } from './scheduler-lock'

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
          authenticated: true,
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
   * Uses a distributed lock to ensure only one worker runs scheduled tasks at a time.
   * The lock prevents duplicate work when multiple workers trigger on the same cron schedule.
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
      // Acquire distributed lock to prevent concurrent scheduled runs
      const lockResult = await withSchedulerLock(
        env.EVENTS_BUCKET,
        async () => {
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

            // Discover namespaces by scanning R2 for cdc prefixes
            // This is more reliable than relying on catalog state since we're sharding by namespace
            const namespaces = new Set<string>()

            // Scan R2 for existing CDC namespaces (format: cdc/{namespace}/{table}/...)
            let cdcCursor: string | undefined
            do {
              const list = await env.EVENTS_BUCKET.list({
                prefix: 'cdc/',
                delimiter: '/',
                cursor: cdcCursor,
              })

              // Extract namespace from common prefixes (format: cdc/{namespace}/)
              for (const prefix of list.delimitedPrefixes || []) {
                const parts = prefix.split('/')
                if (parts.length >= 2 && parts[1]) {
                  namespaces.add(parts[1])
                }
              }

              cdcCursor = list.truncated ? list.cursor : undefined
            } while (cdcCursor)

            // Also check legacy catalog for any namespaces (backwards compatibility)
            const legacyCatalogId = env.CATALOG.idFromName('events-catalog')
            const legacyCatalog = env.CATALOG.get(legacyCatalogId)
            try {
              const legacyNamespaces = await legacyCatalog.listNamespaces()
              for (const ns of legacyNamespaces) {
                namespaces.add(ns)
              }
            } catch {
              // Ignore errors from legacy catalog
            }

            const namespaceList = [...namespaces]
            console.log(`[COMPACT-CDC] Found ${namespaceList.length} namespaces: ${namespaceList.join(', ')}`)

            let totalCompacted = 0
            let totalSkipped = 0
            const results: { ns: string; table: string; result: string; recordCount?: number; deltasProcessed?: number }[] = []

            // 2. For each namespace, get the sharded catalog and list tables
            for (const ns of namespaceList) {
              // Get namespace-sharded catalog
              const catalogId = env.CATALOG.idFromName(ns)
              const catalog = env.CATALOG.get(catalogId)

              // Get tables from sharded catalog
              let tables = await catalog.listTables(ns)

              // Also check legacy catalog for this namespace (migration support)
              if (tables.length === 0) {
                try {
                  tables = await legacyCatalog.listTables(ns)
                } catch {
                  // Ignore errors from legacy catalog
                }
              }

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
        },
        { timeoutMs: 10 * 60 * 1000 } // 10 minute lock timeout
      )

      if (lockResult.skipped) {
        console.log('[SCHEDULED] Skipped - another worker is running scheduled tasks')
      }
    })())
  },

  /**
   * Queue consumer handler - processes EventBatch messages for CDC/subscription fanout.
   *
   * MUTUAL EXCLUSION: This queue consumer ONLY processes events when USE_QUEUE_FANOUT=true.
   * The ingest handler will only send to the queue when queue fanout is enabled,
   * ensuring events are processed by either queue OR direct fanout, never both.
   *
   * This provides an alternative to direct DO calls in the ingest flow, enabling:
   * - Better scalability through queue-based buffering
   * - Automatic retries on failure (up to MAX_RETRIES)
   * - Dead letter logging for permanently failed messages
   * - Decoupled processing from the ingest response path
   * - Event-level idempotency using event IDs
   *
   * Queue message format: EventBatch (same as ingest payload)
   */
  async queue(batch: MessageBatch<EventBatch>, env: Env): Promise<void> {
    const startTime = Date.now()
    const MAX_RETRIES = 5

    console.log(`[QUEUE] Processing batch of ${batch.messages.length} messages (QUEUE fanout mode)`)

    let totalEvents = 0
    let cdcEventsProcessed = 0
    let cdcEventsSkipped = 0
    let subscriptionFanouts = 0
    let subscriptionSkipped = 0
    let deadLettered = 0

    for (const message of batch.messages) {
      const attempts = message.attempts ?? 1

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
          // Use event ID for deduplication to prevent double-processing on retries
          for (const [key, events] of cdcByKey) {
            try {
              // Filter out already-processed events using dedup markers
              const eventsToProcess: CDCEvent[] = []
              for (const event of events) {
                const eventId = (event as { id?: string }).id
                if (eventId) {
                  const dedupKey = `dedup/cdc/${eventId}`
                  const existing = await env.EVENTS_BUCKET.head(dedupKey)
                  if (existing) {
                    cdcEventsSkipped++
                    continue // Skip already processed event
                  }
                }
                eventsToProcess.push(event)
              }

              if (eventsToProcess.length > 0) {
                const processorId = env.CDC_PROCESSOR.idFromName(key)
                const processor = env.CDC_PROCESSOR.get(processorId)
                await processor.process(eventsToProcess)
                cdcEventsProcessed += eventsToProcess.length

                // Write dedup markers for processed events
                for (const event of eventsToProcess) {
                  const eventId = (event as { id?: string }).id
                  if (eventId) {
                    const dedupKey = `dedup/cdc/${eventId}`
                    await env.EVENTS_BUCKET.put(dedupKey, '', {
                      customMetadata: { processedAt: new Date().toISOString() },
                    })
                  }
                }
              }

              console.log(`[QUEUE] CDC processed ${eventsToProcess.length} events for ${key} (skipped ${events.length - eventsToProcess.length} duplicates)`)
            } catch (err) {
              console.error(`[QUEUE] CDC processor error for ${key}:`, err)
              // Don't ack the message yet - will retry or dead letter
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
          // Use event ID for idempotency to prevent duplicate delivery on retries
          for (const [shardKey, events] of eventsByShard) {
            let shardSkipped = 0
            try {
              const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
              const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

              for (const event of events) {
                // Use existing event id for idempotency if available
                const eventId = (event as { id?: string }).id || ulid()

                // Check dedup marker for subscription delivery
                const dedupKey = `dedup/sub/${eventId}`
                const existing = await env.EVENTS_BUCKET.head(dedupKey)
                if (existing) {
                  subscriptionSkipped++
                  shardSkipped++
                  continue // Skip already delivered event
                }

                await subscriptionDO.fanout({
                  id: eventId,
                  type: event.type,
                  ts: event.ts,
                  payload: event,
                })

                // Write dedup marker after successful delivery
                await env.EVENTS_BUCKET.put(dedupKey, '', {
                  customMetadata: { deliveredAt: new Date().toISOString() },
                })

                subscriptionFanouts++
              }

              console.log(`[QUEUE] Subscription fanout: ${events.length - shardSkipped} events to shard ${shardKey} (skipped ${shardSkipped} duplicates)`)
            } catch (err) {
              console.error(`[QUEUE] Subscription shard error for shard ${shardKey}:`, err)
              // Don't ack the message yet - will retry or dead letter
              throw err
            }
          }
        }

        // Successfully processed this message
        message.ack()
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err)

        // Check if we've exceeded max retries
        if (attempts >= MAX_RETRIES) {
          // Permanent failure - log to dead letter and ack to prevent further retries
          console.error(
            `[QUEUE] Dead letter: message failed after ${attempts} attempts. Error: ${errorMsg}`,
            { messageId: message.id, body: message.body }
          )

          // Write to R2 dead letter storage for later inspection/reprocessing
          try {
            const deadLetterKey = `dead-letter/queue/${new Date().toISOString().slice(0, 10)}/${message.id || ulid()}.json`
            await env.EVENTS_BUCKET.put(
              deadLetterKey,
              JSON.stringify({
                messageId: message.id,
                attempts,
                error: errorMsg,
                body: message.body,
                timestamp: new Date().toISOString(),
              }),
              { httpMetadata: { contentType: 'application/json' } }
            )
            console.log(`[QUEUE] Dead letter written to ${deadLetterKey}`)
          } catch (dlErr) {
            console.error(`[QUEUE] Failed to write dead letter:`, dlErr)
          }

          deadLettered++
          message.ack() // Ack to prevent further retries
        } else {
          // Transient failure - retry
          console.warn(
            `[QUEUE] Retrying message (attempt ${attempts}/${MAX_RETRIES}). Error: ${errorMsg}`
          )
          message.retry()
        }
      }
    }

    const durationMs = Date.now() - startTime
    console.log(
      `[QUEUE] Completed in ${durationMs}ms: ${totalEvents} events, ` +
      `CDC: ${cdcEventsProcessed} processed/${cdcEventsSkipped} skipped, ` +
      `Subscriptions: ${subscriptionFanouts} delivered/${subscriptionSkipped} skipped` +
      (deadLettered > 0 ? `, ${deadLettered} dead-lettered` : '')
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
