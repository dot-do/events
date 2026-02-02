/**
 * Ingest route handler - POST /ingest, /e
 *
 * This is the main entry point that composes the ingest middleware chain.
 * The actual middleware implementations are in src/middleware/ingest/
 */

import type { DurableEvent, EventBatch } from '@dotdo/events'
import type { Env } from '../env'
import type { EventRecord } from '../event-writer'
import { ingestWithOverflow } from '../event-writer-do'
import { corsHeaders } from '../utils'
import { successResponse, errorResponse, internalError, ErrorCodes } from '../utils/response'
import { checkRateLimit } from '../middleware/rate-limit'
import { recordIngestMetric, MetricTimer } from '../metrics'
import { incrementEventsIngested } from './metrics'
import {
  extractTenantContext,
  buildNamespacedR2Path,
  type TenantContext,
} from '../middleware/tenant'
import { createLogger, logError } from '../logger'

// Import from the new middleware chain
import {
  type IngestContext,
  type MiddlewareResult,
  composeMiddleware,
  parseJsonMiddleware,
  validateBatchMiddleware,
  validateEventsMiddleware,
  dedupMiddleware,
  schemaValidationMiddleware,
  quotaMiddleware,
  trackEventUsage,
  writeDedupMarker,
  determineFanoutMode,
  executeDirectFanout,
  executeQueueFanout,
  // Re-export validation functions for backwards compatibility
  validateEvent,
  validateBatch,
  validateEventSize,
  // Re-export types for backwards compatibility
  type SchemaValidationError,
} from '../middleware/ingest'

const log = createLogger({ component: 'ingest' })

// ============================================================================
// Backwards Compatibility Exports
// ============================================================================

// Re-export validation functions for any code that imports them from this file
export { validateEvent, validateBatch, validateEventSize }
export type { SchemaValidationError }

// ============================================================================
// Rate Limit Middleware
// ============================================================================

/**
 * Rate limiting middleware
 */
async function rateLimitMiddleware(
  context: IngestContext
): Promise<MiddlewareResult> {
  const { request, env, batch } = context
  const elapsed = performance.now() - context.startTime

  const rateLimitResponse = await checkRateLimit(request, env, batch!.events.length)
  if (rateLimitResponse) {
    recordIngestMetric(env.ANALYTICS, 'rate_limited', batch!.events.length, elapsed)
    return { continue: false, response: rateLimitResponse }
  }

  return { continue: true }
}

// ============================================================================
// Authentication Handler
// ============================================================================

/**
 * Handle authentication and tenant extraction
 * Returns tenant context or error response
 */
async function handleAuthentication(
  request: Request,
  env: Env,
  timer: MetricTimer
): Promise<TenantContext | Response> {
  // Check if namespace-scoped API keys are configured
  const hasNamespaceKeys = !!env.NAMESPACE_API_KEYS

  if (hasNamespaceKeys || env.AUTH_TOKEN) {
    // Tenant extraction handles both namespace-scoped keys and legacy AUTH_TOKEN
    const tenantResult = await extractTenantContext(request, env)
    if (tenantResult instanceof Response) {
      recordIngestMetric(env.ANALYTICS, 'error', 0, timer.elapsed(), 'unauthorized')
      return tenantResult
    }
    log.info('Authenticated request', { namespace: tenantResult.namespace, isAdmin: tenantResult.isAdmin })
    return tenantResult
  }

  if (env.ALLOW_UNAUTHENTICATED_INGEST !== 'true') {
    log.warn('Authentication not configured', {
      hint: 'Set AUTH_TOKEN secret, NAMESPACE_API_KEYS, or ALLOW_UNAUTHENTICATED_INGEST=true',
    })
    return internalError(
      'Server misconfiguration: authentication not configured',
      { headers: corsHeaders() }
    )
  }

  // Unauthenticated access explicitly allowed - use default namespace
  log.warn('Processing unauthenticated request', { allowUnauthenticated: true })
  return {
    namespace: env.DEFAULT_NAMESPACE || 'default',
    isAdmin: false,
    keyId: 'unauthenticated',
  }
}

// ============================================================================
// Ingest Handler
// ============================================================================

/**
 * Main ingest handler using middleware chain
 */
export async function handleIngest(
  request: Request,
  env: Env,
  ctx: ExecutionContext
): Promise<Response> {
  const timer = new MetricTimer()

  // Authentication (before middleware chain)
  const authResult = await handleAuthentication(request, env, timer)
  if (authResult instanceof Response) {
    return authResult
  }
  const tenant = authResult

  // Initialize context for middleware chain
  const context: IngestContext = {
    request,
    env,
    ctx,
    tenant,
    startTime: performance.now(),
  }

  // Compose and execute middleware pipeline
  // Order matters: parse -> validate batch -> rate limit -> quota -> dedup -> validate events -> schema
  const pipeline = composeMiddleware([
    parseJsonMiddleware,
    validateBatchMiddleware,
    rateLimitMiddleware,
    quotaMiddleware,
    dedupMiddleware,
    validateEventsMiddleware,
    schemaValidationMiddleware,
  ])

  const middlewareResult = await pipeline(context)

  // If middleware returned a response (error or dedup), return it
  if (!middlewareResult.continue) {
    return middlewareResult.response
  }

  // If deduplicated, context.deduplicated would have returned early
  // Continue with main processing

  const validatedBatch = context.batch!

  // Convert DurableEvents to EventRecord format for the unified Parquet writer
  // Include namespace in payload for query isolation
  const records: EventRecord[] = validatedBatch.events.map((event: DurableEvent) => ({
    ts: event.ts,
    type: event.type,
    source: 'ingest',
    // Store the full event as payload in the VARIANT column
    // Include namespace for multi-tenant isolation in queries
    payload: { ...event, _namespace: tenant.namespace },
  }))

  // Determine fanout mode ONCE at the start - this ensures mutual exclusion
  const useQueueFanout = determineFanoutMode(env)
  log.info('Fanout mode determined', {
    mode: useQueueFanout ? 'QUEUE' : 'DIRECT',
    useQueueFanout: env.USE_QUEUE_FANOUT,
    hasQueue: !!env.EVENTS_QUEUE,
  })

  // Store in context for fanout handlers
  context.records = records
  context.useQueueFanout = useQueueFanout
  context.namespacedSource = buildNamespacedR2Path(tenant, 'events')

  // Send to EventWriterDO for batched Parquet writes (in background for fast response)
  ctx.waitUntil(
    (async () => {
      const result = await ingestWithOverflow(env, records, context.namespacedSource!)
      if (!result.ok) {
        log.error('Failed to ingest to EventWriterDO', {
          shard: result.shard,
          namespace: tenant.namespace,
        })
      } else {
        log.info('Ingested events', {
          eventCount: records.length,
          shard: result.shard,
          buffered: result.buffered,
          namespace: tenant.namespace,
        })
      }

      // Write dedup marker if batchId was provided
      await writeDedupMarker(context)

      // MUTUAL EXCLUSION: Only send to queue if queue fanout is enabled
      if (useQueueFanout) {
        await executeQueueFanout(context)
      }
    })()
  )

  // MUTUAL EXCLUSION: Only do direct fanout if queue fanout is NOT enabled
  if (!useQueueFanout) {
    ctx.waitUntil(executeDirectFanout(context))
  }

  // Record successful ingest metric
  const elapsedMs = timer.elapsed()
  recordIngestMetric(env.ANALYTICS, 'success', validatedBatch.events.length, elapsedMs)
  incrementEventsIngested(validatedBatch.events.length, elapsedMs)

  return successResponse(
    {
      ok: true,
      received: validatedBatch.events.length,
      namespace: tenant.namespace,
    },
    { headers: corsHeaders() }
  )
}
