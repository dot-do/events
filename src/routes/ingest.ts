/**
 * Ingest route handler - POST /ingest, /e
 */

import type { DurableEvent, EventBatch } from '@dotdo/events'
import type { CDCEvent } from '../../core/src/cdc-processor'
import type { SchemaRegistryDO, ValidationError } from '../../core/src/schema-registry'
import type { Env } from '../env'
import { getEventBuffer, type EventRecord } from '../event-writer'
import { ingestWithOverflow } from '../event-writer-do'
import { ulid } from '../../core/src/ulid'
import { corsHeaders, buildSafeR2Path, InvalidR2PathError } from '../utils'
import { checkRateLimit } from '../middleware/rate-limit'
import { recordIngestMetric, recordCDCMetric, recordSubscriptionMetric, MetricTimer } from '../metrics'
import {
  extractTenantContext,
  buildNamespacedR2Path,
  getNamespacedShardKey,
  type TenantContext,
} from '../middleware/tenant'

// ============================================================================
// Input Validation
// ============================================================================

const MAX_EVENT_SIZE = 10 * 1024 // 10KB per event
const MAX_BATCH_SIZE = 1000 // Max events per batch
const MAX_TYPE_LENGTH = 256

export function validateEvent(event: unknown): event is { type: string; ts: string } {
  if (typeof event !== 'object' || event === null) return false
  const e = event as Record<string, unknown>
  if (typeof e.type !== 'string' || e.type.length === 0 || e.type.length > MAX_TYPE_LENGTH) return false
  if (typeof e.ts !== 'string' || isNaN(Date.parse(e.ts))) return false
  return true
}

export function validateBatch(batch: unknown): batch is { events: unknown[] } {
  if (typeof batch !== 'object' || batch === null) return false
  const b = batch as Record<string, unknown>
  if (!Array.isArray(b.events)) return false
  if (b.events.length > MAX_BATCH_SIZE) return false
  return true
}

export function validateEventSize(event: unknown): boolean {
  const size = JSON.stringify(event).length
  return size <= MAX_EVENT_SIZE
}

// ============================================================================
// Schema Validation
// ============================================================================

/**
 * Schema validation error details returned to callers
 */
export interface SchemaValidationError {
  index: number
  eventType: string
  errors: ValidationError[]
}

/**
 * Type for validation results returned by the schema registry
 */
interface BatchValidationResult {
  valid: boolean
  results: { index: number; eventType: string; valid: boolean; errors: ValidationError[] }[]
}

/**
 * Validate events against registered schemas
 * Returns null if validation passes or is disabled, otherwise returns validation errors
 */
async function validateEventsAgainstSchemas(
  events: { type: string; [key: string]: unknown }[],
  namespace: string,
  env: Env
): Promise<SchemaValidationError[] | null> {
  // Check if schema validation is enabled
  if (env.ENABLE_SCHEMA_VALIDATION !== 'true' || !env.SCHEMA_REGISTRY) {
    return null
  }

  try {
    // Get schema registry instance for this namespace
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    // Validate all events in batch
    const result = await registry.validateEvents(events, namespace) as BatchValidationResult

    if (!result.valid && result.results.length > 0) {
      // Return validation errors that caused rejection
      return result.results
        .filter((r) => !r.valid)
        .map((r) => ({
          index: r.index,
          eventType: r.eventType,
          errors: r.errors,
        }))
    }

    return null
  } catch (err) {
    // Log but don't fail on schema registry errors
    console.error('[ingest] Schema validation error:', err)
    return null
  }
}

// ============================================================================
// Ingest Handler
// ============================================================================

export async function handleIngest(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const timer = new MetricTimer()

  // Multi-tenant authentication
  // Supports: namespace-scoped API keys (ns_<namespace>_<token>), legacy AUTH_TOKEN, or unauthenticated
  let tenant: TenantContext | null = null

  // Check if namespace-scoped API keys are configured
  const hasNamespaceKeys = !!env.NAMESPACE_API_KEYS

  if (hasNamespaceKeys || env.AUTH_TOKEN) {
    // Tenant extraction handles both namespace-scoped keys and legacy AUTH_TOKEN
    const tenantResult = extractTenantContext(request, env)
    if (tenantResult instanceof Response) {
      recordIngestMetric(env.ANALYTICS, 'error', 0, timer.elapsed(), 'unauthorized')
      return tenantResult
    }
    tenant = tenantResult
    console.log(`[ingest] Authenticated request for namespace: ${tenant.namespace} (admin: ${tenant.isAdmin})`)
  } else if (env.ALLOW_UNAUTHENTICATED_INGEST !== 'true') {
    console.warn('[ingest] No AUTH_TOKEN or NAMESPACE_API_KEYS configured. Set AUTH_TOKEN secret, NAMESPACE_API_KEYS, or set ALLOW_UNAUTHENTICATED_INGEST=true to allow open access.')
    return Response.json(
      { error: 'Server misconfiguration: authentication not configured' },
      { status: 500, headers: corsHeaders() }
    )
  } else {
    // Unauthenticated access explicitly allowed - use default namespace
    console.warn('[ingest] Processing unauthenticated request (ALLOW_UNAUTHENTICATED_INGEST=true)')
    tenant = {
      namespace: env.DEFAULT_NAMESPACE || 'default',
      isAdmin: false,
      keyId: 'unauthenticated',
    }
  }

  let batch: unknown
  try {
    batch = await request.json()
  } catch {
    recordIngestMetric(env.ANALYTICS, 'validation_error', 0, timer.elapsed(), 'invalid_json')
    return Response.json({ error: 'Invalid JSON' }, { status: 400, headers: corsHeaders() })
  }

  // Validate batch structure
  if (!validateBatch(batch)) {
    recordIngestMetric(env.ANALYTICS, 'validation_error', 0, timer.elapsed(), 'invalid_batch')
    return Response.json(
      { error: 'Invalid batch: must have events array with max 1000 events' },
      { status: 400, headers: corsHeaders() }
    )
  }

  // Rate limiting check (after batch validation so we know event count)
  const rateLimitResponse = await checkRateLimit(request, env, batch.events.length)
  if (rateLimitResponse) {
    recordIngestMetric(env.ANALYTICS, 'rate_limited', batch.events.length, timer.elapsed())
    return rateLimitResponse
  }

  // Extract optional batchId for deduplication/idempotency
  const batchId = (batch as Record<string, unknown>).batchId as string | undefined

  // Deduplication check: if batchId is provided, check if already ingested
  // Use namespace-isolated dedup keys to prevent cross-tenant collisions
  if (batchId) {
    // Sanitize batchId to prevent path traversal attacks
    let dedupKey: string
    try {
      // Namespace-isolated dedup key: ns/<namespace>/dedup/<batchId>
      dedupKey = buildNamespacedR2Path(tenant!, 'dedup', batchId)
    } catch (err) {
      if (err instanceof InvalidR2PathError) {
        return Response.json(
          { error: `Invalid batchId: ${err.message}` },
          { status: 400, headers: corsHeaders() }
        )
      }
      throw err
    }
    const existing = await env.EVENTS_BUCKET.head(dedupKey)
    if (existing) {
      // Already ingested - return success without re-writing (idempotent)
      return Response.json({
        ok: true,
        received: (batch as EventBatch).events.length,
        deduplicated: true,
        namespace: tenant!.namespace,
      }, { headers: corsHeaders() })
    }
  }

  // Validate each event
  const invalidEvents: number[] = []
  const oversizedEvents: number[] = []
  for (let i = 0; i < batch.events.length; i++) {
    const event = batch.events[i]
    if (!validateEvent(event)) {
      invalidEvents.push(i)
    } else if (!validateEventSize(event)) {
      oversizedEvents.push(i)
    }
  }

  if (invalidEvents.length > 0) {
    recordIngestMetric(env.ANALYTICS, 'validation_error', batch.events.length, timer.elapsed(), 'invalid_events')
    return Response.json(
      { error: `Invalid events at indices: ${invalidEvents.slice(0, 10).join(', ')}${invalidEvents.length > 10 ? '...' : ''}. Each event must have type (string, 1-256 chars) and ts (valid ISO timestamp)` },
      { status: 400, headers: corsHeaders() }
    )
  }

  if (oversizedEvents.length > 0) {
    recordIngestMetric(env.ANALYTICS, 'validation_error', batch.events.length, timer.elapsed(), 'oversized_events')
    return Response.json(
      { error: `Events exceed max size (10KB) at indices: ${oversizedEvents.slice(0, 10).join(', ')}${oversizedEvents.length > 10 ? '...' : ''}` },
      { status: 400, headers: corsHeaders() }
    )
  }

  // Type assertion is now safe after basic validation
  const validatedBatch = batch as EventBatch

  // Schema validation (if enabled)
  // This validates events against registered JSON schemas for the tenant's namespace
  const schemaErrors = await validateEventsAgainstSchemas(
    validatedBatch.events as unknown as { type: string; [key: string]: unknown }[],
    tenant!.namespace,
    env
  )

  if (schemaErrors && schemaErrors.length > 0) {
    recordIngestMetric(env.ANALYTICS, 'validation_error', batch.events.length, timer.elapsed(), 'schema_validation')

    // Format schema validation errors for the response
    const errorSummary = schemaErrors.slice(0, 5).map((e) => {
      const errorMsgs = e.errors.slice(0, 3).map((err) => `${err.path || '(root)'}: ${err.message}`)
      return `Event[${e.index}] (${e.eventType}): ${errorMsgs.join('; ')}`
    })

    return Response.json(
      {
        error: 'Schema validation failed',
        validationErrors: schemaErrors.slice(0, 10),
        summary: errorSummary,
      },
      { status: 400, headers: corsHeaders() }
    )
  }

  // Convert DurableEvents to EventRecord format for the unified Parquet writer
  // Include namespace in payload for query isolation
  const records: EventRecord[] = validatedBatch.events.map((event: DurableEvent) => ({
    ts: event.ts,
    type: event.type,
    source: 'ingest',
    // Store the full event as payload in the VARIANT column
    // Include namespace for multi-tenant isolation in queries
    payload: { ...event, _namespace: tenant!.namespace },
  }))

  // Determine fanout mode ONCE at the start - this ensures mutual exclusion
  // If USE_QUEUE_FANOUT=true AND queue is bound: ONLY use queue for CDC/subscription fanout
  // Otherwise: ONLY use direct DO calls for CDC/subscription fanout
  const useQueueFanout = env.USE_QUEUE_FANOUT === 'true' && !!env.EVENTS_QUEUE
  console.log(`[ingest] Fanout mode: ${useQueueFanout ? 'QUEUE' : 'DIRECT'} (USE_QUEUE_FANOUT=${env.USE_QUEUE_FANOUT}, queue=${!!env.EVENTS_QUEUE})`)

  // Build namespace-isolated source path for events
  // Events are stored under: ns/<namespace>/events/... for tenant isolation
  const namespacedSource = buildNamespacedR2Path(tenant!, 'events')

  // Send to EventWriterDO for batched Parquet writes (in background for fast response)
  // Use namespace-prefixed shard key to isolate DO instances per tenant
  const writerShardKey = getNamespacedShardKey(tenant!, 'events')
  ctx.waitUntil((async () => {
    const result = await ingestWithOverflow(env, records, namespacedSource)
    if (!result.ok) {
      console.error(`[ingest] Failed to ingest to EventWriterDO, shard ${result.shard}, namespace: ${tenant!.namespace}`)
    } else {
      console.log(`[ingest] Ingested ${records.length} events to shard ${result.shard}, buffered: ${result.buffered}, namespace: ${tenant!.namespace}`)
    }

    // Write dedup marker if batchId was provided
    // Note: batchId has already been validated for path safety at request time
    if (batchId) {
      // Use namespace-isolated dedup key
      const dedupKey = buildNamespacedR2Path(tenant!, 'dedup', batchId)
      await env.EVENTS_BUCKET.put(dedupKey, JSON.stringify({
        batchId,
        namespace: tenant!.namespace,
        eventCount: validatedBatch.events.length,
        ingestedAt: new Date().toISOString(),
      }), {
        httpMetadata: { contentType: 'application/json' },
        customMetadata: {
          batchId,
          namespace: tenant!.namespace,
          eventCount: String(validatedBatch.events.length),
          // Note: R2 does not support TTL natively. Configure R2 lifecycle rules
          // on the bucket to auto-delete objects under the dedup/ prefix
          // after a reasonable retention period (e.g., 24-48 hours).
          ttlNote: 'Configure R2 lifecycle rule to expire dedup/ prefix objects',
        },
      })
    }

    // MUTUAL EXCLUSION: Only send to queue if queue fanout is enabled
    // The queue consumer will handle CDC/subscription processing
    if (useQueueFanout) {
      await env.EVENTS_QUEUE!.send(validatedBatch)
      console.log(`[ingest] Sent ${validatedBatch.events.length} events to queue for CDC/subscription fanout`)
    }
    // NOTE: We do NOT send to queue when useQueueFanout=false, even if queue is bound.
    // This prevents duplicate processing - direct fanout handles CDC/subscription below.
  })())

  // MUTUAL EXCLUSION: Only do direct fanout if queue fanout is NOT enabled
  // This ensures CDC and subscription events are processed exactly once
  if (!useQueueFanout) {
    ctx.waitUntil((async () => {
      console.log(`[ingest] Starting DIRECT fanout for ${validatedBatch.events.length} events, namespace: ${tenant!.namespace}`)
      try {
        // Collect CDC events (type starts with 'collection.')
        const cdcEvents = validatedBatch.events.filter((e: DurableEvent) => e.type.startsWith('collection.'))

        if (cdcEvents.length > 0 && env.CDC_PROCESSOR) {
          // Group CDC events by namespace/collection for routing to the correct DO instance
          // Use tenant namespace prefix to isolate CDC processors per tenant
          const cdcByKey = new Map<string, CDCEvent[]>()
          for (const event of cdcEvents) {
            const cdcEvent = event as unknown as CDCEvent
            const ns = cdcEvent.do?.class || cdcEvent.do?.name || 'default'
            const collection = cdcEvent.collection || 'default'
            // Namespace-isolated CDC processor key: <tenant-namespace>:<ns>/<collection>
            const key = getNamespacedShardKey(tenant!, `${ns}/${collection}`)
            const existing = cdcByKey.get(key) ?? []
            existing.push(cdcEvent)
            cdcByKey.set(key, existing)
          }

          // Send each group to the appropriate CDCProcessorDO instance
          const cdcPromises = Array.from(cdcByKey.entries()).map(async ([key, events]) => {
            try {
              const processorId = env.CDC_PROCESSOR.idFromName(key)
              const processor = env.CDC_PROCESSOR.get(processorId)
              await processor.process(events)
              recordCDCMetric(env.ANALYTICS, 'success', events.length, key)
              console.log(`[ingest] DIRECT CDC processed ${events.length} events for ${key}`)
            } catch (err) {
              recordCDCMetric(env.ANALYTICS, 'error', events.length, key)
              console.error(`[ingest] CDC processor error for ${key}:`, err)
            }
          })
          await Promise.all(cdcPromises)
        }

        // Fan out all events to subscription matching and delivery
        // Route to per-type-prefix shards (e.g., "collection.*" -> shard "collection")
        // Use namespace prefix to isolate subscriptions per tenant
        if (env.SUBSCRIPTIONS) {
          // Group events by shard key (first segment before '.', or 'default')
          const eventsByShard = new Map<string, typeof validatedBatch.events>()
          for (const event of validatedBatch.events) {
            const dotIndex = event.type.indexOf('.')
            const baseShardKey = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
            // Namespace-isolated subscription shard key
            const shardKey = getNamespacedShardKey(tenant!, baseShardKey)
            const existing = eventsByShard.get(shardKey) ?? []
            existing.push(event)
            eventsByShard.set(shardKey, existing)
          }

          // Fan out each group to its corresponding shard
          // Use event.id if available for idempotency, otherwise generate ULID
          const shardPromises = Array.from(eventsByShard.entries()).map(async ([shardKey, events]) => {
            let successCount = 0
            let errorCount = 0
            try {
              const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
              const subscriptionDO = env.SUBSCRIPTIONS.get(subId)
              const fanoutPromises = events.map(async (event: DurableEvent) => {
                // Use existing event id for idempotency if available
                const eventId = (event as { id?: string }).id || ulid()
                try {
                  await subscriptionDO.fanout({
                    id: eventId,
                    type: event.type,
                    ts: event.ts,
                    payload: { ...event, _namespace: tenant!.namespace },
                  })
                  successCount++
                } catch (err) {
                  errorCount++
                  console.error(`[ingest] Subscription fanout error for event ${eventId} type ${event.type}:`, err)
                }
              })
              await Promise.all(fanoutPromises)
              recordSubscriptionMetric(env.ANALYTICS, 'success', successCount, shardKey)
              if (errorCount > 0) {
                recordSubscriptionMetric(env.ANALYTICS, 'error', errorCount, shardKey, 'fanout_error')
              }
              console.log(`[ingest] DIRECT subscription fanout: ${events.length} events to shard ${shardKey}`)
            } catch (err) {
              recordSubscriptionMetric(env.ANALYTICS, 'error', events.length, shardKey, 'shard_error')
              console.error(`[ingest] Subscription shard error for shard ${shardKey}:`, err)
            }
          })
          await Promise.all(shardPromises)
        }
      } catch (err) {
        console.error('[ingest] CDC/subscription pipeline error:', err)
      }
    })())
  }

  // Record successful ingest metric
  recordIngestMetric(env.ANALYTICS, 'success', validatedBatch.events.length, timer.elapsed())

  return Response.json({
    ok: true,
    received: validatedBatch.events.length,
    namespace: tenant!.namespace,
  }, { headers: corsHeaders() })
}
