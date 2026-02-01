/**
 * Ingest route handler - POST /ingest, /e
 */

import type { DurableEvent, EventBatch } from '@dotdo/events'
import type { CDCEvent } from '../../core/src/cdc-processor'
import type { Env } from '../env'
import { getEventBuffer, type EventRecord } from '../event-writer'
import { ingestWithOverflow } from '../event-writer-do'
import { ulid } from '../../core/src/ulid'
import { corsHeaders } from '../utils'

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
// Ingest Handler
// ============================================================================

export async function handleIngest(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  // Optional auth
  if (env.AUTH_TOKEN) {
    const auth = request.headers.get('Authorization')
    if (auth !== `Bearer ${env.AUTH_TOKEN}`) {
      return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders() })
    }
  }

  let batch: unknown
  try {
    batch = await request.json()
  } catch {
    return Response.json({ error: 'Invalid JSON' }, { status: 400, headers: corsHeaders() })
  }

  // Validate batch structure
  if (!validateBatch(batch)) {
    return Response.json(
      { error: 'Invalid batch: must have events array with max 1000 events' },
      { status: 400, headers: corsHeaders() }
    )
  }

  // Extract optional batchId for deduplication/idempotency
  const batchId = (batch as Record<string, unknown>).batchId as string | undefined

  // Deduplication check: if batchId is provided, check if already ingested
  if (batchId) {
    const dedupKey = `dedup/${batchId}`
    const existing = await env.EVENTS_BUCKET.head(dedupKey)
    if (existing) {
      // Already ingested - return success without re-writing (idempotent)
      return Response.json({
        ok: true,
        received: (batch as EventBatch).events.length,
        deduplicated: true,
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
    return Response.json(
      { error: `Invalid events at indices: ${invalidEvents.slice(0, 10).join(', ')}${invalidEvents.length > 10 ? '...' : ''}. Each event must have type (string, 1-256 chars) and ts (valid ISO timestamp)` },
      { status: 400, headers: corsHeaders() }
    )
  }

  if (oversizedEvents.length > 0) {
    return Response.json(
      { error: `Events exceed max size (10KB) at indices: ${oversizedEvents.slice(0, 10).join(', ')}${oversizedEvents.length > 10 ? '...' : ''}` },
      { status: 400, headers: corsHeaders() }
    )
  }

  // Type assertion is now safe after validation
  const validatedBatch = batch as EventBatch

  // Convert DurableEvents to EventRecord format for the unified Parquet writer
  const records: EventRecord[] = validatedBatch.events.map(event => ({
    ts: event.ts,
    type: event.type,
    source: 'ingest',
    // Store the full event as payload in the VARIANT column
    payload: event,
  }))

  // Send to EventWriterDO for batched Parquet writes (in background for fast response)
  ctx.waitUntil((async () => {
    const result = await ingestWithOverflow(env, records, 'events')
    if (!result.ok) {
      console.error(`[ingest] Failed to ingest to EventWriterDO, shard ${result.shard}`)
    } else {
      console.log(`[ingest] Ingested ${records.length} events to shard ${result.shard}, buffered: ${result.buffered}`)
    }

    // Write dedup marker if batchId was provided
    if (batchId) {
      const dedupKey = `dedup/${batchId}`
      await env.EVENTS_BUCKET.put(dedupKey, JSON.stringify({
        batchId,
        eventCount: validatedBatch.events.length,
        ingestedAt: new Date().toISOString(),
      }), {
        httpMetadata: { contentType: 'application/json' },
        customMetadata: {
          batchId,
          eventCount: String(validatedBatch.events.length),
          // Note: R2 does not support TTL natively. Configure R2 lifecycle rules
          // on the bucket to auto-delete objects under the dedup/ prefix
          // after a reasonable retention period (e.g., 24-48 hours).
          ttlNote: 'Configure R2 lifecycle rule to expire dedup/ prefix objects',
        },
      })
    }

    // Optionally send to Queue for real-time consumers
    if (env.EVENTS_QUEUE) {
      await env.EVENTS_QUEUE.send(validatedBatch)
    }
  })())

  // Fan out CDC events to CDCProcessorDO and all events to SubscriptionDO (non-blocking)
  ctx.waitUntil((async () => {
    try {
      // Collect CDC events (type starts with 'collection.')
      const cdcEvents = validatedBatch.events.filter(e => e.type.startsWith('collection.'))

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
        const cdcPromises = Array.from(cdcByKey.entries()).map(async ([key, events]) => {
          try {
            const processorId = env.CDC_PROCESSOR.idFromName(key)
            const processor = env.CDC_PROCESSOR.get(processorId)
            await processor.process(events)
          } catch (err) {
            console.error(`[ingest] CDC processor error for ${key}:`, err)
          }
        })
        await Promise.all(cdcPromises)
      }

      // Fan out all events to subscription matching and delivery
      // Route to per-type-prefix shards (e.g., "collection.*" -> shard "collection")
      if (env.SUBSCRIPTIONS) {
        // Group events by shard key (first segment before '.', or 'default')
        const eventsByShard = new Map<string, typeof validatedBatch.events>()
        for (const event of validatedBatch.events) {
          const dotIndex = event.type.indexOf('.')
          const shardKey = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
          const existing = eventsByShard.get(shardKey) ?? []
          existing.push(event)
          eventsByShard.set(shardKey, existing)
        }

        // Fan out each group to its corresponding shard
        const shardPromises = Array.from(eventsByShard.entries()).map(async ([shardKey, events]) => {
          try {
            const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
            const subscriptionDO = env.SUBSCRIPTIONS.get(subId)
            const fanoutPromises = events.map(async (event) => {
              try {
                await subscriptionDO.fanout({
                  id: ulid(),
                  type: event.type,
                  ts: event.ts,
                  payload: event,
                })
              } catch (err) {
                console.error(`[ingest] Subscription fanout error for event type ${event.type}:`, err)
              }
            })
            await Promise.all(fanoutPromises)
          } catch (err) {
            console.error(`[ingest] Subscription shard error for shard ${shardKey}:`, err)
          }
        })
        await Promise.all(shardPromises)
      }
    } catch (err) {
      console.error('[ingest] CDC/subscription pipeline error:', err)
    }
  })())

  return Response.json({
    ok: true,
    received: validatedBatch.events.length,
  }, { headers: corsHeaders() })
}
