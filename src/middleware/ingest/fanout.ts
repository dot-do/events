/**
 * Fanout middleware for ingest
 *
 * Handles CDC and subscription fanout:
 * - Queue-based fanout (async via EVENTS_QUEUE)
 * - Direct fanout (sync via DO calls)
 * - Mutual exclusion to prevent duplicate processing
 */

import type { DurableEvent, EventBatch } from '@dotdo/events'
import { isCollectionChangeEvent, type CDCEvent } from '../../../core/src/cdc-processor'
import { ulid } from '../../../core/src/ulid'
import { recordCDCMetric, recordSubscriptionMetric } from '../../metrics'
import { getNamespacedShardKey } from '../tenant'
import type { TenantContext } from '../tenant'
import type { Env } from '../../env'
import type { IngestContext } from './types'

// ============================================================================
// Fanout Mode Determination
// ============================================================================

/**
 * Determine fanout mode based on environment configuration.
 * Returns true for queue-based fanout, false for direct DO calls.
 */
export function determineFanoutMode(env: Env): boolean {
  return env.USE_QUEUE_FANOUT === 'true' && !!env.EVENTS_QUEUE
}

// ============================================================================
// Queue Fanout
// ============================================================================

/**
 * Send batch to queue for async CDC/subscription fanout
 */
export async function sendToQueue(
  env: Env,
  batch: EventBatch
): Promise<void> {
  if (!env.EVENTS_QUEUE) {
    throw new Error('EVENTS_QUEUE not configured')
  }

  await env.EVENTS_QUEUE.send(batch)
  console.log(`[ingest] Sent ${batch.events.length} events to queue for CDC/subscription fanout`)
}

// ============================================================================
// Direct CDC Fanout
// ============================================================================

/**
 * Process CDC events directly via CDC Processor DOs
 */
export async function processCDCEvents(
  events: DurableEvent[],
  tenant: TenantContext,
  env: Env
): Promise<void> {
  // Filter for CDC events using type guard for proper type narrowing
  const cdcEvents = events.filter(isCollectionChangeEvent)

  if (cdcEvents.length === 0 || !env.CDC_PROCESSOR) {
    return
  }

  // Group CDC events by namespace/collection for routing to the correct DO instance
  // Use tenant namespace prefix to isolate CDC processors per tenant
  const cdcByKey = new Map<string, CDCEvent[]>()

  for (const cdcEvent of cdcEvents) {
    const ns = cdcEvent.do?.class || cdcEvent.do?.name || 'default'
    const collection = cdcEvent.collection || 'default'
    // Namespace-isolated CDC processor key: <tenant-namespace>:<ns>/<collection>
    const key = getNamespacedShardKey(tenant, `${ns}/${collection}`)
    const existing = cdcByKey.get(key) ?? []
    existing.push(cdcEvent)
    cdcByKey.set(key, existing)
  }

  // Send each group to the appropriate CDCProcessorDO instance
  const cdcPromises = Array.from(cdcByKey.entries()).map(async ([key, groupEvents]) => {
    try {
      const processorId = env.CDC_PROCESSOR.idFromName(key)
      const processor = env.CDC_PROCESSOR.get(processorId)
      await processor.process(groupEvents)
      recordCDCMetric(env.ANALYTICS, 'success', groupEvents.length, key)
      console.log(`[ingest] DIRECT CDC processed ${groupEvents.length} events for ${key}`)
    } catch (err) {
      recordCDCMetric(env.ANALYTICS, 'error', groupEvents.length, key)
      console.error(`[ingest] CDC processor error for ${key}:`, err)
    }
  })

  await Promise.all(cdcPromises)
}

// ============================================================================
// Direct Subscription Fanout
// ============================================================================

/**
 * Fan out events to subscription matching and delivery
 */
export async function processSubscriptionFanout(
  events: DurableEvent[],
  tenant: TenantContext,
  env: Env
): Promise<void> {
  if (!env.SUBSCRIPTIONS) {
    return
  }

  // Group events by shard key (first segment before '.', or 'default')
  const eventsByShard = new Map<string, DurableEvent[]>()

  for (const event of events) {
    const dotIndex = event.type.indexOf('.')
    const baseShardKey = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
    // Namespace-isolated subscription shard key
    const shardKey = getNamespacedShardKey(tenant, baseShardKey)
    const existing = eventsByShard.get(shardKey) ?? []
    existing.push(event)
    eventsByShard.set(shardKey, existing)
  }

  // Fan out each group to its corresponding shard
  const shardPromises = Array.from(eventsByShard.entries()).map(
    async ([shardKey, shardEvents]) => {
      let successCount = 0
      let errorCount = 0

      try {
        const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
        const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

        const fanoutPromises = shardEvents.map(async (event) => {
          // Use existing event id for idempotency if available
          const eventId = (event as { id?: string }).id || ulid()
          try {
            await subscriptionDO.fanout({
              id: eventId,
              type: event.type,
              ts: event.ts,
              payload: { ...event, _namespace: tenant.namespace },
            })
            successCount++
          } catch (err) {
            errorCount++
            console.error(
              `[ingest] Subscription fanout error for event ${eventId} type ${event.type}:`,
              err
            )
          }
        })

        await Promise.all(fanoutPromises)
        recordSubscriptionMetric(env.ANALYTICS, 'success', successCount, shardKey)
        if (errorCount > 0) {
          recordSubscriptionMetric(env.ANALYTICS, 'error', errorCount, shardKey, 'fanout_error')
        }
        console.log(
          `[ingest] DIRECT subscription fanout: ${shardEvents.length} events to shard ${shardKey}`
        )
      } catch (err) {
        recordSubscriptionMetric(env.ANALYTICS, 'error', shardEvents.length, shardKey, 'shard_error')
        console.error(`[ingest] Subscription shard error for shard ${shardKey}:`, err)
      }
    }
  )

  await Promise.all(shardPromises)
}

// ============================================================================
// Combined Fanout Handler
// ============================================================================

/**
 * Execute direct fanout for CDC and subscriptions.
 * Called in the background via ctx.waitUntil when queue fanout is disabled.
 */
export async function executeDirectFanout(context: IngestContext): Promise<void> {
  const { batch, tenant, env } = context

  console.log(`[ingest] Starting DIRECT fanout for ${batch!.events.length} events`)

  try {
    await Promise.all([
      processCDCEvents(batch!.events, tenant, env),
      processSubscriptionFanout(batch!.events, tenant, env),
    ])
  } catch (err) {
    console.error('[ingest] CDC/subscription pipeline error:', err)
  }
}

/**
 * Execute queue-based fanout.
 * Called in the background via ctx.waitUntil when queue fanout is enabled.
 */
export async function executeQueueFanout(context: IngestContext): Promise<void> {
  const { batch, env } = context
  await sendToQueue(env, batch!)
}
