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
import { logger, logError } from '../../logger'
import { getSubscriptionRoutingShard } from '../../subscription-shard-coordinator-do'

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
  logger.info('Sent events to queue for CDC/subscription fanout', { component: 'ingest', eventCount: batch.events.length })
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
      logger.info('DIRECT CDC processed events', { component: 'fanout', eventCount: groupEvents.length, cdcKey: key })
    } catch (err) {
      recordCDCMetric(env.ANALYTICS, 'error', groupEvents.length, key)
      logError(logger, 'CDC processor error', err, { component: 'fanout', cdcKey: key, eventCount: groupEvents.length })
    }
  })

  await Promise.all(cdcPromises)
}

// ============================================================================
// Direct Subscription Fanout
// ============================================================================

/**
 * Fan out events to subscription matching and delivery.
 * Uses dynamic sharding when SUBSCRIPTION_SHARD_COORDINATOR is available.
 */
export async function processSubscriptionFanout(
  events: DurableEvent[],
  tenant: TenantContext,
  env: Env
): Promise<void> {
  if (!env.SUBSCRIPTIONS) {
    return
  }

  const namespace = tenant.isAdmin ? undefined : tenant.namespace

  // Group events by base prefix (first segment before '.', or 'default')
  const eventsByPrefix = new Map<string, DurableEvent[]>()

  for (const event of events) {
    const dotIndex = event.type.indexOf('.')
    const basePrefix = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
    const existing = eventsByPrefix.get(basePrefix) ?? []
    existing.push(event)
    eventsByPrefix.set(basePrefix, existing)
  }

  // Fan out each group using dynamic shard routing
  const prefixPromises = Array.from(eventsByPrefix.entries()).map(
    async ([basePrefix, prefixEvents]) => {
      let successCount = 0
      let errorCount = 0

      try {
        // Process events with dynamic shard routing
        // Each event may be routed to a different sub-shard based on load
        const fanoutPromises = prefixEvents.map(async (event) => {
          const eventId = (event as { id?: string }).id || ulid()

          try {
            // Get the routing shard (uses coordinator if available, or falls back to legacy)
            // Use event ID for consistent hashing so retries go to the same shard
            const shardKey = await getSubscriptionRoutingShard(env, basePrefix, namespace, eventId)

            const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
            const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

            await subscriptionDO.fanout({
              id: eventId,
              type: event.type,
              ts: event.ts,
              payload: { ...event, _namespace: tenant.namespace },
            })
            successCount++
          } catch (err) {
            errorCount++
            logError(logger, 'Subscription fanout error', err, {
              component: 'fanout',
              eventId,
              eventType: event.type,
              basePrefix,
            })
          }
        })

        await Promise.all(fanoutPromises)
        recordSubscriptionMetric(env.ANALYTICS, 'success', successCount, basePrefix)
        if (errorCount > 0) {
          recordSubscriptionMetric(env.ANALYTICS, 'error', errorCount, basePrefix, 'fanout_error')
        }
        logger.info('DIRECT subscription fanout completed', {
          component: 'fanout',
          eventCount: prefixEvents.length,
          basePrefix,
          namespace: tenant.namespace,
          successCount,
          errorCount,
          dynamicSharding: !!env.SUBSCRIPTION_SHARD_COORDINATOR,
        })
      } catch (err) {
        recordSubscriptionMetric(env.ANALYTICS, 'error', prefixEvents.length, basePrefix, 'prefix_error')
        logError(logger, 'Subscription prefix error', err, {
          component: 'fanout',
          basePrefix,
          eventCount: prefixEvents.length,
        })
      }
    }
  )

  await Promise.all(prefixPromises)
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

  logger.info('Starting DIRECT fanout', { component: 'fanout', eventCount: batch!.events.length })

  try {
    await Promise.all([
      processCDCEvents(batch!.events, tenant, env),
      processSubscriptionFanout(batch!.events, tenant, env),
    ])
  } catch (err) {
    logError(logger, 'CDC/subscription pipeline error', err, {
      component: 'fanout',
      eventCount: batch!.events.length,
    })
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
