/**
 * Queue handler - Queue consumer logic for CDC/subscription fanout
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
 *
 * OPTIMIZATION: Uses batch dedup checking with bloom filter and R2 list operations
 * instead of N individual HEAD requests. This dramatically reduces R2 API calls.
 */

import type { Env } from '../env'
import type { EventBatch, DurableEvent } from '@dotdo/events'
import { isCollectionChangeEvent, type CDCEvent } from '../../core/src/cdc-processor'
import { ulid } from '../../core/src/ulid'
import {
  batchCheckDuplicates,
  batchWriteDedupMarkers,
  getCdcDedupCache,
  getSubDedupCache,
} from '../../core/src/dedup-cache'
import { recordQueueMetric, recordCDCMetric, recordSubscriptionMetric, MetricTimer } from '../metrics'
import { createLogger, logError, generateCorrelationId } from '../logger'

const MAX_RETRIES = 5

/**
 * Queue consumer handler - processes EventBatch messages for CDC/subscription fanout.
 */
export async function handleQueue(batch: MessageBatch<EventBatch>, env: Env): Promise<void> {
  const timer = new MetricTimer()
  const startTime = Date.now()

  // Generate a correlation ID for this batch processing
  const correlationId = generateCorrelationId()
  const log = createLogger({ component: 'queue', correlationId })

  log.info('Processing batch', { messageCount: batch.messages.length, mode: 'QUEUE fanout' })

  let totalEvents = 0
  let cdcEventsProcessed = 0
  let cdcEventsSkipped = 0
  let subscriptionFanouts = 0
  let subscriptionSkipped = 0
  let deadLettered = 0
  let totalR2Calls = 0
  let totalCacheHits = 0

  // Get global dedup caches
  const cdcCache = getCdcDedupCache()
  const subCache = getSubDedupCache()

  for (const message of batch.messages) {
    const attempts = message.attempts ?? 1

    try {
      const eventBatch = message.body

      if (!eventBatch?.events || !Array.isArray(eventBatch.events)) {
        log.warn('Invalid message format', { reason: 'missing events array', messageId: message.id })
        message.ack()
        continue
      }

      totalEvents += eventBatch.events.length

      // Process CDC events using type guard for proper type narrowing
      const cdcEvents = eventBatch.events.filter(isCollectionChangeEvent)

      if (cdcEvents.length > 0 && env.CDC_PROCESSOR) {
        // Group CDC events by namespace/collection for routing to the correct DO instance
        const cdcByKey = new Map<string, CDCEvent[]>()
        for (const cdcEvent of cdcEvents) {
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
            // Collect event IDs for batch dedup check
            const eventIds = events
              .map(e => (e as { id?: string }).id)
              .filter((id): id is string => !!id)

            // Batch check for duplicates using R2 list + bloom filter
            const dedupResult = await batchCheckDuplicates(
              env.EVENTS_BUCKET,
              'dedup/cdc/',
              eventIds,
              cdcCache
            )
            totalR2Calls += dedupResult.r2Calls
            totalCacheHits += dedupResult.cacheHits

            // Filter out already-processed events
            const eventsToProcess = events.filter(event => {
              const eventId = (event as { id?: string }).id
              if (!eventId) return true // Process events without IDs
              return dedupResult.newEvents.has(eventId)
            })

            cdcEventsSkipped += events.length - eventsToProcess.length

            if (eventsToProcess.length > 0) {
              const processorId = env.CDC_PROCESSOR.idFromName(key)
              const processor = env.CDC_PROCESSOR.get(processorId)
              await processor.process(eventsToProcess)
              cdcEventsProcessed += eventsToProcess.length
              recordCDCMetric(env.ANALYTICS, 'success', eventsToProcess.length, key)

              // Batch write dedup markers for processed events
              const processedIds = eventsToProcess
                .map(e => (e as { id?: string }).id)
                .filter((id): id is string => !!id)

              if (processedIds.length > 0) {
                const writeResult = await batchWriteDedupMarkers(
                  env.EVENTS_BUCKET,
                  'dedup/cdc/',
                  processedIds,
                  { processedAt: new Date().toISOString() },
                  cdcCache
                )
                totalR2Calls += writeResult.r2Calls
              }
            }

            log.info('CDC processed', { key, processed: eventsToProcess.length, skipped: events.length - eventsToProcess.length })
          } catch (err) {
            recordCDCMetric(env.ANALYTICS, 'error', events.length, key)
            logError(log, 'CDC processor error', err, { key })
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
        for (const [shardKey, events] of eventsByShard) {
          try {
            const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
            const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

            // Collect event IDs (generate if missing)
            const eventIdsWithIndices = events.map((event, index) => ({
              index,
              eventId: (event as { id?: string }).id || ulid(),
            }))
            const eventIds = eventIdsWithIndices.map(e => e.eventId)

            // Batch check for duplicates
            const dedupResult = await batchCheckDuplicates(
              env.EVENTS_BUCKET,
              'dedup/sub/',
              eventIds,
              subCache
            )
            totalR2Calls += dedupResult.r2Calls
            totalCacheHits += dedupResult.cacheHits

            // Filter to new events only
            const newEventIndices = eventIdsWithIndices
              .filter(e => dedupResult.newEvents.has(e.eventId))

            const shardSkipped = events.length - newEventIndices.length
            subscriptionSkipped += shardSkipped

            // Process new events
            const processedIds: string[] = []
            for (const { index, eventId } of newEventIndices) {
              const event = events[index]
              if (!event) continue // Should never happen, but satisfy TypeScript
              await subscriptionDO.fanout({
                id: eventId,
                type: event.type,
                ts: event.ts,
                payload: event,
              })
              processedIds.push(eventId)
              subscriptionFanouts++
            }

            // Batch write dedup markers
            if (processedIds.length > 0) {
              const writeResult = await batchWriteDedupMarkers(
                env.EVENTS_BUCKET,
                'dedup/sub/',
                processedIds,
                { deliveredAt: new Date().toISOString() },
                subCache
              )
              totalR2Calls += writeResult.r2Calls
            }

            recordSubscriptionMetric(env.ANALYTICS, 'success', newEventIndices.length, shardKey)
            log.info('Subscription fanout', { shardKey, delivered: newEventIndices.length, skipped: shardSkipped })
          } catch (err) {
            recordSubscriptionMetric(env.ANALYTICS, 'error', events.length, shardKey, 'shard_error')
            logError(log, 'Subscription shard error', err, { shardKey })
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
        log.error('Dead letter: message failed permanently', {
          attempts,
          error: errorMsg,
          messageId: message.id,
        })

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
          log.info('Dead letter written', { key: deadLetterKey })
        } catch (dlErr) {
          logError(log, 'Failed to write dead letter', dlErr)
        }

        deadLettered++
        message.ack() // Ack to prevent further retries
      } else {
        // Transient failure - retry
        log.warn('Retrying message', { attempt: attempts, maxRetries: MAX_RETRIES, error: errorMsg })
        message.retry()
      }
    }
  }

  const durationMs = Date.now() - startTime
  const latencyMs = timer.elapsed()

  // Record overall queue batch metrics
  recordQueueMetric(env.ANALYTICS, 'batch', 'success', {
    messages: batch.messages.length,
    events: totalEvents,
    errors: deadLettered,
  }, latencyMs)

  // Record dead letter metrics separately if any
  if (deadLettered > 0) {
    recordQueueMetric(env.ANALYTICS, 'dead_letter', 'error', {
      messages: deadLettered,
    }, 0)
  }

  log.info('Batch completed', {
    durationMs,
    totalEvents,
    cdc: { processed: cdcEventsProcessed, skipped: cdcEventsSkipped },
    subscriptions: { delivered: subscriptionFanouts, skipped: subscriptionSkipped },
    r2Calls: totalR2Calls,
    cacheHits: totalCacheHits,
    deadLettered: deadLettered > 0 ? deadLettered : undefined,
  })
}
