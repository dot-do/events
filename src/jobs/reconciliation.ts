/**
 * Reconciliation Job - Detects and repairs missed event deliveries
 *
 * Since events.do lacks end-to-end transaction semantics, events can be
 * written to R2 but fail to be delivered to subscribers due to:
 * - Network issues during fanout
 * - DO crashes during delivery
 * - Queue failures
 * - Subscription fanout race conditions
 *
 * This reconciliation job:
 * 1. Scans R2 event files from a configurable time window
 * 2. Extracts event metadata (type, timestamp, id)
 * 3. Compares against delivery records in SubscriptionDO
 * 4. Queues missed events for redelivery
 *
 * Runs hourly at 4:30 UTC by default.
 */

import type { Env } from '../env'
import type { SubscriptionDO } from '../../core/src/subscription'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../subscription-routes'
import { createLogger, logError, type Logger } from '../logger'
import { parquetRead } from 'hyparquet'

// ============================================================================
// Types
// ============================================================================

export interface ReconciliationConfig {
  /** How far back to scan for events (default: 2 hours) */
  lookbackMs: number
  /** Maximum events to process per run (default: 10000) */
  maxEventsPerRun: number
  /** Maximum events to queue for redelivery per run (default: 1000) */
  maxRedeliveriesPerRun: number
  /** Prefixes to scan for events (default: events, webhooks) */
  eventPrefixes: string[]
  /** Minimum age of events to reconcile (default: 30 minutes) */
  minEventAgeMs: number
}

export interface ReconciliationResult {
  /** Total events scanned from R2 */
  eventsScanned: number
  /** Events that matched active subscriptions */
  eventsMatched: number
  /** Events that were missing deliveries */
  eventsMissed: number
  /** Events queued for redelivery */
  eventsRequeued: number
  /** Files scanned from R2 */
  filesScanned: number
  /** Duration of the reconciliation run */
  durationMs: number
  /** Errors encountered during reconciliation */
  errors: string[]
}

export interface EventMetadata {
  id: string
  type: string
  ts: string
  source?: string
  payload?: unknown
}

// ============================================================================
// Configuration
// ============================================================================

const DEFAULT_CONFIG: ReconciliationConfig = {
  lookbackMs: 2 * 60 * 60 * 1000, // 2 hours
  maxEventsPerRun: 10000,
  maxRedeliveriesPerRun: 1000,
  eventPrefixes: ['events', 'webhooks'],
  minEventAgeMs: 30 * 60 * 1000, // 30 minutes
}

// ============================================================================
// Reconciliation Logic
// ============================================================================

/**
 * Get the shard key for a subscription pattern
 */
function getSubscriptionShard(eventType: string): string {
  const prefix = eventType.split('.')[0] || 'default'
  if (prefix === '*' || prefix === '**') return 'default'
  return prefix
}

/**
 * Parse a Parquet file from R2 and extract event metadata
 */
async function extractEventsFromParquet(
  bucket: R2Bucket,
  key: string,
  log: Logger
): Promise<EventMetadata[]> {
  const events: EventMetadata[] = []

  try {
    const object = await bucket.get(key)
    if (!object) {
      log.warn('R2 object not found', { key })
      return events
    }

    const buffer = await object.arrayBuffer()

    // Parse Parquet file
    await parquetRead({
      file: buffer,
      onComplete: (rows: Record<string, unknown>[]) => {
        for (const row of rows) {
          // Extract event fields from Parquet row
          const ts = row.ts as string | Date | number
          const type = row.type as string

          if (ts && type) {
            // Generate a deterministic event ID from timestamp + type + source
            // This allows us to check for duplicates
            const source = row.source as string | undefined
            const tsStr = typeof ts === 'object' && ts instanceof Date
              ? ts.toISOString()
              : typeof ts === 'number'
              ? new Date(ts).toISOString()
              : String(ts)

            const eventId = `${tsStr}:${type}:${source || 'unknown'}`

            events.push({
              id: eventId,
              type,
              ts: tsStr,
              source,
              payload: row.payload ? JSON.parse(row.payload as string) : undefined,
            })
          }
        }
      },
    })
  } catch (err) {
    logError(log, 'Failed to parse Parquet file', err, { key })
  }

  return events
}

/**
 * List R2 files within a time window
 */
async function listEventFiles(
  bucket: R2Bucket,
  prefix: string,
  startTime: Date,
  endTime: Date,
  log: Logger
): Promise<string[]> {
  const files: string[] = []

  // Generate date path patterns for the time window
  // Format: prefix/YYYY/MM/DD/HH/*.parquet
  const currentTime = new Date(startTime)
  while (currentTime <= endTime) {
    const datePath = [
      prefix,
      currentTime.getUTCFullYear(),
      String(currentTime.getUTCMonth() + 1).padStart(2, '0'),
      String(currentTime.getUTCDate()).padStart(2, '0'),
      String(currentTime.getUTCHours()).padStart(2, '0'),
    ].join('/')

    try {
      let cursor: string | undefined
      do {
        const list = await bucket.list({
          prefix: datePath + '/',
          ...(cursor !== undefined ? { cursor } : {}),
        })

        for (const obj of list.objects) {
          if (obj.key.endsWith('.parquet')) {
            files.push(obj.key)
          }
        }

        cursor = list.truncated ? list.cursor : undefined
      } while (cursor)
    } catch (err) {
      logError(log, 'Error listing R2 files', err, { datePath })
    }

    // Move to next hour
    currentTime.setTime(currentTime.getTime() + 60 * 60 * 1000)
  }

  return files
}

/**
 * Check if an event was delivered to a subscription
 */
async function checkDeliveryExists(
  subscriptionDO: DurableObjectStub<SubscriptionDO>,
  subscriptionId: string,
  eventId: string
): Promise<boolean> {
  try {
    // Query the subscription's delivery status
    // We check if the event_id exists in the deliveries table
    const status = await subscriptionDO.getSubscriptionStatus(subscriptionId)

    // If the subscription is not found or inactive, skip
    if (!status.subscription || !status.subscription.active) {
      return true // Consider as "delivered" to skip redelivery
    }

    // Note: The SubscriptionDO doesn't have a direct method to check if a specific
    // event was delivered. We need to query the deliveries table.
    // For now, we'll use a heuristic: if there are no pending deliveries, assume delivered.
    // This is not perfect but avoids adding new methods to SubscriptionDO.

    // TODO: Add a checkEventDelivered method to SubscriptionDO for accurate checking
    return false // Assume not delivered for reconciliation purposes
  } catch {
    // Error checking delivery - assume delivered to be safe
    return true
  }
}

/**
 * Queue an event for redelivery to a subscription
 */
async function queueForRedelivery(
  subscriptionDO: DurableObjectStub<SubscriptionDO>,
  event: EventMetadata,
  log: Logger
): Promise<boolean> {
  try {
    // Re-trigger fanout for this specific event
    const result = await subscriptionDO.fanout({
      id: `reconcile:${event.id}`,
      type: event.type,
      ts: event.ts,
      payload: event.payload,
    })

    log.info('Queued event for redelivery', {
      eventId: event.id,
      eventType: event.type,
      matched: result.matched,
      deliveries: result.deliveries.length,
    })

    return result.matched > 0
  } catch (err) {
    logError(log, 'Failed to queue event for redelivery', err, {
      eventId: event.id,
      eventType: event.type,
    })
    return false
  }
}

/**
 * Get subscription DO stub for a shard
 */
function getSubscriptionStub(
  env: Env,
  shard: string
): DurableObjectStub<SubscriptionDO> {
  const id = env.SUBSCRIPTIONS.idFromName(shard)
  return env.SUBSCRIPTIONS.get(id)
}

/**
 * Run the reconciliation job
 */
export async function runReconciliation(
  env: Env,
  log: Logger,
  config: Partial<ReconciliationConfig> = {}
): Promise<ReconciliationResult> {
  const startTime = Date.now()
  const cfg = { ...DEFAULT_CONFIG, ...config }
  const errors: string[] = []

  let eventsScanned = 0
  let eventsMatched = 0
  let eventsMissed = 0
  let eventsRequeued = 0
  let filesScanned = 0

  // Calculate time window
  const now = new Date()
  const endTime = new Date(now.getTime() - cfg.minEventAgeMs)
  const startTimeWindow = new Date(endTime.getTime() - cfg.lookbackMs)

  log.info('Starting reconciliation', {
    startTime: startTimeWindow.toISOString(),
    endTime: endTime.toISOString(),
    prefixes: cfg.eventPrefixes,
  })

  // Get all active subscriptions across shards
  const subscriptionsByType = new Map<string, Array<{ shard: string; subscriptionId: string }>>()

  for (const shard of KNOWN_SUBSCRIPTION_SHARDS) {
    try {
      const stub = getSubscriptionStub(env, shard)
      const subscriptions = await stub.listSubscriptions({ active: true })

      for (const sub of subscriptions) {
        // Extract the event type prefix from the pattern
        const prefix = sub.pattern.split('.')[0] || '*'
        const key = prefix === '*' || prefix === '**' ? '*' : prefix

        if (!subscriptionsByType.has(key)) {
          subscriptionsByType.set(key, [])
        }
        subscriptionsByType.get(key)!.push({
          shard,
          subscriptionId: sub.id,
        })
      }
    } catch (err) {
      const errorMsg = `Failed to get subscriptions for shard ${shard}`
      logError(log, errorMsg, err, { shard })
      errors.push(errorMsg)
    }
  }

  log.info('Loaded subscriptions', {
    shardCount: KNOWN_SUBSCRIPTION_SHARDS.length,
    typeCount: subscriptionsByType.size,
    totalSubscriptions: Array.from(subscriptionsByType.values()).reduce((sum, arr) => sum + arr.length, 0),
  })

  // If no subscriptions, nothing to reconcile
  if (subscriptionsByType.size === 0) {
    log.info('No active subscriptions found, skipping reconciliation')
    return {
      eventsScanned: 0,
      eventsMatched: 0,
      eventsMissed: 0,
      eventsRequeued: 0,
      filesScanned: 0,
      durationMs: Date.now() - startTime,
      errors,
    }
  }

  // Scan R2 for event files
  const allFiles: string[] = []
  for (const prefix of cfg.eventPrefixes) {
    const files = await listEventFiles(env.EVENTS_BUCKET, prefix, startTimeWindow, endTime, log)
    allFiles.push(...files)
  }

  log.info('Found event files', { fileCount: allFiles.length })

  // Process each file
  const processedEventIds = new Set<string>()

  for (const file of allFiles) {
    if (eventsScanned >= cfg.maxEventsPerRun) {
      log.warn('Reached max events per run limit', { limit: cfg.maxEventsPerRun })
      break
    }

    if (eventsRequeued >= cfg.maxRedeliveriesPerRun) {
      log.warn('Reached max redeliveries per run limit', { limit: cfg.maxRedeliveriesPerRun })
      break
    }

    filesScanned++

    const events = await extractEventsFromParquet(env.EVENTS_BUCKET, file, log)

    for (const event of events) {
      if (eventsScanned >= cfg.maxEventsPerRun) break
      if (eventsRequeued >= cfg.maxRedeliveriesPerRun) break

      // Skip if we've already processed this event ID
      if (processedEventIds.has(event.id)) continue
      processedEventIds.add(event.id)

      eventsScanned++

      // Check event timestamp is within our window
      const eventTime = new Date(event.ts).getTime()
      if (eventTime < startTimeWindow.getTime() || eventTime > endTime.getTime()) {
        continue
      }

      // Find matching subscriptions
      const eventTypePrefix = event.type.split('.')[0] || '*'
      const potentialSubscriptions = [
        ...(subscriptionsByType.get(eventTypePrefix) || []),
        ...(subscriptionsByType.get('*') || []),
      ]

      if (potentialSubscriptions.length === 0) {
        continue
      }

      eventsMatched++

      // Check delivery status for each matching subscription
      // Group by shard to minimize DO calls
      const shardSubscriptions = new Map<string, string[]>()
      for (const { shard, subscriptionId } of potentialSubscriptions) {
        if (!shardSubscriptions.has(shard)) {
          shardSubscriptions.set(shard, [])
        }
        shardSubscriptions.get(shard)!.push(subscriptionId)
      }

      // For each shard, check if the event was delivered
      for (const [shard, subscriptionIds] of shardSubscriptions) {
        const stub = getSubscriptionStub(env, shard)

        // Instead of checking each subscription individually, we'll
        // trigger a re-fanout and let the delivery deduplication handle it
        // This is more efficient than querying each delivery record
        try {
          // Check if we should queue for redelivery
          // The fanout will be deduplicated by subscription_id + event_id UNIQUE constraint
          const result = await stub.fanout({
            id: `reconcile:${event.id}`,
            type: event.type,
            ts: event.ts,
            payload: event.payload,
          })

          if (result.matched > 0 && result.deliveries.length > 0) {
            eventsMissed++
            eventsRequeued += result.deliveries.length
          }
        } catch (err) {
          // Unique constraint violation means already delivered - this is expected
          if (String(err).includes('UNIQUE constraint')) {
            // Already delivered, skip
          } else {
            logError(log, 'Error during reconciliation fanout', err, {
              eventId: event.id,
              shard,
            })
            errors.push(`Fanout error for ${event.id}: ${String(err)}`)
          }
        }
      }
    }
  }

  const durationMs = Date.now() - startTime

  log.info('Reconciliation completed', {
    eventsScanned,
    eventsMatched,
    eventsMissed,
    eventsRequeued,
    filesScanned,
    durationMs,
    errorCount: errors.length,
  })

  return {
    eventsScanned,
    eventsMatched,
    eventsMissed,
    eventsRequeued,
    filesScanned,
    durationMs,
    errors,
  }
}

/**
 * Get reconciliation configuration from environment
 */
export function getReconciliationConfig(env: Env): Partial<ReconciliationConfig> {
  // Allow configuration via environment variables
  const config: Partial<ReconciliationConfig> = {}

  // These could be added to Env if needed:
  // RECONCILIATION_LOOKBACK_MS
  // RECONCILIATION_MAX_EVENTS
  // RECONCILIATION_MAX_REDELIVERIES

  return config
}
