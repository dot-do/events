/**
 * Scheduled handler - Cron/scheduled task logic
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
 * 4. Dead Letter Cleanup: Prunes R2 dead-letter messages older than 30 days (daily at 2:30 UTC)
 * 5. Subscription Dead Letter Cleanup: Triggers cleanup of old dead letters in SubscriptionDO shards (daily at 3:30 UTC)
 */

import type { Env } from '../env'
import { compactCollection } from '../../core/src/cdc-compaction'
import { compactEventStream } from '../../core/src/event-compaction'
import { withSchedulerLock } from '../scheduler-lock'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../subscription-routes'
import { createLogger, logError, generateCorrelationId, type Logger } from '../logger'

const DEDUP_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours
const DEAD_LETTER_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days
const SUBSCRIPTION_DEAD_LETTER_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days

// Note: log is created per-invocation in handleScheduled to include correlation ID

/**
 * Task 1: Dedup marker cleanup (delete markers older than 24 hours)
 */
async function cleanupDedupMarkers(env: Env, log: Logger): Promise<void> {
  try {
    const now = Date.now()
    let dedupDeleted = 0
    let dedupChecked = 0
    let cursor: string | undefined

    log.info('Starting dedup marker cleanup')

    // Paginate through all dedup markers
    do {
      const list = await env.EVENTS_BUCKET.list({
        prefix: 'dedup/',
        limit: 1000,
        ...(cursor !== undefined ? { cursor } : {}),
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

    log.info('Dedup cleanup completed', { checked: dedupChecked, deleted: dedupDeleted })
  } catch (err) {
    logError(log, 'Error during dedup cleanup', err)
  }
}

/**
 * Task 2: CDC Compaction
 */
async function runCDCCompaction(env: Env, log: Logger): Promise<void> {
  try {
    log.info('Starting CDC compaction')

    // Discover namespaces by scanning R2 for cdc prefixes
    // This is more reliable than relying on catalog state since we're sharding by namespace
    const namespaces = new Set<string>()

    // Scan R2 for existing CDC namespaces (format: cdc/{namespace}/{table}/...)
    let cdcCursor: string | undefined
    do {
      const list = await env.EVENTS_BUCKET.list({
        prefix: 'cdc/',
        delimiter: '/',
        ...(cdcCursor !== undefined ? { cursor: cdcCursor } : {}),
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
    log.info('Found namespaces', { count: namespaceList.length, namespaces: namespaceList })

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
            log.info('CDC compacted', {
              namespace: ns,
              table,
              deltasProcessed: compactionResult.deltasProcessed,
              recordCount: compactionResult.recordCount,
            })
          } else {
            totalSkipped++
            results.push({ ns, table, result: 'no-deltas' })
          }
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : String(err)
          logError(log, 'Error compacting collection', err, { namespace: ns, table })
          results.push({ ns, table, result: `error: ${errorMsg}` })
        }
      }
    }

    log.info('CDC compaction completed', { compacted: totalCompacted, skipped: totalSkipped })
  } catch (err) {
    logError(log, 'Error during CDC compaction', err)
  }
}

/**
 * Task 3: Event Stream Compaction (daily at 1:30 UTC only)
 * Compacts hourly Parquet files from EventWriterDO into daily files.
 * Only runs at 1:30 UTC to avoid compacting files still being written.
 */
async function runEventStreamCompaction(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 1) {
    return
  }

  try {
    log.info('Starting event stream compaction')

    const result = await compactEventStream(env.EVENTS_BUCKET, {
      prefixes: ['events', 'webhooks', 'tail'],
      daysBack: 7,
    })

    log.info('Event stream compaction completed', {
      durationMs: result.durationMs,
      daysProcessed: result.days.length,
      sourceFiles: result.totalSourceFiles,
      outputFiles: result.totalOutputFiles,
      totalRecords: result.totalRecords,
    })

    if (result.errors && result.errors.length > 0) {
      log.warn('Event stream compaction had errors', { errors: result.errors })
    }
  } catch (err) {
    logError(log, 'Error during event stream compaction', err)
  }
}

/**
 * Task 4: Dead Letter Cleanup (daily at 2:30 UTC only)
 * Prunes R2 dead-letter messages older than 30 days.
 * Dead letters are stored in: dead-letter/queue/{date}/{id}.json
 */
async function cleanupDeadLetters(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 2) {
    return
  }

  try {
    const now = Date.now()
    let deadLetterDeleted = 0
    let deadLetterChecked = 0
    let cursor: string | undefined

    log.info('Starting dead letter cleanup')

    // Paginate through all dead-letter files
    do {
      const list = await env.EVENTS_BUCKET.list({
        prefix: 'dead-letter/',
        limit: 1000,
        ...(cursor !== undefined ? { cursor } : {}),
      })

      for (const obj of list.objects) {
        deadLetterChecked++
        // Check if dead letter is older than TTL based on uploaded timestamp
        const uploadedAt = obj.uploaded.getTime()
        if (now - uploadedAt > DEAD_LETTER_TTL_MS) {
          await env.EVENTS_BUCKET.delete(obj.key)
          deadLetterDeleted++
        }
      }

      cursor = list.truncated ? list.cursor : undefined
    } while (cursor)

    log.info('Dead letter cleanup completed', { checked: deadLetterChecked, deleted: deadLetterDeleted })
  } catch (err) {
    logError(log, 'Error during dead letter cleanup', err)
  }
}

/**
 * Task 5: Subscription Dead Letter Cleanup (daily at 3:30 UTC only)
 * Triggers cleanup of old dead letters and delivery logs in all SubscriptionDO shards.
 * This helps prevent unbounded growth of the SQLite tables in each shard.
 */
async function cleanupSubscriptionDeadLetters(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 3) {
    return
  }

  if (!env.SUBSCRIPTIONS) {
    log.info('Skipping subscription cleanup - SUBSCRIPTIONS binding not available')
    return
  }

  try {
    log.info('Starting subscription dead letter cleanup')

    const cutoffTs = Date.now() - SUBSCRIPTION_DEAD_LETTER_TTL_MS
    let totalDeadLettersDeleted = 0
    let totalDeliveryLogsDeleted = 0
    let totalDeliveriesDeleted = 0

    // Trigger cleanup on each known subscription shard
    for (const shard of KNOWN_SUBSCRIPTION_SHARDS) {
      try {
        const subId = env.SUBSCRIPTIONS.idFromName(shard)
        const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

        // Call the cleanup method on the SubscriptionDO
        const result = await subscriptionDO.cleanupOldData(cutoffTs)

        totalDeadLettersDeleted += result.deadLettersDeleted
        totalDeliveryLogsDeleted += result.deliveryLogsDeleted
        totalDeliveriesDeleted += result.deliveriesDeleted

        if (result.deadLettersDeleted > 0 || result.deliveryLogsDeleted > 0 || result.deliveriesDeleted > 0) {
          log.info('Shard cleanup', {
            shard,
            deadLettersDeleted: result.deadLettersDeleted,
            logsDeleted: result.deliveryLogsDeleted,
            deliveriesDeleted: result.deliveriesDeleted,
          })
        }
      } catch (err) {
        logError(log, 'Error cleaning up shard', err, { shard })
      }
    }

    log.info('Subscription cleanup completed', {
      deadLettersDeleted: totalDeadLettersDeleted,
      deliveryLogsDeleted: totalDeliveryLogsDeleted,
      deliveriesDeleted: totalDeliveriesDeleted,
    })
  } catch (err) {
    logError(log, 'Error during subscription cleanup', err)
  }
}

/**
 * Scheduled handler - runs all cleanup and compaction tasks.
 * Tasks are scheduled at different hours to spread load:
 * - Dedup cleanup: every hour
 * - CDC compaction: every hour
 * - Event stream compaction: 1:30 UTC daily
 * - Dead letter cleanup: 2:30 UTC daily
 * - Subscription dead letter cleanup: 3:30 UTC daily
 */
export async function handleScheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
  const startTime = Date.now()
  // Generate correlation ID for this scheduled run
  const correlationId = generateCorrelationId()
  const log = createLogger({ component: 'scheduled', correlationId })

  log.info('Scheduled task triggered', { timestamp: new Date().toISOString(), cron: event.cron })

  ctx.waitUntil((async () => {
    // Acquire distributed lock to prevent concurrent scheduled runs
    const lockResult = await withSchedulerLock(
      env.EVENTS_BUCKET,
      async () => {
        // Task 1: Dedup marker cleanup (hourly)
        await cleanupDedupMarkers(env, log)

        // Task 2: CDC Compaction (hourly)
        await runCDCCompaction(env, log)

        // Task 3: Event Stream Compaction (only at 1:30 UTC)
        await runEventStreamCompaction(env, log)

        // Task 4: Dead Letter Cleanup (only at 2:30 UTC)
        await cleanupDeadLetters(env, log)

        // Task 5: Subscription Dead Letter Cleanup (only at 3:30 UTC)
        await cleanupSubscriptionDeadLetters(env, log)

        const durationMs = Date.now() - startTime
        log.info('All scheduled tasks completed', { durationMs })
      },
      { timeoutMs: 10 * 60 * 1000 } // 10 minute lock timeout
    )

    if (lockResult.skipped) {
      log.info('Scheduled tasks skipped - another worker is running')
    }
  })())
}
