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
 * 2. CDC Compaction: Compacts delta files for collections with pending deltas (daily at 2:30 UTC)
 * 3. Event Stream Compaction: Compacts hourly Parquet files into daily files (hourly)
 * 4. Dead Letter Cleanup: Prunes R2 dead-letter messages older than 30 days (daily at 4:30 UTC)
 * 5. Subscription Dead Letter Cleanup: Triggers cleanup of old dead letters in SubscriptionDO shards (daily at 5:30 UTC)
 * 6. Reconciliation: Detects and repairs missed event deliveries (daily at 6:30 UTC)
 * 7. PITR Snapshots: Creates weekly snapshots on Sundays at 3:30 UTC
 */

import type { Env } from '../env'
import { compactCollection } from '../../core/src/cdc-compaction'
import { compactEventStream } from '../../core/src/event-compaction'
import { createSnapshot as createCDCSnapshot, cleanupSnapshots } from '../../core/src/cdc-snapshot'
import { withSchedulerLock } from '../scheduler-lock'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../subscription-routes'
import { createLogger, logError, generateCorrelationId, type Logger } from '../logger'
import { runReconciliation, getReconciliationConfig } from '../jobs/reconciliation'

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
 * Task 2: CDC Compaction (daily at 2:30 UTC only)
 * Compacts CDC delta files into data.parquet for all collections.
 */
async function runCDCCompaction(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 2) {
    return
  }

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
 * Task 3: Event Stream Compaction (hourly)
 * Compacts hourly Parquet files from EventWriterDO into daily files.
 * Runs every hour to keep event files consolidated.
 */
async function runEventStreamCompaction(env: Env, log: Logger): Promise<void> {
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
 * Task 4: Dead Letter Cleanup (daily at 4:30 UTC only)
 * Prunes R2 dead-letter messages older than 30 days.
 * Dead letters are stored in: dead-letter/queue/{date}/{id}.json
 */
async function cleanupDeadLetters(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 4) {
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
 * Task 5: Subscription Dead Letter Cleanup (daily at 5:30 UTC only)
 * Triggers cleanup of old dead letters and delivery logs in all SubscriptionDO shards.
 * This helps prevent unbounded growth of the SQLite tables in each shard.
 */
async function cleanupSubscriptionDeadLetters(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 5) {
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
 * Task 6: Reconciliation Job (daily at 6:30 UTC only)
 * Detects and repairs missed event deliveries by comparing R2 event files
 * against delivery records in SubscriptionDO.
 *
 * This handles the lack of end-to-end transaction semantics - events may be
 * written to R2 but fail to be delivered to subscribers due to network issues,
 * DO crashes, or queue failures.
 */
async function runReconciliationJob(env: Env, log: Logger): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 6) {
    return
  }

  if (!env.SUBSCRIPTIONS) {
    log.info('Skipping reconciliation - SUBSCRIPTIONS binding not available')
    return
  }

  try {
    log.info('Starting reconciliation job')

    const config = getReconciliationConfig(env)
    const result = await runReconciliation(env, log, config)

    log.info('Reconciliation completed', {
      eventsScanned: result.eventsScanned,
      eventsMatched: result.eventsMatched,
      eventsMissed: result.eventsMissed,
      eventsRequeued: result.eventsRequeued,
      filesScanned: result.filesScanned,
      durationMs: result.durationMs,
    })

    if (result.errors.length > 0) {
      log.warn('Reconciliation had errors', { errorCount: result.errors.length, errors: result.errors.slice(0, 10) })
    }
  } catch (err) {
    logError(log, 'Error during reconciliation', err)
  }
}

/**
 * Task 7: PITR Snapshots (weekly on Sundays at 3:30 UTC)
 * Creates point-in-time recovery snapshots for all collections.
 * Weekly snapshots provide restore points for disaster recovery.
 */
async function runPITRSnapshots(env: Env, log: Logger): Promise<void> {
  const now = new Date()
  const currentHour = now.getUTCHours()
  const currentDay = now.getUTCDay() // 0 = Sunday

  // Only run on Sundays at 3am UTC
  if (currentDay !== 0 || currentHour !== 3) {
    return
  }

  try {
    log.info('Starting PITR snapshot creation')

    // Discover namespaces by scanning R2 for cdc prefixes
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

    const namespaceList = [...namespaces]
    log.info('Found namespaces for PITR snapshots', { count: namespaceList.length })

    let totalSnapshots = 0
    let totalSkipped = 0
    let totalErrors = 0

    // For each namespace, get tables and create snapshots
    for (const ns of namespaceList) {
      // Discover tables by scanning R2 for this namespace
      const tables = new Set<string>()
      let tableCursor: string | undefined
      do {
        const list = await env.EVENTS_BUCKET.list({
          prefix: `cdc/${ns}/`,
          delimiter: '/',
          ...(tableCursor !== undefined ? { cursor: tableCursor } : {}),
        })

        // Extract table names from common prefixes (format: cdc/{namespace}/{table}/)
        for (const prefix of list.delimitedPrefixes || []) {
          const parts = prefix.split('/')
          if (parts.length >= 3 && parts[2]) {
            tables.add(parts[2])
          }
        }

        tableCursor = list.truncated ? list.cursor : undefined
      } while (tableCursor)

      for (const table of tables) {
        try {
          // Create daily snapshot (which is used for PITR)
          const result = await createCDCSnapshot(env.EVENTS_BUCKET, ns, table, 'daily')

          if (result.skipped) {
            totalSkipped++
            log.debug('PITR snapshot skipped', { namespace: ns, table, reason: result.reason })
          } else {
            totalSnapshots++
            log.info('PITR snapshot created', {
              namespace: ns,
              table,
              rowCount: result.rowCount,
              sizeBytes: result.sizeBytes,
            })
          }

          // Cleanup old snapshots (retain 7 daily, 3 monthly)
          await cleanupSnapshots(env.EVENTS_BUCKET, ns, table, {
            dailySnapshots: 7,
            monthlySnapshots: 3,
          })
        } catch (err) {
          totalErrors++
          logError(log, 'Error creating PITR snapshot', err, { namespace: ns, table })
        }
      }
    }

    log.info('PITR snapshot creation completed', {
      created: totalSnapshots,
      skipped: totalSkipped,
      errors: totalErrors,
    })
  } catch (err) {
    logError(log, 'Error during PITR snapshot creation', err)
  }
}

/**
 * Scheduled handler - runs all cleanup and compaction tasks.
 * Tasks are scheduled at different hours to spread load:
 * - Dedup cleanup: every hour
 * - CDC compaction: 2:30 UTC daily
 * - Event stream compaction: every hour
 * - PITR snapshots: 3:30 UTC Sundays
 * - Dead letter cleanup: 4:30 UTC daily
 * - Subscription dead letter cleanup: 5:30 UTC daily
 * - Reconciliation: 6:30 UTC daily
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

        // Task 2: CDC Compaction (only at 2:30 UTC)
        await runCDCCompaction(env, log)

        // Task 3: Event Stream Compaction (hourly)
        await runEventStreamCompaction(env, log)

        // Task 4: Dead Letter Cleanup (only at 4:30 UTC)
        await cleanupDeadLetters(env, log)

        // Task 5: Subscription Dead Letter Cleanup (only at 5:30 UTC)
        await cleanupSubscriptionDeadLetters(env, log)

        // Task 6: Reconciliation Job (only at 6:30 UTC)
        await runReconciliationJob(env, log)

        // Task 7: PITR Snapshots (only on Sundays at 3:30 UTC)
        await runPITRSnapshots(env, log)

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
