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

const DEDUP_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours
const DEAD_LETTER_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days
const SUBSCRIPTION_DEAD_LETTER_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days

/**
 * Task 1: Dedup marker cleanup (delete markers older than 24 hours)
 */
async function cleanupDedupMarkers(env: Env): Promise<void> {
  try {
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
}

/**
 * Task 2: CDC Compaction
 */
async function runCDCCompaction(env: Env): Promise<void> {
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
}

/**
 * Task 3: Event Stream Compaction (daily at 1:30 UTC only)
 * Compacts hourly Parquet files from EventWriterDO into daily files.
 * Only runs at 1:30 UTC to avoid compacting files still being written.
 */
async function runEventStreamCompaction(env: Env): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 1) {
    return
  }

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

/**
 * Task 4: Dead Letter Cleanup (daily at 2:30 UTC only)
 * Prunes R2 dead-letter messages older than 30 days.
 * Dead letters are stored in: dead-letter/queue/{date}/{id}.json
 */
async function cleanupDeadLetters(env: Env): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 2) {
    return
  }

  try {
    const now = Date.now()
    let deadLetterDeleted = 0
    let deadLetterChecked = 0
    let cursor: string | undefined

    console.log('[DEAD-LETTER-CLEANUP] Starting dead letter cleanup...')

    // Paginate through all dead-letter files
    do {
      const list = await env.EVENTS_BUCKET.list({
        prefix: 'dead-letter/',
        limit: 1000,
        cursor,
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

    console.log(`[DEAD-LETTER-CLEANUP] Completed: checked ${deadLetterChecked}, deleted ${deadLetterDeleted} expired dead letters`)
  } catch (err) {
    console.error('[DEAD-LETTER-CLEANUP] Error during dead letter cleanup:', err)
  }
}

/**
 * Task 5: Subscription Dead Letter Cleanup (daily at 3:30 UTC only)
 * Triggers cleanup of old dead letters and delivery logs in all SubscriptionDO shards.
 * This helps prevent unbounded growth of the SQLite tables in each shard.
 */
async function cleanupSubscriptionDeadLetters(env: Env): Promise<void> {
  const currentHour = new Date().getUTCHours()
  if (currentHour !== 3) {
    return
  }

  if (!env.SUBSCRIPTIONS) {
    console.log('[SUB-CLEANUP] Skipping - SUBSCRIPTIONS binding not available')
    return
  }

  try {
    console.log('[SUB-CLEANUP] Starting subscription dead letter cleanup...')

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
          console.log(
            `[SUB-CLEANUP] Shard ${shard}: deleted ${result.deadLettersDeleted} dead letters, ` +
            `${result.deliveryLogsDeleted} logs, ${result.deliveriesDeleted} deliveries`
          )
        }
      } catch (err) {
        console.error(`[SUB-CLEANUP] Error cleaning up shard ${shard}:`, err)
      }
    }

    console.log(
      `[SUB-CLEANUP] Completed: deleted ${totalDeadLettersDeleted} dead letters, ` +
      `${totalDeliveryLogsDeleted} delivery logs, ${totalDeliveriesDeleted} old deliveries`
    )
  } catch (err) {
    console.error('[SUB-CLEANUP] Error during subscription cleanup:', err)
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
  console.log(`[SCHEDULED] Triggered at ${new Date().toISOString()}, cron: ${event.cron}`)

  ctx.waitUntil((async () => {
    // Acquire distributed lock to prevent concurrent scheduled runs
    const lockResult = await withSchedulerLock(
      env.EVENTS_BUCKET,
      async () => {
        // Task 1: Dedup marker cleanup (hourly)
        await cleanupDedupMarkers(env)

        // Task 2: CDC Compaction (hourly)
        await runCDCCompaction(env)

        // Task 3: Event Stream Compaction (only at 1:30 UTC)
        await runEventStreamCompaction(env)

        // Task 4: Dead Letter Cleanup (only at 2:30 UTC)
        await cleanupDeadLetters(env)

        // Task 5: Subscription Dead Letter Cleanup (only at 3:30 UTC)
        await cleanupSubscriptionDeadLetters(env)

        const durationMs = Date.now() - startTime
        console.log(`[SCHEDULED] All tasks completed in ${durationMs}ms`)
      },
      { timeoutMs: 10 * 60 * 1000 } // 10 minute lock timeout
    )

    if (lockResult.skipped) {
      console.log('[SCHEDULED] Skipped - another worker is running scheduled tasks')
    }
  })())
}
