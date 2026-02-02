/**
 * EventWriterDO - Shared Durable Object for event ingestion
 *
 * Used by both:
 * - events-tail worker (worker traces)
 * - events.do worker (webhooks, API events)
 *
 * Features:
 * - Sharded writers for parallel ingestion (from lakehouse pattern)
 * - Backpressure with tryNextShard response
 * - Full logging/observability (loop prevented at wrangler level)
 * - Source field distinguishes event origin
 * - Workers RPC for type-safe direct method calls
 * - Dynamic sharding via ShardCoordinatorDO
 */

import { DurableObject } from 'cloudflare:workers'
import { writeEvents, ulid, type EventRecord, type WriteResult } from './event-writer'
import { recordWriterDOMetric, recordR2WriteMetric, MetricTimer } from './metrics'
import type { ShardCoordinatorDO } from './shard-coordinator-do'
import { logger, sanitize, logError, type Logger } from './logger'

/**
 * Internal event with unique ID for deduplication
 */
interface BufferedEvent extends EventRecord {
  _eventId: string
}

const BUFFER_KEY = '_eventWriter:buffer'
const FLUSHED_KEY = '_eventWriter:flushed'
const METRICS_REPORT_INTERVAL_MS = 5_000 // Report metrics every 5 seconds

import type { Env as FullEnv } from './env'

export type Env = Pick<FullEnv, 'EVENTS_BUCKET' | 'EVENT_WRITER' | 'ANALYTICS' | 'SHARD_COORDINATOR'>

// ============================================================================
// Configuration
// ============================================================================

const DEFAULT_CONFIG = {
  countThreshold: 100,
  timeThresholdMs: 10_000,
  maxBufferSize: 10_000,
  maxPendingWrites: 100,  // Backpressure threshold
  flushIntervalMs: 5_000,
}

// ============================================================================
// RPC Response Types
// ============================================================================

export interface IngestResult {
  ok: boolean
  buffered: number
  shard: number
  flushed?: WriteResult | null | undefined
  /** If overloaded, suggests trying this shard instead */
  tryNextShard?: number | undefined
}

export interface StatsResult {
  shard: number
  buffered: number
  pendingWrites: number
  lastFlushTime: string
  timeSinceFlush: number
  flushScheduled: boolean
}

// ============================================================================
// EventWriterDO
// ============================================================================

export class EventWriterDO extends DurableObject<Env> {
  private buffer: BufferedEvent[] = []
  private flushedEventIds: Set<string> = new Set()
  private lastFlushTime = Date.now()
  private lastMetricsReport = 0
  private pendingWrites = 0
  private shardId = 0
  private config = DEFAULT_CONFIG
  private flushScheduled = false

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    // Extract shard ID from DO name (e.g., "events:shard-2" -> 2)
    const name = ctx.id.toString()
    const match = name.match(/:shard-(\d+)$/)
    this.shardId = match?.[1] ? parseInt(match[1], 10) : 0

    // Restore any persisted buffer from storage (survives DO eviction)
    ctx.blockConcurrencyWhile(async () => {
      await this.restoreBuffer()
    })
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Metrics Reporting to Shard Coordinator
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Report metrics to the shard coordinator (called periodically)
   */
  private async reportMetricsToCoordinator(): Promise<void> {
    const now = Date.now()
    if ((now - this.lastMetricsReport) < METRICS_REPORT_INTERVAL_MS) {
      return // Not time to report yet
    }

    // Check if shard coordinator is bound
    if (!this.env.SHARD_COORDINATOR) {
      return
    }

    this.lastMetricsReport = now

    const log = logger.child({ component: 'EventWriterDO', shard: this.shardId })
    try {
      const coordinatorId = this.env.SHARD_COORDINATOR.idFromName('global')
      const coordinator = this.env.SHARD_COORDINATOR.get(coordinatorId)

      await coordinator.reportMetrics({
        shardId: this.shardId,
        buffered: this.buffer.length,
        pendingWrites: this.pendingWrites,
        lastFlushTime: new Date(this.lastFlushTime).toISOString(),
        timeSinceFlush: now - this.lastFlushTime,
        flushScheduled: this.flushScheduled,
      })
    } catch (err) {
      // Non-fatal: coordinator unavailable
      log.warn('Failed to report metrics to coordinator', { error: sanitize.errorMessage(String(err)) })
    }
  }

  /**
   * Report backpressure to coordinator and get alternative shard
   */
  private async reportBackpressureToCoordinator(): Promise<number | undefined> {
    if (!this.env.SHARD_COORDINATOR) {
      // Fall back to simple increment
      return this.shardId + 1
    }

    try {
      const coordinatorId = this.env.SHARD_COORDINATOR.idFromName('global')
      const coordinator = this.env.SHARD_COORDINATOR.get(coordinatorId)

      const result = await coordinator.reportBackpressure(this.shardId)
      return result.alternativeShard
    } catch (err) {
      const log = logger.child({ component: 'EventWriterDO', shard: this.shardId })
      log.warn('Failed to report backpressure', { error: sanitize.errorMessage(String(err)) })
      return this.shardId + 1
    }
  }

  /**
   * Restore buffer from DO storage (after eviction or restart)
   * Filters out any events that were already successfully flushed to R2
   */
  private async restoreBuffer(): Promise<void> {
    try {
      // First, restore the set of flushed event IDs
      const flushedIds = await this.ctx.storage.get<string[]>(FLUSHED_KEY)
      if (flushedIds && flushedIds.length > 0) {
        this.flushedEventIds = new Set(flushedIds)
        logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Restored flushed event markers', { count: flushedIds.length })
      }

      // Then restore buffer, filtering out already-flushed events
      const stored = await this.ctx.storage.get<BufferedEvent[]>(BUFFER_KEY)
      if (stored && stored.length > 0) {
        // Filter out events that were already flushed (handles the partial flush race)
        const unflushedEvents = stored.filter(e => !this.flushedEventIds.has(e._eventId))
        const duplicateCount = stored.length - unflushedEvents.length

        if (duplicateCount > 0) {
          logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Filtered already-flushed events', { duplicateCount })
        }

        if (unflushedEvents.length > 0) {
          this.buffer = unflushedEvents
          logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Restored events from storage', { count: unflushedEvents.length })

          // Record restore metric
          recordWriterDOMetric(this.env.ANALYTICS, 'restore', 'success', {
            events: unflushedEvents.length,
            shard: this.shardId,
          })
        }

        // Clean up: if we filtered events, update persisted buffer and clear flushed markers
        if (duplicateCount > 0) {
          await this.persistBuffer()
          // Clear flushed markers since we've now filtered the buffer
          this.flushedEventIds.clear()
          await this.ctx.storage.delete(FLUSHED_KEY)
        }
      } else {
        // No buffer to restore, clear any stale flushed markers
        if (this.flushedEventIds.size > 0) {
          this.flushedEventIds.clear()
          await this.ctx.storage.delete(FLUSHED_KEY)
        }
      }
    } catch (error) {
      const log = logger.child({ component: 'EventWriterDO', shard: this.shardId })
      logError(log, 'Failed to restore buffer', error)
    }
  }

  /**
   * Persist buffer to DO storage so it survives eviction
   */
  private async persistBuffer(): Promise<void> {
    if (this.buffer.length > 0) {
      await this.ctx.storage.put(BUFFER_KEY, this.buffer)
    } else {
      await this.ctx.storage.delete(BUFFER_KEY)
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // RPC Methods (Workers RPC - type-safe direct calls)
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Ingest events with backpressure support
   * Returns tryNextShard if this shard is overloaded
   */
  async ingest(events: EventRecord[], source?: string): Promise<IngestResult> {
    const timer = new MetricTimer()

    // Backpressure check
    if (this.pendingWrites >= this.config.maxPendingWrites) {
      logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Backpressure triggered', { pendingWrites: this.pendingWrites })
      recordWriterDOMetric(this.env.ANALYTICS, 'backpressure', 'backpressure', {
        events: events.length,
        shard: this.shardId,
      }, timer.elapsed())

      // Report backpressure to coordinator and get alternative shard
      const alternativeShard = await this.reportBackpressureToCoordinator()

      return {
        ok: false,
        buffered: this.buffer.length,
        shard: this.shardId,
        tryNextShard: alternativeShard,
      }
    }

    this.pendingWrites++
    try {
      const result = await this.doIngest(events, source)
      recordWriterDOMetric(this.env.ANALYTICS, 'ingest', 'success', {
        events: events.length,
        shard: this.shardId,
      }, timer.elapsed())

      // Report metrics periodically to coordinator
      await this.reportMetricsToCoordinator()

      return result
    } catch (err) {
      recordWriterDOMetric(this.env.ANALYTICS, 'ingest', 'error', {
        events: events.length,
        shard: this.shardId,
      }, timer.elapsed())
      throw err
    } finally {
      this.pendingWrites--
    }
  }

  private async doIngest(events: EventRecord[], source?: string): Promise<IngestResult> {
    if (!Array.isArray(events) || events.length === 0) {
      return { ok: false, buffered: this.buffer.length, shard: this.shardId }
    }

    // Convert to BufferedEvents with unique IDs for deduplication
    const bufferedEvents: BufferedEvent[] = events.map(event => ({
      ...event,
      source: event.source || source,
      _eventId: ulid(),
    }))

    // Add to buffer and persist to storage
    this.buffer.push(...bufferedEvents)
    await this.persistBuffer()
    logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Buffered events', { count: events.length, total: this.buffer.length })

    // Check if we should flush immediately
    if (this.shouldFlush()) {
      const result = await this.flush()
      return {
        ok: true,
        buffered: 0,
        shard: this.shardId,
        flushed: result,
      }
    }

    // Schedule deferred flush
    this.scheduleFlush()

    return {
      ok: true,
      buffered: this.buffer.length,
      shard: this.shardId,
    }
  }

  /**
   * Force flush buffered events to R2
   */
  async forceFlush(): Promise<WriteResult | null> {
    return this.flush()
  }

  /**
   * Get current stats
   */
  async stats(): Promise<StatsResult> {
    return {
      shard: this.shardId,
      buffered: this.buffer.length,
      pendingWrites: this.pendingWrites,
      lastFlushTime: new Date(this.lastFlushTime).toISOString(),
      timeSinceFlush: Date.now() - this.lastFlushTime,
      flushScheduled: this.flushScheduled,
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Flush logic
  // ──────────────────────────────────────────────────────────────────────────

  private shouldFlush(): boolean {
    return (
      this.buffer.length >= this.config.countThreshold ||
      this.buffer.length >= this.config.maxBufferSize ||
      (Date.now() - this.lastFlushTime) >= this.config.timeThresholdMs
    )
  }

  private scheduleFlush(): void {
    if (this.flushScheduled) return
    this.flushScheduled = true

    // Use DO alarm for reliable deferred flush
    this.ctx.storage.setAlarm(Date.now() + this.config.flushIntervalMs)
  }

  async alarm(): Promise<void> {
    this.flushScheduled = false
    if (this.buffer.length > 0) {
      await this.flush()
    }
  }

  private async flush(): Promise<WriteResult | null> {
    if (this.buffer.length === 0) {
      return null
    }

    const timer = new MetricTimer()
    const events = this.buffer
    this.buffer = []
    this.lastFlushTime = Date.now()

    // Collect event IDs for deduplication tracking
    const eventIds = events.map(e => e._eventId)

    // Group events by source for correct prefix routing
    // Strip internal _eventId before writing to R2
    const eventsBySource = new Map<string, EventRecord[]>()
    for (const event of events) {
      const source = event.source || 'events'
      if (!eventsBySource.has(source)) {
        eventsBySource.set(source, [])
      }
      // Remove internal _eventId field before writing
      const { _eventId, ...eventRecord } = event
      eventsBySource.get(source)!.push(eventRecord)
    }

    logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Flushing events', { count: events.length, sources: eventsBySource.size })

    const results: WriteResult[] = []
    let totalBytes = 0
    try {
      // Write each source group to its own prefix
      for (const [source, sourceEvents] of eventsBySource) {
        const writeTimer = new MetricTimer()
        const result = await writeEvents(this.env.EVENTS_BUCKET, source, sourceEvents)
        logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Flushed', { key: sanitize.id(result.key, 64) })
        results.push(result)
        totalBytes += result.bytes

        // Record R2 write metric for each source
        recordR2WriteMetric(
          this.env.ANALYTICS,
          'success',
          sourceEvents.length,
          result.bytes,
          writeTimer.elapsed(),
          source
        )
      }

      // ATOMIC FLUSH PATTERN:
      // Step 1: Mark these events as flushed BEFORE deleting buffer
      // This survives the race condition where R2 write succeeds but buffer delete fails
      await this.ctx.storage.put(FLUSHED_KEY, eventIds)
      logger.child({ component: 'EventWriterDO', shard: this.shardId }).info('Marked events as flushed', { count: eventIds.length })

      // Step 2: Delete the buffer (if this fails, flushed markers protect against duplication)
      await this.ctx.storage.delete(BUFFER_KEY)

      // Step 3: Clear flushed markers (safe to fail - will be cleaned on next restore)
      await this.ctx.storage.delete(FLUSHED_KEY)

      // Record successful flush metric
      recordWriterDOMetric(this.env.ANALYTICS, 'flush', 'success', {
        events: events.length,
        bytes: totalBytes,
        shard: this.shardId,
      }, timer.elapsed())

      // Return the first result (or combine stats if needed)
      return results[0] ?? null
    } catch (error) {
      // On error, put events back in buffer (storage still has them persisted)
      const log = logger.child({ component: 'EventWriterDO', shard: this.shardId })
      logError(log, 'Flush failed - events returned to buffer', error, { eventCount: events.length })
      this.buffer.unshift(...events)

      // Record failed flush metric
      recordWriterDOMetric(this.env.ANALYTICS, 'flush', 'error', {
        events: events.length,
        shard: this.shardId,
      }, timer.elapsed())

      // Record R2 write failure
      recordR2WriteMetric(this.env.ANALYTICS, 'error', events.length, 0, timer.elapsed())

      throw error
    }
  }
}

// ============================================================================
// Router helpers (for use in workers) - Uses Workers RPC
// ============================================================================

/**
 * Get a specific shard's DO stub (typed for RPC)
 */
export function getEventWriterDO(env: Env, shard: number = 0): DurableObjectStub<EventWriterDO> {
  const name = shard === 0 ? 'events' : `events:shard-${shard}`
  const id = env.EVENT_WRITER.idFromName(name)
  return env.EVENT_WRITER.get(id)
}

/**
 * Get the shard coordinator for dynamic sharding
 */
export function getShardCoordinator(env: Env): DurableObjectStub<ShardCoordinatorDO> | null {
  if (!env.SHARD_COORDINATOR) return null
  const id = env.SHARD_COORDINATOR.idFromName('global')
  return env.SHARD_COORDINATOR.get(id)
}

/**
 * Get active shards from coordinator (with fallback)
 */
export async function getActiveShards(env: Env): Promise<number[]> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) {
    return [0] // Fallback to shard 0 if no coordinator
  }
  try {
    return await coordinator.getActiveShards()
  } catch (err) {
    logError(logger.child({ component: 'Router' }), 'Failed to get active shards from coordinator', err)
    return [0]
  }
}

/**
 * Get recommended routing shard from coordinator
 */
export async function getRoutingShard(env: Env, preferredShard?: number): Promise<number> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) {
    return preferredShard ?? 0
  }
  try {
    return await coordinator.getRoutingShard(preferredShard)
  } catch (err) {
    logError(logger.child({ component: 'Router' }), 'Failed to get routing shard from coordinator', err, { preferredShard })
    return preferredShard ?? 0
  }
}

/**
 * Ingest events with automatic shard overflow via RPC
 * Now uses dynamic shard coordinator when available
 */
export async function ingestWithOverflow(
  env: Env,
  events: EventRecord[],
  source: string,
  startShard?: number,
  maxRetries: number = 16
): Promise<IngestResult> {
  // Get starting shard from coordinator if not specified
  let currentShard = startShard ?? await getRoutingShard(env)

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const stub = getEventWriterDO(env, currentShard)

    // Direct RPC call - type-safe!
    const result = await stub.ingest(events, source)

    if (!result.tryNextShard) {
      return result
    }

    // Overloaded - try next shard (provided by coordinator via EventWriterDO)
    logger.child({ component: 'Router' }).info('Shard overloaded, trying next', { currentShard, nextShard: result.tryNextShard })
    currentShard = result.tryNextShard
  }

  return { ok: false, shard: currentShard, buffered: 0 }
}

/**
 * Ingest events in parallel across multiple shards via RPC
 * Now uses dynamic shard count from coordinator
 */
export async function ingestParallel(
  env: Env,
  events: EventRecord[],
  source: string,
  shardCount?: number
): Promise<{ ok: boolean; results: IngestResult[] }> {
  // Get active shards from coordinator if count not specified
  const activeShards = shardCount
    ? Array.from({ length: shardCount }, (_, i) => i)
    : await getActiveShards(env)

  const effectiveShardCount = activeShards.length

  // Partition events by shard using hash of timestamp + type
  const shards = new Map<number, EventRecord[]>()

  for (const event of events) {
    const hash = simpleHash(`${event.ts}:${event.type}`)
    const shardIndex = hash % effectiveShardCount
    const shardId = activeShards[shardIndex] ?? 0
    const shardEvents = shards.get(shardId)
    if (shardEvents) {
      shardEvents.push(event)
    } else {
      shards.set(shardId, [event])
    }
  }

  // Send to each shard in parallel via RPC
  const results = await Promise.all(
    Array.from(shards.entries()).map(async ([shard, shardEvents]) => {
      const stub = getEventWriterDO(env, shard)

      try {
        return await stub.ingest(shardEvents, source)
      } catch (e) {
        return { ok: false, shard, buffered: 0, error: String(e) } as IngestResult
      }
    })
  )

  return {
    ok: results.every(r => r.ok),
    results,
  }
}

/**
 * Get shard statistics from coordinator
 */
export async function getShardStats(env: Env): Promise<{
  activeShards: number[]
  totalBuffered: number
  totalPendingWrites: number
  averageUtilization: number
} | null> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) {
    return null
  }
  try {
    return await coordinator.getStats()
  } catch (err) {
    logError(logger.child({ component: 'Router' }), 'Failed to get shard stats', err)
    return null
  }
}

/**
 * Force scale to a specific shard count
 */
export async function forceScaleShards(env: Env, targetCount: number): Promise<{
  scaled: boolean
  previousCount: number
  newCount: number
  reason: string
} | null> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) {
    return null
  }
  try {
    return await coordinator.forceScale(targetCount)
  } catch (err) {
    logError(logger.child({ component: 'Router' }), 'Failed to force scale', err, { targetCount })
    return null
  }
}

/** Simple string hash for shard distribution */
function simpleHash(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}
