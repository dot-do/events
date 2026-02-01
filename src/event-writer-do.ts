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
 */

import { DurableObject } from 'cloudflare:workers'
import { writeEvents, ulid, type EventRecord, type WriteResult } from './event-writer'

/**
 * Internal event with unique ID for deduplication
 */
interface BufferedEvent extends EventRecord {
  _eventId: string
}

const BUFFER_KEY = '_eventWriter:buffer'
const FLUSHED_KEY = '_eventWriter:flushed'

import type { Env as FullEnv } from './env'

export type Env = Pick<FullEnv, 'EVENTS_BUCKET' | 'EVENT_WRITER'>

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
  flushed?: WriteResult | null
  /** If overloaded, suggests trying this shard instead */
  tryNextShard?: number
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
        console.log(`[EventWriterDO:${this.shardId}] Restored ${flushedIds.length} flushed event markers`)
      }

      // Then restore buffer, filtering out already-flushed events
      const stored = await this.ctx.storage.get<BufferedEvent[]>(BUFFER_KEY)
      if (stored && stored.length > 0) {
        // Filter out events that were already flushed (handles the partial flush race)
        const unflushedEvents = stored.filter(e => !this.flushedEventIds.has(e._eventId))
        const duplicateCount = stored.length - unflushedEvents.length

        if (duplicateCount > 0) {
          console.log(`[EventWriterDO:${this.shardId}] Filtered ${duplicateCount} already-flushed events`)
        }

        if (unflushedEvents.length > 0) {
          this.buffer = unflushedEvents
          console.log(`[EventWriterDO:${this.shardId}] Restored ${unflushedEvents.length} events from storage`)
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
      console.warn(`[EventWriterDO:${this.shardId}] Failed to restore buffer:`, error)
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
    // Backpressure check
    if (this.pendingWrites >= this.config.maxPendingWrites) {
      console.log(`[EventWriterDO:${this.shardId}] Backpressure triggered: ${this.pendingWrites} pending writes`)
      return {
        ok: false,
        buffered: this.buffer.length,
        shard: this.shardId,
        tryNextShard: this.shardId + 1,
      }
    }

    this.pendingWrites++
    try {
      return await this.doIngest(events, source)
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
    console.log(`[EventWriterDO:${this.shardId}] Buffered ${events.length} events (total: ${this.buffer.length})`)

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

    console.log(`[EventWriterDO:${this.shardId}] Flushing ${events.length} events across ${eventsBySource.size} source(s)`)

    const results: WriteResult[] = []
    try {
      // Write each source group to its own prefix
      for (const [source, sourceEvents] of eventsBySource) {
        const result = await writeEvents(this.env.EVENTS_BUCKET, source, sourceEvents)
        console.log(`[EventWriterDO:${this.shardId}] Flushed: ${result.key}`)
        results.push(result)
      }

      // ATOMIC FLUSH PATTERN:
      // Step 1: Mark these events as flushed BEFORE deleting buffer
      // This survives the race condition where R2 write succeeds but buffer delete fails
      await this.ctx.storage.put(FLUSHED_KEY, eventIds)
      console.log(`[EventWriterDO:${this.shardId}] Marked ${eventIds.length} events as flushed`)

      // Step 2: Delete the buffer (if this fails, flushed markers protect against duplication)
      await this.ctx.storage.delete(BUFFER_KEY)

      // Step 3: Clear flushed markers (safe to fail - will be cleaned on next restore)
      await this.ctx.storage.delete(FLUSHED_KEY)

      // Return the first result (or combine stats if needed)
      return results[0] ?? null
    } catch (error) {
      // On error, put events back in buffer (storage still has them persisted)
      console.error(`[EventWriterDO:${this.shardId}] Flush failed:`, error)
      this.buffer.unshift(...events)
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
 * Ingest events with automatic shard overflow via RPC
 */
export async function ingestWithOverflow(
  env: Env,
  events: EventRecord[],
  source: string,
  startShard: number = 0,
  maxRetries: number = 16
): Promise<IngestResult> {
  let currentShard = startShard

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const stub = getEventWriterDO(env, currentShard)

    // Direct RPC call - type-safe!
    const result = await stub.ingest(events, source)

    if (!result.tryNextShard) {
      return result
    }

    // Overloaded - try next shard
    console.log(`[Router] Shard ${currentShard} overloaded, trying shard ${result.tryNextShard}`)
    currentShard = result.tryNextShard
  }

  return { ok: false, shard: currentShard, buffered: 0 }
}

/**
 * Ingest events in parallel across multiple shards via RPC
 */
export async function ingestParallel(
  env: Env,
  events: EventRecord[],
  source: string,
  shardCount: number = 4
): Promise<{ ok: boolean; results: IngestResult[] }> {
  // Partition events by shard using hash of timestamp + type
  const shards = new Map<number, EventRecord[]>()

  for (const event of events) {
    const hash = simpleHash(`${event.ts}:${event.type}`)
    const shard = hash % shardCount
    if (!shards.has(shard)) shards.set(shard, [])
    shards.get(shard)!.push(event)
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

/** Simple string hash for shard distribution */
function simpleHash(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}
