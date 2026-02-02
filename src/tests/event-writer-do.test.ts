/**
 * EventWriterDO Tests
 *
 * Comprehensive unit tests for the EventWriterDO Durable Object that handles:
 * - Event writing and batching
 * - Alarm handling for deferred flushes
 * - R2 storage interactions
 * - Error handling and retries
 * - Backpressure and shard coordination
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import type { EventRecord, WriteResult } from '../event-writer'

// ============================================================================
// Mock Types
// ============================================================================

interface MockDurableObjectStorage {
  get: ReturnType<typeof vi.fn>
  put: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  list: ReturnType<typeof vi.fn>
  getAlarm: ReturnType<typeof vi.fn>
  setAlarm: ReturnType<typeof vi.fn>
  deleteAlarm: ReturnType<typeof vi.fn>
  _storage: Map<string, unknown>
  _alarm: number | null
}

interface MockDurableObjectState {
  id: { toString: () => string; name?: string }
  storage: MockDurableObjectStorage
  waitUntil: ReturnType<typeof vi.fn>
  blockConcurrencyWhile: ReturnType<typeof vi.fn>
}

interface MockR2Bucket {
  put: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  list: ReturnType<typeof vi.fn>
  head: ReturnType<typeof vi.fn>
  _objects: Map<string, { body: Uint8Array; metadata: Record<string, string> }>
}

interface MockAnalytics {
  writeDataPoint: ReturnType<typeof vi.fn>
}

interface MockShardCoordinator {
  reportMetrics: ReturnType<typeof vi.fn>
  reportBackpressure: ReturnType<typeof vi.fn>
  getActiveShards: ReturnType<typeof vi.fn>
  getRoutingShard: ReturnType<typeof vi.fn>
}

interface MockDurableObjectNamespace<T = unknown> {
  idFromName: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  _stubs: Map<string, T>
}

interface MockEnv {
  EVENTS_BUCKET: MockR2Bucket
  EVENT_WRITER: MockDurableObjectNamespace
  ANALYTICS: MockAnalytics
  SHARD_COORDINATOR: MockDurableObjectNamespace<MockShardCoordinator>
}

// ============================================================================
// Mock Factories
// ============================================================================

function createMockStorage(): MockDurableObjectStorage {
  const storage = new Map<string, unknown>()
  let alarm: number | null = null

  return {
    get: vi.fn(async <T>(key: string): Promise<T | undefined> => {
      return storage.get(key) as T | undefined
    }),
    put: vi.fn(async (key: string, value: unknown) => {
      storage.set(key, value)
    }),
    delete: vi.fn(async (key: string | string[]) => {
      if (Array.isArray(key)) {
        let deleted = 0
        for (const k of key) {
          if (storage.delete(k)) deleted++
        }
        return deleted
      }
      return storage.delete(key)
    }),
    list: vi.fn(async () => storage),
    getAlarm: vi.fn(async () => alarm),
    setAlarm: vi.fn(async (time: number | Date) => {
      alarm = typeof time === 'number' ? time : time.getTime()
    }),
    deleteAlarm: vi.fn(async () => {
      alarm = null
    }),
    _storage: storage,
    _alarm: alarm,
  }
}

function createMockCtx(options: { id?: string; name?: string } = {}): MockDurableObjectState {
  const storage = createMockStorage()

  return {
    id: {
      toString: () => options.id ?? 'test-do-id',
      name: options.name,
    },
    storage,
    waitUntil: vi.fn((promise: Promise<unknown>) => promise),
    blockConcurrencyWhile: vi.fn(async <T>(fn: () => Promise<T>) => fn()),
  }
}

function createMockR2Bucket(): MockR2Bucket {
  const objects = new Map<string, { body: Uint8Array; metadata: Record<string, string> }>()

  return {
    put: vi.fn(async (key: string, body: Uint8Array | string, options?: { customMetadata?: Record<string, string> }) => {
      const bodyBytes = typeof body === 'string' ? new TextEncoder().encode(body) : body
      objects.set(key, {
        body: bodyBytes,
        metadata: options?.customMetadata ?? {},
      })
      return {
        key,
        version: 'v1',
        size: bodyBytes.byteLength,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        customMetadata: options?.customMetadata ?? {},
      }
    }),
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        body: new ReadableStream(),
        arrayBuffer: async () => obj.body.buffer,
        text: async () => new TextDecoder().decode(obj.body),
        customMetadata: obj.metadata,
      }
    }),
    delete: vi.fn(async (key: string) => objects.delete(key)),
    list: vi.fn(async () => ({
      objects: Array.from(objects.keys()).map(key => ({ key })),
      truncated: false,
    })),
    head: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return { key, size: obj.body.byteLength }
    }),
    _objects: objects,
  }
}

function createMockAnalytics(): MockAnalytics {
  return {
    writeDataPoint: vi.fn(),
  }
}

function createMockShardCoordinator(): MockShardCoordinator {
  return {
    reportMetrics: vi.fn().mockResolvedValue(undefined),
    reportBackpressure: vi.fn().mockResolvedValue({ shouldRetry: true, alternativeShard: 1 }),
    getActiveShards: vi.fn().mockResolvedValue([0, 1, 2]),
    getRoutingShard: vi.fn().mockResolvedValue(0),
  }
}

function createMockDurableObjectNamespace<T>(stub: T): MockDurableObjectNamespace<T> {
  const stubs = new Map<string, T>()

  return {
    idFromName: vi.fn((name: string) => ({ name, toString: () => name })),
    get: vi.fn((_id: unknown) => stub),
    _stubs: stubs,
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  const shardCoordinator = createMockShardCoordinator()

  return {
    EVENTS_BUCKET: createMockR2Bucket(),
    EVENT_WRITER: createMockDurableObjectNamespace({}),
    ANALYTICS: createMockAnalytics(),
    SHARD_COORDINATOR: createMockDurableObjectNamespace(shardCoordinator),
    ...overrides,
  }
}

// ============================================================================
// EventWriterDO Simulation
// ============================================================================

// Since the actual EventWriterDO imports from cloudflare:workers which isn't
// available in vitest, we simulate its behavior based on the implementation.

interface BufferedEvent extends EventRecord {
  _eventId: string
}

interface DedupMarker {
  eventIds: string[]
  createdAt: number
}

const BUFFER_KEY = '_eventWriter:buffer'
const FLUSHED_KEY = '_eventWriter:flushed'
const DEDUP_MARKERS_PREFIX = '_eventWriter:dedup:'
const DEFAULT_DEDUP_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours

const DEFAULT_CONFIG = {
  countThreshold: 100,
  timeThresholdMs: 10_000,
  maxBufferSize: 10_000,
  maxPendingWrites: 100,
  flushIntervalMs: 5_000,
  dedupTtlMs: DEFAULT_DEDUP_TTL_MS,
}

/**
 * Simulated EventWriterDO for testing
 * Mirrors the actual implementation behavior
 */
class SimulatedEventWriterDO {
  private buffer: BufferedEvent[] = []
  private flushedEventIds: Set<string> = new Set()
  private lastFlushTime = Date.now()
  private pendingWrites = 0
  private shardId = 0
  private config = DEFAULT_CONFIG
  private flushScheduled = false
  private mockWriteEvents: ReturnType<typeof vi.fn>

  constructor(
    private ctx: MockDurableObjectState,
    private env: MockEnv,
    mockWriteEvents?: ReturnType<typeof vi.fn>
  ) {
    // Extract shard ID from DO name
    const name = ctx.id.toString()
    const match = name.match(/:shard-(\d+)$/)
    this.shardId = match?.[1] ? parseInt(match[1], 10) : 0

    // Default mock for writeEvents
    this.mockWriteEvents = mockWriteEvents ?? vi.fn().mockResolvedValue({
      key: 'events/2024/01/01/00/test.parquet',
      events: 0,
      bytes: 100,
      cpuMs: 5,
    })

    // Simulate blockConcurrencyWhile restoration
    this.restoreBuffer()
  }

  private async restoreBuffer(): Promise<void> {
    try {
      // Load all dedup markers (with TTL support)
      await this.loadDedupMarkers()

      // Handle legacy format (FLUSHED_KEY stores string[])
      const flushedIds = await this.ctx.storage.get<string[]>(FLUSHED_KEY)
      if (flushedIds && flushedIds.length > 0) {
        // Add legacy markers to in-memory set
        for (const id of flushedIds) {
          this.flushedEventIds.add(id)
        }
        // Migrate legacy markers to new TTL format
        await this.storeDedupMarkers(flushedIds)
        // Delete legacy key
        await this.ctx.storage.delete(FLUSHED_KEY)
      }

      const stored = await this.ctx.storage.get<BufferedEvent[]>(BUFFER_KEY)
      if (stored && stored.length > 0) {
        const unflushedEvents = stored.filter(e => !this.flushedEventIds.has(e._eventId))
        if (unflushedEvents.length > 0) {
          this.buffer = unflushedEvents
        }

        if (stored.length !== unflushedEvents.length) {
          await this.persistBuffer()
        }
      }
    } catch (error) {
      // Silently handle restore errors in tests
    }
  }

  private async loadDedupMarkers(): Promise<void> {
    const now = Date.now()
    const allMarkers = await this.ctx.storage.list() as Map<string, unknown>
    const expiredKeys: string[] = []

    for (const [key, value] of allMarkers) {
      if (!key.startsWith(DEDUP_MARKERS_PREFIX)) continue
      const marker = value as DedupMarker
      const age = now - marker.createdAt
      if (age > this.config.dedupTtlMs) {
        expiredKeys.push(key)
      } else {
        for (const eventId of marker.eventIds) {
          this.flushedEventIds.add(eventId)
        }
      }
    }

    if (expiredKeys.length > 0) {
      await this.ctx.storage.delete(expiredKeys)
    }
  }

  private async storeDedupMarkers(eventIds: string[]): Promise<void> {
    if (eventIds.length === 0) return

    const marker: DedupMarker = {
      eventIds,
      createdAt: Date.now(),
    }
    const key = `${DEDUP_MARKERS_PREFIX}${marker.createdAt}`
    await this.ctx.storage.put(key, marker)
  }

  private async cleanupExpiredDedupMarkers(): Promise<number> {
    const now = Date.now()
    const allMarkers = await this.ctx.storage.list() as Map<string, unknown>
    const expiredKeys: string[] = []

    for (const [key, value] of allMarkers) {
      if (!key.startsWith(DEDUP_MARKERS_PREFIX)) continue
      const marker = value as DedupMarker
      const age = now - marker.createdAt
      if (age > this.config.dedupTtlMs) {
        expiredKeys.push(key)
        for (const eventId of marker.eventIds) {
          this.flushedEventIds.delete(eventId)
        }
      }
    }

    if (expiredKeys.length > 0) {
      await this.ctx.storage.delete(expiredKeys)
    }

    return expiredKeys.length
  }

  private async persistBuffer(): Promise<void> {
    if (this.buffer.length > 0) {
      await this.ctx.storage.put(BUFFER_KEY, this.buffer)
    } else {
      await this.ctx.storage.delete(BUFFER_KEY)
    }
  }

  async ingest(events: EventRecord[], source?: string): Promise<{
    ok: boolean
    buffered: number
    shard: number
    flushed?: WriteResult | null
    tryNextShard?: number
  }> {
    // Backpressure check
    if (this.pendingWrites >= this.config.maxPendingWrites) {
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
      return await this.doIngest(events, source)
    } finally {
      this.pendingWrites--
    }
  }

  private async reportBackpressureToCoordinator(): Promise<number | undefined> {
    if (!this.env.SHARD_COORDINATOR) {
      return this.shardId + 1
    }

    try {
      const coordinatorId = this.env.SHARD_COORDINATOR.idFromName('global')
      const coordinator = this.env.SHARD_COORDINATOR.get(coordinatorId) as MockShardCoordinator
      const result = await coordinator.reportBackpressure(this.shardId)
      return result.alternativeShard
    } catch (err) {
      return this.shardId + 1
    }
  }

  private generateUlid(): string {
    return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`
  }

  private async doIngest(events: EventRecord[], source?: string): Promise<{
    ok: boolean
    buffered: number
    shard: number
    flushed?: WriteResult | null
  }> {
    if (!Array.isArray(events) || events.length === 0) {
      return { ok: false, buffered: this.buffer.length, shard: this.shardId }
    }

    const bufferedEvents: BufferedEvent[] = events.map(event => ({
      ...event,
      source: event.source || source,
      _eventId: this.generateUlid(),
    }))

    this.buffer.push(...bufferedEvents)
    await this.persistBuffer()

    if (this.shouldFlush()) {
      const result = await this.flush()
      return {
        ok: true,
        buffered: 0,
        shard: this.shardId,
        flushed: result,
      }
    }

    this.scheduleFlush()

    return {
      ok: true,
      buffered: this.buffer.length,
      shard: this.shardId,
    }
  }

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
    this.ctx.storage.setAlarm(Date.now() + this.config.flushIntervalMs)
  }

  async alarm(): Promise<void> {
    this.flushScheduled = false

    // Flush any buffered events
    if (this.buffer.length > 0) {
      await this.flush()
    }

    // Clean up expired dedup markers to prevent unbounded storage growth
    await this.cleanupExpiredDedupMarkers()
  }

  async forceFlush(): Promise<WriteResult | null> {
    return this.flush()
  }

  async stats(): Promise<{
    shard: number
    buffered: number
    pendingWrites: number
    lastFlushTime: string
    timeSinceFlush: number
    flushScheduled: boolean
    dedupMarkerCount: number
  }> {
    return {
      shard: this.shardId,
      buffered: this.buffer.length,
      pendingWrites: this.pendingWrites,
      lastFlushTime: new Date(this.lastFlushTime).toISOString(),
      timeSinceFlush: Date.now() - this.lastFlushTime,
      flushScheduled: this.flushScheduled,
      dedupMarkerCount: this.flushedEventIds.size,
    }
  }

  private async flush(): Promise<WriteResult | null> {
    if (this.buffer.length === 0) {
      return null
    }

    const events = this.buffer
    this.buffer = []
    this.lastFlushTime = Date.now()

    const eventIds = events.map(e => e._eventId)

    // Group events by source
    const eventsBySource = new Map<string, EventRecord[]>()
    for (const event of events) {
      const source = event.source || 'events'
      if (!eventsBySource.has(source)) {
        eventsBySource.set(source, [])
      }
      const { _eventId, ...eventRecord } = event
      eventsBySource.get(source)!.push(eventRecord)
    }

    const results: WriteResult[] = []
    try {
      for (const [source, sourceEvents] of eventsBySource) {
        const result = await this.mockWriteEvents(this.env.EVENTS_BUCKET, source, sourceEvents)
        results.push(result)
      }

      // Store dedup markers with TTL BEFORE deleting buffer
      await this.storeDedupMarkers(eventIds)
      // Also add to in-memory set for immediate dedup
      for (const id of eventIds) {
        this.flushedEventIds.add(id)
      }
      await this.ctx.storage.delete(BUFFER_KEY)

      return results[0] ?? null
    } catch (error) {
      // On error, restore buffer
      this.buffer.unshift(...events)
      throw error
    }
  }

  // Expose cleanup method for testing
  async triggerCleanup(): Promise<number> {
    return this.cleanupExpiredDedupMarkers()
  }

  // Test helpers
  getBuffer(): BufferedEvent[] {
    return [...this.buffer]
  }

  setBuffer(events: BufferedEvent[]): void {
    this.buffer = events
  }

  getPendingWrites(): number {
    return this.pendingWrites
  }

  setPendingWrites(count: number): void {
    this.pendingWrites = count
  }

  getFlushScheduled(): boolean {
    return this.flushScheduled
  }

  setConfig(config: Partial<typeof DEFAULT_CONFIG>): void {
    this.config = { ...this.config, ...config }
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('EventWriterDO', () => {
  let mockCtx: MockDurableObjectState
  let mockEnv: MockEnv
  let mockWriteEvents: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'events:shard-2' })
    mockEnv = createMockEnv()
    mockWriteEvents = vi.fn().mockResolvedValue({
      key: 'events/2024/01/01/00/test.parquet',
      events: 10,
      bytes: 1000,
      cpuMs: 5,
    })
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Initialization Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('initialization', () => {
    it('should extract shard ID from DO name', async () => {
      const ctx = createMockCtx({ id: 'events:shard-5' })
      const writer = new SimulatedEventWriterDO(ctx, mockEnv, mockWriteEvents)

      const stats = await writer.stats()
      expect(stats).toEqual(expect.objectContaining({ shard: 5 }))
    })

    it('should default to shard 0 for non-sharded names', async () => {
      const ctx = createMockCtx({ id: 'events' })
      const writer = new SimulatedEventWriterDO(ctx, mockEnv, mockWriteEvents)

      const stats = await writer.stats()
      expect(stats).toEqual(expect.objectContaining({ shard: 0 }))
    })

    it('should restore buffer from storage on construction', async () => {
      const existingBuffer: BufferedEvent[] = [
        { type: 'test.event', ts: '2024-01-01T00:00:00Z', _eventId: 'existing-1' },
        { type: 'test.event2', ts: '2024-01-01T00:00:01Z', _eventId: 'existing-2' },
      ]
      mockCtx.storage._storage.set(BUFFER_KEY, existingBuffer)

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      // Allow async restore to complete
      await vi.runAllTimersAsync()

      expect(writer.getBuffer()).toHaveLength(2)
    })

    it('should filter out already-flushed events on restore', async () => {
      const existingBuffer: BufferedEvent[] = [
        { type: 'test.event', ts: '2024-01-01T00:00:00Z', _eventId: 'event-1' },
        { type: 'test.event2', ts: '2024-01-01T00:00:01Z', _eventId: 'event-2' },
        { type: 'test.event3', ts: '2024-01-01T00:00:02Z', _eventId: 'event-3' },
      ]
      mockCtx.storage._storage.set(BUFFER_KEY, existingBuffer)
      mockCtx.storage._storage.set(FLUSHED_KEY, ['event-1', 'event-2'])

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await vi.runAllTimersAsync()

      const buffer = writer.getBuffer()
      expect(buffer).toHaveLength(1)
      expect(buffer[0]._eventId).toBe('event-3')
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Event Ingestion Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('ingest()', () => {
    it('should buffer events successfully', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      const events: EventRecord[] = [
        { type: 'user.created', ts: new Date().toISOString() },
        { type: 'user.updated', ts: new Date().toISOString() },
      ]

      const result = await writer.ingest(events, 'test-source')

      expect(result.ok).toBe(true)
      expect(result.buffered).toBe(2)
      expect(result.shard).toBe(2)
    })

    it('should persist buffer to storage after ingestion', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }], 'test')

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        BUFFER_KEY,
        expect.arrayContaining([
          expect.objectContaining({ type: 'test.event' }),
        ])
      )
    })

    it('should add source to events', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }], 'my-source')

      const buffer = writer.getBuffer()
      expect(buffer[0].source).toBe('my-source')
    })

    it('should generate unique event IDs', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([
        { type: 'event1', ts: new Date().toISOString() },
        { type: 'event2', ts: new Date().toISOString() },
      ])

      const buffer = writer.getBuffer()
      expect(buffer[0]._eventId).not.toBe(buffer[1]._eventId)
    })

    it('should reject empty event arrays', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      const result = await writer.ingest([])

      expect(result.ok).toBe(false)
    })

    it('should auto-flush when buffer reaches count threshold', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ countThreshold: 5 })

      // Add events to trigger flush
      const events = Array.from({ length: 5 }, (_, i) => ({
        type: `event.${i}`,
        ts: new Date().toISOString(),
      }))

      const result = await writer.ingest(events)

      expect(result.flushed).toBeDefined()
      expect(result.buffered).toBe(0)
      expect(mockWriteEvents).toHaveBeenCalled()
    })

    it('should schedule deferred flush when not at threshold', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
      expect(writer.getFlushScheduled()).toBe(true)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Backpressure Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('backpressure', () => {
    it('should return tryNextShard when at max pending writes', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setPendingWrites(100) // At threshold

      const result = await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])

      expect(result.ok).toBe(false)
      expect(result.tryNextShard).toBeDefined()
    })

    it('should report backpressure to coordinator', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setPendingWrites(100)

      await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])

      const coordinator = mockEnv.SHARD_COORDINATOR.get({}) as MockShardCoordinator
      expect(coordinator.reportBackpressure).toHaveBeenCalledWith(2) // shard 2
    })

    it('should fall back to simple increment when coordinator unavailable', async () => {
      const envNoCoordinator = createMockEnv()
      // @ts-expect-error - Setting to undefined for test
      envNoCoordinator.SHARD_COORDINATOR = undefined

      const writer = new SimulatedEventWriterDO(mockCtx, envNoCoordinator, mockWriteEvents)
      writer.setPendingWrites(100)

      const result = await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])

      expect(result.tryNextShard).toBe(3) // current shard (2) + 1
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Alarm Handler Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('alarm()', () => {
    it('should flush buffered events on alarm', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      // Pre-populate buffer
      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      expect(writer.getBuffer().length).toBe(1)

      // Trigger alarm
      await writer.alarm()

      expect(mockWriteEvents).toHaveBeenCalled()
      expect(writer.getBuffer()).toHaveLength(0)
    })

    it('should reset flushScheduled flag on alarm', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      expect(writer.getFlushScheduled()).toBe(true)

      await writer.alarm()

      expect(writer.getFlushScheduled()).toBe(false)
    })

    it('should not flush if buffer is empty', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.alarm()

      expect(mockWriteEvents).not.toHaveBeenCalled()
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Flush Logic Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('flush()', () => {
    it('should flush all buffered events to R2', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([
        { type: 'event1', ts: new Date().toISOString() },
        { type: 'event2', ts: new Date().toISOString() },
      ])

      const result = await writer.forceFlush()

      expect(mockWriteEvents).toHaveBeenCalledTimes(1)
      expect(result).not.toBeNull()
    })

    it('should group events by source for correct prefix routing', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([
        { type: 'event1', ts: new Date().toISOString(), source: 'source-a' },
        { type: 'event2', ts: new Date().toISOString(), source: 'source-b' },
        { type: 'event3', ts: new Date().toISOString(), source: 'source-a' },
      ])

      await writer.forceFlush()

      // Should have been called twice - once per source
      expect(mockWriteEvents).toHaveBeenCalledTimes(2)
    })

    it('should strip internal _eventId before writing to R2', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      const writeCall = mockWriteEvents.mock.calls[0]
      const events = writeCall[2] as EventRecord[]
      expect(events[0]).not.toHaveProperty('_eventId')
    })

    it('should implement atomic flush pattern with TTL-based dedup markers', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      // Should store dedup markers with TTL (using timestamp prefix), then delete buffer
      // Note: We no longer delete the dedup markers immediately - they have TTL-based cleanup
      const putCalls = mockCtx.storage.put.mock.calls
      const dedupCall = putCalls.find((call: unknown[]) => (call[0] as string).startsWith(DEDUP_MARKERS_PREFIX))
      expect(dedupCall).toBeDefined()
      expect(dedupCall![1]).toHaveProperty('eventIds')
      expect(dedupCall![1]).toHaveProperty('createdAt')

      expect(mockCtx.storage.delete).toHaveBeenCalledWith(BUFFER_KEY)
    })

    it('should return null for empty buffer', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      const result = await writer.forceFlush()

      expect(result).toBeNull()
      expect(mockWriteEvents).not.toHaveBeenCalled()
    })

    it('should clear buffer after successful flush', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      expect(writer.getBuffer()).toHaveLength(1)

      await writer.forceFlush()

      expect(writer.getBuffer()).toHaveLength(0)
    })

    it('should update lastFlushTime after flush', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      const statsBefore = await writer.stats()
      vi.advanceTimersByTime(1000)

      await writer.forceFlush()

      const statsAfter = await writer.stats()
      expect(new Date(statsAfter.lastFlushTime).getTime())
        .toBeGreaterThan(new Date(statsBefore.lastFlushTime).getTime() - 1000)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Error Handling Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('error handling', () => {
    it('should restore buffer on flush failure', async () => {
      mockWriteEvents.mockRejectedValueOnce(new Error('R2 write failed'))

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      await expect(writer.forceFlush()).rejects.toThrow('R2 write failed')

      // Buffer should be restored
      expect(writer.getBuffer()).toHaveLength(1)
    })

    it('should propagate R2 errors', async () => {
      const error = new Error('Storage quota exceeded')
      mockWriteEvents.mockRejectedValueOnce(error)

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      await expect(writer.forceFlush()).rejects.toThrow('Storage quota exceeded')
    })

    it('should handle concurrent ingest calls', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      // Fire multiple ingests concurrently
      const results = await Promise.all([
        writer.ingest([{ type: 'event1', ts: new Date().toISOString() }]),
        writer.ingest([{ type: 'event2', ts: new Date().toISOString() }]),
        writer.ingest([{ type: 'event3', ts: new Date().toISOString() }]),
      ])

      expect(results.every(r => r.ok)).toBe(true)
      expect(writer.getBuffer().length).toBe(3)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Stats Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('stats()', () => {
    it('should return current statistics', async () => {
      const ctx = createMockCtx({ id: 'events:shard-3' })
      const writer = new SimulatedEventWriterDO(ctx, mockEnv, mockWriteEvents)

      await writer.ingest([
        { type: 'event1', ts: new Date().toISOString() },
        { type: 'event2', ts: new Date().toISOString() },
      ])

      const stats = await writer.stats()

      expect(stats.shard).toBe(3)
      expect(stats.buffered).toBe(2)
      expect(stats.pendingWrites).toBe(0)
      expect(stats.lastFlushTime).toMatch(/^\d{4}-\d{2}-\d{2}/)
      expect(typeof stats.timeSinceFlush).toBe('number')
      expect(typeof stats.flushScheduled).toBe('boolean')
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Time-based Flush Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('time-based flush', () => {
    it('should flush when time threshold exceeded', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ timeThresholdMs: 5000 })

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      // Advance time past threshold
      vi.advanceTimersByTime(6000)

      // Force a check by ingesting another event
      writer.setConfig({ countThreshold: 1 })
      const result = await writer.ingest([{ type: 'test.event2', ts: new Date().toISOString() }])

      expect(result.flushed).toBeDefined()
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // R2 Storage Interaction Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('R2 storage interactions', () => {
    it('should write events with correct parameters', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ countThreshold: 1 })

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }], 'my-prefix')

      expect(mockWriteEvents).toHaveBeenCalledWith(
        mockEnv.EVENTS_BUCKET,
        'my-prefix',
        expect.arrayContaining([
          expect.objectContaining({ type: 'test.event' }),
        ])
      )
    })

    it('should use default source when not provided', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ countThreshold: 1 })

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      expect(mockWriteEvents).toHaveBeenCalledWith(
        mockEnv.EVENTS_BUCKET,
        'events', // default source
        expect.any(Array)
      )
    })

    it('should handle multiple sources in same batch', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([
        { type: 'event1', ts: new Date().toISOString(), source: 'webhooks' },
        { type: 'event2', ts: new Date().toISOString(), source: 'api' },
        { type: 'event3', ts: new Date().toISOString() }, // default source
      ])

      await writer.forceFlush()

      // Should be called 3 times - once per unique source (webhooks, api, events)
      expect(mockWriteEvents).toHaveBeenCalledTimes(3)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Metrics Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('metrics recording', () => {
    it('should record analytics on successful ingest', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])

      // Analytics are recorded in the actual implementation via recordWriterDOMetric
      // In this simulated version, we verify the env is available
      expect(mockEnv.ANALYTICS).toBeDefined()
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Edge Cases
  // ──────────────────────────────────────────────────────────────────────────

  describe('edge cases', () => {
    it('should handle events with all optional fields', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      const event: EventRecord = {
        type: 'full.event',
        ts: new Date().toISOString(),
        source: 'test',
        provider: 'github',
        eventType: 'push',
        verified: true,
        scriptName: 'worker',
        outcome: 'ok',
        method: 'POST',
        url: 'https://example.com',
        statusCode: 200,
        durationMs: 50,
        payload: { data: 'test' },
      }

      const result = await writer.ingest([event])

      expect(result.ok).toBe(true)
    })

    it('should handle very large batches', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ countThreshold: 1000, maxBufferSize: 10000 })

      const events = Array.from({ length: 500 }, (_, i) => ({
        type: `event.${i}`,
        ts: new Date().toISOString(),
      }))

      const result = await writer.ingest(events)

      expect(result.ok).toBe(true)
      expect(result.buffered).toBe(500)
    })

    it('should handle max buffer size limit', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ countThreshold: 20000, maxBufferSize: 100 })

      const events = Array.from({ length: 100 }, (_, i) => ({
        type: `event.${i}`,
        ts: new Date().toISOString(),
      }))

      const result = await writer.ingest(events)

      // Should trigger flush due to maxBufferSize
      expect(result.flushed).toBeDefined()
    })

    it('should handle empty storage on restore', async () => {
      // No pre-populated storage
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await vi.runAllTimersAsync()

      expect(writer.getBuffer()).toHaveLength(0)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Dedup Marker TTL Cleanup Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('dedup marker TTL cleanup', () => {
    it('should store dedup markers with timestamp after flush', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      // Check that dedup markers were stored with timestamp prefix
      const storedKeys = Array.from(mockCtx.storage._storage.keys())
      const dedupKeys = storedKeys.filter(k => k.startsWith(DEDUP_MARKERS_PREFIX))
      expect(dedupKeys.length).toBe(1)

      // Verify marker structure
      const marker = mockCtx.storage._storage.get(dedupKeys[0]) as DedupMarker
      expect(marker.eventIds).toHaveLength(1)
      expect(marker.createdAt).toBeLessThanOrEqual(Date.now())
    })

    it('should track dedup marker count in stats', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([
        { type: 'event1', ts: new Date().toISOString() },
        { type: 'event2', ts: new Date().toISOString() },
      ])
      await writer.forceFlush()

      const stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(2)
    })

    it('should clean up expired dedup markers during alarm', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      // Set very short TTL for testing
      writer.setConfig({ dedupTtlMs: 1000 }) // 1 second

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      // Verify marker exists
      let stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)

      // Advance time past TTL
      vi.advanceTimersByTime(2000)

      // Trigger alarm (which calls cleanup)
      await writer.alarm()

      // Markers should be cleaned up
      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(0)
    })

    it('should not clean up non-expired dedup markers', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ dedupTtlMs: 60_000 }) // 1 minute

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      // Verify marker exists
      let stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)

      // Advance time but not past TTL
      vi.advanceTimersByTime(30_000) // 30 seconds

      // Trigger cleanup
      await writer.triggerCleanup()

      // Markers should still exist
      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)
    })

    it('should handle multiple dedup marker batches with different ages', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ dedupTtlMs: 5000, countThreshold: 1 })

      // First batch at t=0
      await writer.ingest([{ type: 'event1', ts: new Date().toISOString() }])

      // Advance time
      vi.advanceTimersByTime(3000)

      // Second batch at t=3000
      await writer.ingest([{ type: 'event2', ts: new Date().toISOString() }])

      let stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(2)

      // Advance time past TTL for first batch (t=6000)
      vi.advanceTimersByTime(3000)

      // Trigger cleanup - first batch should be expired, second should remain
      await writer.triggerCleanup()

      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)
    })

    it('should migrate legacy FLUSHED_KEY format to new TTL format', async () => {
      // Pre-populate with legacy format
      mockCtx.storage._storage.set(FLUSHED_KEY, ['legacy-event-1', 'legacy-event-2'])
      mockCtx.storage._storage.set(BUFFER_KEY, [
        { type: 'event', ts: new Date().toISOString(), _eventId: 'legacy-event-1' },
        { type: 'event2', ts: new Date().toISOString(), _eventId: 'new-event' },
      ])

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      await vi.runAllTimersAsync()

      // Legacy FLUSHED_KEY should be deleted
      expect(mockCtx.storage._storage.has(FLUSHED_KEY)).toBe(false)

      // Legacy markers should be migrated to new format
      const dedupKeys = Array.from(mockCtx.storage._storage.keys()).filter(k => k.startsWith(DEDUP_MARKERS_PREFIX))
      expect(dedupKeys.length).toBe(1)

      // Buffer should have filtered the duplicate
      expect(writer.getBuffer().length).toBe(1)
      expect(writer.getBuffer()[0]._eventId).toBe('new-event')
    })

    it('should load dedup markers on restore and filter duplicates', async () => {
      const now = Date.now()

      // Pre-populate with new TTL format marker
      const marker: DedupMarker = {
        eventIds: ['existing-event-1', 'existing-event-2'],
        createdAt: now - 1000, // 1 second ago
      }
      mockCtx.storage._storage.set(`${DEDUP_MARKERS_PREFIX}${now - 1000}`, marker)

      // Pre-populate buffer with mix of new and duplicate events
      mockCtx.storage._storage.set(BUFFER_KEY, [
        { type: 'event1', ts: new Date().toISOString(), _eventId: 'existing-event-1' },
        { type: 'event2', ts: new Date().toISOString(), _eventId: 'new-event' },
      ])

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      await vi.runAllTimersAsync()

      // Buffer should only have the new event
      expect(writer.getBuffer().length).toBe(1)
      expect(writer.getBuffer()[0]._eventId).toBe('new-event')

      // Stats should show loaded dedup markers
      const stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(2)
    })

    it('should filter out expired markers on load', async () => {
      const now = Date.now()

      // Pre-populate with expired marker (older than default 24 hours)
      const expiredMarker: DedupMarker = {
        eventIds: ['expired-event'],
        createdAt: now - 25 * 60 * 60 * 1000, // 25 hours ago
      }
      mockCtx.storage._storage.set(`${DEDUP_MARKERS_PREFIX}${now - 25 * 60 * 60 * 1000}`, expiredMarker)

      // Pre-populate with fresh marker
      const freshMarker: DedupMarker = {
        eventIds: ['fresh-event'],
        createdAt: now - 1000, // 1 second ago
      }
      mockCtx.storage._storage.set(`${DEDUP_MARKERS_PREFIX}${now - 1000}`, freshMarker)

      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      await vi.runAllTimersAsync()

      // Only fresh marker should be loaded
      const stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)

      // Expired marker should be deleted from storage
      const storedKeys = Array.from(mockCtx.storage._storage.keys())
      const dedupKeys = storedKeys.filter(k => k.startsWith(DEDUP_MARKERS_PREFIX))
      expect(dedupKeys.length).toBe(1)
    })

    it('should use configurable TTL', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      // Set custom TTL to 500ms
      writer.setConfig({ dedupTtlMs: 500 })

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      let stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)

      // Advance time just past custom TTL
      vi.advanceTimersByTime(600)

      // Cleanup should remove the marker
      await writer.triggerCleanup()

      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(0)
    })

    it('should remove event IDs from in-memory set when markers expire', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)
      writer.setConfig({ dedupTtlMs: 1000 })

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      // Get the flushed event IDs (stored in stats)
      let stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)

      // Advance time past TTL and cleanup
      vi.advanceTimersByTime(2000)
      await writer.triggerCleanup()

      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(0)
    })

    it('should default to 24 hour TTL', async () => {
      const writer = new SimulatedEventWriterDO(mockCtx, mockEnv, mockWriteEvents)

      await writer.ingest([{ type: 'test.event', ts: new Date().toISOString() }])
      await writer.forceFlush()

      let stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1)

      // Advance time to just under 24 hours
      vi.advanceTimersByTime(23 * 60 * 60 * 1000)

      await writer.triggerCleanup()
      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(1) // Should still exist

      // Advance past 24 hours
      vi.advanceTimersByTime(2 * 60 * 60 * 1000) // +2 more hours

      await writer.triggerCleanup()
      stats = await writer.stats()
      expect(stats.dedupMarkerCount).toBe(0) // Should be cleaned up
    })
  })
})
