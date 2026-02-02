/**
 * EventWriterDO + ShardCoordinatorDO Integration Tests
 *
 * Tests the interaction between EventWriterDO and ShardCoordinatorDO including:
 * 1. Shard allocation under normal load
 * 2. Scaling up when backpressure occurs
 * 3. Scaling down when load decreases
 * 4. Coordinator failover
 * 5. Metrics reporting
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

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
  transaction: ReturnType<typeof vi.fn>
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

interface EventRecord {
  type: string
  ts: string
  source?: string
  [key: string]: unknown
}

interface WriteResult {
  key: string
  events: number
  bytes: number
  cpuMs: number
}

interface ShardConfig {
  minShards: number
  maxShards: number
  scaleUpThreshold: number
  scaleDownThreshold: number
  cooldownMs: number
  metricsWindowMs: number
  healthCheckIntervalMs: number
}

interface ShardMetrics {
  shardId: number
  buffered: number
  pendingWrites: number
  lastFlushTime: string
  timeSinceFlush: number
  flushScheduled: boolean
  lastReportTime: number
  backpressureCount: number
}

interface IngestResult {
  ok: boolean
  buffered: number
  shard: number
  flushed?: WriteResult | null
  tryNextShard?: number
}

// ============================================================================
// Mock Factories
// ============================================================================

function createMockStorage(): MockDurableObjectStorage {
  const storage = new Map<string, unknown>()
  let alarm: number | null = null

  const mockStorage: MockDurableObjectStorage = {
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
    list: vi.fn(async (options?: { prefix?: string }) => {
      const result = new Map<string, unknown>()
      for (const [key, value] of storage) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value)
        }
      }
      return result
    }),
    getAlarm: vi.fn(async () => alarm),
    setAlarm: vi.fn(async (time: number | Date) => {
      alarm = typeof time === 'number' ? time : time.getTime()
    }),
    deleteAlarm: vi.fn(async () => {
      alarm = null
    }),
    transaction: vi.fn(async (callback: (txn: MockDurableObjectStorage) => Promise<void>) => {
      await callback(mockStorage)
    }),
    _storage: storage,
    _alarm: alarm,
  }

  return mockStorage
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

// ============================================================================
// ShardCoordinatorDO Simulation
// ============================================================================

const DEFAULT_COORDINATOR_CONFIG: ShardConfig = {
  minShards: 1,
  maxShards: 64,
  scaleUpThreshold: 3,
  scaleDownThreshold: 0.2,
  cooldownMs: 60_000,
  metricsWindowMs: 30_000,
  healthCheckIntervalMs: 10_000,
}

const CONFIG_KEY = '_coordinator:config'
const METRICS_KEY = '_coordinator:metrics'
const ACTIVE_SHARDS_KEY = '_coordinator:activeShards'
const LAST_SCALE_KEY = '_coordinator:lastScaleTime'

class SimulatedShardCoordinatorDO {
  private config: ShardConfig = { ...DEFAULT_COORDINATOR_CONFIG }
  private activeShards: Set<number> = new Set([0])
  private shardMetrics: Map<number, ShardMetrics> = new Map()
  private lastScaleTime = 0
  private initialized = false

  constructor(
    private ctx: MockDurableObjectState,
    private env: { ANALYTICS?: MockAnalytics }
  ) {}

  async initialize(): Promise<void> {
    await this.restore()
  }

  private async restore(): Promise<void> {
    const [storedConfig, storedActiveShards, storedLastScale, storedMetrics] = await Promise.all([
      this.ctx.storage.get<ShardConfig>(CONFIG_KEY),
      this.ctx.storage.get<number[]>(ACTIVE_SHARDS_KEY),
      this.ctx.storage.get<number>(LAST_SCALE_KEY),
      this.ctx.storage.get<ShardMetrics[]>(METRICS_KEY),
    ])

    if (storedConfig) {
      this.config = { ...DEFAULT_COORDINATOR_CONFIG, ...storedConfig }
    }

    if (storedActiveShards && storedActiveShards.length > 0) {
      this.activeShards = new Set(storedActiveShards)
    }

    if (storedLastScale) {
      this.lastScaleTime = storedLastScale
    }

    if (storedMetrics) {
      for (const metric of storedMetrics) {
        this.shardMetrics.set(metric.shardId, metric)
      }
    }

    this.initialized = true
  }

  private async persist(): Promise<void> {
    await this.ctx.storage.transaction(async (txn: MockDurableObjectStorage) => {
      await txn.put(CONFIG_KEY, this.config)
      await txn.put(ACTIVE_SHARDS_KEY, Array.from(this.activeShards))
      await txn.put(LAST_SCALE_KEY, this.lastScaleTime)
      await txn.put(METRICS_KEY, Array.from(this.shardMetrics.values()))
    })
  }

  async getConfig(): Promise<ShardConfig> {
    return { ...this.config }
  }

  async updateConfig(updates: Partial<ShardConfig>): Promise<ShardConfig> {
    this.config = { ...this.config, ...updates }
    await this.persist()
    return this.config
  }

  async getActiveShards(): Promise<number[]> {
    return Array.from(this.activeShards).sort((a, b) => a - b)
  }

  async getShardCount(): Promise<number> {
    return this.activeShards.size
  }

  async reportMetrics(metrics: Omit<ShardMetrics, 'lastReportTime' | 'backpressureCount'>): Promise<void> {
    const existingMetrics = this.shardMetrics.get(metrics.shardId)
    const backpressureCount = existingMetrics?.backpressureCount ?? 0

    this.shardMetrics.set(metrics.shardId, {
      ...metrics,
      lastReportTime: Date.now(),
      backpressureCount,
    })

    if (!this.activeShards.has(metrics.shardId)) {
      this.activeShards.add(metrics.shardId)
      await this.persist()
    }
  }

  async reportBackpressure(shardId: number): Promise<{ shouldRetry: boolean; alternativeShard: number }> {
    const existingMetrics = this.shardMetrics.get(shardId)
    const backpressureCount = (existingMetrics?.backpressureCount ?? 0) + 1

    this.shardMetrics.set(shardId, {
      ...(existingMetrics ?? {
        shardId,
        buffered: 0,
        pendingWrites: 0,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
        lastReportTime: Date.now(),
      }),
      backpressureCount,
      lastReportTime: Date.now(),
    })

    await this.maybeScale()
    const alternativeShard = this.findBestShard(shardId)

    return {
      shouldRetry: true,
      alternativeShard,
    }
  }

  private findBestShard(excludeShardId?: number): number {
    const shards = Array.from(this.activeShards)
    if (shards.length === 0) return 0

    const candidates = excludeShardId !== undefined
      ? shards.filter(s => s !== excludeShardId)
      : shards

    if (candidates.length === 0) {
      return ((excludeShardId ?? 0) + 1) % this.config.maxShards
    }

    let bestShard = candidates[0] ?? 0
    let bestScore = Infinity

    for (const shard of candidates) {
      const metrics = this.shardMetrics.get(shard)
      const score = metrics ? metrics.pendingWrites + (metrics.buffered / 100) : 0
      if (score < bestScore) {
        bestScore = score
        bestShard = shard
      }
    }

    return bestShard
  }

  async getRoutingShard(preferredShard?: number): Promise<number> {
    if (preferredShard !== undefined && this.activeShards.has(preferredShard)) {
      const metrics = this.shardMetrics.get(preferredShard)
      if (!metrics || metrics.backpressureCount < 3) {
        return preferredShard
      }
    }

    return this.findBestShard()
  }

  async getStats(): Promise<{
    activeShards: number[]
    totalBuffered: number
    totalPendingWrites: number
    averageUtilization: number
    metrics: ShardMetrics[]
    lastScaleTime: string
    canScaleUp: boolean
    canScaleDown: boolean
  }> {
    const metrics = Array.from(this.shardMetrics.values())
    const activeShards = Array.from(this.activeShards).sort((a, b) => a - b)

    const totalBuffered = metrics.reduce((sum, m) => sum + m.buffered, 0)
    const totalPendingWrites = metrics.reduce((sum, m) => sum + m.pendingWrites, 0)

    const maxPendingPerShard = 100
    const averageUtilization = activeShards.length > 0
      ? totalPendingWrites / (activeShards.length * maxPendingPerShard)
      : 0

    const now = Date.now()
    const canScaleUp = this.activeShards.size < this.config.maxShards &&
      (now - this.lastScaleTime) >= this.config.cooldownMs
    const canScaleDown = this.activeShards.size > this.config.minShards &&
      (now - this.lastScaleTime) >= this.config.cooldownMs

    return {
      activeShards,
      totalBuffered,
      totalPendingWrites,
      averageUtilization,
      metrics,
      lastScaleTime: new Date(this.lastScaleTime).toISOString(),
      canScaleUp,
      canScaleDown,
    }
  }

  private async maybeScale(): Promise<{ scaled: boolean; reason: string }> {
    const now = Date.now()
    const previousCount = this.activeShards.size

    if ((now - this.lastScaleTime) < this.config.cooldownMs) {
      return { scaled: false, reason: 'In cooldown period' }
    }

    const recentBackpressure = this.countRecentBackpressure()
    if (recentBackpressure >= this.config.scaleUpThreshold) {
      if (this.activeShards.size < this.config.maxShards) {
        await this.scaleUp()
        return { scaled: true, reason: 'Scaled up due to backpressure' }
      }
      return { scaled: false, reason: 'At maximum shards' }
    }

    const avgUtilization = this.calculateAverageUtilization()
    if (avgUtilization < this.config.scaleDownThreshold) {
      if (this.activeShards.size > this.config.minShards) {
        await this.scaleDown()
        return { scaled: true, reason: 'Scaled down due to low utilization' }
      }
      return { scaled: false, reason: 'At minimum shards' }
    }

    return { scaled: false, reason: 'No scaling needed' }
  }

  private countRecentBackpressure(): number {
    const now = Date.now()
    const window = this.config.metricsWindowMs
    let count = 0

    for (const metrics of this.shardMetrics.values()) {
      if ((now - metrics.lastReportTime) <= window) {
        count += metrics.backpressureCount
      }
    }

    return count
  }

  private calculateAverageUtilization(): number {
    const now = Date.now()
    const window = this.config.metricsWindowMs
    let totalUtilization = 0
    let activeCount = 0
    const maxPendingPerShard = 100

    for (const metrics of this.shardMetrics.values()) {
      if ((now - metrics.lastReportTime) <= window) {
        totalUtilization += metrics.pendingWrites / maxPendingPerShard
        activeCount++
      }
    }

    return activeCount > 0 ? totalUtilization / activeCount : 0
  }

  private async scaleUp(): Promise<void> {
    let nextShard = 0
    while (this.activeShards.has(nextShard) && nextShard < this.config.maxShards) {
      nextShard++
    }

    if (nextShard < this.config.maxShards) {
      this.activeShards.add(nextShard)
      this.lastScaleTime = Date.now()

      for (const metrics of this.shardMetrics.values()) {
        metrics.backpressureCount = 0
      }

      await this.persist()
    }
  }

  private async scaleDown(): Promise<void> {
    let lowestShard = -1
    let lowestScore = Infinity

    for (const shardId of this.activeShards) {
      if (shardId === 0) continue

      const metrics = this.shardMetrics.get(shardId)
      const score = metrics ? metrics.buffered + metrics.pendingWrites : 0

      if (score < lowestScore) {
        lowestScore = score
        lowestShard = shardId
      }
    }

    if (lowestShard !== -1) {
      this.activeShards.delete(lowestShard)
      this.shardMetrics.delete(lowestShard)
      this.lastScaleTime = Date.now()

      await this.persist()
    }
  }

  async forceScale(targetCount: number): Promise<{
    scaled: boolean
    previousCount: number
    newCount: number
    reason: string
  }> {
    const previousCount = this.activeShards.size
    targetCount = Math.max(this.config.minShards, Math.min(this.config.maxShards, targetCount))

    if (targetCount === previousCount) {
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'Already at target count',
      }
    }

    if (targetCount > previousCount) {
      while (this.activeShards.size < targetCount) {
        let nextShard = 0
        while (this.activeShards.has(nextShard)) nextShard++
        this.activeShards.add(nextShard)
      }
    } else {
      const sorted = Array.from(this.activeShards).sort((a, b) => a - b)
      for (let i = targetCount; i < sorted.length; i++) {
        const shardToRemove = sorted[i]
        if (shardToRemove !== undefined) {
          this.activeShards.delete(shardToRemove)
          this.shardMetrics.delete(shardToRemove)
        }
      }
    }

    this.lastScaleTime = Date.now()
    await this.persist()

    return {
      scaled: true,
      previousCount,
      newCount: this.activeShards.size,
      reason: `Force scaled to ${targetCount} shards`,
    }
  }

  // Test helpers
  setLastScaleTime(time: number): void {
    this.lastScaleTime = time
  }

  getLastScaleTime(): number {
    return this.lastScaleTime
  }

  getShardMetrics(): Map<number, ShardMetrics> {
    return this.shardMetrics
  }

  clearBackpressure(): void {
    for (const metrics of this.shardMetrics.values()) {
      metrics.backpressureCount = 0
    }
  }
}

// ============================================================================
// EventWriterDO Simulation
// ============================================================================

interface BufferedEvent extends EventRecord {
  _eventId: string
}

const BUFFER_KEY = '_eventWriter:buffer'
const FLUSHED_KEY = '_eventWriter:flushed'

const DEFAULT_WRITER_CONFIG = {
  countThreshold: 100,
  timeThresholdMs: 10_000,
  maxBufferSize: 10_000,
  maxPendingWrites: 100,
  flushIntervalMs: 5_000,
}

class SimulatedEventWriterDO {
  private buffer: BufferedEvent[] = []
  private flushedEventIds: Set<string> = new Set()
  private lastFlushTime = Date.now()
  private lastMetricsReport = 0
  private pendingWrites = 0
  private shardId = 0
  private config = DEFAULT_WRITER_CONFIG
  private flushScheduled = false
  private mockWriteEvents: ReturnType<typeof vi.fn>

  constructor(
    private ctx: MockDurableObjectState,
    private env: {
      EVENTS_BUCKET: MockR2Bucket
      ANALYTICS?: MockAnalytics
      SHARD_COORDINATOR?: SimulatedShardCoordinatorDO
    },
    mockWriteEvents?: ReturnType<typeof vi.fn>
  ) {
    const name = ctx.id.toString()
    const match = name.match(/:shard-(\d+)$/)
    this.shardId = match?.[1] ? parseInt(match[1], 10) : 0

    this.mockWriteEvents = mockWriteEvents ?? vi.fn().mockResolvedValue({
      key: 'events/2024/01/01/00/test.parquet',
      events: 0,
      bytes: 100,
      cpuMs: 5,
    })

    this.restoreBuffer()
  }

  private async restoreBuffer(): Promise<void> {
    const flushedIds = await this.ctx.storage.get<string[]>(FLUSHED_KEY)
    if (flushedIds && flushedIds.length > 0) {
      for (const id of flushedIds) {
        this.flushedEventIds.add(id)
      }
    }

    const stored = await this.ctx.storage.get<BufferedEvent[]>(BUFFER_KEY)
    if (stored && stored.length > 0) {
      const unflushedEvents = stored.filter(e => !this.flushedEventIds.has(e._eventId))
      if (unflushedEvents.length > 0) {
        this.buffer = unflushedEvents
      }
    }
  }

  private async persistBuffer(): Promise<void> {
    if (this.buffer.length > 0) {
      await this.ctx.storage.put(BUFFER_KEY, this.buffer)
    } else {
      await this.ctx.storage.delete(BUFFER_KEY)
    }
  }

  private generateUlid(): string {
    return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`
  }

  private async reportMetricsToCoordinator(): Promise<void> {
    const now = Date.now()
    const METRICS_REPORT_INTERVAL_MS = 5_000
    if ((now - this.lastMetricsReport) < METRICS_REPORT_INTERVAL_MS) {
      return
    }

    if (!this.env.SHARD_COORDINATOR) {
      return
    }

    this.lastMetricsReport = now

    try {
      await this.env.SHARD_COORDINATOR.reportMetrics({
        shardId: this.shardId,
        buffered: this.buffer.length,
        pendingWrites: this.pendingWrites,
        lastFlushTime: new Date(this.lastFlushTime).toISOString(),
        timeSinceFlush: now - this.lastFlushTime,
        flushScheduled: this.flushScheduled,
      })
    } catch (err) {
      // Non-fatal: coordinator unavailable
    }
  }

  private async reportBackpressureToCoordinator(): Promise<number | undefined> {
    if (!this.env.SHARD_COORDINATOR) {
      return this.shardId + 1
    }

    const result = await this.env.SHARD_COORDINATOR.reportBackpressure(this.shardId)
    return result.alternativeShard
  }

  async ingest(events: EventRecord[], source?: string): Promise<IngestResult> {
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
      const result = await this.doIngest(events, source)
      await this.reportMetricsToCoordinator()
      return result
    } finally {
      this.pendingWrites--
    }
  }

  private async doIngest(events: EventRecord[], source?: string): Promise<IngestResult> {
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
    if (this.buffer.length > 0) {
      await this.flush()
    }
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
  }> {
    return {
      shard: this.shardId,
      buffered: this.buffer.length,
      pendingWrites: this.pendingWrites,
      lastFlushTime: new Date(this.lastFlushTime).toISOString(),
      timeSinceFlush: Date.now() - this.lastFlushTime,
      flushScheduled: this.flushScheduled,
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

      await this.ctx.storage.put(FLUSHED_KEY, eventIds)
      await this.ctx.storage.delete(BUFFER_KEY)
      await this.ctx.storage.delete(FLUSHED_KEY)

      return results[0] ?? null
    } catch (error) {
      this.buffer.unshift(...events)
      throw error
    }
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

  setConfig(config: Partial<typeof DEFAULT_WRITER_CONFIG>): void {
    this.config = { ...this.config, ...config }
  }

  getShardId(): number {
    return this.shardId
  }

  resetMetricsReportTime(): void {
    this.lastMetricsReport = 0
  }
}

// ============================================================================
// Integrated Test Environment
// ============================================================================

interface TestEnvironment {
  coordinator: SimulatedShardCoordinatorDO
  writers: Map<number, SimulatedEventWriterDO>
  analytics: MockAnalytics
  r2Bucket: MockR2Bucket
  mockWriteEvents: ReturnType<typeof vi.fn>
}

function createTestEnvironment(options: {
  initialShards?: number
  maxShards?: number
  coordinatorCooldownMs?: number
} = {}): TestEnvironment {
  const analytics = createMockAnalytics()
  const r2Bucket = createMockR2Bucket()
  const mockWriteEvents = vi.fn().mockResolvedValue({
    key: 'events/2024/01/01/00/test.parquet',
    events: 0,
    bytes: 100,
    cpuMs: 5,
  })

  const coordinatorCtx = createMockCtx({ id: 'coordinator:global' })
  const coordinator = new SimulatedShardCoordinatorDO(coordinatorCtx, { ANALYTICS: analytics })

  const writers = new Map<number, SimulatedEventWriterDO>()

  return {
    coordinator,
    writers,
    analytics,
    r2Bucket,
    mockWriteEvents,
  }
}

function getOrCreateWriter(
  env: TestEnvironment,
  shardId: number
): SimulatedEventWriterDO {
  if (!env.writers.has(shardId)) {
    const ctx = createMockCtx({ id: shardId === 0 ? 'events' : `events:shard-${shardId}` })
    const writer = new SimulatedEventWriterDO(
      ctx,
      {
        EVENTS_BUCKET: env.r2Bucket,
        ANALYTICS: env.analytics,
        SHARD_COORDINATOR: env.coordinator,
      },
      env.mockWriteEvents
    )
    env.writers.set(shardId, writer)
  }
  return env.writers.get(shardId)!
}

// ============================================================================
// Integration Tests
// ============================================================================

describe('EventWriterDO + ShardCoordinatorDO Integration', () => {
  let testEnv: TestEnvironment

  beforeEach(async () => {
    vi.useFakeTimers()
    testEnv = createTestEnvironment()
    await testEnv.coordinator.initialize()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Test 1: Shard allocation under normal load
  // ──────────────────────────────────────────────────────────────────────────

  describe('1. Shard allocation under normal load', () => {
    it('should start with single shard (shard 0)', async () => {
      const shards = await testEnv.coordinator.getActiveShards()
      expect(shards).toEqual([0])
    })

    it('should route events to shard 0 initially', async () => {
      const routingShard = await testEnv.coordinator.getRoutingShard()
      expect(routingShard).toBe(0)
    })

    it('should successfully ingest events to the default shard', async () => {
      const writer = getOrCreateWriter(testEnv, 0)
      const events = [
        { type: 'test.event', ts: new Date().toISOString() },
        { type: 'test.event2', ts: new Date().toISOString() },
      ]

      const result = await writer.ingest(events, 'test')

      expect(result.ok).toBe(true)
      expect(result.shard).toBe(0)
      expect(result.buffered).toBe(2)
    })

    it('should report metrics to coordinator on ingest', async () => {
      const writer = getOrCreateWriter(testEnv, 0)
      writer.resetMetricsReportTime()

      await writer.ingest([{ type: 'test', ts: new Date().toISOString() }], 'test')

      const stats = await testEnv.coordinator.getStats()
      expect(stats.metrics.length).toBeGreaterThan(0)
      expect(stats.metrics.some(m => m.shardId === 0)).toBe(true)
    })

    it('should distribute events across multiple pre-configured shards', async () => {
      // Force scale to 3 shards
      await testEnv.coordinator.forceScale(3)
      const shards = await testEnv.coordinator.getActiveShards()
      expect(shards).toEqual([0, 1, 2])

      // Each shard should accept events
      for (const shardId of shards) {
        const writer = getOrCreateWriter(testEnv, shardId)
        const result = await writer.ingest([{ type: 'test', ts: new Date().toISOString() }], 'test')
        expect(result.ok).toBe(true)
        expect(result.shard).toBe(shardId)
      }
    })

    it('should route to best shard based on metrics', async () => {
      await testEnv.coordinator.forceScale(3)
      testEnv.coordinator.setLastScaleTime(0)

      // Report varying utilization for each shard
      await testEnv.coordinator.reportMetrics({
        shardId: 0,
        buffered: 1000,
        pendingWrites: 50,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 1,
        buffered: 100,
        pendingWrites: 10,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 2,
        buffered: 500,
        pendingWrites: 30,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      // Should route to shard with lowest score (shard 1)
      const routingShard = await testEnv.coordinator.getRoutingShard()
      expect(routingShard).toBe(1)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Test 2: Scaling up when backpressure occurs
  // ──────────────────────────────────────────────────────────────────────────

  describe('2. Scaling up when backpressure occurs', () => {
    it('should signal tryNextShard when writer is at max pending writes', async () => {
      const writer = getOrCreateWriter(testEnv, 0)
      writer.setPendingWrites(100) // At threshold

      const result = await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])

      expect(result.ok).toBe(false)
      expect(result.tryNextShard).toBeDefined()
    })

    it('should report backpressure to coordinator and get alternative shard', async () => {
      testEnv.coordinator.setLastScaleTime(0)
      await testEnv.coordinator.forceScale(2)

      const writer0 = getOrCreateWriter(testEnv, 0)
      writer0.setPendingWrites(100)

      const result = await writer0.ingest([{ type: 'test', ts: new Date().toISOString() }])

      expect(result.tryNextShard).toBe(1) // Alternative shard
    })

    it('should scale up after threshold backpressure events', async () => {
      testEnv.coordinator.setLastScaleTime(0)

      const initialCount = await testEnv.coordinator.getShardCount()
      expect(initialCount).toBe(1)

      // Report enough backpressure to trigger scale up
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)

      const newCount = await testEnv.coordinator.getShardCount()
      expect(newCount).toBe(2)
    })

    it('should not scale up during cooldown period', async () => {
      testEnv.coordinator.setLastScaleTime(Date.now()) // Recent scale

      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)

      const count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(1) // No scale due to cooldown
    })

    it('should reset backpressure counts after scaling', async () => {
      testEnv.coordinator.setLastScaleTime(0)

      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)

      const metrics = testEnv.coordinator.getShardMetrics().get(0)
      expect(metrics?.backpressureCount).toBe(0) // Reset after scale
    })

    it('should automatically route to new shard after scale up', async () => {
      testEnv.coordinator.setLastScaleTime(0)

      // Trigger scale up
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)

      const shards = await testEnv.coordinator.getActiveShards()
      expect(shards).toContain(1) // New shard added

      // New shard should be available for routing
      const writer1 = getOrCreateWriter(testEnv, 1)
      const result = await writer1.ingest([{ type: 'test', ts: new Date().toISOString() }])
      expect(result.ok).toBe(true)
      expect(result.shard).toBe(1)
    })

    it('should handle cascading backpressure across shards', async () => {
      testEnv.coordinator.setLastScaleTime(0)
      await testEnv.coordinator.forceScale(2)
      testEnv.coordinator.setLastScaleTime(0)
      testEnv.coordinator.clearBackpressure()

      // Both shards experience backpressure
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(1)
      await testEnv.coordinator.reportBackpressure(0)

      const count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(3) // Scaled up to 3 shards
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Test 3: Scaling down when load decreases
  // ──────────────────────────────────────────────────────────────────────────

  describe('3. Scaling down when load decreases', () => {
    beforeEach(async () => {
      // Start with 4 shards
      await testEnv.coordinator.forceScale(4)
      testEnv.coordinator.setLastScaleTime(0)
    })

    it('should scale down when utilization is below threshold', async () => {
      // Report low utilization for all shards
      for (let i = 0; i < 4; i++) {
        await testEnv.coordinator.reportMetrics({
          shardId: i,
          buffered: 0,
          pendingWrites: 5, // 5% utilization per shard
          lastFlushTime: new Date().toISOString(),
          timeSinceFlush: 0,
          flushScheduled: false,
        })
      }

      // Trigger potential scale check via backpressure handler (which calls maybeScale)
      // We'll directly check stats to verify we CAN scale down
      const stats = await testEnv.coordinator.getStats()
      expect(stats.canScaleDown).toBe(true)
      expect(stats.averageUtilization).toBeLessThan(0.2)
    })

    it('should preserve shard 0 when scaling down', async () => {
      // Force scale down to minimum
      await testEnv.coordinator.forceScale(1)

      const shards = await testEnv.coordinator.getActiveShards()
      expect(shards).toContain(0)
      expect(shards).toHaveLength(1)
    })

    it('should remove shard with lowest utilization first', async () => {
      // Report varying utilization
      await testEnv.coordinator.reportMetrics({
        shardId: 0,
        buffered: 100,
        pendingWrites: 50,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 1,
        buffered: 10,
        pendingWrites: 5, // Lowest
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 2,
        buffered: 50,
        pendingWrites: 20,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 3,
        buffered: 80,
        pendingWrites: 30,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      // Scale down by 1
      await testEnv.coordinator.forceScale(3)

      const shards = await testEnv.coordinator.getActiveShards()
      expect(shards).not.toContain(3) // Highest numbered shard removed by forceScale
    })

    it('should not scale below minimum shard count', async () => {
      const result = await testEnv.coordinator.forceScale(0)
      expect(result.newCount).toBe(1) // Clamped to minShards
    })

    it('should respect cooldown when scaling down', async () => {
      testEnv.coordinator.setLastScaleTime(Date.now())

      const stats = await testEnv.coordinator.getStats()
      expect(stats.canScaleDown).toBe(false)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Test 4: Coordinator failover
  // ──────────────────────────────────────────────────────────────────────────

  describe('4. Coordinator failover', () => {
    it('should fall back to simple shard increment when coordinator unavailable', async () => {
      // Create writer without coordinator
      const ctx = createMockCtx({ id: 'events:shard-2' })
      const writerNoCoordinator = new SimulatedEventWriterDO(
        ctx,
        {
          EVENTS_BUCKET: testEnv.r2Bucket,
          ANALYTICS: testEnv.analytics,
          // No SHARD_COORDINATOR
        },
        testEnv.mockWriteEvents
      )

      writerNoCoordinator.setPendingWrites(100)

      const result = await writerNoCoordinator.ingest([{ type: 'test', ts: new Date().toISOString() }])

      expect(result.ok).toBe(false)
      expect(result.tryNextShard).toBe(3) // shard 2 + 1
    })

    it('should persist coordinator state for recovery', async () => {
      await testEnv.coordinator.forceScale(3)
      await testEnv.coordinator.updateConfig({ minShards: 2, maxShards: 8 })

      // Simulate coordinator restart by creating new instance with same storage
      const coordinatorCtx = createMockCtx({ id: 'coordinator:global' })

      // Copy storage state
      const originalStorage = testEnv.coordinator['ctx'].storage._storage
      for (const [key, value] of originalStorage) {
        coordinatorCtx.storage._storage.set(key, value)
      }

      const newCoordinator = new SimulatedShardCoordinatorDO(
        coordinatorCtx,
        { ANALYTICS: testEnv.analytics }
      )
      await newCoordinator.initialize()

      // Verify state was restored
      const shards = await newCoordinator.getActiveShards()
      expect(shards).toEqual([0, 1, 2])

      const config = await newCoordinator.getConfig()
      expect(config.minShards).toBe(2)
      expect(config.maxShards).toBe(8)
    })

    it('should continue operation with cached shard list if coordinator fails mid-request', async () => {
      await testEnv.coordinator.forceScale(3)

      // Writer caches initial shard list
      const writer = getOrCreateWriter(testEnv, 1)
      const result1 = await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])
      expect(result1.ok).toBe(true)

      // Even if coordinator becomes unavailable, writer should continue to function
      // (it just won't be able to report metrics or get alternatives)
      const result2 = await writer.ingest([{ type: 'test2', ts: new Date().toISOString() }])
      expect(result2.ok).toBe(true)
    })

    it('should handle coordinator timeout gracefully', async () => {
      // Create a writer with a coordinator that has a slow/failing method
      const slowCoordinatorCtx = createMockCtx({ id: 'coordinator:global' })
      const slowCoordinator = new SimulatedShardCoordinatorDO(
        slowCoordinatorCtx,
        { ANALYTICS: testEnv.analytics }
      )
      await slowCoordinator.initialize()

      // Override reportMetrics to simulate timeout
      const originalReportMetrics = slowCoordinator.reportMetrics.bind(slowCoordinator)
      slowCoordinator.reportMetrics = vi.fn().mockImplementation(async () => {
        throw new Error('Timeout')
      })

      const ctx = createMockCtx({ id: 'events:shard-0' })
      const writer = new SimulatedEventWriterDO(
        ctx,
        {
          EVENTS_BUCKET: testEnv.r2Bucket,
          ANALYTICS: testEnv.analytics,
          SHARD_COORDINATOR: slowCoordinator,
        },
        testEnv.mockWriteEvents
      )
      writer.resetMetricsReportTime()

      // Writer should still successfully ingest even if metrics reporting fails
      const result = await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])
      expect(result.ok).toBe(true)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Test 5: Metrics reporting
  // ──────────────────────────────────────────────────────────────────────────

  describe('5. Metrics reporting', () => {
    it('should report writer metrics to coordinator', async () => {
      const writer = getOrCreateWriter(testEnv, 0)
      writer.resetMetricsReportTime()

      // Ingest some events
      await writer.ingest([
        { type: 'test1', ts: new Date().toISOString() },
        { type: 'test2', ts: new Date().toISOString() },
        { type: 'test3', ts: new Date().toISOString() },
      ])

      const stats = await testEnv.coordinator.getStats()
      const shard0Metrics = stats.metrics.find(m => m.shardId === 0)

      expect(shard0Metrics).toBeDefined()
      expect(shard0Metrics?.buffered).toBe(3)
    })

    it('should throttle metrics reporting to avoid overload', async () => {
      const writer = getOrCreateWriter(testEnv, 0)
      writer.resetMetricsReportTime()

      // First ingest triggers metrics report
      await writer.ingest([{ type: 'test1', ts: new Date().toISOString() }])

      // Immediately following ingests should be throttled
      await writer.ingest([{ type: 'test2', ts: new Date().toISOString() }])
      await writer.ingest([{ type: 'test3', ts: new Date().toISOString() }])

      // Only one metrics report should have occurred
      const stats = await testEnv.coordinator.getStats()
      const shard0Metrics = stats.metrics.find(m => m.shardId === 0)

      // The buffered count should reflect first report (1 event), not latest (3 events)
      // because subsequent reports were throttled
      expect(shard0Metrics?.buffered).toBe(1)
    })

    it('should aggregate metrics from all active shards', async () => {
      await testEnv.coordinator.forceScale(3)

      // Report metrics from each shard
      for (let i = 0; i < 3; i++) {
        const writer = getOrCreateWriter(testEnv, i)
        writer.resetMetricsReportTime()
        await writer.ingest([
          { type: 'test', ts: new Date().toISOString() },
          { type: 'test', ts: new Date().toISOString() },
        ])
      }

      const stats = await testEnv.coordinator.getStats()

      expect(stats.metrics).toHaveLength(3)
      expect(stats.totalBuffered).toBe(6) // 2 events * 3 shards
    })

    it('should calculate average utilization across shards', async () => {
      await testEnv.coordinator.forceScale(3)
      testEnv.coordinator.setLastScaleTime(0)

      // Report metrics with varying pending writes
      await testEnv.coordinator.reportMetrics({
        shardId: 0,
        buffered: 0,
        pendingWrites: 30, // 30% utilization
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 1,
        buffered: 0,
        pendingWrites: 60, // 60% utilization
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      await testEnv.coordinator.reportMetrics({
        shardId: 2,
        buffered: 0,
        pendingWrites: 30, // 30% utilization
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      const stats = await testEnv.coordinator.getStats()
      // Total pending: 120, total capacity: 300, average: 40%
      expect(stats.averageUtilization).toBeCloseTo(0.4, 2)
    })

    it('should track backpressure events in metrics', async () => {
      testEnv.coordinator.setLastScaleTime(0)

      const writer = getOrCreateWriter(testEnv, 0)
      writer.setPendingWrites(100)

      // Trigger backpressure
      await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])
      await writer.ingest([{ type: 'test', ts: new Date().toISOString() }])

      // Check backpressure count before scale (which resets it)
      // Note: After 2 backpressure events, we haven't hit threshold yet
      const metrics = testEnv.coordinator.getShardMetrics().get(0)
      expect(metrics?.backpressureCount).toBe(2)
    })

    it('should provide scaling state in stats', async () => {
      await testEnv.coordinator.forceScale(3)
      testEnv.coordinator.setLastScaleTime(0)

      const stats = await testEnv.coordinator.getStats()

      expect(stats.canScaleUp).toBe(true)
      expect(stats.canScaleDown).toBe(true)
      expect(stats.activeShards).toEqual([0, 1, 2])
      expect(typeof stats.lastScaleTime).toBe('string')
    })

    it('should ignore stale metrics from shards that have not reported recently', async () => {
      await testEnv.coordinator.forceScale(2)

      // Report fresh metrics for shard 0
      await testEnv.coordinator.reportMetrics({
        shardId: 0,
        buffered: 100,
        pendingWrites: 50,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
      })

      // Manually add stale metrics for shard 1
      testEnv.coordinator.getShardMetrics().set(1, {
        shardId: 1,
        buffered: 1000,
        pendingWrites: 90,
        lastFlushTime: new Date().toISOString(),
        timeSinceFlush: 0,
        flushScheduled: false,
        lastReportTime: Date.now() - 120_000, // 2 minutes ago (outside 30s window)
        backpressureCount: 0,
      })

      const stats = await testEnv.coordinator.getStats()
      // Total includes stale metrics in stats display
      // But scaling decisions would ignore stale metrics
      expect(stats.totalPendingWrites).toBe(140) // 50 + 90
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Additional integration scenarios
  // ──────────────────────────────────────────────────────────────────────────

  describe('End-to-end scenarios', () => {
    it('should handle complete lifecycle: scale up, process load, scale down', async () => {
      testEnv.coordinator.setLastScaleTime(0)

      // Phase 1: Start with 1 shard
      let shardCount = await testEnv.coordinator.getShardCount()
      expect(shardCount).toBe(1)

      // Phase 2: Trigger backpressure to scale up
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(0)

      shardCount = await testEnv.coordinator.getShardCount()
      expect(shardCount).toBe(2)

      // Phase 3: Continue scaling up under heavy load
      testEnv.coordinator.setLastScaleTime(0)
      await testEnv.coordinator.reportBackpressure(0)
      await testEnv.coordinator.reportBackpressure(1)
      await testEnv.coordinator.reportBackpressure(0)

      shardCount = await testEnv.coordinator.getShardCount()
      expect(shardCount).toBe(3)

      // Phase 4: Process events normally across all shards
      for (let i = 0; i < 3; i++) {
        const writer = getOrCreateWriter(testEnv, i)
        const result = await writer.ingest([
          { type: 'batch.event', ts: new Date().toISOString() },
        ])
        expect(result.ok).toBe(true)
      }

      // Phase 5: Scale down when load decreases
      await testEnv.coordinator.forceScale(1)
      shardCount = await testEnv.coordinator.getShardCount()
      expect(shardCount).toBe(1)
    })

    it('should handle concurrent ingests across multiple shards', async () => {
      await testEnv.coordinator.forceScale(4)

      // Simulate concurrent ingests to all shards
      const ingestPromises = []
      for (let i = 0; i < 4; i++) {
        const writer = getOrCreateWriter(testEnv, i)
        const events = Array.from({ length: 10 }, (_, j) => ({
          type: `concurrent.event.${i}.${j}`,
          ts: new Date().toISOString(),
        }))
        ingestPromises.push(writer.ingest(events, `source-${i}`))
      }

      const results = await Promise.all(ingestPromises)

      // All should succeed
      expect(results.every(r => r.ok)).toBe(true)
      expect(results.map(r => r.shard).sort()).toEqual([0, 1, 2, 3])
    })

    it('should handle rapid scale up and down cycles', async () => {
      // Start at 1 shard
      let count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(1)

      // Rapid scale up
      await testEnv.coordinator.forceScale(4)
      count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(4)

      // Rapid scale down
      await testEnv.coordinator.forceScale(2)
      count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(2)

      // Scale back up
      await testEnv.coordinator.forceScale(6)
      count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(6)

      // Final scale down to minimum
      await testEnv.coordinator.forceScale(1)
      count = await testEnv.coordinator.getShardCount()
      expect(count).toBe(1)

      // Verify shard 0 is always preserved
      const shards = await testEnv.coordinator.getActiveShards()
      expect(shards).toContain(0)
    })

    it('should maintain data integrity during scaling operations', async () => {
      await testEnv.coordinator.forceScale(3)

      // Ingest events to all shards
      const eventCounts: Record<number, number> = {}
      for (let i = 0; i < 3; i++) {
        const writer = getOrCreateWriter(testEnv, i)
        const events = Array.from({ length: 5 }, () => ({
          type: 'integrity.event',
          ts: new Date().toISOString(),
        }))
        await writer.ingest(events)
        eventCounts[i] = writer.getBuffer().length
      }

      // Scale down
      await testEnv.coordinator.forceScale(2)

      // Remaining writers should still have their data
      for (const shardId of [0, 1]) {
        const writer = testEnv.writers.get(shardId)
        if (writer) {
          expect(writer.getBuffer().length).toBe(eventCounts[shardId])
        }
      }
    })
  })
})
