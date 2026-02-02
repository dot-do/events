/**
 * ShardCoordinatorDO - Dynamic shard management for EventWriterDO
 *
 * Responsibilities:
 * - Track shard utilization metrics from all EventWriterDO instances
 * - Auto-scale shard count based on backpressure signals
 * - Provide shard discovery for queries
 * - Maintain configuration for min/max shards
 *
 * Scaling signals:
 * - Backpressure events from shards (scale up)
 * - Low utilization across all shards (scale down)
 * - Time-based cooldown to prevent thrashing
 */

import { DurableObject } from 'cloudflare:workers'
import type { Env as FullEnv } from './env'
import { recordWriterDOMetric, MetricTimer } from './metrics'

export type Env = Pick<FullEnv, 'EVENTS_BUCKET' | 'EVENT_WRITER' | 'ANALYTICS' | 'SHARD_COORDINATOR'>

// ============================================================================
// Configuration
// ============================================================================

export interface ShardConfig {
  minShards: number
  maxShards: number
  scaleUpThreshold: number      // Backpressure events in window to trigger scale up
  scaleDownThreshold: number    // Average utilization below this to trigger scale down
  cooldownMs: number            // Minimum time between scaling operations
  metricsWindowMs: number       // Time window for metrics aggregation
  healthCheckIntervalMs: number // Interval for shard health checks
}

const DEFAULT_CONFIG: ShardConfig = {
  minShards: 1,
  maxShards: 64,
  scaleUpThreshold: 3,          // 3 backpressure events in window triggers scale up
  scaleDownThreshold: 0.2,      // Average utilization below 20% triggers scale down
  cooldownMs: 60_000,           // 1 minute cooldown between scaling
  metricsWindowMs: 30_000,      // 30 second metrics window
  healthCheckIntervalMs: 10_000, // 10 second health checks
}

// Storage keys
const CONFIG_KEY = '_coordinator:config'
const METRICS_KEY = '_coordinator:metrics'
const ACTIVE_SHARDS_KEY = '_coordinator:activeShards'
const LAST_SCALE_KEY = '_coordinator:lastScaleTime'

// ============================================================================
// Types
// ============================================================================

export interface ShardMetrics {
  shardId: number
  buffered: number
  pendingWrites: number
  lastFlushTime: string
  timeSinceFlush: number
  flushScheduled: boolean
  lastReportTime: number
  backpressureCount: number
}

export interface ShardStats {
  activeShards: number[]
  totalBuffered: number
  totalPendingWrites: number
  averageUtilization: number
  metrics: ShardMetrics[]
  lastScaleTime: string
  canScaleUp: boolean
  canScaleDown: boolean
}

export interface ScaleResult {
  scaled: boolean
  direction?: 'up' | 'down'
  previousCount: number
  newCount: number
  reason: string
}

// ============================================================================
// ShardCoordinatorDO
// ============================================================================

export class ShardCoordinatorDO extends DurableObject<Env> {
  private config: ShardConfig = DEFAULT_CONFIG
  private activeShards: Set<number> = new Set([0]) // Start with shard 0
  private shardMetrics: Map<number, ShardMetrics> = new Map()
  private lastScaleTime = 0
  private initialized = false

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    ctx.blockConcurrencyWhile(async () => {
      await this.restore()
    })
  }

  // ──────────────────────────────────────────────────────────────────────────
  // State Management
  // ──────────────────────────────────────────────────────────────────────────

  private async restore(): Promise<void> {
    try {
      const [storedConfig, storedActiveShards, storedLastScale, storedMetrics] = await Promise.all([
        this.ctx.storage.get<ShardConfig>(CONFIG_KEY),
        this.ctx.storage.get<number[]>(ACTIVE_SHARDS_KEY),
        this.ctx.storage.get<number>(LAST_SCALE_KEY),
        this.ctx.storage.get<ShardMetrics[]>(METRICS_KEY),
      ])

      if (storedConfig) {
        this.config = { ...DEFAULT_CONFIG, ...storedConfig }
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
      console.log(`[ShardCoordinator] Restored state: ${this.activeShards.size} active shards`)
    } catch (error) {
      console.error('[ShardCoordinator] Failed to restore state:', error)
      this.initialized = true
    }
  }

  private async persist(): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      await txn.put(CONFIG_KEY, this.config)
      await txn.put(ACTIVE_SHARDS_KEY, Array.from(this.activeShards))
      await txn.put(LAST_SCALE_KEY, this.lastScaleTime)
      await txn.put(METRICS_KEY, Array.from(this.shardMetrics.values()))
    })
  }

  // ──────────────────────────────────────────────────────────────────────────
  // RPC Methods
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Get current configuration
   */
  async getConfig(): Promise<ShardConfig> {
    return { ...this.config }
  }

  /**
   * Update configuration
   */
  async updateConfig(updates: Partial<ShardConfig>): Promise<ShardConfig> {
    this.config = { ...this.config, ...updates }
    await this.persist()
    console.log(`[ShardCoordinator] Config updated:`, this.config)
    return this.config
  }

  /**
   * Get active shards for routing
   */
  async getActiveShards(): Promise<number[]> {
    return Array.from(this.activeShards).sort((a, b) => a - b)
  }

  /**
   * Get current shard count
   */
  async getShardCount(): Promise<number> {
    return this.activeShards.size
  }

  /**
   * Report metrics from an EventWriterDO shard
   */
  async reportMetrics(metrics: Omit<ShardMetrics, 'lastReportTime' | 'backpressureCount'>): Promise<void> {
    const existingMetrics = this.shardMetrics.get(metrics.shardId)
    const backpressureCount = existingMetrics?.backpressureCount ?? 0

    this.shardMetrics.set(metrics.shardId, {
      ...metrics,
      lastReportTime: Date.now(),
      backpressureCount,
    })

    // Ensure this shard is marked as active
    if (!this.activeShards.has(metrics.shardId)) {
      this.activeShards.add(metrics.shardId)
      await this.persist()
    }

    // Record metric
    recordWriterDOMetric(this.env.ANALYTICS, 'ingest', 'success', {
      events: metrics.buffered,
      shard: metrics.shardId,
    })
  }

  /**
   * Report backpressure event from an EventWriterDO shard
   */
  async reportBackpressure(shardId: number): Promise<{ shouldRetry: boolean; alternativeShard: number }> {
    const timer = new MetricTimer()

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

    // Check if we should scale up
    const scaleResult = await this.maybeScale()

    // Record backpressure metric
    recordWriterDOMetric(this.env.ANALYTICS, 'backpressure', 'backpressure', {
      shard: shardId,
    }, timer.elapsed())

    // Find an alternative shard with lower utilization
    const alternativeShard = this.findBestShard(shardId)

    return {
      shouldRetry: true,
      alternativeShard,
    }
  }

  /**
   * Find the best shard for routing (lowest utilization)
   */
  private findBestShard(excludeShardId?: number): number {
    const shards = Array.from(this.activeShards)
    if (shards.length === 0) return 0

    // Filter out excluded shard
    const candidates = excludeShardId !== undefined
      ? shards.filter(s => s !== excludeShardId)
      : shards

    if (candidates.length === 0) {
      // All shards are the excluded one, wrap around
      // excludeShardId must be defined here since candidates is only empty when we filtered out the excluded shard
      return ((excludeShardId ?? 0) + 1) % this.config.maxShards
    }

    // Find shard with lowest pending writes
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

  /**
   * Get routing recommendation for a request
   */
  async getRoutingShard(preferredShard?: number): Promise<number> {
    const shards = Array.from(this.activeShards)

    // If preferred shard is valid and active, use it
    if (preferredShard !== undefined && this.activeShards.has(preferredShard)) {
      const metrics = this.shardMetrics.get(preferredShard)
      // Only use if not under heavy backpressure
      if (!metrics || metrics.backpressureCount < 3) {
        return preferredShard
      }
    }

    // Otherwise, find best shard
    return this.findBestShard()
  }

  /**
   * Get full statistics for monitoring
   */
  async getStats(): Promise<ShardStats> {
    const metrics = Array.from(this.shardMetrics.values())
    const activeShards = Array.from(this.activeShards).sort((a, b) => a - b)

    const totalBuffered = metrics.reduce((sum, m) => sum + m.buffered, 0)
    const totalPendingWrites = metrics.reduce((sum, m) => sum + m.pendingWrites, 0)

    // Calculate average utilization (pending writes relative to max)
    const maxPendingPerShard = 100 // From EventWriterDO config
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

  // ──────────────────────────────────────────────────────────────────────────
  // Scaling Logic
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Check if scaling is needed and perform it
   */
  private async maybeScale(): Promise<ScaleResult> {
    const now = Date.now()
    const previousCount = this.activeShards.size

    // Check cooldown
    if ((now - this.lastScaleTime) < this.config.cooldownMs) {
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'In cooldown period',
      }
    }

    // Check for scale up triggers
    const recentBackpressure = this.countRecentBackpressure()
    if (recentBackpressure >= this.config.scaleUpThreshold) {
      if (this.activeShards.size < this.config.maxShards) {
        const result = await this.scaleUp()
        return result
      }
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'At maximum shards',
      }
    }

    // Check for scale down triggers
    const avgUtilization = this.calculateAverageUtilization()
    if (avgUtilization < this.config.scaleDownThreshold) {
      if (this.activeShards.size > this.config.minShards) {
        const result = await this.scaleDown()
        return result
      }
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'At minimum shards',
      }
    }

    return {
      scaled: false,
      previousCount,
      newCount: previousCount,
      reason: 'No scaling needed',
    }
  }

  private countRecentBackpressure(): number {
    const now = Date.now()
    const window = this.config.metricsWindowMs
    let count = 0

    for (const metrics of this.shardMetrics.values()) {
      // Only count if metrics are recent
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
    const maxPendingPerShard = 100 // From EventWriterDO config

    for (const metrics of this.shardMetrics.values()) {
      // Only consider recent metrics
      if ((now - metrics.lastReportTime) <= window) {
        totalUtilization += metrics.pendingWrites / maxPendingPerShard
        activeCount++
      }
    }

    return activeCount > 0 ? totalUtilization / activeCount : 0
  }

  /**
   * Scale up by adding a new shard
   */
  private async scaleUp(): Promise<ScaleResult> {
    const previousCount = this.activeShards.size

    // Find the next available shard ID
    let nextShard = 0
    while (this.activeShards.has(nextShard) && nextShard < this.config.maxShards) {
      nextShard++
    }

    if (nextShard >= this.config.maxShards) {
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'No available shard IDs',
      }
    }

    this.activeShards.add(nextShard)
    this.lastScaleTime = Date.now()

    // Reset backpressure counts
    for (const metrics of this.shardMetrics.values()) {
      metrics.backpressureCount = 0
    }

    await this.persist()

    console.log(`[ShardCoordinator] Scaled UP: ${previousCount} -> ${this.activeShards.size} shards (added shard ${nextShard})`)

    recordWriterDOMetric(this.env.ANALYTICS, 'restore', 'success', {
      events: this.activeShards.size,
    })

    return {
      scaled: true,
      direction: 'up',
      previousCount,
      newCount: this.activeShards.size,
      reason: `Added shard ${nextShard} due to backpressure`,
    }
  }

  /**
   * Scale down by removing a shard
   */
  private async scaleDown(): Promise<ScaleResult> {
    const previousCount = this.activeShards.size

    // Find the shard with lowest utilization to remove
    let lowestShard = -1
    let lowestScore = Infinity

    for (const shardId of this.activeShards) {
      // Don't remove shard 0 (always keep at least one stable shard)
      if (shardId === 0) continue

      const metrics = this.shardMetrics.get(shardId)
      const score = metrics ? metrics.buffered + metrics.pendingWrites : 0

      if (score < lowestScore) {
        lowestScore = score
        lowestShard = shardId
      }
    }

    if (lowestShard === -1) {
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'No removable shards',
      }
    }

    // Remove the shard
    this.activeShards.delete(lowestShard)
    this.shardMetrics.delete(lowestShard)
    this.lastScaleTime = Date.now()

    await this.persist()

    console.log(`[ShardCoordinator] Scaled DOWN: ${previousCount} -> ${this.activeShards.size} shards (removed shard ${lowestShard})`)

    return {
      scaled: true,
      direction: 'down',
      previousCount,
      newCount: this.activeShards.size,
      reason: `Removed shard ${lowestShard} due to low utilization`,
    }
  }

  /**
   * Force scale to a specific shard count
   */
  async forceScale(targetCount: number): Promise<ScaleResult> {
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
      // Add shards
      while (this.activeShards.size < targetCount) {
        let nextShard = 0
        while (this.activeShards.has(nextShard)) nextShard++
        this.activeShards.add(nextShard)
      }
    } else {
      // Remove shards (keep lowest numbered ones)
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

    console.log(`[ShardCoordinator] Force scaled: ${previousCount} -> ${targetCount} shards`)

    return {
      scaled: true,
      direction: targetCount > previousCount ? 'up' : 'down',
      previousCount,
      newCount: this.activeShards.size,
      reason: `Force scaled to ${targetCount} shards`,
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Alarm for periodic maintenance
  // ──────────────────────────────────────────────────────────────────────────

  async alarm(): Promise<void> {
    // Periodic health check and potential scale down
    const stats = await this.getStats()
    console.log(`[ShardCoordinator] Health check: ${stats.activeShards.length} shards, ${stats.averageUtilization.toFixed(2)} avg utilization`)

    // Try to scale if needed
    await this.maybeScale()

    // Schedule next health check
    this.ctx.storage.setAlarm(Date.now() + this.config.healthCheckIntervalMs)
  }

  /**
   * Start periodic health checks
   */
  async startHealthChecks(): Promise<void> {
    const alarm = await this.ctx.storage.getAlarm()
    if (!alarm) {
      this.ctx.storage.setAlarm(Date.now() + this.config.healthCheckIntervalMs)
      console.log('[ShardCoordinator] Started health checks')
    }
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get the shard coordinator instance
 */
export function getShardCoordinator(env: Env): DurableObjectStub<ShardCoordinatorDO> | null {
  if (!env.SHARD_COORDINATOR) return null
  const id = env.SHARD_COORDINATOR.idFromName('global')
  return env.SHARD_COORDINATOR.get(id)
}

/**
 * Get active shards from coordinator
 */
export async function getActiveShards(env: Env): Promise<number[]> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) return [0]
  return coordinator.getActiveShards()
}

/**
 * Get routing shard from coordinator
 */
export async function getRoutingShard(env: Env, preferredShard?: number): Promise<number> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) return preferredShard ?? 0
  return coordinator.getRoutingShard(preferredShard)
}
