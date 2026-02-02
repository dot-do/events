/**
 * SubscriptionShardCoordinatorDO - Dynamic shard management for SubscriptionDO
 *
 * Similar to ShardCoordinatorDO for EventWriterDO, this manages dynamic sharding
 * for the subscription system. It coordinates multiple SubscriptionDO instances
 * to handle high-volume subscription fanout.
 *
 * Responsibilities:
 * - Track shard utilization metrics from all SubscriptionDO instances
 * - Auto-scale shard count based on load signals
 * - Provide shard discovery for routing and queries
 * - Maintain configuration for min/max shards
 * - Support namespace isolation for multi-tenant deployments
 *
 * Sharding Strategy:
 * - Shards are keyed by base prefix (e.g., "collection", "webhook", "rpc")
 * - Each base prefix can have multiple sub-shards: "collection:0", "collection:1", etc.
 * - Multi-tenant namespaces are prefixed: "acme:collection:0"
 * - The coordinator tracks which sub-shards are active for each base prefix
 */

import { DurableObject } from 'cloudflare:workers'
import type { SubscriptionDO } from '../core/src/subscription'
import { logger, sanitize, logError } from './logger'
import { simpleHash } from './utils/hash'

const log = logger.child({ component: 'SubscriptionShardCoordinator' })

// ============================================================================
// Configuration
// ============================================================================

export interface SubscriptionShardConfig {
  minShardsPerPrefix: number
  maxShardsPerPrefix: number
  scaleUpThreshold: number        // Pending deliveries threshold to trigger scale up
  scaleDownThreshold: number      // Pending deliveries below this to trigger scale down
  cooldownMs: number              // Minimum time between scaling operations
  metricsWindowMs: number         // Time window for metrics aggregation
  healthCheckIntervalMs: number   // Interval for shard health checks
}

const DEFAULT_CONFIG: SubscriptionShardConfig = {
  minShardsPerPrefix: 1,
  maxShardsPerPrefix: 16,
  scaleUpThreshold: 1000,         // Scale up when pending deliveries > 1000
  scaleDownThreshold: 100,        // Scale down when pending deliveries < 100
  cooldownMs: 60_000,             // 1 minute cooldown between scaling
  metricsWindowMs: 30_000,        // 30 second metrics window
  healthCheckIntervalMs: 30_000,  // 30 second health checks
}

// Well-known base shard prefixes
export const KNOWN_BASE_PREFIXES = [
  'collection',
  'rpc',
  'do',
  'ws',
  'webhook',
  'default',
]

// Storage keys
const CONFIG_KEY = '_subCoordinator:config'
const SHARD_STATE_KEY = '_subCoordinator:shardState'
const LAST_SCALE_KEY = '_subCoordinator:lastScaleTime'

// ============================================================================
// Types
// ============================================================================

export interface ShardMetrics {
  shardKey: string
  basePrefix: string
  shardIndex: number
  pendingDeliveries: number
  activeSubscriptions: number
  lastReportTime: number
  loadScore: number
}

export interface PrefixShardState {
  basePrefix: string
  activeShardCount: number
  lastScaleTime: number
  metrics: ShardMetrics[]
}

export interface SubscriptionShardStats {
  totalShards: number
  prefixStates: Record<string, PrefixShardState>
  lastScaleTime: string
  config: SubscriptionShardConfig
}

export interface ScaleResult {
  scaled: boolean
  direction?: 'up' | 'down'
  previousCount: number
  newCount: number
  reason: string
  prefix: string
}

export type Env = {
  SUBSCRIPTIONS: DurableObjectNamespace<SubscriptionDO>
  SUBSCRIPTION_SHARD_COORDINATOR?: DurableObjectNamespace<SubscriptionShardCoordinatorDO>
}

// ============================================================================
// SubscriptionShardCoordinatorDO
// ============================================================================

export class SubscriptionShardCoordinatorDO extends DurableObject<Env> {
  private config: SubscriptionShardConfig = DEFAULT_CONFIG
  private prefixStates: Map<string, PrefixShardState> = new Map()
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
      const [storedConfig, storedShardState, storedLastScale] = await Promise.all([
        this.ctx.storage.get<SubscriptionShardConfig>(CONFIG_KEY),
        this.ctx.storage.get<Record<string, PrefixShardState>>(SHARD_STATE_KEY),
        this.ctx.storage.get<number>(LAST_SCALE_KEY),
      ])

      if (storedConfig) {
        this.config = { ...DEFAULT_CONFIG, ...storedConfig }
      }

      if (storedShardState) {
        this.prefixStates = new Map(Object.entries(storedShardState))
      } else {
        // Initialize with default state for known prefixes
        for (const prefix of KNOWN_BASE_PREFIXES) {
          this.prefixStates.set(prefix, {
            basePrefix: prefix,
            activeShardCount: 1,
            lastScaleTime: 0,
            metrics: [],
          })
        }
      }

      if (storedLastScale) {
        this.lastScaleTime = storedLastScale
      }

      this.initialized = true
      log.info('Restored state', { prefixCount: this.prefixStates.size })
    } catch (error) {
      logError(log, 'Failed to restore state', error)
      this.initialized = true
    }
  }

  private async persist(): Promise<void> {
    const shardStateObj: Record<string, PrefixShardState> = {}
    for (const [key, value] of this.prefixStates) {
      shardStateObj[key] = value
    }

    await this.ctx.storage.transaction(async (txn) => {
      await txn.put(CONFIG_KEY, this.config)
      await txn.put(SHARD_STATE_KEY, shardStateObj)
      await txn.put(LAST_SCALE_KEY, this.lastScaleTime)
    })
  }

  // ──────────────────────────────────────────────────────────────────────────
  // RPC Methods
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Get current configuration
   */
  async getConfig(): Promise<SubscriptionShardConfig> {
    return { ...this.config }
  }

  /**
   * Update configuration
   */
  async updateConfig(updates: Partial<SubscriptionShardConfig>): Promise<SubscriptionShardConfig> {
    this.config = { ...this.config, ...updates }
    await this.persist()
    log.info('Config updated', { config: sanitize.payload(this.config) })
    return this.config
  }

  /**
   * Get active shard keys for a given base prefix and optional namespace
   *
   * @param basePrefix - The base prefix (e.g., "collection", "webhook")
   * @param namespace - Optional namespace for multi-tenant isolation
   * @returns Array of shard keys (e.g., ["acme:collection:0", "acme:collection:1"])
   */
  async getActiveShards(basePrefix: string, namespace?: string): Promise<string[]> {
    const state = this.prefixStates.get(basePrefix)
    const shardCount = state?.activeShardCount ?? 1

    const shards: string[] = []
    for (let i = 0; i < shardCount; i++) {
      const shardKey = this.buildShardKey(basePrefix, i, namespace)
      shards.push(shardKey)
    }
    return shards
  }

  /**
   * Get all active shard keys across all prefixes for a namespace
   */
  async getAllActiveShards(namespace?: string): Promise<string[]> {
    const allShards: string[] = []

    for (const [basePrefix, state] of this.prefixStates) {
      for (let i = 0; i < state.activeShardCount; i++) {
        allShards.push(this.buildShardKey(basePrefix, i, namespace))
      }
    }

    return allShards
  }

  /**
   * Get the shard count for a given base prefix
   */
  async getShardCount(basePrefix: string): Promise<number> {
    const state = this.prefixStates.get(basePrefix)
    return state?.activeShardCount ?? 1
  }

  /**
   * Get a routing shard for a given base prefix using load balancing
   *
   * @param basePrefix - The base prefix
   * @param namespace - Optional namespace
   * @param hashKey - Optional key to use for consistent hashing (e.g., event ID)
   * @returns The shard key to route to
   */
  async getRoutingShard(
    basePrefix: string,
    namespace?: string,
    hashKey?: string
  ): Promise<string> {
    const state = this.prefixStates.get(basePrefix)
    const shardCount = state?.activeShardCount ?? 1

    let shardIndex: number
    if (hashKey) {
      // Consistent hashing based on the provided key
      shardIndex = simpleHash(hashKey) % shardCount
    } else if (state?.metrics && state.metrics.length > 0) {
      // Load-based routing: pick the shard with lowest load
      const lowestLoadShard = state.metrics.reduce((prev, curr) =>
        curr.loadScore < prev.loadScore ? curr : prev
      )
      shardIndex = lowestLoadShard.shardIndex
    } else {
      // Random distribution when no metrics available
      shardIndex = Math.floor(Math.random() * shardCount)
    }

    return this.buildShardKey(basePrefix, shardIndex, namespace)
  }

  /**
   * Report metrics from a SubscriptionDO shard
   */
  async reportMetrics(metrics: {
    shardKey: string
    pendingDeliveries: number
    activeSubscriptions: number
  }): Promise<void> {
    const { basePrefix, shardIndex } = this.parseShardKey(metrics.shardKey)

    // Ensure prefix state exists
    if (!this.prefixStates.has(basePrefix)) {
      this.prefixStates.set(basePrefix, {
        basePrefix,
        activeShardCount: 1,
        lastScaleTime: 0,
        metrics: [],
      })
    }

    const state = this.prefixStates.get(basePrefix)!

    // Calculate load score (weighted by pending deliveries and subscription count)
    const loadScore = metrics.pendingDeliveries * 2 + metrics.activeSubscriptions

    // Update or add metrics for this shard
    const existingIndex = state.metrics.findIndex(m => m.shardKey === metrics.shardKey)
    const newMetric: ShardMetrics = {
      shardKey: metrics.shardKey,
      basePrefix,
      shardIndex,
      pendingDeliveries: metrics.pendingDeliveries,
      activeSubscriptions: metrics.activeSubscriptions,
      lastReportTime: Date.now(),
      loadScore,
    }

    if (existingIndex >= 0) {
      state.metrics[existingIndex] = newMetric
    } else {
      state.metrics.push(newMetric)
    }

    // Check if scaling is needed
    await this.maybeScale(basePrefix)
  }

  /**
   * Report high load from a shard (triggers scale up consideration)
   */
  async reportHighLoad(shardKey: string, pendingDeliveries: number): Promise<{
    shouldRedirect: boolean
    alternativeShard?: string
  }> {
    const { basePrefix, namespace } = this.parseShardKey(shardKey)

    // Report as high-load metric
    await this.reportMetrics({
      shardKey,
      pendingDeliveries,
      activeSubscriptions: 0, // Unknown, but pending deliveries is the key signal
    })

    // Check if we can scale up
    const scaleResult = await this.maybeScale(basePrefix)

    if (scaleResult.scaled && scaleResult.direction === 'up') {
      // New shard was added, redirect to it
      const newShardIndex = scaleResult.newCount - 1
      const newShardKey = this.buildShardKey(basePrefix, newShardIndex, namespace)
      return { shouldRedirect: true, alternativeShard: newShardKey }
    }

    // Find the least loaded shard as alternative
    const state = this.prefixStates.get(basePrefix)
    if (state && state.metrics.length > 0) {
      const lowestLoadShard = state.metrics
        .filter(m => m.shardKey !== shardKey)
        .sort((a, b) => a.loadScore - b.loadScore)[0]

      if (lowestLoadShard) {
        return { shouldRedirect: true, alternativeShard: lowestLoadShard.shardKey }
      }
    }

    return { shouldRedirect: false }
  }

  /**
   * Get full statistics for monitoring
   */
  async getStats(): Promise<SubscriptionShardStats> {
    const prefixStates: Record<string, PrefixShardState> = {}

    for (const [key, value] of this.prefixStates) {
      prefixStates[key] = value
    }

    const totalShards = Array.from(this.prefixStates.values())
      .reduce((sum, state) => sum + state.activeShardCount, 0)

    return {
      totalShards,
      prefixStates,
      lastScaleTime: new Date(this.lastScaleTime).toISOString(),
      config: this.config,
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Scaling Logic
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Check if scaling is needed for a prefix and perform it
   */
  private async maybeScale(basePrefix: string): Promise<ScaleResult> {
    const now = Date.now()
    const state = this.prefixStates.get(basePrefix)

    if (!state) {
      return {
        scaled: false,
        previousCount: 0,
        newCount: 0,
        reason: 'Unknown prefix',
        prefix: basePrefix,
      }
    }

    const previousCount = state.activeShardCount

    // Check cooldown
    if ((now - state.lastScaleTime) < this.config.cooldownMs) {
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'In cooldown period',
        prefix: basePrefix,
      }
    }

    // Calculate total pending deliveries across all shards for this prefix
    const totalPending = state.metrics.reduce((sum, m) => sum + m.pendingDeliveries, 0)
    const avgPending = state.metrics.length > 0 ? totalPending / state.metrics.length : 0

    // Check for scale up
    if (avgPending >= this.config.scaleUpThreshold) {
      if (state.activeShardCount < this.config.maxShardsPerPrefix) {
        return await this.scaleUp(basePrefix)
      }
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'At maximum shards for prefix',
        prefix: basePrefix,
      }
    }

    // Check for scale down
    if (avgPending < this.config.scaleDownThreshold) {
      if (state.activeShardCount > this.config.minShardsPerPrefix) {
        return await this.scaleDown(basePrefix)
      }
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'At minimum shards for prefix',
        prefix: basePrefix,
      }
    }

    return {
      scaled: false,
      previousCount,
      newCount: previousCount,
      reason: 'No scaling needed',
      prefix: basePrefix,
    }
  }

  /**
   * Scale up by adding a new shard to a prefix
   */
  private async scaleUp(basePrefix: string): Promise<ScaleResult> {
    const state = this.prefixStates.get(basePrefix)
    if (!state) {
      return {
        scaled: false,
        previousCount: 0,
        newCount: 0,
        reason: 'Unknown prefix',
        prefix: basePrefix,
      }
    }

    const previousCount = state.activeShardCount
    state.activeShardCount++
    state.lastScaleTime = Date.now()
    this.lastScaleTime = Date.now()

    await this.persist()

    log.info('Scaled UP', { basePrefix, previousCount, newCount: state.activeShardCount })

    return {
      scaled: true,
      direction: 'up',
      previousCount,
      newCount: state.activeShardCount,
      reason: `Added shard due to high load`,
      prefix: basePrefix,
    }
  }

  /**
   * Scale down by removing a shard from a prefix
   */
  private async scaleDown(basePrefix: string): Promise<ScaleResult> {
    const state = this.prefixStates.get(basePrefix)
    if (!state) {
      return {
        scaled: false,
        previousCount: 0,
        newCount: 0,
        reason: 'Unknown prefix',
        prefix: basePrefix,
      }
    }

    const previousCount = state.activeShardCount
    state.activeShardCount--
    state.lastScaleTime = Date.now()
    this.lastScaleTime = Date.now()

    // Remove metrics for the removed shard
    const removedShardIndex = state.activeShardCount
    state.metrics = state.metrics.filter(m => m.shardIndex < removedShardIndex)

    await this.persist()

    log.info('Scaled DOWN', { basePrefix, previousCount, newCount: state.activeShardCount })

    return {
      scaled: true,
      direction: 'down',
      previousCount,
      newCount: state.activeShardCount,
      reason: `Removed shard due to low load`,
      prefix: basePrefix,
    }
  }

  /**
   * Force scale a prefix to a specific shard count
   */
  async forceScale(basePrefix: string, targetCount: number): Promise<ScaleResult> {
    // Ensure prefix state exists
    if (!this.prefixStates.has(basePrefix)) {
      this.prefixStates.set(basePrefix, {
        basePrefix,
        activeShardCount: 1,
        lastScaleTime: 0,
        metrics: [],
      })
    }

    const state = this.prefixStates.get(basePrefix)!
    const previousCount = state.activeShardCount

    // Clamp to valid range
    targetCount = Math.max(this.config.minShardsPerPrefix, Math.min(this.config.maxShardsPerPrefix, targetCount))

    if (targetCount === previousCount) {
      return {
        scaled: false,
        previousCount,
        newCount: previousCount,
        reason: 'Already at target count',
        prefix: basePrefix,
      }
    }

    state.activeShardCount = targetCount
    state.lastScaleTime = Date.now()
    this.lastScaleTime = Date.now()

    // Clean up metrics if scaling down
    if (targetCount < previousCount) {
      state.metrics = state.metrics.filter(m => m.shardIndex < targetCount)
    }

    await this.persist()

    log.info('Force scaled', { basePrefix, previousCount, targetCount })

    return {
      scaled: true,
      direction: targetCount > previousCount ? 'up' : 'down',
      previousCount,
      newCount: targetCount,
      reason: `Force scaled to ${targetCount} shards`,
      prefix: basePrefix,
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Alarm for periodic maintenance
  // ──────────────────────────────────────────────────────────────────────────

  async alarm(): Promise<void> {
    log.info('Health check running')

    // Clean up stale metrics (older than metrics window)
    const cutoff = Date.now() - this.config.metricsWindowMs
    for (const state of this.prefixStates.values()) {
      state.metrics = state.metrics.filter(m => m.lastReportTime > cutoff)
    }

    await this.persist()

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
      log.info('Started health checks')
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Helper Methods
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Build a shard key from components
   * Format: [namespace:]<basePrefix>:<shardIndex>
   *
   * Examples:
   * - "collection:0"
   * - "acme:collection:0"
   */
  private buildShardKey(basePrefix: string, shardIndex: number, namespace?: string): string {
    if (namespace) {
      return `${namespace}:${basePrefix}:${shardIndex}`
    }
    return `${basePrefix}:${shardIndex}`
  }

  /**
   * Parse a shard key into its components
   */
  private parseShardKey(shardKey: string): { basePrefix: string; shardIndex: number; namespace: string | undefined } {
    const parts = shardKey.split(':')

    if (parts.length === 3) {
      // Namespace-prefixed: "acme:collection:0"
      return {
        namespace: parts[0],
        basePrefix: parts[1]!,
        shardIndex: parseInt(parts[2]!, 10),
      }
    } else if (parts.length === 2) {
      // Simple: "collection:0"
      return {
        basePrefix: parts[0]!,
        shardIndex: parseInt(parts[1]!, 10),
        namespace: undefined,
      }
    } else {
      // Legacy format: just the prefix
      return {
        basePrefix: shardKey,
        shardIndex: 0,
        namespace: undefined,
      }
    }
  }


  // ──────────────────────────────────────────────────────────────────────────
  // HTTP Fetch Handler - Health Check
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Handle HTTP requests to the DO
   * GET /health - Returns health diagnostics with internal state metrics
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname === '/health' || url.pathname === '/diagnostics') {
      const now = Date.now()

      // Calculate aggregate metrics across all prefixes
      let totalShards = 0
      let totalPendingDeliveries = 0
      let totalActiveSubscriptions = 0
      const prefixSummaries: Record<string, {
        shardCount: number
        pendingDeliveries: number
        activeSubscriptions: number
        lastScaleTime: string
      }> = {}

      for (const [prefix, state] of this.prefixStates) {
        totalShards += state.activeShardCount

        const prefixPending = state.metrics.reduce((sum, m) => sum + m.pendingDeliveries, 0)
        const prefixSubscriptions = state.metrics.reduce((sum, m) => sum + m.activeSubscriptions, 0)

        totalPendingDeliveries += prefixPending
        totalActiveSubscriptions += prefixSubscriptions

        prefixSummaries[prefix] = {
          shardCount: state.activeShardCount,
          pendingDeliveries: prefixPending,
          activeSubscriptions: prefixSubscriptions,
          lastScaleTime: new Date(state.lastScaleTime).toISOString(),
        }
      }

      const health = {
        status: 'healthy',
        initialized: this.initialized,
        shards: {
          totalCount: totalShards,
          prefixCount: this.prefixStates.size,
          minShardsPerPrefix: this.config.minShardsPerPrefix,
          maxShardsPerPrefix: this.config.maxShardsPerPrefix,
        },
        metrics: {
          totalPendingDeliveries,
          totalActiveSubscriptions,
          scaleUpThreshold: this.config.scaleUpThreshold,
          scaleDownThreshold: this.config.scaleDownThreshold,
        },
        prefixes: prefixSummaries,
        scaling: {
          lastScaleTime: new Date(this.lastScaleTime).toISOString(),
          cooldownMs: this.config.cooldownMs,
          metricsWindowMs: this.config.metricsWindowMs,
        },
        config: {
          minShardsPerPrefix: this.config.minShardsPerPrefix,
          maxShardsPerPrefix: this.config.maxShardsPerPrefix,
          scaleUpThreshold: this.config.scaleUpThreshold,
          scaleDownThreshold: this.config.scaleDownThreshold,
          cooldownMs: this.config.cooldownMs,
          metricsWindowMs: this.config.metricsWindowMs,
          healthCheckIntervalMs: this.config.healthCheckIntervalMs,
        },
        timestamp: new Date(now).toISOString(),
      }

      return new Response(JSON.stringify(health, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response('Not Found', { status: 404 })
  }
}

// ============================================================================
// Helper Functions for Worker Use
// ============================================================================

/**
 * Get the subscription shard coordinator instance
 */
export function getSubscriptionShardCoordinator(
  env: Env
): DurableObjectStub<SubscriptionShardCoordinatorDO> | null {
  if (!env.SUBSCRIPTION_SHARD_COORDINATOR) return null
  const id = env.SUBSCRIPTION_SHARD_COORDINATOR.idFromName('global')
  return env.SUBSCRIPTION_SHARD_COORDINATOR.get(id)
}

/**
 * Get active subscription shards for a base prefix
 */
export async function getActiveSubscriptionShards(
  env: Env,
  basePrefix: string,
  namespace?: string
): Promise<string[]> {
  const coordinator = getSubscriptionShardCoordinator(env)
  if (!coordinator) {
    // Fallback to single shard per prefix (legacy behavior)
    return [namespace ? `${namespace}:${basePrefix}` : basePrefix]
  }
  try {
    return await coordinator.getActiveShards(basePrefix, namespace)
  } catch (err) {
    logError(log, 'Failed to get active subscription shards', err, { basePrefix, namespace })
    return [namespace ? `${namespace}:${basePrefix}` : basePrefix]
  }
}

/**
 * Get a routing shard for subscription fanout
 */
export async function getSubscriptionRoutingShard(
  env: Env,
  basePrefix: string,
  namespace?: string,
  hashKey?: string
): Promise<string> {
  const coordinator = getSubscriptionShardCoordinator(env)
  if (!coordinator) {
    // Fallback to single shard per prefix
    return namespace ? `${namespace}:${basePrefix}` : basePrefix
  }
  try {
    return await coordinator.getRoutingShard(basePrefix, namespace, hashKey)
  } catch (err) {
    logError(log, 'Failed to get routing shard', err, { basePrefix, namespace, hashKey })
    return namespace ? `${namespace}:${basePrefix}` : basePrefix
  }
}

/**
 * Get all active subscription shards for a namespace (across all prefixes)
 */
export async function getAllSubscriptionShards(
  env: Env,
  namespace?: string
): Promise<string[]> {
  const coordinator = getSubscriptionShardCoordinator(env)
  if (!coordinator) {
    // Fallback to known prefixes with single shard each
    return KNOWN_BASE_PREFIXES.map(prefix =>
      namespace ? `${namespace}:${prefix}` : prefix
    )
  }
  try {
    return await coordinator.getAllActiveShards(namespace)
  } catch (err) {
    logError(log, 'Failed to get all subscription shards', err, { namespace })
    return KNOWN_BASE_PREFIXES.map(prefix =>
      namespace ? `${namespace}:${prefix}` : prefix
    )
  }
}
