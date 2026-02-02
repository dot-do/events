/**
 * CDCShardCoordinatorDO - Dynamic shard management for CDCProcessorDO
 *
 * Responsibilities:
 * - Track shard utilization metrics from all CDCProcessorDO instances
 * - Provide shard discovery for queries (namespace + collection routing)
 * - Maintain configuration for min/max shards per namespace
 * - Route CDC events to appropriate shard based on collection hash
 *
 * Routing strategy:
 * - Each namespace gets its own set of CDC shards
 * - Events are routed by hash(namespace + collection) to distribute load
 * - Shards are named: CDCProcessor:{namespace}:shard-{shardId}
 */

import { DurableObject } from 'cloudflare:workers'
import type { Env as FullEnv } from './env'
import { logger, sanitize, logError } from './logger'
import { simpleHash } from './utils/hash'

const log = logger.child({ component: 'CDCShardCoordinator' })

export type Env = Pick<FullEnv, 'EVENTS_BUCKET' | 'CDC_PROCESSOR' | 'ANALYTICS' | 'CDC_SHARD_COORDINATOR'>

// ============================================================================
// Configuration
// ============================================================================

export interface CDCShardConfig {
  minShards: number
  maxShards: number
  cooldownMs: number            // Minimum time between scaling operations
  metricsWindowMs: number       // Time window for metrics aggregation
}

const DEFAULT_CONFIG: CDCShardConfig = {
  minShards: 1,
  maxShards: 16,
  cooldownMs: 60_000,           // 1 minute cooldown between scaling
  metricsWindowMs: 30_000,      // 30 second metrics window
}

// Storage keys
const CONFIG_KEY = '_cdcCoordinator:config'
const ACTIVE_SHARDS_KEY = '_cdcCoordinator:activeShards'
const LAST_SCALE_KEY = '_cdcCoordinator:lastScaleTime'
const NAMESPACE_SHARD_COUNTS_KEY = '_cdcCoordinator:namespaceShardCounts'

// ============================================================================
// Types
// ============================================================================

export interface CDCShardMetrics {
  shardId: number
  namespace: string
  pendingDeltas: number
  lastFlushTime: string
  collectionsProcessed: number
  lastReportTime: number
}

export interface CDCShardStats {
  activeShards: number[]
  namespace: string
  shardCount: number
  totalPendingDeltas: number
  metrics: CDCShardMetrics[]
  lastScaleTime: string
}

// ============================================================================
// CDCShardCoordinatorDO
// ============================================================================

export class CDCShardCoordinatorDO extends DurableObject<Env> {
  private config: CDCShardConfig = DEFAULT_CONFIG
  private activeShards: Set<number> = new Set([0]) // Start with shard 0
  private shardMetrics: Map<number, CDCShardMetrics> = new Map()
  private lastScaleTime = 0
  private namespace: string = 'default'
  private initialized = false

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    // Extract namespace from DO name (e.g., "cdc-coordinator:myns" -> "myns")
    const name = ctx.id.name ?? 'default'
    const match = name.match(/^cdc-coordinator:(.+)$/)
    this.namespace = match?.[1] ?? name

    ctx.blockConcurrencyWhile(async () => {
      await this.restore()
    })
  }

  // ──────────────────────────────────────────────────────────────────────────
  // State Management
  // ──────────────────────────────────────────────────────────────────────────

  private async restore(): Promise<void> {
    try {
      const [storedConfig, storedActiveShards, storedLastScale] = await Promise.all([
        this.ctx.storage.get<CDCShardConfig>(CONFIG_KEY),
        this.ctx.storage.get<number[]>(ACTIVE_SHARDS_KEY),
        this.ctx.storage.get<number>(LAST_SCALE_KEY),
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

      this.initialized = true
      log.info('Restored state', { namespace: this.namespace, activeShards: this.activeShards.size })
    } catch (error) {
      logError(log, 'Failed to restore state', error)
      this.initialized = true
    }
  }

  private async persist(): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      await txn.put(CONFIG_KEY, this.config)
      await txn.put(ACTIVE_SHARDS_KEY, Array.from(this.activeShards))
      await txn.put(LAST_SCALE_KEY, this.lastScaleTime)
    })
  }

  // ──────────────────────────────────────────────────────────────────────────
  // RPC Methods
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Get current configuration
   */
  async getConfig(): Promise<CDCShardConfig> {
    return { ...this.config }
  }

  /**
   * Update configuration
   */
  async updateConfig(updates: Partial<CDCShardConfig>): Promise<CDCShardConfig> {
    this.config = { ...this.config, ...updates }
    await this.persist()
    log.info('Config updated', { config: sanitize.payload(this.config) })
    return this.config
  }

  /**
   * Get active shards for this namespace
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
   * Get the shard ID for a given collection.
   * Uses consistent hashing to distribute collections across shards.
   */
  async getShardForCollection(collection: string): Promise<number> {
    const activeShards = Array.from(this.activeShards)
    if (activeShards.length === 0) return 0

    // Hash the collection name to get consistent shard assignment
    const hash = simpleHash(collection)
    const shardIndex = hash % activeShards.length
    return activeShards[shardIndex] ?? 0
  }

  /**
   * Report metrics from a CDCProcessorDO shard
   */
  async reportMetrics(metrics: Omit<CDCShardMetrics, 'lastReportTime'>): Promise<void> {
    this.shardMetrics.set(metrics.shardId, {
      ...metrics,
      lastReportTime: Date.now(),
    })

    // Ensure this shard is marked as active
    if (!this.activeShards.has(metrics.shardId)) {
      this.activeShards.add(metrics.shardId)
      await this.persist()
    }
  }

  /**
   * Get full statistics for monitoring
   */
  async getStats(): Promise<CDCShardStats> {
    const metrics = Array.from(this.shardMetrics.values())
    const activeShards = Array.from(this.activeShards).sort((a, b) => a - b)

    const totalPendingDeltas = metrics.reduce((sum, m) => sum + m.pendingDeltas, 0)

    return {
      activeShards,
      namespace: this.namespace,
      shardCount: this.activeShards.size,
      totalPendingDeltas,
      metrics,
      lastScaleTime: new Date(this.lastScaleTime).toISOString(),
    }
  }

  /**
   * Force scale to a specific shard count
   */
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

    log.info('Force scaled', { namespace: this.namespace, previousCount, targetCount })

    return {
      scaled: true,
      previousCount,
      newCount: this.activeShards.size,
      reason: `Force scaled to ${targetCount} shards`,
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
      const stats = await this.getStats()

      const health = {
        status: 'healthy',
        initialized: this.initialized,
        namespace: this.namespace,
        activeShards: {
          count: this.activeShards.size,
          ids: Array.from(this.activeShards).sort((a, b) => a - b),
          minShards: this.config.minShards,
          maxShards: this.config.maxShards,
        },
        metrics: {
          totalPendingDeltas: stats.totalPendingDeltas,
          shardMetricsCount: this.shardMetrics.size,
          metricsWindowMs: this.config.metricsWindowMs,
        },
        config: {
          minShards: this.config.minShards,
          maxShards: this.config.maxShards,
          cooldownMs: this.config.cooldownMs,
          metricsWindowMs: this.config.metricsWindowMs,
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
// Helper Functions for Routing
// ============================================================================

/**
 * Get the CDC shard coordinator DO stub for a namespace
 */
export function getCDCShardCoordinator(
  env: { CDC_SHARD_COORDINATOR?: DurableObjectNamespace<CDCShardCoordinatorDO> },
  namespace: string
): DurableObjectStub<CDCShardCoordinatorDO> | null {
  if (!env.CDC_SHARD_COORDINATOR) {
    return null
  }
  const id = env.CDC_SHARD_COORDINATOR.idFromName(`cdc-coordinator:${namespace}`)
  return env.CDC_SHARD_COORDINATOR.get(id)
}

/**
 * Get the CDC processor DO stub for a namespace and collection.
 * Routes to the appropriate shard based on collection hash.
 *
 * @param env - Environment with CDC_PROCESSOR binding
 * @param namespace - The namespace for CDC events
 * @param collection - The collection name (used for shard routing)
 * @param shardId - Optional explicit shard ID (overrides automatic routing)
 * @returns The CDC processor DO stub
 */
export async function getCDCProcessorShard(
  env: { CDC_PROCESSOR: DurableObjectNamespace; CDC_SHARD_COORDINATOR?: DurableObjectNamespace<CDCShardCoordinatorDO> },
  namespace: string,
  collection: string,
  shardId?: number
): Promise<DurableObjectStub> {
  let shard = shardId ?? 0

  // If coordinator is available and no explicit shard, get shard from coordinator
  if (shardId === undefined && env.CDC_SHARD_COORDINATOR) {
    const coordinator = getCDCShardCoordinator(env, namespace)
    if (coordinator) {
      try {
        shard = await coordinator.getShardForCollection(collection)
      } catch (err) {
        // Fall back to simple hash-based routing
        const hash = simpleHash(collection)
        shard = hash % 4 // Default to 4 shards when coordinator unavailable
        log.warn('CDC coordinator unavailable, using fallback routing', {
          namespace,
          collection,
          shard,
        })
      }
    }
  } else if (shardId === undefined) {
    // No coordinator, use simple hash-based routing
    const hash = simpleHash(collection)
    shard = hash % 4 // Default to 4 shards
  }

  // Construct the shard name
  const shardName = shard === 0
    ? `CDCProcessor:${namespace}`
    : `CDCProcessor:${namespace}:shard-${shard}`

  const id = env.CDC_PROCESSOR.idFromName(shardName)
  return env.CDC_PROCESSOR.get(id)
}

/**
 * Get active CDC shard IDs for a namespace
 */
export async function getActiveCDCShards(
  env: { CDC_SHARD_COORDINATOR?: DurableObjectNamespace<CDCShardCoordinatorDO> },
  namespace: string
): Promise<number[]> {
  const coordinator = getCDCShardCoordinator(env, namespace)
  if (!coordinator) {
    return [0] // Default to single shard
  }
  try {
    return await coordinator.getActiveShards()
  } catch (err) {
    log.warn('Failed to get CDC shards from coordinator', { namespace })
    return [0]
  }
}
