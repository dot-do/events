/**
 * Shared sharding utilities for EventWriterDO coordination
 *
 * This module provides common helper functions used by both:
 * - event-writer-do.ts (EventWriterDO router helpers)
 * - shard-coordinator-do.ts (ShardCoordinatorDO)
 * - routes/shards.ts (Shard management routes)
 *
 * These functions abstract the interaction with ShardCoordinatorDO
 * for shard discovery, routing, and coordination.
 */

import type { ShardCoordinatorDO } from '../shard-coordinator-do'
import { logger, logError } from '../logger'

/**
 * Environment type for sharding utilities.
 * Requires the SHARD_COORDINATOR binding for dynamic sharding.
 */
export type ShardingEnv = {
  SHARD_COORDINATOR?: DurableObjectNamespace<ShardCoordinatorDO>
}

/**
 * Get the shard coordinator Durable Object stub.
 *
 * Returns null if SHARD_COORDINATOR is not bound, which indicates
 * dynamic sharding is disabled (fallback to static single shard).
 *
 * @param env - Environment with optional SHARD_COORDINATOR binding
 * @returns ShardCoordinatorDO stub or null if not available
 *
 * @example
 * const coordinator = getShardCoordinator(env)
 * if (coordinator) {
 *   const stats = await coordinator.getStats()
 * }
 */
export function getShardCoordinator(env: ShardingEnv): DurableObjectStub<ShardCoordinatorDO> | null {
  if (!env.SHARD_COORDINATOR) return null
  const id = env.SHARD_COORDINATOR.idFromName('global')
  return env.SHARD_COORDINATOR.get(id)
}

/**
 * Get active shard IDs from the coordinator.
 *
 * Falls back to [0] (single shard) if coordinator is unavailable.
 * This ensures graceful degradation when dynamic sharding is disabled.
 *
 * @param env - Environment with optional SHARD_COORDINATOR binding
 * @returns Array of active shard IDs, sorted ascending
 *
 * @example
 * const shards = await getActiveShards(env)
 * // shards = [0, 1, 2] or [0] if coordinator unavailable
 */
export async function getActiveShards(env: ShardingEnv): Promise<number[]> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) {
    return [0] // Fallback to shard 0 if no coordinator
  }
  try {
    return await coordinator.getActiveShards()
  } catch (err) {
    logError(logger.child({ component: 'Sharding' }), 'Failed to get active shards from coordinator', err)
    return [0]
  }
}

/**
 * Get recommended shard ID for routing a request.
 *
 * The coordinator considers:
 * - Current shard utilization metrics
 * - Backpressure history
 * - Preferred shard hint (for sticky routing)
 *
 * Falls back to preferredShard or 0 if coordinator is unavailable.
 *
 * @param env - Environment with optional SHARD_COORDINATOR binding
 * @param preferredShard - Optional preferred shard ID for sticky routing
 * @returns Recommended shard ID for the request
 *
 * @example
 * // Basic routing
 * const shard = await getRoutingShard(env)
 *
 * @example
 * // Sticky routing with preferred shard
 * const shard = await getRoutingShard(env, lastUsedShard)
 */
export async function getRoutingShard(env: ShardingEnv, preferredShard?: number): Promise<number> {
  const coordinator = getShardCoordinator(env)
  if (!coordinator) {
    return preferredShard ?? 0
  }
  try {
    return await coordinator.getRoutingShard(preferredShard)
  } catch (err) {
    logError(logger.child({ component: 'Sharding' }), 'Failed to get routing shard from coordinator', err, { preferredShard })
    return preferredShard ?? 0
  }
}
