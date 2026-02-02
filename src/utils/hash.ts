/**
 * Hash utilities for event distribution and ID correlation
 *
 * This module provides simple hash functions used across the codebase for:
 * - Shard distribution in event writers
 * - Consistent routing for subscriptions
 * - ID correlation for logging
 */

/**
 * Simple string hash for distribution and correlation.
 *
 * Uses a djb2-like algorithm that produces consistent,
 * well-distributed hash values across the integer range.
 *
 * @param str - The string to hash
 * @returns A non-negative integer hash value
 *
 * @example
 * // Shard distribution
 * const shardIndex = simpleHash(`${event.ts}:${event.type}`) % shardCount
 *
 * @example
 * // ID correlation for logging
 * const correlationSuffix = simpleHash(fullId).toString(16).slice(0, 4)
 */
export function simpleHash(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}
