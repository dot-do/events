/**
 * SubscriptionShardCoordinatorDO Tests
 *
 * Tests for dynamic subscription sharding behavior including:
 * - Shard key generation and parsing
 * - Routing logic
 * - Scaling decisions
 * - Multi-tenant namespace isolation
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Test helpers that mirror coordinator logic
// ============================================================================

/**
 * Build a shard key from components
 * Format: [namespace:]<basePrefix>:<shardIndex>
 */
function buildShardKey(basePrefix: string, shardIndex: number, namespace?: string): string {
  if (namespace) {
    return `${namespace}:${basePrefix}:${shardIndex}`
  }
  return `${basePrefix}:${shardIndex}`
}

/**
 * Parse a shard key into its components
 */
function parseShardKey(shardKey: string): { basePrefix: string; shardIndex: number; namespace: string | undefined } {
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

/**
 * Simple string hash for consistent shard distribution
 */
function simpleHash(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

/**
 * Get routing shard based on hash key
 */
function getRoutingShard(
  basePrefix: string,
  shardCount: number,
  namespace?: string,
  hashKey?: string
): string {
  let shardIndex: number
  if (hashKey) {
    shardIndex = simpleHash(hashKey) % shardCount
  } else {
    shardIndex = Math.floor(Math.random() * shardCount)
  }
  return buildShardKey(basePrefix, shardIndex, namespace)
}

/**
 * Configuration type
 */
interface ShardConfig {
  minShardsPerPrefix: number
  maxShardsPerPrefix: number
  scaleUpThreshold: number
  scaleDownThreshold: number
  cooldownMs: number
}

const DEFAULT_CONFIG: ShardConfig = {
  minShardsPerPrefix: 1,
  maxShardsPerPrefix: 16,
  scaleUpThreshold: 1000,
  scaleDownThreshold: 100,
  cooldownMs: 60_000,
}

/**
 * Scaling decision logic
 */
function shouldScale(
  avgPending: number,
  currentShards: number,
  config: ShardConfig
): { scale: boolean; direction?: 'up' | 'down' } {
  if (avgPending >= config.scaleUpThreshold) {
    if (currentShards < config.maxShardsPerPrefix) {
      return { scale: true, direction: 'up' }
    }
    return { scale: false }
  }

  if (avgPending < config.scaleDownThreshold) {
    if (currentShards > config.minShardsPerPrefix) {
      return { scale: true, direction: 'down' }
    }
    return { scale: false }
  }

  return { scale: false }
}

// ============================================================================
// Tests
// ============================================================================

describe('SubscriptionShardCoordinator Logic', () => {
  describe('Shard Key Building', () => {
    it('builds simple shard keys without namespace', () => {
      expect(buildShardKey('collection', 0)).toBe('collection:0')
      expect(buildShardKey('webhook', 1)).toBe('webhook:1')
      expect(buildShardKey('rpc', 15)).toBe('rpc:15')
    })

    it('builds namespace-prefixed shard keys', () => {
      expect(buildShardKey('collection', 0, 'acme')).toBe('acme:collection:0')
      expect(buildShardKey('webhook', 2, 'beta')).toBe('beta:webhook:2')
      expect(buildShardKey('default', 0, 'tenant123')).toBe('tenant123:default:0')
    })
  })

  describe('Shard Key Parsing', () => {
    it('parses simple shard keys', () => {
      expect(parseShardKey('collection:0')).toEqual({
        basePrefix: 'collection',
        shardIndex: 0,
        namespace: undefined,
      })

      expect(parseShardKey('webhook:5')).toEqual({
        basePrefix: 'webhook',
        shardIndex: 5,
        namespace: undefined,
      })
    })

    it('parses namespace-prefixed shard keys', () => {
      expect(parseShardKey('acme:collection:0')).toEqual({
        basePrefix: 'collection',
        shardIndex: 0,
        namespace: 'acme',
      })

      expect(parseShardKey('tenant123:rpc:3')).toEqual({
        basePrefix: 'rpc',
        shardIndex: 3,
        namespace: 'tenant123',
      })
    })

    it('handles legacy shard keys (no index)', () => {
      expect(parseShardKey('collection')).toEqual({
        basePrefix: 'collection',
        shardIndex: 0,
        namespace: undefined,
      })
    })
  })

  describe('Consistent Hash Routing', () => {
    it('produces consistent shard assignment for same hash key', () => {
      const shardCount = 4
      const hashKey = 'event-12345'

      const shard1 = getRoutingShard('collection', shardCount, undefined, hashKey)
      const shard2 = getRoutingShard('collection', shardCount, undefined, hashKey)
      const shard3 = getRoutingShard('collection', shardCount, undefined, hashKey)

      expect(shard1).toBe(shard2)
      expect(shard2).toBe(shard3)
    })

    it('distributes different hash keys across shards', () => {
      const shardCount = 4
      const shardDistribution = new Map<string, number>()

      // Generate many events and check distribution
      for (let i = 0; i < 1000; i++) {
        const hashKey = `event-${i}`
        const shard = getRoutingShard('collection', shardCount, undefined, hashKey)
        shardDistribution.set(shard, (shardDistribution.get(shard) ?? 0) + 1)
      }

      // Expect all shards to receive some events (with 1000 events across 4 shards)
      expect(shardDistribution.size).toBe(shardCount)

      // Each shard should get roughly 250 events, allow for variance
      for (const count of shardDistribution.values()) {
        expect(count).toBeGreaterThan(150) // At least 15% of expected
        expect(count).toBeLessThan(400)    // At most 40% above expected
      }
    })

    it('includes namespace in routing when provided', () => {
      const shardCount = 2
      const hashKey = 'event-xyz'

      const acmeShard = getRoutingShard('collection', shardCount, 'acme', hashKey)
      const betaShard = getRoutingShard('collection', shardCount, 'beta', hashKey)

      expect(acmeShard).toContain('acme:')
      expect(betaShard).toContain('beta:')

      // Same base shard index due to same hash key
      const acmeParsed = parseShardKey(acmeShard)
      const betaParsed = parseShardKey(betaShard)
      expect(acmeParsed.shardIndex).toBe(betaParsed.shardIndex)
    })
  })

  describe('Scaling Decisions', () => {
    const config = DEFAULT_CONFIG

    it('scales up when pending exceeds threshold', () => {
      const result = shouldScale(1500, 2, config)
      expect(result.scale).toBe(true)
      expect(result.direction).toBe('up')
    })

    it('does not scale up when at max shards', () => {
      const result = shouldScale(1500, config.maxShardsPerPrefix, config)
      expect(result.scale).toBe(false)
    })

    it('scales down when pending is below threshold', () => {
      const result = shouldScale(50, 4, config)
      expect(result.scale).toBe(true)
      expect(result.direction).toBe('down')
    })

    it('does not scale down when at min shards', () => {
      const result = shouldScale(50, config.minShardsPerPrefix, config)
      expect(result.scale).toBe(false)
    })

    it('does not scale when within thresholds', () => {
      const result = shouldScale(500, 3, config)
      expect(result.scale).toBe(false)
    })

    it('respects custom configuration', () => {
      const customConfig: ShardConfig = {
        minShardsPerPrefix: 2,
        maxShardsPerPrefix: 8,
        scaleUpThreshold: 500,
        scaleDownThreshold: 50,
        cooldownMs: 30_000,
      }

      // Should scale up at 500 instead of 1000
      expect(shouldScale(600, 3, customConfig)).toEqual({ scale: true, direction: 'up' })

      // Should not scale down when at custom min
      expect(shouldScale(20, 2, customConfig)).toEqual({ scale: false })
    })
  })

  describe('Multi-tenant Isolation', () => {
    it('generates separate shard keys per tenant', () => {
      const basePrefix = 'webhook'
      const shardIndex = 0

      const acmeKey = buildShardKey(basePrefix, shardIndex, 'acme')
      const betaKey = buildShardKey(basePrefix, shardIndex, 'beta')
      const noNamespaceKey = buildShardKey(basePrefix, shardIndex)

      expect(acmeKey).not.toBe(betaKey)
      expect(acmeKey).not.toBe(noNamespaceKey)
      expect(betaKey).not.toBe(noNamespaceKey)

      expect(acmeKey).toBe('acme:webhook:0')
      expect(betaKey).toBe('beta:webhook:0')
      expect(noNamespaceKey).toBe('webhook:0')
    })

    it('maintains shard isolation across all prefixes', () => {
      const prefixes = ['collection', 'rpc', 'webhook', 'default']
      const namespaces = ['acme', 'beta', 'gamma']

      const allKeys = new Set<string>()

      for (const prefix of prefixes) {
        for (const ns of namespaces) {
          for (let i = 0; i < 4; i++) {
            const key = buildShardKey(prefix, i, ns)
            expect(allKeys.has(key)).toBe(false) // No duplicates
            allKeys.add(key)
          }
        }
      }

      // Should have 4 prefixes * 3 namespaces * 4 shards = 48 unique keys
      expect(allKeys.size).toBe(48)
    })
  })

  describe('Load-based Routing Simulation', () => {
    interface ShardMetrics {
      shardIndex: number
      loadScore: number
    }

    function findLowestLoadShard(metrics: ShardMetrics[]): number {
      if (metrics.length === 0) return 0
      return metrics.reduce((prev, curr) =>
        curr.loadScore < prev.loadScore ? curr : prev
      ).shardIndex
    }

    it('routes to lowest load shard', () => {
      const metrics: ShardMetrics[] = [
        { shardIndex: 0, loadScore: 500 },
        { shardIndex: 1, loadScore: 100 },  // Lowest
        { shardIndex: 2, loadScore: 300 },
        { shardIndex: 3, loadScore: 200 },
      ]

      expect(findLowestLoadShard(metrics)).toBe(1)
    })

    it('returns first shard when all loads are equal', () => {
      const metrics: ShardMetrics[] = [
        { shardIndex: 0, loadScore: 100 },
        { shardIndex: 1, loadScore: 100 },
        { shardIndex: 2, loadScore: 100 },
      ]

      expect(findLowestLoadShard(metrics)).toBe(0)
    })

    it('handles empty metrics by returning shard 0', () => {
      expect(findLowestLoadShard([])).toBe(0)
    })
  })

  describe('Active Shard Generation', () => {
    function getActiveShards(basePrefix: string, shardCount: number, namespace?: string): string[] {
      const shards: string[] = []
      for (let i = 0; i < shardCount; i++) {
        shards.push(buildShardKey(basePrefix, i, namespace))
      }
      return shards
    }

    it('generates correct number of active shards', () => {
      const shards = getActiveShards('collection', 4)
      expect(shards).toHaveLength(4)
      expect(shards).toEqual([
        'collection:0',
        'collection:1',
        'collection:2',
        'collection:3',
      ])
    })

    it('generates namespace-prefixed active shards', () => {
      const shards = getActiveShards('webhook', 2, 'acme')
      expect(shards).toHaveLength(2)
      expect(shards).toEqual([
        'acme:webhook:0',
        'acme:webhook:1',
      ])
    })

    it('returns single shard for count of 1', () => {
      const shards = getActiveShards('rpc', 1)
      expect(shards).toHaveLength(1)
      expect(shards).toEqual(['rpc:0'])
    })
  })

  describe('Shard Key Compatibility', () => {
    it('handles legacy shard keys gracefully', () => {
      // Legacy keys (without shard index) should work with parse
      const legacyKey = 'collection'
      const parsed = parseShardKey(legacyKey)

      expect(parsed.basePrefix).toBe('collection')
      expect(parsed.shardIndex).toBe(0)
      expect(parsed.namespace).toBeUndefined()
    })

    it('handles legacy namespace-prefixed keys', () => {
      // Legacy format: "namespace:prefix" (no shard index)
      // Should be treated as "namespace" prefix with "prefix" index
      const legacyKey = 'acme:collection'
      const parsed = parseShardKey(legacyKey)

      // This would parse as prefix:index format
      expect(parsed.basePrefix).toBe('acme')
      expect(parsed.shardIndex).toBeNaN() // "collection" is not a number
    })

    it('differentiates new vs legacy format correctly', () => {
      // New format always has numeric suffix
      const isNewFormat = (key: string): boolean => {
        const lastPart = key.split(':').pop()
        return /^\d+$/.test(lastPart ?? '')
      }

      expect(isNewFormat('collection:0')).toBe(true)
      expect(isNewFormat('acme:collection:0')).toBe(true)
      expect(isNewFormat('collection')).toBe(false)
      expect(isNewFormat('acme:collection')).toBe(false)
    })
  })
})

describe('Subscription Fanout with Dynamic Sharding', () => {
  describe('Event grouping by prefix', () => {
    function groupEventsByPrefix(events: { type: string }[]): Map<string, { type: string }[]> {
      const eventsByPrefix = new Map<string, { type: string }[]>()

      for (const event of events) {
        const dotIndex = event.type.indexOf('.')
        const basePrefix = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
        const existing = eventsByPrefix.get(basePrefix) ?? []
        existing.push(event)
        eventsByPrefix.set(basePrefix, existing)
      }

      return eventsByPrefix
    }

    it('groups events by first segment of type', () => {
      const events = [
        { type: 'collection.insert.users' },
        { type: 'collection.update.orders' },
        { type: 'webhook.github.push' },
        { type: 'rpc.call.getUser' },
        { type: 'single_segment_event' },
      ]

      const grouped = groupEventsByPrefix(events)

      expect(grouped.size).toBe(4) // collection, webhook, rpc, default
      expect(grouped.get('collection')).toHaveLength(2)
      expect(grouped.get('webhook')).toHaveLength(1)
      expect(grouped.get('rpc')).toHaveLength(1)
      expect(grouped.get('default')).toHaveLength(1) // single_segment_event has no dot
    })

    it('uses default for events without dots', () => {
      const events = [
        { type: 'simple' },
        { type: 'another' },
        { type: 'noprefix' },
      ]

      const grouped = groupEventsByPrefix(events)

      expect(grouped.size).toBe(1)
      expect(grouped.get('default')).toHaveLength(3)
    })
  })

  describe('Consistent routing for retries', () => {
    it('routes retries to same shard using event ID', () => {
      const eventId = 'EVT-12345'
      const shardCount = 8

      // Simulate multiple retry attempts
      const routings: string[] = []
      for (let attempt = 0; attempt < 5; attempt++) {
        const shard = getRoutingShard('webhook', shardCount, 'acme', eventId)
        routings.push(shard)
      }

      // All retries should go to the same shard
      expect(new Set(routings).size).toBe(1)
    })

    it('different events route to potentially different shards', () => {
      const shardCount = 4
      const shards = new Set<string>()

      for (let i = 0; i < 100; i++) {
        const eventId = `EVT-${i}`
        const shard = getRoutingShard('collection', shardCount, undefined, eventId)
        shards.add(shard)
      }

      // With 100 events across 4 shards, we should see all shards used
      expect(shards.size).toBe(shardCount)
    })
  })
})

describe('Backpressure Handling', () => {
  interface BackpressureResult {
    shouldRedirect: boolean
    alternativeShard?: string
  }

  function handleBackpressure(
    currentShard: string,
    allShards: string[],
    shardLoads: Map<string, number>
  ): BackpressureResult {
    // Find the least loaded shard that isn't the current one
    let lowestLoadShard: string | undefined
    let lowestLoad = Infinity

    for (const shard of allShards) {
      if (shard === currentShard) continue
      const load = shardLoads.get(shard) ?? 0
      if (load < lowestLoad) {
        lowestLoad = load
        lowestLoadShard = shard
      }
    }

    if (lowestLoadShard) {
      return { shouldRedirect: true, alternativeShard: lowestLoadShard }
    }

    return { shouldRedirect: false }
  }

  it('redirects to least loaded shard on backpressure', () => {
    const allShards = ['collection:0', 'collection:1', 'collection:2', 'collection:3']
    const shardLoads = new Map([
      ['collection:0', 1000], // Current, overloaded
      ['collection:1', 500],
      ['collection:2', 200],  // Lowest
      ['collection:3', 400],
    ])

    const result = handleBackpressure('collection:0', allShards, shardLoads)

    expect(result.shouldRedirect).toBe(true)
    expect(result.alternativeShard).toBe('collection:2')
  })

  it('does not redirect when only one shard exists', () => {
    const allShards = ['collection:0']
    const shardLoads = new Map([['collection:0', 1000]])

    const result = handleBackpressure('collection:0', allShards, shardLoads)

    expect(result.shouldRedirect).toBe(false)
    expect(result.alternativeShard).toBeUndefined()
  })

  it('picks first available when all others have same load', () => {
    const allShards = ['collection:0', 'collection:1', 'collection:2']
    const shardLoads = new Map([
      ['collection:0', 1000],
      ['collection:1', 500],
      ['collection:2', 500],
    ])

    const result = handleBackpressure('collection:0', allShards, shardLoads)

    expect(result.shouldRedirect).toBe(true)
    expect(['collection:1', 'collection:2']).toContain(result.alternativeShard)
  })
})
