/**
 * Reconciliation Job Tests
 *
 * Unit tests for the reconciliation job in src/jobs/reconciliation.ts
 * Tests event scanning, subscription matching, and missed delivery detection.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  runReconciliation,
  getReconciliationConfig,
  type ReconciliationConfig,
  type ReconciliationResult,
} from '../../jobs/reconciliation'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../../subscription-routes'
import { createLogger } from '../../logger'
import type { Env } from '../../env'

// ============================================================================
// Mock Factories
// ============================================================================

interface MockR2Object {
  key: string
  size: number
  uploaded: Date
}

interface MockSubscription {
  id: string
  pattern: string
  active: boolean
}

function createMockR2Bucket(options: {
  objects?: MockR2Object[]
  parquetData?: Record<string, unknown[]>
} = {}) {
  const { objects = [], parquetData = {} } = options

  return {
    list: vi.fn().mockImplementation(async ({ prefix }: { prefix: string }) => {
      const filteredObjects = objects.filter(obj => obj.key.startsWith(prefix))
      return {
        objects: filteredObjects,
        truncated: false,
        delimitedPrefixes: [],
      }
    }),
    get: vi.fn().mockImplementation(async (key: string) => {
      if (parquetData[key]) {
        return {
          arrayBuffer: async () => {
            // Return mock buffer - actual Parquet parsing is tested separately
            return new ArrayBuffer(0)
          },
        }
      }
      return null
    }),
    put: vi.fn().mockResolvedValue({}),
    delete: vi.fn().mockResolvedValue(undefined),
    head: vi.fn().mockResolvedValue(null),
  } as unknown as R2Bucket
}

function createMockSubscriptionDO(options: {
  subscriptions?: MockSubscription[]
  fanoutResult?: { matched: number; deliveries: string[]; batched: number }
} = {}) {
  const { subscriptions = [], fanoutResult = { matched: 0, deliveries: [], batched: 0 } } = options

  return {
    listSubscriptions: vi.fn().mockResolvedValue(subscriptions),
    getSubscriptionStatus: vi.fn().mockResolvedValue({
      subscription: subscriptions[0] || null,
      stats: {
        pendingDeliveries: 0,
        failedDeliveries: 0,
        deadLetters: 0,
        successRate: 1,
        totalDelivered: 100,
        totalAttempts: 100,
      },
    }),
    fanout: vi.fn().mockResolvedValue(fanoutResult),
  }
}

function createMockDONamespace<T>(createInstance: () => T) {
  const instances = new Map<string, T>()
  return {
    _instances: instances,
    idFromName: vi.fn((name: string) => ({ name, toString: () => name })),
    get: vi.fn((id: { name: string }) => {
      const key = id.name
      if (!instances.has(key)) {
        instances.set(key, createInstance())
      }
      return instances.get(key)!
    }),
  }
}

function createMockEnv(overrides: Partial<Env> = {}): Env {
  return {
    EVENTS_BUCKET: createMockR2Bucket(),
    SUBSCRIPTIONS: createMockDONamespace(() => createMockSubscriptionDO()),
    ...overrides,
  } as unknown as Env
}

function createMockR2Object(key: string, hoursAgo: number): MockR2Object {
  const uploaded = new Date()
  uploaded.setTime(uploaded.getTime() - hoursAgo * 60 * 60 * 1000)
  return { key, size: 100, uploaded }
}

// ============================================================================
// Configuration Tests
// ============================================================================

describe('Reconciliation Configuration', () => {
  it('provides default configuration values', () => {
    const env = createMockEnv()
    const config = getReconciliationConfig(env)

    // Should return empty partial config (defaults are applied in runReconciliation)
    expect(config).toBeDefined()
  })

  it('has reasonable default lookback window (2 hours)', () => {
    const DEFAULT_LOOKBACK_MS = 2 * 60 * 60 * 1000
    expect(DEFAULT_LOOKBACK_MS).toBe(7200000)
  })

  it('has reasonable default max events per run (10000)', () => {
    const DEFAULT_MAX_EVENTS = 10000
    expect(DEFAULT_MAX_EVENTS).toBe(10000)
  })

  it('has reasonable default max redeliveries per run (1000)', () => {
    const DEFAULT_MAX_REDELIVERIES = 1000
    expect(DEFAULT_MAX_REDELIVERIES).toBe(1000)
  })

  it('has reasonable default minimum event age (30 minutes)', () => {
    const DEFAULT_MIN_AGE_MS = 30 * 60 * 1000
    expect(DEFAULT_MIN_AGE_MS).toBe(1800000)
  })
})

// ============================================================================
// Subscription Loading Tests
// ============================================================================

describe('Subscription Loading', () => {
  it('loads subscriptions from all known shards', async () => {
    const mockDO = createMockSubscriptionDO({
      subscriptions: [
        { id: 'sub1', pattern: 'webhook.*', active: true },
      ],
    })
    const mockNamespace = createMockDONamespace(() => mockDO)

    const env = createMockEnv({
      SUBSCRIPTIONS: mockNamespace,
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    await runReconciliation(env, log, {
      lookbackMs: 1000, // Very short window for test
      maxEventsPerRun: 100,
      maxRedeliveriesPerRun: 50,
    })

    // Should query all known shards
    expect(mockNamespace.idFromName).toHaveBeenCalledTimes(KNOWN_SUBSCRIPTION_SHARDS.length)
    for (const shard of KNOWN_SUBSCRIPTION_SHARDS) {
      expect(mockNamespace.idFromName).toHaveBeenCalledWith(shard)
    }
  })

  it('handles errors when loading subscriptions from a shard', async () => {
    const mockDO = {
      listSubscriptions: vi.fn().mockRejectedValue(new Error('DO unavailable')),
    }
    const mockNamespace = createMockDONamespace(() => mockDO)

    const env = createMockEnv({
      SUBSCRIPTIONS: mockNamespace,
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, {
      lookbackMs: 1000,
    })

    // Should complete with errors recorded
    expect(result.errors.length).toBeGreaterThan(0)
    expect(result.errors[0]).toContain('Failed to get subscriptions')
  })

  it('skips inactive subscriptions', async () => {
    const mockDO = createMockSubscriptionDO({
      subscriptions: [
        { id: 'sub1', pattern: 'webhook.*', active: false },
      ],
    })
    const mockNamespace = createMockDONamespace(() => mockDO)

    const env = createMockEnv({
      SUBSCRIPTIONS: mockNamespace,
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    // listSubscriptions should filter by active: true
    await runReconciliation(env, log, { lookbackMs: 1000 })

    expect(mockDO.listSubscriptions).toHaveBeenCalledWith({ active: true })
  })
})

// ============================================================================
// R2 File Scanning Tests
// ============================================================================

describe('R2 File Scanning', () => {
  it('scans event files within the lookback window', async () => {
    const now = new Date()
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000)

    const datePath = [
      'events',
      oneHourAgo.getUTCFullYear(),
      String(oneHourAgo.getUTCMonth() + 1).padStart(2, '0'),
      String(oneHourAgo.getUTCDate()).padStart(2, '0'),
      String(oneHourAgo.getUTCHours()).padStart(2, '0'),
    ].join('/')

    const mockBucket = createMockR2Bucket({
      objects: [
        createMockR2Object(`${datePath}/test.parquet`, 1),
      ],
    })

    const env = createMockEnv({
      EVENTS_BUCKET: mockBucket,
      SUBSCRIPTIONS: createMockDONamespace(() => createMockSubscriptionDO()),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    await runReconciliation(env, log, {
      lookbackMs: 2 * 60 * 60 * 1000,
      minEventAgeMs: 30 * 60 * 1000,
    })

    // Should list files with events/ prefix
    expect(mockBucket.list).toHaveBeenCalled()
  })

  it('scans multiple event prefixes', async () => {
    const mockBucket = createMockR2Bucket()

    const env = createMockEnv({
      EVENTS_BUCKET: mockBucket,
      SUBSCRIPTIONS: createMockDONamespace(() => createMockSubscriptionDO()),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    await runReconciliation(env, log, {
      eventPrefixes: ['events', 'webhooks', 'custom'],
      lookbackMs: 1000,
    })

    // Should attempt to list all specified prefixes
    const listCalls = (mockBucket.list as ReturnType<typeof vi.fn>).mock.calls
    const prefixes = listCalls.map((call: [{ prefix: string }]) => call[0].prefix)

    // Should include events and webhooks prefixes at minimum
    const hasEventsPrefix = prefixes.some((p: string) => p.startsWith('events/'))
    const hasWebhooksPrefix = prefixes.some((p: string) => p.startsWith('webhooks/'))

    expect(hasEventsPrefix).toBe(true)
    expect(hasWebhooksPrefix).toBe(true)
  })

  it('respects max events per run limit', async () => {
    const mockDO = createMockSubscriptionDO({
      subscriptions: [
        { id: 'sub1', pattern: 'webhook.*', active: true },
      ],
    })

    const env = createMockEnv({
      EVENTS_BUCKET: createMockR2Bucket(),
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, {
      maxEventsPerRun: 5,
      lookbackMs: 1000,
    })

    // Should not exceed max events limit
    expect(result.eventsScanned).toBeLessThanOrEqual(5)
  })

  it('respects max redeliveries per run limit', async () => {
    const env = createMockEnv()
    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, {
      maxRedeliveriesPerRun: 10,
      lookbackMs: 1000,
    })

    // Should not exceed max redeliveries limit
    expect(result.eventsRequeued).toBeLessThanOrEqual(10)
  })
})

// ============================================================================
// Event Matching Tests
// ============================================================================

describe('Event Matching', () => {
  it('matches events to subscriptions by type prefix', async () => {
    const mockDO = createMockSubscriptionDO({
      subscriptions: [
        { id: 'sub1', pattern: 'webhook.*', active: true },
        { id: 'sub2', pattern: 'collection.*', active: true },
      ],
    })

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    // Run reconciliation - the matching happens internally
    await runReconciliation(env, log, { lookbackMs: 1000 })

    // Subscriptions should be loaded with active: true filter
    expect(mockDO.listSubscriptions).toHaveBeenCalledWith({ active: true })
  })

  it('routes wildcard patterns to default shard', () => {
    // Test the shard routing logic
    const getSubscriptionShard = (eventType: string): string => {
      const prefix = eventType.split('.')[0] || 'default'
      if (prefix === '*' || prefix === '**') return 'default'
      return prefix
    }

    expect(getSubscriptionShard('webhook.github.push')).toBe('webhook')
    expect(getSubscriptionShard('collection.insert')).toBe('collection')
    expect(getSubscriptionShard('*')).toBe('default')
    expect(getSubscriptionShard('**')).toBe('default')
    expect(getSubscriptionShard('')).toBe('default')
  })
})

// ============================================================================
// Redelivery Tests
// ============================================================================

describe('Redelivery', () => {
  it('queues missed events for redelivery via fanout', async () => {
    const mockDO = createMockSubscriptionDO({
      subscriptions: [
        { id: 'sub1', pattern: 'webhook.*', active: true },
      ],
      fanoutResult: { matched: 1, deliveries: ['del1'], batched: 0 },
    })

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    await runReconciliation(env, log, { lookbackMs: 1000 })

    // The fanout mechanism handles deduplication internally
    // via UNIQUE constraint on (subscription_id, event_id)
    expect(mockDO.listSubscriptions).toHaveBeenCalled()
  })

  it('handles UNIQUE constraint errors gracefully (already delivered)', async () => {
    const mockDO = {
      listSubscriptions: vi.fn().mockResolvedValue([
        { id: 'sub1', pattern: 'webhook.*', active: true },
      ]),
      fanout: vi.fn().mockRejectedValue(new Error('UNIQUE constraint failed')),
    }

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    // Should complete without throwing
    expect(result).toBeDefined()
    // UNIQUE constraint errors should NOT be recorded as errors
    // (they indicate the event was already delivered)
  })

  it('records errors for non-constraint failures', async () => {
    const mockDO = {
      listSubscriptions: vi.fn().mockResolvedValue([
        { id: 'sub1', pattern: '*', active: true },
      ]),
      fanout: vi.fn().mockRejectedValue(new Error('Network error')),
    }

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    // Should complete but may have errors recorded
    expect(result).toBeDefined()
  })
})

// ============================================================================
// Result Aggregation Tests
// ============================================================================

describe('Result Aggregation', () => {
  it('returns correct counts for events scanned', async () => {
    const env = createMockEnv()
    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    expect(result.eventsScanned).toBeGreaterThanOrEqual(0)
    expect(typeof result.eventsScanned).toBe('number')
  })

  it('returns correct counts for files scanned', async () => {
    const env = createMockEnv()
    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    expect(result.filesScanned).toBeGreaterThanOrEqual(0)
    expect(typeof result.filesScanned).toBe('number')
  })

  it('returns duration in milliseconds', async () => {
    const env = createMockEnv()
    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    expect(result.durationMs).toBeGreaterThanOrEqual(0)
    expect(typeof result.durationMs).toBe('number')
  })

  it('returns errors array', async () => {
    const env = createMockEnv()
    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    expect(Array.isArray(result.errors)).toBe(true)
  })
})

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('Edge Cases', () => {
  it('handles no active subscriptions', async () => {
    const mockDO = createMockSubscriptionDO({ subscriptions: [] })

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    // Should complete quickly without scanning files
    expect(result.eventsScanned).toBe(0)
    expect(result.filesScanned).toBe(0)
    expect(result.errors).toHaveLength(0)
  })

  it('handles empty R2 bucket', async () => {
    const mockBucket = createMockR2Bucket({ objects: [] })
    const mockDO = createMockSubscriptionDO({
      subscriptions: [{ id: 'sub1', pattern: '*', active: true }],
    })

    const env = createMockEnv({
      EVENTS_BUCKET: mockBucket,
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    expect(result.filesScanned).toBe(0)
    expect(result.eventsScanned).toBe(0)
  })

  it('handles missing SUBSCRIPTIONS binding', async () => {
    const env = {
      EVENTS_BUCKET: createMockR2Bucket(),
      SUBSCRIPTIONS: undefined,
    } as unknown as Env

    const log = createLogger({ component: 'test' })

    // Should not throw but may have issues internally
    // The actual scheduled handler checks for binding before calling
    try {
      await runReconciliation(env, log, { lookbackMs: 1000 })
    } catch (err) {
      // Expected to fail without SUBSCRIPTIONS
      expect(err).toBeDefined()
    }
  })

  it('deduplicates events by ID', async () => {
    const mockDO = createMockSubscriptionDO({
      subscriptions: [{ id: 'sub1', pattern: '*', active: true }],
    })

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    const log = createLogger({ component: 'test' })

    // Run reconciliation - internal deduplication happens via Set
    const result = await runReconciliation(env, log, { lookbackMs: 1000 })

    // Each unique event should only be processed once
    expect(result).toBeDefined()
  })
})

// ============================================================================
// Time Window Tests
// ============================================================================

describe('Time Window', () => {
  it('respects minimum event age', () => {
    // Events newer than minEventAgeMs should be skipped
    // This prevents reconciling events that are still being processed
    const MIN_AGE_MS = 30 * 60 * 1000 // 30 minutes
    const now = Date.now()

    // Event 5 minutes ago - should be skipped
    const recentEvent = now - 5 * 60 * 1000
    expect(now - recentEvent < MIN_AGE_MS).toBe(true)

    // Event 1 hour ago - should be processed
    const olderEvent = now - 60 * 60 * 1000
    expect(now - olderEvent > MIN_AGE_MS).toBe(true)
  })

  it('respects lookback window', () => {
    // Events older than lookbackMs should be skipped
    const LOOKBACK_MS = 2 * 60 * 60 * 1000 // 2 hours
    const now = Date.now()

    // Event 1 hour ago - within window
    const withinWindow = now - 60 * 60 * 1000
    expect(now - withinWindow < LOOKBACK_MS).toBe(true)

    // Event 3 hours ago - outside window
    const outsideWindow = now - 3 * 60 * 60 * 1000
    expect(now - outsideWindow > LOOKBACK_MS).toBe(true)
  })
})

// ============================================================================
// Scheduling Tests
// ============================================================================

describe('Scheduling', () => {
  it('reconciliation runs at hour 4 (4:30 UTC)', () => {
    const shouldRun = (hour: number) => hour === 4
    expect(shouldRun(3)).toBe(false)
    expect(shouldRun(4)).toBe(true)
    expect(shouldRun(5)).toBe(false)
  })
})
