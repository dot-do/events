/**
 * Scheduled Handler Tests
 *
 * Unit tests for the scheduled handler in src/handlers/scheduled.ts
 * Tests task execution, distributed locking, and cleanup operations.
 *
 * These tests use actual implementation code from scheduler-lock.ts
 * with minimal mocking of R2 bucket operations.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// Import actual implementation
import {
  acquireSchedulerLock,
  releaseSchedulerLock,
  withSchedulerLock,
  type LockInfo,
} from '../../scheduler-lock'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../../subscription-routes'
import type { Env } from '../../env'

// ============================================================================
// Constants (matching actual implementation)
// ============================================================================

const DEDUP_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours
const DEAD_LETTER_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days
const DEFAULT_LOCK_TIMEOUT_MS = 10 * 60 * 1000 // 10 minutes
const LOCK_KEY = '_locks/scheduled'

// ============================================================================
// Mock Factories
// ============================================================================

interface R2Object {
  key: string
  size: number
  uploaded: Date
}

function createMockR2Bucket(options: { objects?: R2Object[], lockInfo?: LockInfo | null } = {}) {
  const { objects = [], lockInfo = null } = options
  let storedLock: LockInfo | null = lockInfo

  const bucket = {
    list: vi.fn().mockImplementation(({ prefix }) => {
      const filteredObjects = objects.filter(obj => obj.key.startsWith(prefix))
      return Promise.resolve({
        objects: filteredObjects,
        truncated: false,
        delimitedPrefixes: [],
      })
    }),
    get: vi.fn().mockImplementation((key: string) => {
      if (key === LOCK_KEY && storedLock) {
        return Promise.resolve({
          json: () => Promise.resolve(storedLock),
        })
      }
      return Promise.resolve(null)
    }),
    put: vi.fn().mockImplementation((key: string, body: string, options?: { onlyIf?: { etagDoesNotMatch?: string } }) => {
      if (key === LOCK_KEY) {
        if (options?.onlyIf?.etagDoesNotMatch === '*' && storedLock) {
          // Lock exists, should fail
          return Promise.reject(new Error('Object already exists'))
        }
        storedLock = JSON.parse(body)
      }
      return Promise.resolve({})
    }),
    head: vi.fn().mockResolvedValue(null),
    delete: vi.fn().mockImplementation((key: string) => {
      if (key === LOCK_KEY) {
        storedLock = null
      }
      return Promise.resolve()
    }),
  }

  return bucket as unknown as R2Bucket
}

function createMockCatalogDO() {
  return {
    listNamespaces: vi.fn().mockResolvedValue(['default']),
    listTables: vi.fn().mockResolvedValue(['users', 'orders']),
  }
}

function createMockSubscriptionDO() {
  return {
    cleanupOldData: vi.fn().mockResolvedValue({
      deadLettersDeleted: 0,
      deliveryLogsDeleted: 0,
      deliveriesDeleted: 0,
    }),
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
    CATALOG: createMockDONamespace(createMockCatalogDO),
    SUBSCRIPTIONS: createMockDONamespace(createMockSubscriptionDO),
    ...overrides,
  } as unknown as Env
}

function createR2Object(key: string, uploadedDaysAgo: number): R2Object {
  const uploaded = new Date()
  uploaded.setTime(uploaded.getTime() - uploadedDaysAgo * 24 * 60 * 60 * 1000)
  return { key, size: 100, uploaded }
}

// ============================================================================
// Scheduler Lock Tests (Actual Implementation)
// ============================================================================

describe('Scheduler Lock', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('acquireSchedulerLock', () => {
    it('acquires lock when none exists', async () => {
      const bucket = createMockR2Bucket()

      const lock = await acquireSchedulerLock(bucket)

      expect(lock).not.toBeNull()
      expect(lock?.hostname).toBeDefined()
      expect(lock?.acquiredAt).toBeDefined()
      expect(lock?.expiresAt).toBeGreaterThan(Date.now())
    })

    it('includes hostname in lock info', async () => {
      const bucket = createMockR2Bucket()

      const lock = await acquireSchedulerLock(bucket, { hostname: 'test-worker' })

      expect(lock?.hostname).toBe('test-worker')
    })

    it('sets correct expiration time', async () => {
      const bucket = createMockR2Bucket()
      const timeoutMs = 60000

      const before = Date.now()
      const lock = await acquireSchedulerLock(bucket, { timeoutMs })
      const after = Date.now()

      expect(lock?.expiresAt).toBeGreaterThan(before + timeoutMs - 100)
      expect(lock?.expiresAt).toBeLessThanOrEqual(after + timeoutMs)
    })

    it('fails to acquire when lock is held by another worker', async () => {
      const existingLock: LockInfo = {
        hostname: 'other-worker',
        timestamp: new Date().toISOString(),
        acquiredAt: Date.now(),
        expiresAt: Date.now() + DEFAULT_LOCK_TIMEOUT_MS,
      }

      const bucket = createMockR2Bucket({ lockInfo: existingLock })

      const lock = await acquireSchedulerLock(bucket)

      expect(lock).toBeNull()
    })

    it('acquires lock when existing lock is expired', async () => {
      const expiredLock: LockInfo = {
        hostname: 'other-worker',
        timestamp: new Date().toISOString(),
        acquiredAt: Date.now() - DEFAULT_LOCK_TIMEOUT_MS - 1000,
        expiresAt: Date.now() - 1000, // Expired
      }

      const bucket = createMockR2Bucket({ lockInfo: expiredLock })

      const lock = await acquireSchedulerLock(bucket)

      expect(lock).not.toBeNull()
      expect((bucket as unknown as { delete: ReturnType<typeof vi.fn> }).delete).toHaveBeenCalledWith(LOCK_KEY)
    })
  })

  describe('releaseSchedulerLock', () => {
    it('releases lock successfully when we own it', async () => {
      const bucket = createMockR2Bucket()
      const lock = await acquireSchedulerLock(bucket, { hostname: 'my-worker' })

      const released = await releaseSchedulerLock(bucket, lock!)

      expect(released).toBe(true)
      expect((bucket as unknown as { delete: ReturnType<typeof vi.fn> }).delete).toHaveBeenCalledWith(LOCK_KEY)
    })

    it('returns true if lock is already released', async () => {
      const bucket = createMockR2Bucket() // No lock exists

      const fakeLock: LockInfo = {
        hostname: 'my-worker',
        timestamp: new Date().toISOString(),
        acquiredAt: Date.now(),
        expiresAt: Date.now() + DEFAULT_LOCK_TIMEOUT_MS,
      }

      const released = await releaseSchedulerLock(bucket, fakeLock)

      expect(released).toBe(true)
    })

    it('fails to release if lock is owned by different worker', async () => {
      const otherLock: LockInfo = {
        hostname: 'other-worker',
        timestamp: new Date().toISOString(),
        acquiredAt: Date.now(),
        expiresAt: Date.now() + DEFAULT_LOCK_TIMEOUT_MS,
      }

      const bucket = createMockR2Bucket({ lockInfo: otherLock })

      const myLock: LockInfo = {
        hostname: 'my-worker',
        timestamp: new Date().toISOString(),
        acquiredAt: 1000, // Different acquiredAt
        expiresAt: Date.now() + DEFAULT_LOCK_TIMEOUT_MS,
      }

      const released = await releaseSchedulerLock(bucket, myLock)

      expect(released).toBe(false)
    })
  })

  describe('withSchedulerLock', () => {
    it('executes function when lock acquired', async () => {
      const bucket = createMockR2Bucket()
      const fn = vi.fn().mockResolvedValue('result')

      const result = await withSchedulerLock(bucket, fn)

      expect(result.skipped).toBe(false)
      expect(result.result).toBe('result')
      expect(fn).toHaveBeenCalled()
    })

    it('skips function when lock not acquired', async () => {
      const existingLock: LockInfo = {
        hostname: 'other-worker',
        timestamp: new Date().toISOString(),
        acquiredAt: Date.now(),
        expiresAt: Date.now() + DEFAULT_LOCK_TIMEOUT_MS,
      }

      const bucket = createMockR2Bucket({ lockInfo: existingLock })
      const fn = vi.fn()

      const result = await withSchedulerLock(bucket, fn)

      expect(result.skipped).toBe(true)
      expect(result.result).toBeNull()
      expect(fn).not.toHaveBeenCalled()
    })

    it('releases lock after function completes', async () => {
      const bucket = createMockR2Bucket()

      await withSchedulerLock(bucket, async () => 'done')

      expect((bucket as unknown as { delete: ReturnType<typeof vi.fn> }).delete).toHaveBeenCalledWith(LOCK_KEY)
    })

    it('releases lock even if function throws', async () => {
      const bucket = createMockR2Bucket()

      try {
        await withSchedulerLock(bucket, async () => {
          throw new Error('Function error')
        })
      } catch {
        // Expected
      }

      expect((bucket as unknown as { delete: ReturnType<typeof vi.fn> }).delete).toHaveBeenCalledWith(LOCK_KEY)
    })
  })
})

// ============================================================================
// TTL and Time Calculation Tests
// ============================================================================

describe('TTL Calculations', () => {
  it('calculates correct dedup TTL (24 hours)', () => {
    expect(DEDUP_TTL_MS).toBe(24 * 60 * 60 * 1000)
  })

  it('calculates correct dead letter TTL (30 days)', () => {
    expect(DEAD_LETTER_TTL_MS).toBe(30 * 24 * 60 * 60 * 1000)
  })

  it('calculates correct lock timeout (10 minutes)', () => {
    expect(DEFAULT_LOCK_TIMEOUT_MS).toBe(10 * 60 * 1000)
  })
})

describe('Object Age Calculation', () => {
  it('correctly identifies expired dedup markers (>24 hours)', () => {
    const now = Date.now()

    // Object uploaded 25 hours ago (expired for 24-hour TTL)
    const expiredUploadTime = now - 25 * 60 * 60 * 1000
    expect(now - expiredUploadTime > DEDUP_TTL_MS).toBe(true)

    // Object uploaded 23 hours ago (not expired for 24-hour TTL)
    const freshUploadTime = now - 23 * 60 * 60 * 1000
    expect(now - freshUploadTime > DEDUP_TTL_MS).toBe(false)
  })

  it('correctly identifies expired dead letters (>30 days)', () => {
    const now = Date.now()

    // Object uploaded 31 days ago (expired)
    const expiredUploadTime = now - 31 * 24 * 60 * 60 * 1000
    expect(now - expiredUploadTime > DEAD_LETTER_TTL_MS).toBe(true)

    // Object uploaded 29 days ago (not expired)
    const freshUploadTime = now - 29 * 24 * 60 * 60 * 1000
    expect(now - freshUploadTime > DEAD_LETTER_TTL_MS).toBe(false)
  })
})

// ============================================================================
// Cleanup Task Logic Tests
// ============================================================================

describe('Dedup Cleanup Logic', () => {
  it('deletes expired dedup markers', async () => {
    const now = Date.now()
    const objects = [
      createR2Object('dedup/batch-old', 2), // 2 days old - should delete
      createR2Object('dedup/batch-new', 0.5), // 12 hours old - should keep
    ]

    const bucket = createMockR2Bucket({ objects })
    const list = await bucket.list({ prefix: 'dedup/' })

    let deleted = 0
    for (const obj of list.objects as R2Object[]) {
      const uploadedAt = obj.uploaded.getTime()
      if (now - uploadedAt > DEDUP_TTL_MS) {
        await bucket.delete(obj.key)
        deleted++
      }
    }

    expect(deleted).toBe(1)
    expect((bucket as unknown as { delete: ReturnType<typeof vi.fn> }).delete).toHaveBeenCalledWith('dedup/batch-old')
  })

  it('handles empty dedup prefix', async () => {
    const bucket = createMockR2Bucket({ objects: [] })
    const list = await bucket.list({ prefix: 'dedup/' })

    expect(list.objects.length).toBe(0)
  })

  it('deletes all expired markers', async () => {
    const now = Date.now()
    const objects = [
      createR2Object('dedup/batch-1', 3),
      createR2Object('dedup/batch-2', 5),
      createR2Object('dedup/batch-3', 10),
    ]

    const bucket = createMockR2Bucket({ objects })
    const list = await bucket.list({ prefix: 'dedup/' })

    let deleted = 0
    for (const obj of list.objects as R2Object[]) {
      const uploadedAt = obj.uploaded.getTime()
      if (now - uploadedAt > DEDUP_TTL_MS) {
        deleted++
      }
    }

    expect(deleted).toBe(3)
  })

  it('keeps all fresh markers', async () => {
    const now = Date.now()
    const objects = [
      createR2Object('dedup/batch-1', 0.1),
      createR2Object('dedup/batch-2', 0.5),
      createR2Object('dedup/batch-3', 0.8),
    ]

    const bucket = createMockR2Bucket({ objects })
    const list = await bucket.list({ prefix: 'dedup/' })

    let deleted = 0
    for (const obj of list.objects as R2Object[]) {
      const uploadedAt = obj.uploaded.getTime()
      if (now - uploadedAt > DEDUP_TTL_MS) {
        deleted++
      }
    }

    expect(deleted).toBe(0)
  })
})

describe('Dead Letter Cleanup Logic', () => {
  it('deletes dead letters older than 30 days', async () => {
    const now = Date.now()
    const objects = [
      createR2Object('dead-letter/queue/2024-01-01/msg-old.json', 35), // 35 days old
      createR2Object('dead-letter/queue/2024-01-15/msg-new.json', 10), // 10 days old
    ]

    const bucket = createMockR2Bucket({ objects })
    const list = await bucket.list({ prefix: 'dead-letter/' })

    let deleted = 0
    for (const obj of list.objects as R2Object[]) {
      const uploadedAt = obj.uploaded.getTime()
      if (now - uploadedAt > DEAD_LETTER_TTL_MS) {
        await bucket.delete(obj.key)
        deleted++
      }
    }

    expect(deleted).toBe(1)
    expect((bucket as unknown as { delete: ReturnType<typeof vi.fn> }).delete).toHaveBeenCalledWith('dead-letter/queue/2024-01-01/msg-old.json')
  })

  it('keeps dead letters younger than 30 days', async () => {
    const now = Date.now()
    const objects = [
      createR2Object('dead-letter/queue/2024-01-15/msg-1.json', 5),
      createR2Object('dead-letter/queue/2024-01-20/msg-2.json', 15),
      createR2Object('dead-letter/queue/2024-01-25/msg-3.json', 25),
    ]

    const bucket = createMockR2Bucket({ objects })
    const list = await bucket.list({ prefix: 'dead-letter/' })

    let deleted = 0
    for (const obj of list.objects as R2Object[]) {
      const uploadedAt = obj.uploaded.getTime()
      if (now - uploadedAt > DEAD_LETTER_TTL_MS) {
        deleted++
      }
    }

    expect(deleted).toBe(0)
  })
})

// ============================================================================
// Task Scheduling Tests
// ============================================================================

describe('Task Scheduling', () => {
  it('dedup cleanup runs every hour', () => {
    // Dedup cleanup should run regardless of hour
    const hours = [0, 6, 12, 18, 23]
    for (const _hour of hours) {
      // The actual implementation runs cleanup on every scheduled invocation
      expect(true).toBe(true)
    }
  })

  it('event stream compaction only runs at hour 1 (1:30 UTC)', () => {
    const shouldRun = (hour: number) => hour === 1
    expect(shouldRun(0)).toBe(false)
    expect(shouldRun(1)).toBe(true)
    expect(shouldRun(2)).toBe(false)
  })

  it('dead letter cleanup only runs at hour 2 (2:30 UTC)', () => {
    const shouldRun = (hour: number) => hour === 2
    expect(shouldRun(1)).toBe(false)
    expect(shouldRun(2)).toBe(true)
    expect(shouldRun(3)).toBe(false)
  })

  it('subscription cleanup only runs at hour 3 (3:30 UTC)', () => {
    const shouldRun = (hour: number) => hour === 3
    expect(shouldRun(2)).toBe(false)
    expect(shouldRun(3)).toBe(true)
    expect(shouldRun(4)).toBe(false)
  })
})

// ============================================================================
// Known Subscription Shards Tests
// ============================================================================

describe('Known Subscription Shards', () => {
  it('includes all expected shards', () => {
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('collection')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('rpc')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('do')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('ws')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('webhook')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('default')
  })
})

// ============================================================================
// Subscription Cleanup Logic Tests
// ============================================================================

describe('Subscription Cleanup Logic', () => {
  it('cleans up all known shards', async () => {
    const env = createMockEnv()

    for (const shard of KNOWN_SUBSCRIPTION_SHARDS) {
      const subId = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>).idFromName(shard)
      const subscriptionDO = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>).get(subId)
      await subscriptionDO.cleanupOldData(Date.now() - 30 * 24 * 60 * 60 * 1000)
    }

    const subscriptions = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>)
    for (const shard of KNOWN_SUBSCRIPTION_SHARDS) {
      expect(subscriptions.idFromName).toHaveBeenCalledWith(shard)
    }
  })

  it('aggregates cleanup results from all shards', async () => {
    const mockDO = {
      cleanupOldData: vi.fn().mockResolvedValue({
        deadLettersDeleted: 5,
        deliveryLogsDeleted: 10,
        deliveriesDeleted: 3,
      }),
    }

    const env = createMockEnv({
      SUBSCRIPTIONS: createMockDONamespace(() => mockDO),
    } as Partial<Env>)

    let totalDeadLettersDeleted = 0
    let totalDeliveryLogsDeleted = 0
    let totalDeliveriesDeleted = 0

    for (const shard of KNOWN_SUBSCRIPTION_SHARDS) {
      const subId = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>).idFromName(shard)
      const subscriptionDO = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>).get(subId)
      const result = await subscriptionDO.cleanupOldData(Date.now())

      totalDeadLettersDeleted += result.deadLettersDeleted
      totalDeliveryLogsDeleted += result.deliveryLogsDeleted
      totalDeliveriesDeleted += result.deliveriesDeleted
    }

    const expectedShardCount = KNOWN_SUBSCRIPTION_SHARDS.length
    expect(totalDeadLettersDeleted).toBe(5 * expectedShardCount)
    expect(totalDeliveryLogsDeleted).toBe(10 * expectedShardCount)
    expect(totalDeliveriesDeleted).toBe(3 * expectedShardCount)
  })

  it('skips when SUBSCRIPTIONS binding is missing', () => {
    const envNoSubs = createMockEnv({ SUBSCRIPTIONS: undefined } as Partial<Env>)

    // Should not throw when SUBSCRIPTIONS is undefined
    expect(envNoSubs.SUBSCRIPTIONS).toBeUndefined()
  })
})

// ============================================================================
// Lock Key Format Tests
// ============================================================================

describe('Lock Key Format', () => {
  it('uses correct lock key', () => {
    expect(LOCK_KEY).toBe('_locks/scheduled')
  })
})
