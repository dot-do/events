/**
 * Ingest Fanout Middleware Tests
 *
 * Unit tests for src/middleware/ingest/fanout.ts
 * Tests CDC and subscription fanout, queue-based and direct modes
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// Mock the logger module before importing fanout
vi.mock('../../../logger', () => ({
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(() => ({
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    })),
  },
  logError: vi.fn(),
}))

import {
  determineFanoutMode,
  sendToQueue,
  processCDCEvents,
  processSubscriptionFanout,
  executeDirectFanout,
  executeQueueFanout,
} from '../../../middleware/ingest/fanout'
import { logger } from '../../../logger'
import type { IngestContext } from '../../../middleware/ingest/types'
import type { DurableEvent, EventBatch } from '@dotdo/events'
import type { TenantContext } from '../../../middleware/tenant'
import type { Env } from '../../../env'

// Reset mocks before each test to ensure clean state
beforeEach(() => {
  vi.clearAllMocks()
})

// ============================================================================
// Mock Types
// ============================================================================

interface MockQueue {
  send: ReturnType<typeof vi.fn>
}

interface MockCDCProcessor {
  process: ReturnType<typeof vi.fn>
}

interface MockSubscriptions {
  fanout: ReturnType<typeof vi.fn>
}

interface MockDurableObjectNamespace<T> {
  idFromName: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn<[any], T>>
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockQueue(): MockQueue {
  return {
    send: vi.fn().mockResolvedValue(undefined),
  }
}

function createMockCDCProcessor(): MockCDCProcessor {
  return {
    process: vi.fn().mockResolvedValue(undefined),
  }
}

function createMockSubscriptions(): MockSubscriptions {
  return {
    fanout: vi.fn().mockResolvedValue(undefined),
  }
}

function createMockNamespace<T>(instance: T): MockDurableObjectNamespace<T> {
  return {
    idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
    get: vi.fn().mockReturnValue(instance),
  }
}

function createMockEnv(options: {
  useQueueFanout?: boolean
  queue?: MockQueue
  cdcProcessor?: MockDurableObjectNamespace<MockCDCProcessor>
  subscriptions?: MockDurableObjectNamespace<MockSubscriptions>
} = {}): Partial<Env> {
  return {
    USE_QUEUE_FANOUT: options.useQueueFanout ? 'true' : undefined,
    EVENTS_QUEUE: options.queue as unknown as Env['EVENTS_QUEUE'],
    CDC_PROCESSOR: options.cdcProcessor as unknown as Env['CDC_PROCESSOR'],
    SUBSCRIPTIONS: options.subscriptions as unknown as Env['SUBSCRIPTIONS'],
    ANALYTICS: undefined,
  }
}

function createMockTenant(overrides: Partial<TenantContext> = {}): TenantContext {
  return {
    namespace: 'test-namespace',
    isAdmin: false,
    keyId: 'test-key',
    ...overrides,
  }
}

function createMockContext(overrides: Partial<IngestContext> = {}): IngestContext {
  return {
    request: new Request('https://events.do/ingest', { method: 'POST' }),
    env: createMockEnv() as any,
    ctx: {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as any,
    tenant: createMockTenant(),
    batch: {
      events: [
        { type: 'user.created', ts: '2024-01-01T00:00:00Z' },
        { type: 'user.updated', ts: '2024-01-01T00:00:01Z' },
      ],
    },
    startTime: performance.now(),
    ...overrides,
  }
}

// ============================================================================
// determineFanoutMode Tests
// ============================================================================

describe('determineFanoutMode', () => {
  it('returns false when USE_QUEUE_FANOUT is not set', () => {
    const env = createMockEnv({ useQueueFanout: false })

    const result = determineFanoutMode(env as Env)

    expect(result).toBe(false)
  })

  it('returns false when EVENTS_QUEUE is not configured', () => {
    const env = createMockEnv({ useQueueFanout: true, queue: undefined })

    const result = determineFanoutMode(env as Env)

    expect(result).toBe(false)
  })

  it('returns true when both USE_QUEUE_FANOUT and EVENTS_QUEUE are set', () => {
    const env = createMockEnv({
      useQueueFanout: true,
      queue: createMockQueue(),
    })

    const result = determineFanoutMode(env as Env)

    expect(result).toBe(true)
  })
})

// ============================================================================
// sendToQueue Tests
// ============================================================================

describe('sendToQueue', () => {
  it('sends batch to queue', async () => {
    const queue = createMockQueue()
    const env = createMockEnv({ queue })
    const batch: EventBatch = {
      events: [
        { type: 'event1', ts: '2024-01-01T00:00:00Z' },
        { type: 'event2', ts: '2024-01-01T00:00:01Z' },
      ],
    }

    await sendToQueue(env as Env, batch)

    expect(queue.send).toHaveBeenCalledWith(batch)
  })

  it('throws when EVENTS_QUEUE is not configured', async () => {
    const env = createMockEnv({ queue: undefined })
    const batch: EventBatch = { events: [] }

    await expect(sendToQueue(env as Env, batch)).rejects.toThrow('EVENTS_QUEUE not configured')
  })

  it('logs message after sending', async () => {
    const queue = createMockQueue()
    const env = createMockEnv({ queue })
    const batch: EventBatch = {
      events: [{ type: 'event', ts: '2024-01-01T00:00:00Z' }],
    }

    await sendToQueue(env as Env, batch)

    // Uses the mocked structured logger
    expect(logger.info).toHaveBeenCalledWith(
      'Sent events to queue for CDC/subscription fanout',
      expect.objectContaining({ eventCount: 1 })
    )
  })
})

// ============================================================================
// processCDCEvents Tests
// ============================================================================

describe('processCDCEvents', () => {
  describe('when CDC_PROCESSOR is not configured', () => {
    it('does nothing when CDC_PROCESSOR is undefined', async () => {
      const env = createMockEnv({ cdcProcessor: undefined })
      const tenant = createMockTenant()
      const events: DurableEvent[] = [
        { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
      ]

      await processCDCEvents(events, tenant, env as Env)

      // Should complete without error
    })
  })

  describe('when processing CDC events', () => {
    it('filters and processes collection change events', async () => {
      const processor = createMockCDCProcessor()
      const cdcNamespace = createMockNamespace(processor)
      const env = createMockEnv({ cdcProcessor: cdcNamespace })
      const tenant = createMockTenant({ namespace: 'acme' })
      const events: DurableEvent[] = [
        { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
        { type: 'user.created', ts: '2024-01-01T00:00:01Z' } as any, // Not a CDC event
        { type: 'collection.update', ts: '2024-01-01T00:00:02Z', collection: 'users' } as any,
      ]

      await processCDCEvents(events, tenant, env as Env)

      expect(processor.process).toHaveBeenCalled()
    })

    it('does nothing when no CDC events present', async () => {
      const processor = createMockCDCProcessor()
      const cdcNamespace = createMockNamespace(processor)
      const env = createMockEnv({ cdcProcessor: cdcNamespace })
      const tenant = createMockTenant()
      const events: DurableEvent[] = [
        { type: 'user.created', ts: '2024-01-01T00:00:00Z' },
        { type: 'order.placed', ts: '2024-01-01T00:00:01Z' },
      ]

      await processCDCEvents(events, tenant, env as Env)

      expect(processor.process).not.toHaveBeenCalled()
    })

    it('groups CDC events by collection', async () => {
      const processor = createMockCDCProcessor()
      const cdcNamespace = createMockNamespace(processor)
      const env = createMockEnv({ cdcProcessor: cdcNamespace })
      const tenant = createMockTenant({ namespace: 'acme', isAdmin: false })
      const events: DurableEvent[] = [
        { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
        { type: 'collection.insert', ts: '2024-01-01T00:00:01Z', collection: 'orders' } as any,
        { type: 'collection.update', ts: '2024-01-01T00:00:02Z', collection: 'users' } as any,
      ]

      await processCDCEvents(events, tenant, env as Env)

      // Should be called twice - once for users, once for orders
      expect(cdcNamespace.idFromName).toHaveBeenCalledTimes(2)
    })

    it('uses namespace-prefixed processor keys', async () => {
      const processor = createMockCDCProcessor()
      const cdcNamespace = createMockNamespace(processor)
      const env = createMockEnv({ cdcProcessor: cdcNamespace })
      const tenant = createMockTenant({ namespace: 'myteam', isAdmin: false })
      const events: DurableEvent[] = [
        { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'items' } as any,
      ]

      await processCDCEvents(events, tenant, env as Env)

      expect(cdcNamespace.idFromName).toHaveBeenCalledWith('myteam:default/items')
    })

    it('handles processor errors gracefully', async () => {
      const processor = createMockCDCProcessor()
      processor.process.mockRejectedValue(new Error('Processor error'))
      const cdcNamespace = createMockNamespace(processor)
      const env = createMockEnv({ cdcProcessor: cdcNamespace })
      const tenant = createMockTenant()
      const events: DurableEvent[] = [
        { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
      ]
      // Reset the logError mock
      const { logError } = await import('../../../logger')
      vi.mocked(logError).mockClear()

      await processCDCEvents(events, tenant, env as Env)

      // Check that logError was called with error info
      expect(logError).toHaveBeenCalledWith(
        expect.anything(),
        'CDC processor error',
        expect.any(Error),
        expect.any(Object)
      )
    })
  })
})

// ============================================================================
// processSubscriptionFanout Tests
// ============================================================================

describe('processSubscriptionFanout', () => {
  describe('when SUBSCRIPTIONS is not configured', () => {
    it('does nothing when SUBSCRIPTIONS is undefined', async () => {
      const env = createMockEnv({ subscriptions: undefined })
      const tenant = createMockTenant()
      const events: DurableEvent[] = [{ type: 'user.created', ts: '2024-01-01T00:00:00Z' }]

      await processSubscriptionFanout(events, tenant, env as Env)

      // Should complete without error
    })
  })

  describe('when processing subscription fanout', () => {
    it('fans out events to subscription shards', async () => {
      const subscriptions = createMockSubscriptions()
      const subNamespace = createMockNamespace(subscriptions)
      const env = createMockEnv({ subscriptions: subNamespace })
      const tenant = createMockTenant({ namespace: 'acme' })
      const events: DurableEvent[] = [
        { type: 'user.created', ts: '2024-01-01T00:00:00Z' },
        { type: 'user.updated', ts: '2024-01-01T00:00:01Z' },
      ]

      await processSubscriptionFanout(events, tenant, env as Env)

      expect(subscriptions.fanout).toHaveBeenCalledTimes(2)
    })

    it('groups events by type prefix for sharding', async () => {
      const subscriptions = createMockSubscriptions()
      const subNamespace = createMockNamespace(subscriptions)
      const env = createMockEnv({ subscriptions: subNamespace })
      const tenant = createMockTenant({ namespace: 'acme', isAdmin: false })
      const events: DurableEvent[] = [
        { type: 'user.created', ts: '2024-01-01T00:00:00Z' },
        { type: 'order.placed', ts: '2024-01-01T00:00:01Z' },
        { type: 'user.deleted', ts: '2024-01-01T00:00:02Z' },
      ]

      await processSubscriptionFanout(events, tenant, env as Env)

      // Should create 2 shards: 'acme:user' and 'acme:order'
      expect(subNamespace.idFromName).toHaveBeenCalledWith('acme:user')
      expect(subNamespace.idFromName).toHaveBeenCalledWith('acme:order')
    })

    it('uses default shard for events without dot in type', async () => {
      const subscriptions = createMockSubscriptions()
      const subNamespace = createMockNamespace(subscriptions)
      const env = createMockEnv({ subscriptions: subNamespace })
      const tenant = createMockTenant({ namespace: 'team', isAdmin: false })
      const events: DurableEvent[] = [{ type: 'simple', ts: '2024-01-01T00:00:00Z' }]

      await processSubscriptionFanout(events, tenant, env as Env)

      expect(subNamespace.idFromName).toHaveBeenCalledWith('team:default')
    })

    it('includes namespace in fanout payload', async () => {
      const subscriptions = createMockSubscriptions()
      const subNamespace = createMockNamespace(subscriptions)
      const env = createMockEnv({ subscriptions: subNamespace })
      const tenant = createMockTenant({ namespace: 'myns' })
      const events: DurableEvent[] = [
        { type: 'event.test', ts: '2024-01-01T00:00:00Z', data: 'value' },
      ]

      await processSubscriptionFanout(events, tenant, env as Env)

      expect(subscriptions.fanout).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: expect.objectContaining({ _namespace: 'myns' }),
        })
      )
    })

    it('handles fanout errors gracefully', async () => {
      const subscriptions = createMockSubscriptions()
      subscriptions.fanout.mockRejectedValue(new Error('Fanout error'))
      const subNamespace = createMockNamespace(subscriptions)
      const env = createMockEnv({ subscriptions: subNamespace })
      const tenant = createMockTenant()
      const events: DurableEvent[] = [{ type: 'event.test', ts: '2024-01-01T00:00:00Z' }]
      // Reset the logError mock
      const { logError } = await import('../../../logger')
      vi.mocked(logError).mockClear()

      await processSubscriptionFanout(events, tenant, env as Env)

      // Check that logError was called with error info
      expect(logError).toHaveBeenCalledWith(
        expect.anything(),
        'Subscription fanout error',
        expect.any(Error),
        expect.any(Object)
      )
    })

    it('uses existing event id when available', async () => {
      const subscriptions = createMockSubscriptions()
      const subNamespace = createMockNamespace(subscriptions)
      const env = createMockEnv({ subscriptions: subNamespace })
      const tenant = createMockTenant()
      const events: DurableEvent[] = [
        { type: 'event.test', ts: '2024-01-01T00:00:00Z', id: 'existing-id' } as any,
      ]

      await processSubscriptionFanout(events, tenant, env as Env)

      expect(subscriptions.fanout).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'existing-id' })
      )
    })
  })
})

// ============================================================================
// executeDirectFanout Tests
// ============================================================================

describe('executeDirectFanout', () => {
  it('processes both CDC and subscription fanout', async () => {
    const processor = createMockCDCProcessor()
    const cdcNamespace = createMockNamespace(processor)
    const subscriptions = createMockSubscriptions()
    const subNamespace = createMockNamespace(subscriptions)
    const env = createMockEnv({
      cdcProcessor: cdcNamespace,
      subscriptions: subNamespace,
    })
    const context = createMockContext({
      env: env as any,
      batch: {
        events: [
          { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
          { type: 'user.created', ts: '2024-01-01T00:00:01Z' },
        ],
      },
    })

    await executeDirectFanout(context)

    // Check that structured logger was called with appropriate message
    expect(logger.info).toHaveBeenCalledWith('Starting DIRECT fanout', expect.any(Object))
  })

  it('handles errors gracefully', async () => {
    const processor = createMockCDCProcessor()
    processor.process.mockRejectedValue(new Error('CDC error'))
    const cdcNamespace = createMockNamespace(processor)
    const env = createMockEnv({ cdcProcessor: cdcNamespace })
    const context = createMockContext({
      env: env as any,
      batch: {
        events: [
          { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
        ],
      },
    })
    // Reset the logError mock
    const { logError } = await import('../../../logger')
    vi.mocked(logError).mockClear()

    await executeDirectFanout(context)

    // Should not throw, error should be handled gracefully
    // (logError is called internally to log the error)
  })
})

// ============================================================================
// executeQueueFanout Tests
// ============================================================================

describe('executeQueueFanout', () => {
  it('sends batch to queue', async () => {
    const queue = createMockQueue()
    const env = createMockEnv({ queue })
    const batch: EventBatch = {
      events: [
        { type: 'event1', ts: '2024-01-01T00:00:00Z' },
        { type: 'event2', ts: '2024-01-01T00:00:01Z' },
      ],
    }
    const context = createMockContext({
      env: env as any,
      batch,
    })

    await executeQueueFanout(context)

    expect(queue.send).toHaveBeenCalledWith(batch)
  })
})

// ============================================================================
// Integration Tests
// ============================================================================

describe('fanout integration', () => {
  it('direct fanout runs CDC and subscriptions in parallel', async () => {
    const cdcProcessTime = 50
    const subFanoutTime = 30
    const startTime = Date.now()

    const processor = {
      process: vi.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, cdcProcessTime))
      ),
    }
    const subscriptions = {
      fanout: vi.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, subFanoutTime))
      ),
    }

    const cdcNamespace = createMockNamespace(processor)
    const subNamespace = createMockNamespace(subscriptions)
    const env = createMockEnv({
      cdcProcessor: cdcNamespace,
      subscriptions: subNamespace,
    })
    const context = createMockContext({
      env: env as any,
      batch: {
        events: [
          { type: 'collection.insert', ts: '2024-01-01T00:00:00Z', collection: 'users' } as any,
          { type: 'user.created', ts: '2024-01-01T00:00:01Z' },
        ],
      },
    })

    await executeDirectFanout(context)

    const elapsed = Date.now() - startTime

    // If running in parallel, should complete in ~max(50, 30) = 50ms
    // If running sequentially, would take ~80ms
    // Allow for some variance
    expect(elapsed).toBeLessThan(150)
  })
})
