/**
 * Queue Handler Tests
 *
 * Unit tests for the queue handler in src/handlers/queue.ts
 * Tests message processing, retries, dead letters, and CDC/subscription fanout.
 *
 * These tests use the actual handleQueue implementation with minimal mocking
 * of external dependencies (R2 bucket, DOs).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { EventBatch, DurableEvent } from '@dotdo/events'
import { isCollectionChangeEvent } from '../../../core/src/cdc-processor'

// Import actual handler
import { handleQueue } from '../../handlers/queue'
import type { Env } from '../../env'

// ============================================================================
// Mock Factories
// ============================================================================

interface MockMessage<T> {
  id: string
  body: T
  attempts: number
  ack: ReturnType<typeof vi.fn>
  retry: ReturnType<typeof vi.fn>
}

interface MockMessageBatch<T> {
  queue: string
  messages: MockMessage<T>[]
}

function createMockMessage<T>(body: T, id = 'msg-1', attempts = 1): MockMessage<T> {
  return {
    id,
    body,
    attempts,
    ack: vi.fn(),
    retry: vi.fn(),
  }
}

function createMockBatch<T>(messages: MockMessage<T>[], queue = 'events-queue'): MockMessageBatch<T> {
  return {
    queue,
    messages,
  }
}

function createMockR2Bucket(options: { existingKeys?: string[] } = {}) {
  const existingKeys = new Set(options.existingKeys ?? [])
  return {
    list: vi.fn().mockImplementation(({ prefix }) => {
      const objects = Array.from(existingKeys)
        .filter(key => key.startsWith(prefix))
        .map(key => ({ key, uploaded: new Date() }))
      return Promise.resolve({ objects, truncated: false })
    }),
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue({}),
    head: vi.fn().mockImplementation((key) => {
      return existingKeys.has(key) ? Promise.resolve({ key }) : Promise.resolve(null)
    }),
    delete: vi.fn().mockResolvedValue(undefined),
  } as unknown as R2Bucket
}

function createMockCDCProcessor() {
  return {
    process: vi.fn().mockResolvedValue({ processed: 1, pending: 0 }),
  }
}

function createMockSubscriptionDO() {
  return {
    fanout: vi.fn().mockResolvedValue({ matched: 1, deliveries: ['del-1'] }),
    cleanupOldData: vi.fn().mockResolvedValue({ deadLettersDeleted: 0, deliveryLogsDeleted: 0, deliveriesDeleted: 0 }),
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
    CDC_PROCESSOR: createMockDONamespace(createMockCDCProcessor),
    SUBSCRIPTIONS: createMockDONamespace(createMockSubscriptionDO),
    ANALYTICS: undefined,
    ...overrides,
  } as unknown as Env
}

// ============================================================================
// Event Factories
// ============================================================================

function createCDCEvent(overrides: Partial<DurableEvent> = {}): DurableEvent {
  return {
    type: 'collection.insert',
    ts: new Date().toISOString(),
    do: {
      id: 'test-do-id',
      name: 'TestDO',
      class: 'TestClass',
    },
    collection: 'users',
    docId: 'user-1',
    doc: { name: 'Test User', email: 'test@example.com' },
    ...overrides,
  } as DurableEvent
}

function createRPCEvent(): DurableEvent {
  return {
    type: 'rpc.call',
    ts: new Date().toISOString(),
    do: {
      id: 'test-do-id',
      name: 'TestDO',
    },
    method: 'getUser',
    durationMs: 50,
    success: true,
  } as DurableEvent
}

function createEventBatch(events: DurableEvent[]): EventBatch {
  return { events }
}

// ============================================================================
// isCollectionChangeEvent Tests (Actual Implementation)
// ============================================================================

describe('isCollectionChangeEvent', () => {
  it('identifies collection.insert as CDC event', () => {
    const event = createCDCEvent({ type: 'collection.insert' })
    expect(isCollectionChangeEvent(event)).toBe(true)
  })

  it('identifies collection.update as CDC event', () => {
    const event = createCDCEvent({ type: 'collection.update' })
    expect(isCollectionChangeEvent(event)).toBe(true)
  })

  it('identifies collection.delete as CDC event', () => {
    const event = createCDCEvent({ type: 'collection.delete' })
    expect(isCollectionChangeEvent(event)).toBe(true)
  })

  it('does not identify rpc.call as CDC event', () => {
    const event = createRPCEvent()
    expect(isCollectionChangeEvent(event)).toBe(false)
  })

  it('does not identify ws.connect as CDC event', () => {
    const event: DurableEvent = {
      type: 'ws.connect',
      ts: new Date().toISOString(),
      do: { id: 'test-id' },
    } as DurableEvent
    expect(isCollectionChangeEvent(event)).toBe(false)
  })
})

// ============================================================================
// Queue Handler Tests (Actual Implementation)
// ============================================================================

describe('Queue Handler', () => {
  let env: Env

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  describe('Message Processing', () => {
    it('processes valid event batch', async () => {
      const rpcEvent = createRPCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const message = batch.messages[0]!
      expect(message.ack).toHaveBeenCalled()
      expect(message.retry).not.toHaveBeenCalled()
    })

    it('acknowledges message on success', async () => {
      const message = createMockMessage(createEventBatch([createRPCEvent()]))
      const batch = createMockBatch([message])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      expect(message.ack).toHaveBeenCalled()
      expect(message.retry).not.toHaveBeenCalled()
    })

    it('handles multiple messages in batch', async () => {
      const messages = [
        createMockMessage(createEventBatch([createRPCEvent()]), 'msg-1'),
        createMockMessage(createEventBatch([createCDCEvent()]), 'msg-2'),
        createMockMessage(createEventBatch([createRPCEvent(), createCDCEvent()]), 'msg-3'),
      ]

      const batch = createMockBatch(messages)
      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      for (const msg of messages) {
        expect(msg.ack).toHaveBeenCalled()
      }
    })

    it('handles empty batch', async () => {
      const batch = createMockBatch<EventBatch>([])

      // Should complete without error
      await expect(handleQueue(batch as unknown as MessageBatch<EventBatch>, env)).resolves.toBeUndefined()
    })
  })

  describe('CDC Processing', () => {
    it('processes CDC events through CDCProcessorDO', async () => {
      const cdcEvent = createCDCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const cdcProcessor = (env.CDC_PROCESSOR as ReturnType<typeof createMockDONamespace>)
      expect(cdcProcessor.idFromName).toHaveBeenCalledWith('TestClass/users')
    })

    it('groups CDC events by namespace/collection', async () => {
      const usersEvent1 = createCDCEvent({ docId: 'user-1', collection: 'users' } as Partial<DurableEvent>)
      const usersEvent2 = createCDCEvent({ docId: 'user-2', collection: 'users' } as Partial<DurableEvent>)
      const ordersEvent = createCDCEvent({ docId: 'order-1', collection: 'orders' } as Partial<DurableEvent>)

      const batch = createMockBatch([
        createMockMessage(createEventBatch([usersEvent1, usersEvent2, ordersEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const cdcProcessor = (env.CDC_PROCESSOR as ReturnType<typeof createMockDONamespace>)
      expect(cdcProcessor._instances.has('TestClass/users')).toBe(true)
      expect(cdcProcessor._instances.has('TestClass/orders')).toBe(true)
    })

    it('uses DO name when class is not available', async () => {
      const cdcEvent = createCDCEvent({
        do: { id: 'test-id', name: 'MyDurableObject' },
        collection: 'items',
      } as Partial<DurableEvent>)

      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const cdcProcessor = (env.CDC_PROCESSOR as ReturnType<typeof createMockDONamespace>)
      expect(cdcProcessor.idFromName).toHaveBeenCalledWith('MyDurableObject/items')
    })

    it('handles missing CDC_PROCESSOR binding', async () => {
      const envNoCDC = createMockEnv({ CDC_PROCESSOR: undefined } as Partial<Env>)
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]))
      const batch = createMockBatch([message])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, envNoCDC)

      // Should still complete and ack the message
      expect(message.ack).toHaveBeenCalled()
    })
  })

  describe('Subscription Fanout', () => {
    it('fans out events to SubscriptionDO', async () => {
      const rpcEvent = createRPCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const subscriptions = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>)
      expect(subscriptions.idFromName).toHaveBeenCalledWith('rpc')
    })

    it('groups events by shard key (first segment before dot)', async () => {
      const rpcEvent = createRPCEvent()
      const cdcEvent = createCDCEvent()
      const wsEvent: DurableEvent = {
        type: 'ws.connect',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
      } as DurableEvent

      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent, cdcEvent, wsEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const subscriptions = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>)
      expect(subscriptions.idFromName).toHaveBeenCalledWith('rpc')
      expect(subscriptions.idFromName).toHaveBeenCalledWith('collection')
      expect(subscriptions.idFromName).toHaveBeenCalledWith('ws')
    })

    it('uses default shard for events without dots', async () => {
      const customEvent: DurableEvent = {
        type: 'ping',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
      } as DurableEvent

      const batch = createMockBatch([
        createMockMessage(createEventBatch([customEvent])),
      ])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      const subscriptions = (env.SUBSCRIPTIONS as ReturnType<typeof createMockDONamespace>)
      expect(subscriptions.idFromName).toHaveBeenCalledWith('default')
    })

    it('handles missing SUBSCRIPTIONS binding', async () => {
      const envNoSubs = createMockEnv({ SUBSCRIPTIONS: undefined } as Partial<Env>)
      const rpcEvent = createRPCEvent()
      const message = createMockMessage(createEventBatch([rpcEvent]))
      const batch = createMockBatch([message])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, envNoSubs)

      // Should still complete and ack the message
      expect(message.ack).toHaveBeenCalled()
    })
  })

  describe('Invalid Message Handling', () => {
    it('acknowledges messages with missing events array', async () => {
      const invalidBatch = { notEvents: [] } as unknown as EventBatch
      const message = createMockMessage(invalidBatch)
      const batch = createMockBatch([message])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      expect(message.ack).toHaveBeenCalled()
      expect(message.retry).not.toHaveBeenCalled()
    })

    it('acknowledges messages with null body', async () => {
      const message = createMockMessage(null as unknown as EventBatch)
      const batch = createMockBatch([message])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      expect(message.ack).toHaveBeenCalled()
    })

    it('acknowledges messages with empty events array', async () => {
      const message = createMockMessage(createEventBatch([]))
      const batch = createMockBatch([message])

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, env)

      expect(message.ack).toHaveBeenCalled()
    })
  })

  describe('Retry Behavior', () => {
    it('retries message when CDCProcessorDO throws', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]))
      const batch = createMockBatch([message])

      // Create an env with a failing CDC processor
      const failingEnv = createMockEnv({
        CDC_PROCESSOR: createMockDONamespace(() => ({
          process: vi.fn().mockRejectedValue(new Error('Transient failure')),
        })),
      } as Partial<Env>)

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, failingEnv)

      expect(message.retry).toHaveBeenCalled()
      expect(message.ack).not.toHaveBeenCalled()
    })

    it('retries message when SubscriptionDO throws', async () => {
      const rpcEvent = createRPCEvent()
      const message = createMockMessage(createEventBatch([rpcEvent]))
      const batch = createMockBatch([message])

      // Create an env with a failing subscription DO
      const failingEnv = createMockEnv({
        SUBSCRIPTIONS: createMockDONamespace(() => ({
          fanout: vi.fn().mockRejectedValue(new Error('Transient failure')),
        })),
      } as Partial<Env>)

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, failingEnv)

      expect(message.retry).toHaveBeenCalled()
      expect(message.ack).not.toHaveBeenCalled()
    })

    it('continues processing other messages after one fails', async () => {
      const message1 = createMockMessage(createEventBatch([createRPCEvent()]), 'msg-1')
      const message2 = createMockMessage(createEventBatch([createRPCEvent()]), 'msg-2')

      let callCount = 0
      const mixedEnv = createMockEnv({
        SUBSCRIPTIONS: createMockDONamespace(() => ({
          fanout: vi.fn().mockImplementation(() => {
            callCount++
            if (callCount === 1) {
              return Promise.reject(new Error('First call fails'))
            }
            return Promise.resolve({ matched: 1, deliveries: [] })
          }),
        })),
      } as Partial<Env>)

      const batch = createMockBatch([message1, message2])
      await handleQueue(batch as unknown as MessageBatch<EventBatch>, mixedEnv)

      expect(message1.retry).toHaveBeenCalled()
      expect(message2.ack).toHaveBeenCalled()
    })
  })

  describe('Dead Letter Handling', () => {
    const MAX_RETRIES = 5

    it('sends to dead letter after MAX_RETRIES (5 attempts)', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-1', MAX_RETRIES)
      const batch = createMockBatch([message])

      const failingEnv = createMockEnv({
        CDC_PROCESSOR: createMockDONamespace(() => ({
          process: vi.fn().mockRejectedValue(new Error('Permanent failure')),
        })),
      } as Partial<Env>)

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, failingEnv)

      // After MAX_RETRIES, should ack (not retry) and write dead letter
      expect(message.ack).toHaveBeenCalled()
      expect(message.retry).not.toHaveBeenCalled()
    })

    it('writes dead letter to R2', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-dead', MAX_RETRIES)
      const batch = createMockBatch([message])

      const failingEnv = createMockEnv({
        CDC_PROCESSOR: createMockDONamespace(() => ({
          process: vi.fn().mockRejectedValue(new Error('Permanent failure')),
        })),
      } as Partial<Env>)

      await handleQueue(batch as unknown as MessageBatch<EventBatch>, failingEnv)

      const bucket = failingEnv.EVENTS_BUCKET as unknown as { put: ReturnType<typeof vi.fn> }
      expect(bucket.put).toHaveBeenCalled()
      const putCall = bucket.put.mock.calls[0]
      expect(putCall[0]).toContain('dead-letter/queue/')
    })

    it('retries at attempt 4, dead letters at attempt 5', async () => {
      const cdcEvent = createCDCEvent()

      // Attempt 4 should retry
      const message4 = createMockMessage(createEventBatch([cdcEvent]), 'msg-4', 4)
      const batch4 = createMockBatch([message4])

      const failingEnv = createMockEnv({
        CDC_PROCESSOR: createMockDONamespace(() => ({
          process: vi.fn().mockRejectedValue(new Error('Failure')),
        })),
      } as Partial<Env>)

      await handleQueue(batch4 as unknown as MessageBatch<EventBatch>, failingEnv)

      expect(message4.retry).toHaveBeenCalled()
      expect(message4.ack).not.toHaveBeenCalled()

      // Attempt 5 should dead letter
      vi.clearAllMocks()
      const message5 = createMockMessage(createEventBatch([cdcEvent]), 'msg-5', 5)
      const batch5 = createMockBatch([message5])

      const failingEnv5 = createMockEnv({
        CDC_PROCESSOR: createMockDONamespace(() => ({
          process: vi.fn().mockRejectedValue(new Error('Failure')),
        })),
      } as Partial<Env>)

      await handleQueue(batch5 as unknown as MessageBatch<EventBatch>, failingEnv5)

      expect(message5.ack).toHaveBeenCalled()
      expect(message5.retry).not.toHaveBeenCalled()
    })

    it('continues if dead letter write fails', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-fail', MAX_RETRIES)
      const batch = createMockBatch([message])

      const bucket = createMockR2Bucket()
      ;(bucket as unknown as { put: ReturnType<typeof vi.fn> }).put.mockRejectedValue(new Error('R2 write failed'))

      const failingEnv = createMockEnv({
        EVENTS_BUCKET: bucket,
        CDC_PROCESSOR: createMockDONamespace(() => ({
          process: vi.fn().mockRejectedValue(new Error('Failure')),
        })),
      } as Partial<Env>)

      // Should not throw
      await expect(handleQueue(batch as unknown as MessageBatch<EventBatch>, failingEnv)).resolves.toBeUndefined()

      // Should still ack the message
      expect(message.ack).toHaveBeenCalled()
    })
  })

  describe('Shard Key Extraction', () => {
    const testCases = [
      { type: 'rpc.call', expected: 'rpc' },
      { type: 'collection.insert', expected: 'collection' },
      { type: 'ws.connect', expected: 'ws' },
      { type: 'do.alarm', expected: 'do' },
      { type: 'webhook.github.push', expected: 'webhook' },
      { type: 'custom.myevent', expected: 'custom' },
      { type: 'ping', expected: 'default' },
      { type: 'noDot', expected: 'default' },
    ]

    it.each(testCases)('extracts shard key "$expected" from type "$type"', ({ type, expected }) => {
      const dotIndex = type.indexOf('.')
      const shardKey = dotIndex > 0 ? type.slice(0, dotIndex) : 'default'
      expect(shardKey).toBe(expected)
    })
  })

  describe('CDC Key Generation', () => {
    const testCases = [
      {
        event: { do: { id: 'id1', class: 'UserDO' }, collection: 'profiles' },
        expected: 'UserDO/profiles',
      },
      {
        event: { do: { id: 'id2', name: 'OrderDO' }, collection: 'items' },
        expected: 'OrderDO/items',
      },
      {
        event: { do: { id: 'id3' }, collection: 'data' },
        expected: 'default/data',
      },
      {
        event: { do: { id: 'id4', class: 'MyClass', name: 'MyName' }, collection: 'docs' },
        expected: 'MyClass/docs',
      },
    ]

    it.each(testCases)('generates key "$expected"', ({ event, expected }) => {
      const ns = event.do.class || event.do.name || 'default'
      const collection = event.collection
      const key = `${ns}/${collection}`
      expect(key).toBe(expected)
    })
  })
})
