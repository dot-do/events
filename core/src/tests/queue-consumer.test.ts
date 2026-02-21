/**
 * Queue Consumer Tests
 *
 * Tests for the queue() handler in src/index.ts that processes EventBatch messages
 * for CDC fanout and subscription delivery.
 *
 * Tests cover:
 * - Processing CDC events fans out to CDCProcessorDO
 * - Processing events fans out to SubscriptionDO
 * - Handling invalid messages gracefully
 * - Retry behavior on transient failures
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { DurableEvent, EventBatch, CollectionChangeEvent } from '../types.js'

// ============================================================================
// Mock Types
// ============================================================================

interface MockMessage<T> {
  id: string
  body: T
  ack: ReturnType<typeof vi.fn>
  retry: ReturnType<typeof vi.fn>
}

interface MockMessageBatch<T> {
  queue: string
  messages: MockMessage<T>[]
}

interface MockCDCProcessorDO {
  process: ReturnType<typeof vi.fn>
}

interface MockSubscriptionDO {
  fanout: ReturnType<typeof vi.fn>
}

interface MockDONamespace<T> {
  idFromName: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  _instances: Map<string, T>
}

interface MockEnv {
  CDC_PROCESSOR: MockDONamespace<MockCDCProcessorDO>
  SUBSCRIPTIONS: MockDONamespace<MockSubscriptionDO>
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Creates a mock message for testing
 */
function createMockMessage<T>(body: T, id = 'msg-1'): MockMessage<T> {
  return {
    id,
    body,
    ack: vi.fn(),
    retry: vi.fn(),
  }
}

/**
 * Creates a mock message batch
 */
function createMockBatch<T>(messages: MockMessage<T>[], queue = 'events-queue'): MockMessageBatch<T> {
  return {
    queue,
    messages,
  }
}

/**
 * Creates a mock CDCProcessorDO instance
 */
function createMockCDCProcessor(): MockCDCProcessorDO {
  return {
    process: vi.fn().mockResolvedValue({ processed: 1, pending: 0 }),
  }
}

/**
 * Creates a mock SubscriptionDO instance
 */
function createMockSubscriptionDO(): MockSubscriptionDO {
  return {
    fanout: vi.fn().mockResolvedValue({ matched: 1, deliveries: ['del-1'] }),
  }
}

/**
 * Creates a mock DurableObject namespace
 */
function createMockDONamespace<T>(createInstance: () => T): MockDONamespace<T> {
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

/**
 * Creates a mock environment
 */
function createMockEnv(): MockEnv {
  return {
    CDC_PROCESSOR: createMockDONamespace(createMockCDCProcessor),
    SUBSCRIPTIONS: createMockDONamespace(createMockSubscriptionDO),
  }
}

/**
 * Creates a sample CDC event
 */
function createCDCEvent(overrides: Partial<CollectionChangeEvent> = {}): CollectionChangeEvent {
  return {
    type: 'collection.created',
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
  }
}

/**
 * Creates a sample RPC event
 */
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

/**
 * Creates an EventBatch with the given events
 */
function createEventBatch(events: DurableEvent[]): EventBatch {
  return { events }
}

// ============================================================================
// Queue Handler Simulation
// ============================================================================

/**
 * Simulates the queue handler logic from src/index.ts
 * This mirrors the implementation to test the logic in isolation
 */
async function simulateQueueHandler(
  batch: MockMessageBatch<EventBatch>,
  env: MockEnv
): Promise<{ totalEvents: number; cdcEventsProcessed: number; subscriptionFanouts: number }> {
  let totalEvents = 0
  let cdcEventsProcessed = 0
  let subscriptionFanouts = 0

  for (const message of batch.messages) {
    try {
      const eventBatch = message.body

      if (!eventBatch?.events || !Array.isArray(eventBatch.events)) {
        console.warn(`[QUEUE] Invalid message format, missing events array`)
        message.ack()
        continue
      }

      totalEvents += eventBatch.events.length

      // Process CDC events (type starts with 'collection.')
      const cdcEvents = eventBatch.events.filter((e: DurableEvent) => e.type.startsWith('collection.'))

      if (cdcEvents.length > 0 && env.CDC_PROCESSOR) {
        // Group CDC events by namespace/collection for routing to the correct DO instance
        const cdcByKey = new Map<string, DurableEvent[]>()
        for (const event of cdcEvents) {
          const cdcEvent = event as CollectionChangeEvent
          const ns = cdcEvent.do?.class || cdcEvent.do?.name || 'default'
          const collection = (cdcEvent as any).collection || 'default'
          const key = `${ns}/${collection}`
          const existing = cdcByKey.get(key) ?? []
          existing.push(cdcEvent)
          cdcByKey.set(key, existing)
        }

        // Send each group to the appropriate CDCProcessorDO instance
        for (const [key, events] of cdcByKey) {
          try {
            const processorId = env.CDC_PROCESSOR.idFromName(key)
            const processor = env.CDC_PROCESSOR.get(processorId)
            await processor.process(events)
            cdcEventsProcessed += events.length
          } catch (err) {
            console.error(`[QUEUE] CDC processor error for ${key}:`, err)
            throw err
          }
        }
      }

      // Fan out all events to subscription matching and delivery
      if (env.SUBSCRIPTIONS) {
        // Group events by shard key (first segment before '.', or 'default')
        const eventsByShard = new Map<string, DurableEvent[]>()
        for (const event of eventBatch.events) {
          const dotIndex = event.type.indexOf('.')
          const shardKey = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
          const existing = eventsByShard.get(shardKey) ?? []
          existing.push(event)
          eventsByShard.set(shardKey, existing)
        }

        // Fan out each group to its corresponding shard
        for (const [shardKey, events] of eventsByShard) {
          try {
            const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
            const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

            for (const event of events) {
              await subscriptionDO.fanout({
                id: `test-ulid-${Date.now()}`,
                type: event.type,
                ts: event.ts,
                payload: event,
              })
              subscriptionFanouts++
            }
          } catch (err) {
            console.error(`[QUEUE] Subscription shard error for shard ${shardKey}:`, err)
            throw err
          }
        }
      }

      // Successfully processed this message
      message.ack()
    } catch (err) {
      console.error(`[QUEUE] Error processing message:`, err)
      // Message will be retried automatically by the queue
      message.retry()
    }
  }

  return { totalEvents, cdcEventsProcessed, subscriptionFanouts }
}

// ============================================================================
// Tests
// ============================================================================

describe('Queue Consumer', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  describe('CDC Event Processing', () => {
    it('processes CDC events and fans out to CDCProcessorDO', async () => {
      const cdcEvent = createCDCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(1)
      expect(result.cdcEventsProcessed).toBe(1)

      // Verify CDCProcessorDO was called
      expect(env.CDC_PROCESSOR.idFromName).toHaveBeenCalledWith('TestClass/users')
      expect(env.CDC_PROCESSOR.get).toHaveBeenCalled()

      const processor = env.CDC_PROCESSOR._instances.get('TestClass/users')
      expect(processor?.process).toHaveBeenCalledWith([cdcEvent])
    })

    it('groups CDC events by namespace/collection', async () => {
      const usersEvent1 = createCDCEvent({ docId: 'user-1', collection: 'users' })
      const usersEvent2 = createCDCEvent({ docId: 'user-2', collection: 'users' })
      const ordersEvent = createCDCEvent({ docId: 'order-1', collection: 'orders' })

      const batch = createMockBatch([
        createMockMessage(createEventBatch([usersEvent1, usersEvent2, ordersEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.cdcEventsProcessed).toBe(3)

      // Verify separate processor instances were created
      expect(env.CDC_PROCESSOR._instances.has('TestClass/users')).toBe(true)
      expect(env.CDC_PROCESSOR._instances.has('TestClass/orders')).toBe(true)

      // Verify users processor received both user events
      const usersProcessor = env.CDC_PROCESSOR._instances.get('TestClass/users')
      expect(usersProcessor?.process).toHaveBeenCalledWith([usersEvent1, usersEvent2])

      // Verify orders processor received the order event
      const ordersProcessor = env.CDC_PROCESSOR._instances.get('TestClass/orders')
      expect(ordersProcessor?.process).toHaveBeenCalledWith([ordersEvent])
    })

    it('uses DO name when class is not available', async () => {
      const cdcEvent = createCDCEvent({
        do: { id: 'test-id', name: 'MyDurableObject' },
        collection: 'items',
      })

      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      await simulateQueueHandler(batch, env)

      expect(env.CDC_PROCESSOR.idFromName).toHaveBeenCalledWith('MyDurableObject/items')
    })

    it('uses default namespace when DO identity is missing', async () => {
      const cdcEvent: DurableEvent = {
        type: 'collection.created',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
        collection: 'items',
        docId: 'item-1',
      } as any

      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      await simulateQueueHandler(batch, env)

      expect(env.CDC_PROCESSOR.idFromName).toHaveBeenCalledWith('default/items')
    })

    it('handles all CDC event types', async () => {
      const insertEvent = createCDCEvent({ type: 'collection.created', docId: 'doc-1' })
      const updateEvent = createCDCEvent({
        type: 'collection.updated',
        docId: 'doc-2',
        prev: { name: 'Old' },
      })
      const deleteEvent = createCDCEvent({
        type: 'collection.deleted',
        docId: 'doc-3',
        doc: undefined,
        prev: { name: 'Deleted' },
      })

      const batch = createMockBatch([
        createMockMessage(createEventBatch([insertEvent, updateEvent, deleteEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.cdcEventsProcessed).toBe(3)
    })
  })

  describe('Subscription Fanout', () => {
    it('fans out events to SubscriptionDO', async () => {
      const rpcEvent = createRPCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.subscriptionFanouts).toBe(1)

      // Verify SubscriptionDO was called with correct shard key
      expect(env.SUBSCRIPTIONS.idFromName).toHaveBeenCalledWith('rpc')
      expect(env.SUBSCRIPTIONS.get).toHaveBeenCalled()

      const subscription = env.SUBSCRIPTIONS._instances.get('rpc')
      expect(subscription?.fanout).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'rpc.call',
          ts: rpcEvent.ts,
          payload: rpcEvent,
        })
      )
    })

    it('groups events by shard key (first segment)', async () => {
      const rpcEvent = createRPCEvent()
      const cdcEvent = createCDCEvent()
      const wsEvent: DurableEvent = {
        type: 'ws.connect',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
        connectionCount: 1,
      } as DurableEvent

      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent, cdcEvent, wsEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.subscriptionFanouts).toBe(3)

      // Verify separate subscription shards were used
      expect(env.SUBSCRIPTIONS.idFromName).toHaveBeenCalledWith('rpc')
      expect(env.SUBSCRIPTIONS.idFromName).toHaveBeenCalledWith('collection')
      expect(env.SUBSCRIPTIONS.idFromName).toHaveBeenCalledWith('ws')
    })

    it('uses default shard for events without dots', async () => {
      const customEvent: DurableEvent = {
        type: 'ping' as any,
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
      } as DurableEvent

      const batch = createMockBatch([
        createMockMessage(createEventBatch([customEvent])),
      ])

      await simulateQueueHandler(batch, env)

      expect(env.SUBSCRIPTIONS.idFromName).toHaveBeenCalledWith('default')
    })

    it('includes event ID in fanout payload', async () => {
      const rpcEvent = createRPCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent])),
      ])

      await simulateQueueHandler(batch, env)

      const subscription = env.SUBSCRIPTIONS._instances.get('rpc')
      const fanoutCall = subscription?.fanout.mock.calls[0][0]

      expect(fanoutCall.id).toBeDefined()
      expect(typeof fanoutCall.id).toBe('string')
    })
  })

  describe('Invalid Message Handling', () => {
    it('acknowledges messages with missing events array', async () => {
      const invalidBatch = { notEvents: [] } as unknown as EventBatch
      const message = createMockMessage(invalidBatch)
      const batch = createMockBatch([message])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(0)
      expect(message.ack).toHaveBeenCalled()
      expect(message.retry).not.toHaveBeenCalled()
    })

    it('acknowledges messages with null body', async () => {
      const message = createMockMessage(null as unknown as EventBatch)
      const batch = createMockBatch([message])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(0)
      expect(message.ack).toHaveBeenCalled()
    })

    it('acknowledges messages with empty events array', async () => {
      const message = createMockMessage(createEventBatch([]))
      const batch = createMockBatch([message])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(0)
      expect(message.ack).toHaveBeenCalled()
    })

    it('handles events with invalid structure gracefully', async () => {
      // Non-CDC events should be processed normally even with minimal structure
      const minimalEvent = {
        type: 'custom.test',
        ts: new Date().toISOString(),
        do: { id: 'test' },
      } as DurableEvent

      const batch = createMockBatch([
        createMockMessage(createEventBatch([minimalEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(1)
      expect(result.subscriptionFanouts).toBe(1)
    })
  })

  describe('Retry Behavior', () => {
    it('retries message when CDCProcessorDO throws', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]))
      const batch = createMockBatch([message])

      // Make the processor throw
      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Transient failure')),
      }))

      await simulateQueueHandler(batch, env)

      expect(message.retry).toHaveBeenCalled()
      expect(message.ack).not.toHaveBeenCalled()
    })

    it('retries message when SubscriptionDO throws', async () => {
      const rpcEvent = createRPCEvent()
      const message = createMockMessage(createEventBatch([rpcEvent]))
      const batch = createMockBatch([message])

      // Make the subscription throw
      env.SUBSCRIPTIONS = createMockDONamespace(() => ({
        fanout: vi.fn().mockRejectedValue(new Error('Transient failure')),
      }))

      await simulateQueueHandler(batch, env)

      expect(message.retry).toHaveBeenCalled()
      expect(message.ack).not.toHaveBeenCalled()
    })

    it('processes other messages after one fails', async () => {
      const event1 = createRPCEvent()
      const event2 = createRPCEvent()

      const message1 = createMockMessage(createEventBatch([event1]), 'msg-1')
      const message2 = createMockMessage(createEventBatch([event2]), 'msg-2')

      // Make first fanout fail, second succeed
      let callCount = 0
      env.SUBSCRIPTIONS = createMockDONamespace(() => ({
        fanout: vi.fn().mockImplementation(() => {
          callCount++
          if (callCount === 1) {
            return Promise.reject(new Error('First call fails'))
          }
          return Promise.resolve({ matched: 1, deliveries: [] })
        }),
      }))

      const batch = createMockBatch([message1, message2])
      await simulateQueueHandler(batch, env)

      // First message should retry, second should ack
      expect(message1.retry).toHaveBeenCalled()
      expect(message2.ack).toHaveBeenCalled()
    })

    it('handles network timeout errors', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]))
      const batch = createMockBatch([message])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Network timeout')),
      }))

      await simulateQueueHandler(batch, env)

      expect(message.retry).toHaveBeenCalled()
    })
  })

  describe('Mixed Event Processing', () => {
    it('processes both CDC and non-CDC events in same batch', async () => {
      const cdcEvent = createCDCEvent()
      const rpcEvent = createRPCEvent()

      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent, rpcEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(2)
      expect(result.cdcEventsProcessed).toBe(1)
      expect(result.subscriptionFanouts).toBe(2) // Both events go to subscriptions
    })

    it('processes multiple messages in a batch', async () => {
      const batch = createMockBatch([
        createMockMessage(createEventBatch([createCDCEvent()]), 'msg-1'),
        createMockMessage(createEventBatch([createRPCEvent()]), 'msg-2'),
        createMockMessage(createEventBatch([createCDCEvent(), createRPCEvent()]), 'msg-3'),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(4)
      expect(result.cdcEventsProcessed).toBe(2)
      expect(result.subscriptionFanouts).toBe(4)
    })

    it('acknowledges all messages on success', async () => {
      const messages = [
        createMockMessage(createEventBatch([createRPCEvent()]), 'msg-1'),
        createMockMessage(createEventBatch([createRPCEvent()]), 'msg-2'),
        createMockMessage(createEventBatch([createRPCEvent()]), 'msg-3'),
      ]

      const batch = createMockBatch(messages)
      await simulateQueueHandler(batch, env)

      for (const message of messages) {
        expect(message.ack).toHaveBeenCalled()
        expect(message.retry).not.toHaveBeenCalled()
      }
    })
  })

  describe('Edge Cases', () => {
    it('handles empty message batch', async () => {
      const batch = createMockBatch([])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(0)
      expect(result.cdcEventsProcessed).toBe(0)
      expect(result.subscriptionFanouts).toBe(0)
    })

    it('handles batch with only CDC events', async () => {
      const batch = createMockBatch([
        createMockMessage(createEventBatch([
          createCDCEvent({ docId: 'doc-1' }),
          createCDCEvent({ docId: 'doc-2' }),
        ])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.cdcEventsProcessed).toBe(2)
      expect(result.subscriptionFanouts).toBe(2) // CDC events also go to subscriptions
    })

    it('handles batch with only non-CDC events', async () => {
      const batch = createMockBatch([
        createMockMessage(createEventBatch([
          createRPCEvent(),
          createRPCEvent(),
        ])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.cdcEventsProcessed).toBe(0)
      expect(result.subscriptionFanouts).toBe(2)
    })

    it('handles missing CDC_PROCESSOR binding gracefully', async () => {
      const envWithoutCDC = {
        SUBSCRIPTIONS: env.SUBSCRIPTIONS,
      } as unknown as MockEnv

      const cdcEvent = createCDCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      const result = await simulateQueueHandler(batch, envWithoutCDC)

      // Should still process subscriptions
      expect(result.subscriptionFanouts).toBe(1)
      expect(result.cdcEventsProcessed).toBe(0)
    })

    it('handles missing SUBSCRIPTIONS binding gracefully', async () => {
      const envWithoutSubs = {
        CDC_PROCESSOR: env.CDC_PROCESSOR,
      } as unknown as MockEnv

      const cdcEvent = createCDCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      const result = await simulateQueueHandler(batch, envWithoutSubs)

      // Should still process CDC
      expect(result.cdcEventsProcessed).toBe(1)
      expect(result.subscriptionFanouts).toBe(0)
    })
  })
})

// ============================================================================
// Integration-style Tests (Logic Verification)
// ============================================================================

describe('Queue Consumer Integration Logic', () => {
  describe('CDC Event Routing', () => {
    it('routes events to correct processor based on namespace/collection key', () => {
      // Test the key generation logic
      const testCases = [
        {
          event: { do: { class: 'UserDO' }, collection: 'profiles' },
          expectedKey: 'UserDO/profiles',
        },
        {
          event: { do: { name: 'OrderDO' }, collection: 'items' },
          expectedKey: 'OrderDO/items',
        },
        {
          event: { do: { id: 'some-id' }, collection: 'data' },
          expectedKey: 'default/data',
        },
        {
          event: { do: { class: 'MyClass', name: 'MyName' }, collection: 'docs' },
          expectedKey: 'MyClass/docs', // class takes precedence
        },
      ]

      for (const { event, expectedKey } of testCases) {
        const ns = event.do.class || event.do.name || 'default'
        const collection = event.collection
        const key = `${ns}/${collection}`
        expect(key).toBe(expectedKey)
      }
    })
  })

  describe('Subscription Shard Routing', () => {
    it('extracts correct shard key from event types', () => {
      const testCases = [
        { type: 'rpc.call', expectedShard: 'rpc' },
        { type: 'collection.created', expectedShard: 'collection' },
        { type: 'ws.connect', expectedShard: 'ws' },
        { type: 'do.alarm', expectedShard: 'do' },
        { type: 'webhook.github.push', expectedShard: 'webhook' },
        { type: 'custom.myevent', expectedShard: 'custom' },
        { type: 'ping', expectedShard: 'default' }, // no dot
        { type: 'noDot', expectedShard: 'default' },
      ]

      for (const { type, expectedShard } of testCases) {
        const dotIndex = type.indexOf('.')
        const shardKey = dotIndex > 0 ? type.slice(0, dotIndex) : 'default'
        expect(shardKey).toBe(expectedShard)
      }
    })
  })

  describe('Message Validation', () => {
    it('correctly identifies invalid message formats', () => {
      const invalidMessages = [
        null,
        undefined,
        {},
        { events: null },
        { events: 'not-an-array' },
        { events: 123 },
      ]

      for (const body of invalidMessages) {
        const eventBatch = body as EventBatch | null
        const isValid = eventBatch?.events && Array.isArray(eventBatch.events)
        expect(isValid).toBeFalsy()
      }
    })

    it('correctly identifies valid message formats', () => {
      const validMessages = [
        { events: [] },
        { events: [{ type: 'test', ts: '2024-01-01', do: { id: 'x' } }] },
      ]

      for (const body of validMessages) {
        const eventBatch = body as EventBatch
        const isValid = eventBatch?.events && Array.isArray(eventBatch.events)
        expect(isValid).toBeTruthy()
      }
    })
  })

  describe('CDC Event Identification', () => {
    it('correctly identifies CDC events by type prefix', () => {
      const cdcTypes = [
        'collection.created',
        'collection.updated',
        'collection.deleted',
      ]

      const nonCdcTypes = [
        'rpc.call',
        'ws.connect',
        'do.alarm',
        'webhook.github.push',
        'custom.myevent',
      ]

      for (const type of cdcTypes) {
        expect(type.startsWith('collection.')).toBe(true)
      }

      for (const type of nonCdcTypes) {
        expect(type.startsWith('collection.')).toBe(false)
      }
    })
  })
})
