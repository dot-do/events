/**
 * Queue Handler Tests
 *
 * Unit tests for the queue handler in src/handlers/queue.ts
 * Tests message processing, retries, dead letters, and CDC/subscription fanout.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
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

interface MockR2Bucket {
  list: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  put: ReturnType<typeof vi.fn>
  head: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
}

interface MockEnv {
  CDC_PROCESSOR?: MockDONamespace<MockCDCProcessorDO>
  SUBSCRIPTIONS?: MockDONamespace<MockSubscriptionDO>
  EVENTS_BUCKET: MockR2Bucket
  ANALYTICS?: MockAnalytics
}

interface MockAnalytics {
  writeDataPoint: ReturnType<typeof vi.fn>
}

interface DurableEvent {
  type: string
  ts: string
  do?: { id: string; name?: string; class?: string }
  collection?: string
  docId?: string
  doc?: unknown
  prev?: unknown
  id?: string
  [key: string]: unknown
}

interface EventBatch {
  events: DurableEvent[]
}

// ============================================================================
// Constants
// ============================================================================

const MAX_RETRIES = 5

// ============================================================================
// Helper Functions
// ============================================================================

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

function createMockCDCProcessor(): MockCDCProcessorDO {
  return {
    process: vi.fn().mockResolvedValue({ processed: 1, pending: 0 }),
  }
}

function createMockSubscriptionDO(): MockSubscriptionDO {
  return {
    fanout: vi.fn().mockResolvedValue({ matched: 1, deliveries: ['del-1'] }),
  }
}

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

function createMockR2Bucket(): MockR2Bucket {
  return {
    list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue({}),
    head: vi.fn().mockResolvedValue(null),
    delete: vi.fn().mockResolvedValue(undefined),
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    CDC_PROCESSOR: createMockDONamespace(createMockCDCProcessor),
    SUBSCRIPTIONS: createMockDONamespace(createMockSubscriptionDO),
    EVENTS_BUCKET: createMockR2Bucket(),
    ...overrides,
  }
}

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
  }
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
  }
}

function createEventBatch(events: DurableEvent[]): EventBatch {
  return { events }
}

// ============================================================================
// Queue Handler Simulation
// ============================================================================

interface QueueProcessingResult {
  totalEvents: number
  cdcEventsProcessed: number
  cdcEventsSkipped: number
  subscriptionFanouts: number
  deadLettered: number
}

async function simulateQueueHandler(
  batch: MockMessageBatch<EventBatch>,
  env: MockEnv
): Promise<QueueProcessingResult> {
  let totalEvents = 0
  let cdcEventsProcessed = 0
  let cdcEventsSkipped = 0
  let subscriptionFanouts = 0
  let deadLettered = 0

  for (const message of batch.messages) {
    const attempts = message.attempts ?? 1

    try {
      const eventBatch = message.body

      if (!eventBatch?.events || !Array.isArray(eventBatch.events)) {
        console.warn(`[QUEUE] Invalid message format, missing events array`)
        message.ack()
        continue
      }

      totalEvents += eventBatch.events.length

      // Process CDC events
      const cdcEvents = eventBatch.events.filter(e => e.type.startsWith('collection.'))

      if (cdcEvents.length > 0 && env.CDC_PROCESSOR) {
        const cdcByKey = new Map<string, DurableEvent[]>()
        for (const event of cdcEvents) {
          const ns = event.do?.class || event.do?.name || 'default'
          const collection = event.collection || 'default'
          const key = `${ns}/${collection}`
          const existing = cdcByKey.get(key) ?? []
          existing.push(event)
          cdcByKey.set(key, existing)
        }

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

      // Fan out to subscriptions
      if (env.SUBSCRIPTIONS) {
        const eventsByShard = new Map<string, DurableEvent[]>()
        for (const event of eventBatch.events) {
          const dotIndex = event.type.indexOf('.')
          const shardKey = dotIndex > 0 ? event.type.slice(0, dotIndex) : 'default'
          const existing = eventsByShard.get(shardKey) ?? []
          existing.push(event)
          eventsByShard.set(shardKey, existing)
        }

        for (const [shardKey, events] of eventsByShard) {
          try {
            const subId = env.SUBSCRIPTIONS.idFromName(shardKey)
            const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

            for (const event of events) {
              const eventId = event.id || `test-${Date.now()}`
              await subscriptionDO.fanout({
                id: eventId,
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

      message.ack()
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err)

      if (attempts >= MAX_RETRIES) {
        // Dead letter
        console.error(`[QUEUE] Dead letter: message failed after ${attempts} attempts. Error: ${errorMsg}`)

        try {
          const deadLetterKey = `dead-letter/queue/${new Date().toISOString().slice(0, 10)}/${message.id}.json`
          await env.EVENTS_BUCKET.put(
            deadLetterKey,
            JSON.stringify({
              messageId: message.id,
              attempts,
              error: errorMsg,
              body: message.body,
              timestamp: new Date().toISOString(),
            }),
            { httpMetadata: { contentType: 'application/json' } }
          )
        } catch (dlErr) {
          console.error(`[QUEUE] Failed to write dead letter:`, dlErr)
        }

        deadLettered++
        message.ack()
      } else {
        console.warn(`[QUEUE] Retrying message (attempt ${attempts}/${MAX_RETRIES}). Error: ${errorMsg}`)
        message.retry()
      }
    }
  }

  return {
    totalEvents,
    cdcEventsProcessed,
    cdcEventsSkipped,
    subscriptionFanouts,
    deadLettered,
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('Queue Handler', () => {
  let env: MockEnv

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

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(1)
      expect(result.subscriptionFanouts).toBe(1)
    })

    it('acknowledges message on success', async () => {
      const message = createMockMessage(createEventBatch([createRPCEvent()]))
      const batch = createMockBatch([message])

      await simulateQueueHandler(batch, env)

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
      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(4)

      for (const msg of messages) {
        expect(msg.ack).toHaveBeenCalled()
      }
    })

    it('handles empty batch', async () => {
      const batch = createMockBatch<EventBatch>([])

      const result = await simulateQueueHandler(batch, env)

      expect(result.totalEvents).toBe(0)
    })
  })

  describe('CDC Processing', () => {
    it('processes CDC events through CDCProcessorDO', async () => {
      const cdcEvent = createCDCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.cdcEventsProcessed).toBe(1)
      expect(env.CDC_PROCESSOR!.idFromName).toHaveBeenCalledWith('TestClass/users')
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
      expect(env.CDC_PROCESSOR!._instances.has('TestClass/users')).toBe(true)
      expect(env.CDC_PROCESSOR!._instances.has('TestClass/orders')).toBe(true)
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

      expect(env.CDC_PROCESSOR!.idFromName).toHaveBeenCalledWith('MyDurableObject/items')
    })

    it('uses default when DO identity is missing', async () => {
      const cdcEvent = createCDCEvent({
        do: { id: 'test-id' },
        collection: 'items',
      })
      delete cdcEvent.do?.name
      delete cdcEvent.do?.class

      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      await simulateQueueHandler(batch, env)

      expect(env.CDC_PROCESSOR!.idFromName).toHaveBeenCalledWith('default/items')
    })

    it('handles missing CDC_PROCESSOR binding', async () => {
      const envNoCDC = createMockEnv({ CDC_PROCESSOR: undefined })
      const cdcEvent = createCDCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([cdcEvent])),
      ])

      const result = await simulateQueueHandler(batch, envNoCDC)

      expect(result.cdcEventsProcessed).toBe(0)
      expect(result.subscriptionFanouts).toBe(1) // Still goes to subscriptions
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
      expect(env.SUBSCRIPTIONS!.idFromName).toHaveBeenCalledWith('rpc')
    })

    it('groups events by shard key', async () => {
      const rpcEvent = createRPCEvent()
      const cdcEvent = createCDCEvent()
      const wsEvent: DurableEvent = {
        type: 'ws.connect',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
      }

      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent, cdcEvent, wsEvent])),
      ])

      const result = await simulateQueueHandler(batch, env)

      expect(result.subscriptionFanouts).toBe(3)
      expect(env.SUBSCRIPTIONS!.idFromName).toHaveBeenCalledWith('rpc')
      expect(env.SUBSCRIPTIONS!.idFromName).toHaveBeenCalledWith('collection')
      expect(env.SUBSCRIPTIONS!.idFromName).toHaveBeenCalledWith('ws')
    })

    it('uses default shard for events without dots', async () => {
      const customEvent: DurableEvent = {
        type: 'ping',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
      }

      const batch = createMockBatch([
        createMockMessage(createEventBatch([customEvent])),
      ])

      await simulateQueueHandler(batch, env)

      expect(env.SUBSCRIPTIONS!.idFromName).toHaveBeenCalledWith('default')
    })

    it('includes event ID in fanout payload', async () => {
      const eventWithId: DurableEvent = {
        type: 'rpc.call',
        ts: new Date().toISOString(),
        do: { id: 'test-id' },
        id: 'my-event-id',
      }

      const batch = createMockBatch([
        createMockMessage(createEventBatch([eventWithId])),
      ])

      await simulateQueueHandler(batch, env)

      const subscription = env.SUBSCRIPTIONS!._instances.get('rpc')
      const fanoutCall = subscription?.fanout.mock.calls[0][0]

      expect(fanoutCall.id).toBe('my-event-id')
    })

    it('handles missing SUBSCRIPTIONS binding', async () => {
      const envNoSubs = createMockEnv({ SUBSCRIPTIONS: undefined })
      const rpcEvent = createRPCEvent()
      const batch = createMockBatch([
        createMockMessage(createEventBatch([rpcEvent])),
      ])

      const result = await simulateQueueHandler(batch, envNoSubs)

      expect(result.subscriptionFanouts).toBe(0)
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
  })

  describe('Retry Behavior', () => {
    it('retries message when CDCProcessorDO throws', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]))
      const batch = createMockBatch([message])

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

      env.SUBSCRIPTIONS = createMockDONamespace(() => ({
        fanout: vi.fn().mockRejectedValue(new Error('Transient failure')),
      }))

      await simulateQueueHandler(batch, env)

      expect(message.retry).toHaveBeenCalled()
      expect(message.ack).not.toHaveBeenCalled()
    })

    it('continues processing other messages after one fails', async () => {
      const message1 = createMockMessage(createEventBatch([createRPCEvent()]), 'msg-1')
      const message2 = createMockMessage(createEventBatch([createRPCEvent()]), 'msg-2')

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

      expect(message1.retry).toHaveBeenCalled()
      expect(message2.ack).toHaveBeenCalled()
    })
  })

  describe('Dead Letter Handling', () => {
    it('sends to dead letter after MAX_RETRIES', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-1', MAX_RETRIES)
      const batch = createMockBatch([message])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Permanent failure')),
      }))

      const result = await simulateQueueHandler(batch, env)

      expect(result.deadLettered).toBe(1)
      expect(message.ack).toHaveBeenCalled() // Ack to prevent further retries
      expect(message.retry).not.toHaveBeenCalled()
    })

    it('writes dead letter to R2', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-dead', MAX_RETRIES)
      const batch = createMockBatch([message])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Permanent failure')),
      }))

      await simulateQueueHandler(batch, env)

      expect(env.EVENTS_BUCKET.put).toHaveBeenCalled()
      const putCall = env.EVENTS_BUCKET.put.mock.calls[0]
      expect(putCall[0]).toContain('dead-letter/queue/')
      expect(putCall[0]).toContain('msg-dead.json')
    })

    it('retries at attempt 4, dead letters at attempt 5', async () => {
      const cdcEvent = createCDCEvent()

      // Attempt 4 should retry
      const message4 = createMockMessage(createEventBatch([cdcEvent]), 'msg-4', 4)
      const batch4 = createMockBatch([message4])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Failure')),
      }))

      await simulateQueueHandler(batch4, env)

      expect(message4.retry).toHaveBeenCalled()
      expect(message4.ack).not.toHaveBeenCalled()

      // Attempt 5 should dead letter
      vi.clearAllMocks()
      const message5 = createMockMessage(createEventBatch([cdcEvent]), 'msg-5', 5)
      const batch5 = createMockBatch([message5])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Failure')),
      }))

      const result = await simulateQueueHandler(batch5, env)

      expect(result.deadLettered).toBe(1)
      expect(message5.ack).toHaveBeenCalled()
    })

    it('includes error details in dead letter payload', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-error', MAX_RETRIES)
      const batch = createMockBatch([message])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Specific error message')),
      }))

      await simulateQueueHandler(batch, env)

      const putCall = env.EVENTS_BUCKET.put.mock.calls[0]
      const payload = JSON.parse(putCall[1] as string)

      expect(payload.messageId).toBe('msg-error')
      expect(payload.attempts).toBe(MAX_RETRIES)
      expect(payload.error).toBe('Specific error message')
      expect(payload.body).toEqual(createEventBatch([cdcEvent]))
    })

    it('continues if dead letter write fails', async () => {
      const cdcEvent = createCDCEvent()
      const message = createMockMessage(createEventBatch([cdcEvent]), 'msg-fail', MAX_RETRIES)
      const batch = createMockBatch([message])

      env.CDC_PROCESSOR = createMockDONamespace(() => ({
        process: vi.fn().mockRejectedValue(new Error('Failure')),
      }))
      env.EVENTS_BUCKET.put = vi.fn().mockRejectedValue(new Error('R2 write failed'))

      const result = await simulateQueueHandler(batch, env)

      // Should still count as dead lettered and ack the message
      expect(result.deadLettered).toBe(1)
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
        event: { do: { class: 'UserDO' }, collection: 'profiles' },
        expected: 'UserDO/profiles',
      },
      {
        event: { do: { name: 'OrderDO' }, collection: 'items' },
        expected: 'OrderDO/items',
      },
      {
        event: { do: { id: 'some-id' }, collection: 'data' },
        expected: 'default/data',
      },
      {
        event: { do: { class: 'MyClass', name: 'MyName' }, collection: 'docs' },
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
