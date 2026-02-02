/**
 * Queue Mock Tests
 *
 * Tests for createMockQueue() to verify it correctly mocks the Cloudflare Workers Queue interface.
 * These tests demonstrate usage patterns for testing queue producers.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { createMockQueue } from './mocks.js'

// ============================================================================
// Type Definitions for Test Events
// ============================================================================

interface TestEvent {
  type: string
  data?: unknown
  timestamp?: string
}

// ============================================================================
// Basic Usage Tests
// ============================================================================

describe('createMockQueue', () => {
  describe('send()', () => {
    it('captures sent messages', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'user.created', data: { id: 'user-1' } })

      expect(queue.messages).toHaveLength(1)
      expect(queue.messages[0].body).toEqual({
        type: 'user.created',
        data: { id: 'user-1' },
      })
    })

    it('captures multiple sent messages in order', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'event-1' })
      await queue.send({ type: 'event-2' })
      await queue.send({ type: 'event-3' })

      expect(queue.messages).toHaveLength(3)
      expect(queue.messages[0].body.type).toBe('event-1')
      expect(queue.messages[1].body.type).toBe('event-2')
      expect(queue.messages[2].body.type).toBe('event-3')
    })

    it('captures send options (contentType, delaySeconds)', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'delayed-event' }, { delaySeconds: 60 })
      await queue.send({ type: 'json-event' }, { contentType: 'application/json' })

      expect(queue.messages[0].delaySeconds).toBe(60)
      expect(queue.messages[1].contentType).toBe('application/json')
    })

    it('increments sendCount on each send', async () => {
      const queue = createMockQueue<TestEvent>()

      expect(queue.sendCount).toBe(0)

      await queue.send({ type: 'event-1' })
      expect(queue.sendCount).toBe(1)

      await queue.send({ type: 'event-2' })
      expect(queue.sendCount).toBe(2)
    })

    it('is a vitest mock function for assertions', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'test' })

      expect(queue.send).toHaveBeenCalledTimes(1)
      expect(queue.send).toHaveBeenCalledWith({ type: 'test' })
    })
  })

  describe('sendBatch()', () => {
    it('captures all messages from a batch', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.sendBatch([
        { body: { type: 'batch-1' } },
        { body: { type: 'batch-2' } },
        { body: { type: 'batch-3' } },
      ])

      expect(queue.messages).toHaveLength(3)
      expect(queue.messages[0].body.type).toBe('batch-1')
      expect(queue.messages[1].body.type).toBe('batch-2')
      expect(queue.messages[2].body.type).toBe('batch-3')
    })

    it('captures batch options per message', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.sendBatch([
        { body: { type: 'immediate' } },
        { body: { type: 'delayed' }, delaySeconds: 120 },
        { body: { type: 'custom-type' }, contentType: 'application/x-custom' },
      ])

      expect(queue.messages[0].delaySeconds).toBeUndefined()
      expect(queue.messages[1].delaySeconds).toBe(120)
      expect(queue.messages[2].contentType).toBe('application/x-custom')
    })

    it('increments sendCount for each message in batch', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.sendBatch([
        { body: { type: 'a' } },
        { body: { type: 'b' } },
        { body: { type: 'c' } },
      ])

      expect(queue.sendCount).toBe(3)
    })

    it('is a vitest mock function for assertions', async () => {
      const queue = createMockQueue<TestEvent>()

      const batch = [{ body: { type: 'test' } }]
      await queue.sendBatch(batch)

      expect(queue.sendBatch).toHaveBeenCalledTimes(1)
      expect(queue.sendBatch).toHaveBeenCalledWith(batch)
    })

    it('handles empty batch', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.sendBatch([])

      expect(queue.messages).toHaveLength(0)
      expect(queue.sendCount).toBe(0)
    })
  })

  describe('clear()', () => {
    it('removes all messages', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'event-1' })
      await queue.send({ type: 'event-2' })
      expect(queue.messages).toHaveLength(2)

      queue.clear()

      expect(queue.messages).toHaveLength(0)
    })

    it('resets sendCount', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'event' })
      expect(queue.sendCount).toBe(1)

      queue.clear()

      expect(queue.sendCount).toBe(0)
    })
  })

  describe('mixed send() and sendBatch()', () => {
    it('captures messages from both methods in order', async () => {
      const queue = createMockQueue<TestEvent>()

      await queue.send({ type: 'single-1' })
      await queue.sendBatch([
        { body: { type: 'batch-1' } },
        { body: { type: 'batch-2' } },
      ])
      await queue.send({ type: 'single-2' })

      expect(queue.messages).toHaveLength(4)
      expect(queue.messages.map(m => m.body.type)).toEqual([
        'single-1',
        'batch-1',
        'batch-2',
        'single-2',
      ])
    })
  })
})

// ============================================================================
// Error Injection Tests
// ============================================================================

describe('createMockQueue error injection', () => {
  describe('sendError option', () => {
    it('throws on send()', async () => {
      const queue = createMockQueue<TestEvent>({
        sendError: new Error('Queue is full'),
      })

      await expect(queue.send({ type: 'test' })).rejects.toThrow('Queue is full')
    })

    it('does not capture the message on error', async () => {
      const queue = createMockQueue<TestEvent>({
        sendError: new Error('Queue is full'),
      })

      try {
        await queue.send({ type: 'test' })
      } catch {
        // Expected
      }

      expect(queue.messages).toHaveLength(0)
    })
  })

  describe('sendBatchError option', () => {
    it('throws on sendBatch()', async () => {
      const queue = createMockQueue<TestEvent>({
        sendBatchError: new Error('Batch too large'),
      })

      await expect(
        queue.sendBatch([{ body: { type: 'test' } }])
      ).rejects.toThrow('Batch too large')
    })

    it('does not capture messages on error', async () => {
      const queue = createMockQueue<TestEvent>({
        sendBatchError: new Error('Batch too large'),
      })

      try {
        await queue.sendBatch([
          { body: { type: 'a' } },
          { body: { type: 'b' } },
        ])
      } catch {
        // Expected
      }

      expect(queue.messages).toHaveLength(0)
    })
  })

  describe('failAfter option', () => {
    it('succeeds until failAfter threshold', async () => {
      const queue = createMockQueue<TestEvent>({
        failAfter: 3,
        sendError: new Error('Quota exceeded'),
      })

      // These should succeed
      await queue.send({ type: 'ok-1' })
      await queue.send({ type: 'ok-2' })
      await queue.send({ type: 'ok-3' })

      // This should fail
      await expect(queue.send({ type: 'fail' })).rejects.toThrow('Quota exceeded')

      expect(queue.messages).toHaveLength(3)
    })

    it('works with sendBatch after threshold reached', async () => {
      const queue = createMockQueue<TestEvent>({
        failAfter: 2,
        sendBatchError: new Error('Rate limited'),
      })

      // These sends succeed (using failAfter with sendBatchError means send() works)
      await queue.send({ type: 'single-1' })
      await queue.send({ type: 'single-2' })

      // sendCount is now 2, which equals failAfter threshold
      // Next sendBatch should fail
      await expect(
        queue.sendBatch([
          { body: { type: 'batch-1' } },
        ])
      ).rejects.toThrow('Rate limited')
    })
  })
})

// ============================================================================
// Typed Queue Tests
// ============================================================================

describe('createMockQueue with typed messages', () => {
  interface OrderEvent {
    orderId: string
    status: 'pending' | 'completed' | 'cancelled'
    items: Array<{ sku: string; qty: number }>
  }

  it('provides type safety for message body', async () => {
    const queue = createMockQueue<OrderEvent>()

    await queue.send({
      orderId: 'order-123',
      status: 'pending',
      items: [{ sku: 'SKU-001', qty: 2 }],
    })

    const message = queue.messages[0]
    expect(message.body.orderId).toBe('order-123')
    expect(message.body.status).toBe('pending')
    expect(message.body.items[0].sku).toBe('SKU-001')
  })

  it('works with complex nested types', async () => {
    interface ComplexEvent {
      metadata: {
        source: string
        version: number
        tags: string[]
      }
      payload: unknown
    }

    const queue = createMockQueue<ComplexEvent>()

    await queue.send({
      metadata: {
        source: 'test',
        version: 1,
        tags: ['a', 'b'],
      },
      payload: { nested: { deep: true } },
    })

    expect(queue.messages[0].body.metadata.tags).toEqual(['a', 'b'])
  })
})

// ============================================================================
// Real-World Usage Examples
// ============================================================================

describe('Queue Mock usage examples', () => {
  // Example: Testing a service that sends events to a queue
  class EventPublisher {
    constructor(private queue: Queue<TestEvent>) {}

    async publishUserCreated(userId: string, email: string): Promise<void> {
      await this.queue.send({
        type: 'user.created',
        data: { userId, email },
        timestamp: new Date().toISOString(),
      })
    }

    async publishBulkUpdates(userIds: string[]): Promise<void> {
      await this.queue.sendBatch(
        userIds.map(userId => ({
          body: {
            type: 'user.updated',
            data: { userId },
          },
        }))
      )
    }
  }

  it('tests EventPublisher.publishUserCreated', async () => {
    const queue = createMockQueue<TestEvent>()
    const publisher = new EventPublisher(queue)

    await publisher.publishUserCreated('user-123', 'test@example.com')

    expect(queue.messages).toHaveLength(1)
    expect(queue.messages[0].body.type).toBe('user.created')
    expect(queue.messages[0].body.data).toEqual({
      userId: 'user-123',
      email: 'test@example.com',
    })
  })

  it('tests EventPublisher.publishBulkUpdates', async () => {
    const queue = createMockQueue<TestEvent>()
    const publisher = new EventPublisher(queue)

    await publisher.publishBulkUpdates(['user-1', 'user-2', 'user-3'])

    expect(queue.messages).toHaveLength(3)
    expect(queue.messages.every(m => m.body.type === 'user.updated')).toBe(true)
  })

  it('tests error handling in EventPublisher', async () => {
    const queue = createMockQueue<TestEvent>({
      sendError: new Error('Queue unavailable'),
    })
    const publisher = new EventPublisher(queue)

    await expect(
      publisher.publishUserCreated('user-123', 'test@example.com')
    ).rejects.toThrow('Queue unavailable')
  })

  // Example: Testing retry logic
  class RetryingPublisher {
    constructor(
      private queue: Queue<TestEvent>,
      private maxRetries: number = 3
    ) {}

    async publish(event: TestEvent): Promise<boolean> {
      for (let attempt = 0; attempt < this.maxRetries; attempt++) {
        try {
          await this.queue.send(event)
          return true
        } catch {
          if (attempt === this.maxRetries - 1) {
            return false
          }
          // In real code, add exponential backoff here
        }
      }
      return false
    }
  }

  it('tests RetryingPublisher exhausts retries', async () => {
    const queue = createMockQueue<TestEvent>({
      sendError: new Error('Always fails'),
    })
    const publisher = new RetryingPublisher(queue, 3)

    const result = await publisher.publish({ type: 'test' })

    expect(result).toBe(false)
    expect(queue.send).toHaveBeenCalledTimes(3)
  })

  // Example: Testing with beforeEach cleanup
  describe('with beforeEach cleanup pattern', () => {
    let queue: ReturnType<typeof createMockQueue<TestEvent>>

    beforeEach(() => {
      queue = createMockQueue<TestEvent>()
    })

    it('test 1: starts with empty queue', async () => {
      expect(queue.messages).toHaveLength(0)
      await queue.send({ type: 'test-1' })
      expect(queue.messages).toHaveLength(1)
    })

    it('test 2: also starts with empty queue', async () => {
      expect(queue.messages).toHaveLength(0)
      await queue.send({ type: 'test-2' })
      expect(queue.messages).toHaveLength(1)
    })
  })
})
