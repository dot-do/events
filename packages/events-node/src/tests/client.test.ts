/**
 * EventsClient Tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { EventsClient, EventBufferFullError, type Event, type SendResult } from '../index.js'

describe('EventsClient', () => {
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    vi.useFakeTimers()
    mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ok: true, received: 1, namespace: 'default' }),
    })
    vi.stubGlobal('fetch', mockFetch)
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('constructor', () => {
    it('should use default configuration', async () => {
      const client = new EventsClient()

      client.track('test_event')
      await client.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        'https://events.workers.do/ingest',
        expect.objectContaining({
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        })
      )

      await client.shutdown()
    })

    it('should accept custom endpoint', async () => {
      const client = new EventsClient({ endpoint: 'https://custom.endpoint.com/ingest' })

      client.track('test_event')
      await client.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        'https://custom.endpoint.com/ingest',
        expect.any(Object)
      )

      await client.shutdown()
    })

    it('should include API key in Authorization header', async () => {
      const client = new EventsClient({ apiKey: 'test-api-key' })

      client.track('test_event')
      await client.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer test-api-key',
          },
        })
      )

      await client.shutdown()
    })
  })

  describe('track()', () => {
    it('should queue track events', async () => {
      const client = new EventsClient({ batchSize: 100 })

      client.track('button_click', { buttonId: 'signup' })

      const stats = client.getStats()
      expect(stats.queuedCount).toBe(1)

      await client.shutdown()
    })

    it('should send event with correct structure', async () => {
      const client = new EventsClient()

      client.track('purchase', { amount: 99.99, currency: 'USD' }, 'user-123', 'anon-456')
      await client.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events).toHaveLength(1)
      expect(body.events[0]).toMatchObject({
        type: 'track',
        event: 'purchase',
        properties: { amount: 99.99, currency: 'USD' },
        userId: 'user-123',
        anonymousId: 'anon-456',
      })
      expect(body.events[0].ts).toBeDefined()

      await client.shutdown()
    })

    it('should auto-flush when batch size is reached', async () => {
      const client = new EventsClient({ batchSize: 3, flushIntervalMs: 60000 })

      client.track('event1')
      client.track('event2')
      expect(mockFetch).not.toHaveBeenCalled()

      client.track('event3')
      // Wait for the promise to resolve
      await Promise.resolve()
      await Promise.resolve()

      expect(mockFetch).toHaveBeenCalledTimes(1)
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events).toHaveLength(3)

      await client.shutdown()
    })
  })

  describe('page()', () => {
    it('should track page view events', async () => {
      const client = new EventsClient()

      client.page('Home', { title: 'Welcome', url: 'https://example.com' })
      await client.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0]).toMatchObject({
        type: 'page',
        name: 'Home',
        properties: { title: 'Welcome', url: 'https://example.com' },
      })

      await client.shutdown()
    })
  })

  describe('identify()', () => {
    it('should track identify events', async () => {
      const client = new EventsClient()

      client.identify('user-789', { email: 'test@example.com', plan: 'pro' })
      await client.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0]).toMatchObject({
        type: 'identify',
        userId: 'user-789',
        traits: { email: 'test@example.com', plan: 'pro' },
      })

      await client.shutdown()
    })
  })

  describe('trackRaw()', () => {
    it('should track raw event objects', async () => {
      const client = new EventsClient()

      client.trackRaw({
        type: 'custom.analytics',
        data: { metric: 'cpu_usage', value: 85 },
      } as Event)
      await client.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0]).toMatchObject({
        type: 'custom.analytics',
        data: { metric: 'cpu_usage', value: 85 },
      })

      await client.shutdown()
    })

    it('should preserve existing timestamp', async () => {
      const client = new EventsClient()
      const customTs = '2024-01-15T10:00:00.000Z'

      client.trackRaw({
        type: 'track',
        event: 'test',
        ts: customTs,
      })
      await client.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].ts).toBe(customTs)

      await client.shutdown()
    })
  })

  describe('flush()', () => {
    it('should send all queued events', async () => {
      const client = new EventsClient({ batchSize: 100 })

      client.track('event1')
      client.track('event2')
      client.track('event3')

      await client.flush()

      expect(mockFetch).toHaveBeenCalledTimes(1)
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events).toHaveLength(3)

      await client.shutdown()
    })

    it('should clear queue after flush', async () => {
      const client = new EventsClient()

      client.track('event1')
      await client.flush()
      await client.flush()

      expect(mockFetch).toHaveBeenCalledTimes(1)

      await client.shutdown()
    })

    it('should not send if queue is empty', async () => {
      const client = new EventsClient()

      await client.flush()

      expect(mockFetch).not.toHaveBeenCalled()

      await client.shutdown()
    })
  })

  describe('auto-flush interval', () => {
    it('should auto-flush at configured interval', async () => {
      const client = new EventsClient({ flushIntervalMs: 1000, batchSize: 100 })

      client.track('event1')
      expect(mockFetch).not.toHaveBeenCalled()

      await vi.advanceTimersByTimeAsync(1000)

      expect(mockFetch).toHaveBeenCalledTimes(1)

      await client.shutdown()
    })
  })

  describe('retry logic', () => {
    it('should retry failed requests with exponential backoff', async () => {
      mockFetch
        .mockRejectedValueOnce(new Error('Network error'))
        .mockRejectedValueOnce(new Error('Network error'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ ok: true, received: 1 }),
        })

      const onSuccess = vi.fn()
      const client = new EventsClient({
        maxRetries: 3,
        retryDelayMs: 100,
        onSuccess,
        batchSize: 100,
      })

      client.track('retry_event')
      await client.flush()

      // First attempt failed, event queued for retry
      expect(client.getStats().retryCount).toBe(1)

      // Advance past first retry delay (100ms)
      await vi.advanceTimersByTimeAsync(110)
      // Second attempt failed
      expect(client.getStats().retryCount).toBe(1)

      // Advance past second retry delay (200ms)
      await vi.advanceTimersByTimeAsync(220)

      // Third attempt should succeed
      expect(onSuccess).toHaveBeenCalled()
      expect(client.getStats().retryCount).toBe(0)

      await client.shutdown()
    })

    it('should call onError after max retries exceeded', async () => {
      mockFetch.mockRejectedValue(new Error('Network error'))

      const onError = vi.fn()
      const client = new EventsClient({
        maxRetries: 2,
        retryDelayMs: 100,
        onError,
        batchSize: 100,
        flushIntervalMs: 60000,
      })

      client.track('failing_event')
      await client.flush()

      // First retry attempt
      await vi.advanceTimersByTimeAsync(110)
      await Promise.resolve()
      await Promise.resolve()

      // Second retry attempt
      await vi.advanceTimersByTimeAsync(220)
      await Promise.resolve()
      await Promise.resolve()

      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: expect.stringContaining('Failed to send events after 2 attempts'),
        }),
        expect.arrayContaining([expect.objectContaining({ event: 'failing_event' })])
      )

      await client.shutdown()
    })

    it('should retry on HTTP error status codes', async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: false,
          status: 500,
          text: async () => 'Internal Server Error',
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ ok: true, received: 1 }),
        })

      const onSuccess = vi.fn()
      const client = new EventsClient({
        maxRetries: 3,
        retryDelayMs: 100,
        onSuccess,
        batchSize: 100,
      })

      client.track('http_error_event')
      await client.flush()

      expect(client.getStats().retryCount).toBe(1)

      await vi.advanceTimersByTimeAsync(110)

      expect(onSuccess).toHaveBeenCalled()

      await client.shutdown()
    })
  })

  describe('backpressure handling', () => {
    it('should respect maxRetryQueueSize limit', async () => {
      mockFetch.mockRejectedValue(new Error('Network error'))

      const onDrop = vi.fn()
      const client = new EventsClient({
        maxRetries: 10,
        retryDelayMs: 100000, // Very long delay to prevent retry processing
        maxRetryQueueSize: 5,
        onDrop,
        batchSize: 100,
        flushIntervalMs: 600000, // Very long to prevent auto-flush
      })

      // Queue 10 events
      for (let i = 0; i < 10; i++) {
        client.track(`event_${i}`)
      }
      await client.flush()
      await Promise.resolve()

      // Only 5 events should be in retry queue (newest 5 kept)
      expect(client.getStats().retryCount).toBe(5)
      expect(onDrop).toHaveBeenCalledWith(
        expect.any(Array),
        'retry_queue_overflow'
      )

      await client.shutdown(0)
    })

    it('should drop events when retry queue is full', async () => {
      mockFetch.mockRejectedValue(new Error('Network error'))

      const onDrop = vi.fn()
      const client = new EventsClient({
        maxRetries: 10,
        retryDelayMs: 600000, // Very long delay
        maxRetryQueueSize: 3,
        onDrop,
        batchSize: 100,
        flushIntervalMs: 600000, // Very long to prevent auto-flush
      })

      // First batch fills queue
      for (let i = 0; i < 3; i++) {
        client.track(`batch1_event_${i}`)
      }
      await client.flush()
      await Promise.resolve()
      expect(client.getStats().retryCount).toBe(3)

      // Second batch should be dropped
      for (let i = 0; i < 2; i++) {
        client.track(`batch2_event_${i}`)
      }

      let caughtError: Error | null = null
      try {
        await client.flush()
      } catch (err) {
        caughtError = err as Error
      }

      expect(caughtError).toBeInstanceOf(EventBufferFullError)
      expect(onDrop).toHaveBeenCalledWith(expect.any(Array), 'retry_queue_full')

      await client.shutdown(0)
    })
  })

  describe('callbacks', () => {
    it('should call onSuccess with events and result', async () => {
      const mockResult: SendResult = { ok: true, received: 2, namespace: 'test' }
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => mockResult,
      })

      const onSuccess = vi.fn()
      const client = new EventsClient({ onSuccess })

      client.track('event1')
      client.track('event2')
      await client.flush()

      expect(onSuccess).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({ event: 'event1' }),
          expect.objectContaining({ event: 'event2' }),
        ]),
        mockResult
      )

      await client.shutdown()
    })
  })

  describe('getStats()', () => {
    it('should return accurate statistics', async () => {
      const client = new EventsClient({ batchSize: 100 })

      client.track('event1')
      client.track('event2')

      let stats = client.getStats()
      expect(stats.queuedCount).toBe(2)
      expect(stats.retryCount).toBe(0)
      expect(stats.sentCount).toBe(0)
      expect(stats.droppedCount).toBe(0)

      await client.flush()

      stats = client.getStats()
      expect(stats.queuedCount).toBe(0)
      expect(stats.sentCount).toBe(2)

      await client.shutdown()
    })
  })

  describe('shutdown()', () => {
    it('should flush remaining events on shutdown', async () => {
      const client = new EventsClient({ batchSize: 100 })

      client.track('final_event')
      await client.shutdown()

      expect(mockFetch).toHaveBeenCalled()
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].event).toBe('final_event')
    })

    it('should drop events queued after shutdown starts', async () => {
      const onDrop = vi.fn()
      const client = new EventsClient({ onDrop })

      // Start shutdown
      const shutdownPromise = client.shutdown()

      // Try to track after shutdown started
      client.track('late_event')

      await shutdownPromise

      expect(onDrop).toHaveBeenCalledWith(
        expect.arrayContaining([expect.objectContaining({ event: 'late_event' })]),
        'shutting_down'
      )
    })

    it('should stop auto-flush timer on shutdown', async () => {
      const client = new EventsClient({ flushIntervalMs: 100, batchSize: 100 })

      await client.shutdown()

      // Add event after shutdown
      client.track('post_shutdown')

      // Advance time past flush interval
      await vi.advanceTimersByTimeAsync(200)

      // Event should have been dropped, not flushed
      const body = mockFetch.mock.calls[0]?.[1]?.body
      if (body) {
        const parsed = JSON.parse(body)
        expect(parsed.events.find((e: Event) => e.type === 'track' && 'event' in e && e.event === 'post_shutdown')).toBeUndefined()
      }
    })
  })

  describe('debug mode', () => {
    it('should log when debug is enabled', async () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
      const client = new EventsClient({ debug: true })

      client.track('debug_event')
      await client.flush()

      expect(consoleSpy).toHaveBeenCalledWith('[events-node]', expect.any(String))

      await client.shutdown()
      consoleSpy.mockRestore()
    })

    it('should not log when debug is disabled', async () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
      const client = new EventsClient({ debug: false })

      client.track('silent_event')
      await client.flush()

      expect(consoleSpy).not.toHaveBeenCalledWith('[events-node]', expect.any(String))

      await client.shutdown()
      consoleSpy.mockRestore()
    })
  })
})
