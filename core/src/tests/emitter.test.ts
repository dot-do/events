/**
 * EventEmitter Tests
 *
 * TDD Red Phase: Tests for EventEmitter functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { EventEmitter } from '../emitter.js'
import type { DurableEvent } from '../types.js'
import { createMockCtx, createMockR2Bucket, createMockRequest } from './mocks.js'

// ============================================================================
// Tests
// ============================================================================

describe('EventEmitter', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}

    // Mock global fetch
    mockFetch = vi.fn().mockResolvedValue(new Response('OK', { status: 200 }))
    vi.stubGlobal('fetch', mockFetch)

    // Use fake timers
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('emit()', () => {
    it('should add events to batch', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event', data: 'hello' })

      expect(emitter.pendingCount).toBe(1)
    })

    it('should add timestamp and DO identity to events', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event', data: 'hello' })

      // Access batch through pendingCount check and flush
      expect(emitter.pendingCount).toBe(1)
      expect(emitter.doIdentity).toEqual({
        id: 'test-do-123',
        name: 'test-object',
      })
    })

    it('should batch multiple events', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'event1' })
      emitter.emit({ type: 'event2' })
      emitter.emit({ type: 'event3' })

      expect(emitter.pendingCount).toBe(3)
    })
  })

  describe('auto-flush on batch size', () => {
    it('should auto-flush when batch reaches batchSize', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { batchSize: 3 })

      emitter.emit({ type: 'event1' })
      emitter.emit({ type: 'event2' })

      // Should not have flushed yet
      expect(mockFetch).not.toHaveBeenCalled()

      emitter.emit({ type: 'event3' })

      // Should trigger flush
      await vi.runAllTimersAsync()

      expect(mockFetch).toHaveBeenCalledTimes(1)
      expect(emitter.pendingCount).toBe(0)
    })

    it('should send events to configured endpoint', async () => {
      const customEndpoint = 'https://custom.events.api/ingest'
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        endpoint: customEndpoint,
        batchSize: 1,
      })

      emitter.emit({ type: 'test.event' })
      await vi.runAllTimersAsync()

      expect(mockFetch).toHaveBeenCalledWith(
        customEndpoint,
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({
            'Content-Type': 'application/json',
          }),
        })
      )
    })
  })

  describe('flush()', () => {
    it('should send events to endpoint', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event', data: 123 })
      await emitter.flush()

      expect(mockFetch).toHaveBeenCalledTimes(1)
      const [url, options] = mockFetch.mock.calls[0]
      expect(url).toBe('https://events.workers.do/ingest')
      expect(options.method).toBe('POST')

      const body = JSON.parse(options.body)
      expect(body.events).toHaveLength(1)
      expect(body.events[0].type).toBe('test.event')
      expect(body.events[0].data).toBe(123)
    })

    it('should clear batch after successful flush', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event' })
      expect(emitter.pendingCount).toBe(1)

      await emitter.flush()

      expect(emitter.pendingCount).toBe(0)
    })

    it('should not send if batch is empty', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      await emitter.flush()

      expect(mockFetch).not.toHaveBeenCalled()
    })

    it('should include API key in Authorization header when configured', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        apiKey: 'secret-api-key',
      })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer secret-api-key',
          }),
        })
      )
    })
  })

  describe('flush() error handling and retries', () => {
    it('should schedule retry on network error', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // Should have stored events for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )

      // Should have scheduled alarm
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should schedule retry on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce(new Response('Error', { status: 500 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // Should have stored events for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should combine events with existing retry queue', async () => {
      // Pre-populate retry queue
      const existingEvents: DurableEvent[] = [
        { type: 'old.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      mockFetch.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'new.event' })
      await emitter.flush()

      // Should have combined events
      const putCalls = mockCtx.storage.put.mock.calls
      const retryCall = putCalls.find((call: unknown[]) => call[0] === '_events:retry')
      expect(retryCall).toBeDefined()
      expect((retryCall?.[1] as DurableEvent[]).length).toBe(2)
    })
  })

  describe('handleAlarm()', () => {
    it('should retry failed events', async () => {
      const failedEvents: DurableEvent[] = [
        { type: 'failed.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)

      const emitter = new EventEmitter(mockCtx, mockEnv)

      await emitter.handleAlarm()

      expect(mockFetch).toHaveBeenCalledTimes(1)
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events).toEqual(failedEvents)
    })

    it('should clear retry queue on success', async () => {
      const failedEvents: DurableEvent[] = [
        { type: 'failed.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)

      const emitter = new EventEmitter(mockCtx, mockEnv)

      await emitter.handleAlarm()

      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retry')
    })

    it('should schedule another alarm on retry failure', async () => {
      const failedEvents: DurableEvent[] = [
        { type: 'failed.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)
      mockFetch.mockRejectedValueOnce(new Error('Still failing'))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      await emitter.handleAlarm()

      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should do nothing if no events to retry', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      await emitter.handleAlarm()

      expect(mockFetch).not.toHaveBeenCalled()
    })
  })

  describe('emitChange() - CDC events', () => {
    it('should not emit when cdc is disabled', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { cdc: false })

      emitter.emitChange('insert', 'users', 'user-1', { name: 'Alice' })

      expect(emitter.pendingCount).toBe(0)
    })

    it('should emit insert event with cdc enabled', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { cdc: true })

      emitter.emitChange('insert', 'users', 'user-1', { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()

      expect(emitter.pendingCount).toBe(1)
    })

    it('should emit update event with previous value when trackPrevious enabled', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        cdc: true,
        trackPrevious: true,
      })

      emitter.emitChange('update', 'users', 'user-1', { name: 'Bob' }, { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()

      expect(emitter.pendingCount).toBe(1)
    })

    it('should capture SQLite bookmark for PITR', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { cdc: true })

      emitter.emitChange('insert', 'users', 'user-1', { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()
      await emitter.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].bookmark).toBe('bookmark-123')
    })

    it('should emit delete event', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { cdc: true })

      emitter.emitChange('delete', 'users', 'user-1', undefined, { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()

      expect(emitter.pendingCount).toBe(1)
    })
  })

  describe('enrichFromRequest()', () => {
    it('should capture colo from CF properties', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)
      const request = createMockRequest({ colo: 'LAX' })

      emitter.enrichFromRequest(request)

      expect(emitter.doIdentity.colo).toBe('LAX')
    })

    it('should capture worker from headers', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)
      const request = createMockRequest({ worker: 'my-worker' })

      emitter.enrichFromRequest(request)

      expect(emitter.doIdentity.worker).toBe('my-worker')
    })

    it('should capture DO class from headers', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)
      const request = createMockRequest({ doClass: 'MyDurableObject' })

      emitter.enrichFromRequest(request)

      expect(emitter.doIdentity.class).toBe('MyDurableObject')
    })

    it('should include enriched identity in emitted events', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)
      const request = createMockRequest({
        colo: 'DFW',
        worker: 'api-worker',
        doClass: 'UserDO',
      })

      emitter.enrichFromRequest(request)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].do).toEqual({
        id: 'test-do-123',
        name: 'test-object',
        colo: 'DFW',
        worker: 'api-worker',
        class: 'UserDO',
      })
    })
  })

  describe('persistBatch() / restoreBatch() - hibernation support', () => {
    it('should persist batch to storage', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'event1' })
      emitter.emit({ type: 'event2' })

      await emitter.persistBatch()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:batch',
        expect.arrayContaining([
          expect.objectContaining({ type: 'event1' }),
          expect.objectContaining({ type: 'event2' }),
        ])
      )
    })

    it('should not persist if batch is empty', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      await emitter.persistBatch()

      // put should not be called with batch key
      const putCalls = mockCtx.storage.put.mock.calls
      const batchCall = putCalls.find((call: unknown[]) => call[0] === '_events:batch')
      expect(batchCall).toBeUndefined()
    })

    it('should restore batch from storage on construction', async () => {
      // Pre-populate batch in storage
      const persistedBatch: DurableEvent[] = [
        { type: 'persisted.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:batch', persistedBatch)

      const emitter = new EventEmitter(mockCtx, mockEnv)

      // Wait for async restore
      await vi.runAllTimersAsync()

      expect(emitter.pendingCount).toBe(1)
    })

    it('should delete batch from storage after restore', async () => {
      const persistedBatch: DurableEvent[] = [
        { type: 'persisted.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:batch', persistedBatch)

      new EventEmitter(mockCtx, mockEnv)

      // Wait for async restore
      await vi.runAllTimersAsync()

      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:batch')
    })
  })

  describe('R2 streaming - lakehouse writes', () => {
    let mockR2: ReturnType<typeof createMockR2Bucket>

    beforeEach(() => {
      mockR2 = createMockR2Bucket()
    })

    it('should stream events to R2 on successful flush', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockR2.put).toHaveBeenCalled()
    })

    it('should write JSONL format to R2', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      emitter.emit({ type: 'event1', data: 1 })
      emitter.emit({ type: 'event2', data: 2 })
      await emitter.flush()

      const [key, body] = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls[0]
      const lines = body.split('\n')
      expect(lines).toHaveLength(2)

      // Each line should be valid JSON
      const event1 = JSON.parse(lines[0])
      const event2 = JSON.parse(lines[1])
      expect(event1.type).toBe('event1')
      expect(event2.type).toBe('event2')
    })

    it('should use Parquet-friendly path structure', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      const [key] = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls[0]
      // Path should be: events/{year}/{month}/{day}/{hour}/{do_id}_{timestamp}.jsonl
      expect(key).toMatch(/^events\/\d{4}\/\d{2}\/\d{2}\/\d{2}\/[\w-]+_\d+\.jsonl$/)
    })

    it('should include metadata in R2 object', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      const [, , options] = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls[0]
      expect(options.httpMetadata.contentType).toBe('application/x-ndjson')
      expect(options.customMetadata.doId).toBe('test-do-123')
      expect(options.customMetadata.eventCount).toBe('1')
    })

    it('should not write to R2 if r2Bucket not configured', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv) // No r2Bucket

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockR2.put).not.toHaveBeenCalled()
    })

    it('should stream to R2 on successful retry', async () => {
      const failedEvents: DurableEvent[] = [
        { type: 'failed.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)

      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      await emitter.handleAlarm()

      expect(mockR2.put).toHaveBeenCalled()
    })
  })

  describe('flush interval timer', () => {
    it('should auto-flush after flushIntervalMs', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { flushIntervalMs: 500 })

      emitter.emit({ type: 'test.event' })

      // Not flushed yet
      expect(mockFetch).not.toHaveBeenCalled()

      // Advance timer
      await vi.advanceTimersByTimeAsync(500)

      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should clear timer on manual flush', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { flushIntervalMs: 1000 })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // Advance timer past the flush interval
      await vi.advanceTimersByTimeAsync(1500)

      // Should only have flushed once (from manual flush)
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
  })

  describe('retry queue cap', () => {
    it('should cap retry queue at 10000 events', async () => {
      // Pre-populate with 9999 events
      const existingEvents: DurableEvent[] = Array.from({ length: 9999 }, (_, i) => ({
        type: `old.event.${i}`,
        ts: '2024-01-01T00:00:00Z',
        do: { id: 'test' },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      mockFetch.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      // Add 5 more events that will fail
      for (let i = 0; i < 5; i++) {
        emitter.emit({ type: `new.event.${i}` })
      }
      await emitter.flush()

      // Should have stored max 10000 events (truncating old ones)
      const putCalls = mockCtx.storage.put.mock.calls
      const retryCall = putCalls.find((call: unknown[]) => call[0] === '_events:retry')
      expect(retryCall).toBeDefined()
      expect((retryCall?.[1] as DurableEvent[]).length).toBe(10000)
    })
  })

  describe('default options', () => {
    it('should use default endpoint', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        'https://events.workers.do/ingest',
        expect.any(Object)
      )
    })

    it('should use default batchSize of 100', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      // Emit 99 events
      for (let i = 0; i < 99; i++) {
        emitter.emit({ type: `event.${i}` })
      }

      // Should not have flushed yet
      expect(mockFetch).not.toHaveBeenCalled()
      expect(emitter.pendingCount).toBe(99)

      // Emit 100th event
      emitter.emit({ type: 'event.99' })

      // Should trigger auto-flush
      expect(emitter.pendingCount).toBe(0)
    })
  })

  describe('emitChange without trackPrevious', () => {
    it('should not include prev when trackPrevious is disabled', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        cdc: true,
        trackPrevious: false,
      })

      emitter.emitChange('update', 'users', 'user-1', { name: 'Bob' }, { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()
      await emitter.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].prev).toBeUndefined()
    })
  })

  describe('edge cases', () => {
    it('should handle empty DO name', () => {
      const ctxWithNoName = createMockCtx({ id: 'test-id' })
      const emitter = new EventEmitter(ctxWithNoName, mockEnv)

      expect(emitter.doIdentity.name).toBeUndefined()
    })

    it('should handle request without CF properties', () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)
      const request = new Request('https://example.com')

      emitter.enrichFromRequest(request)

      expect(emitter.doIdentity.colo).toBeUndefined()
    })

    it('should handle concurrent flush calls', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'event1' })

      // Start two flushes concurrently
      const flush1 = emitter.flush()
      const flush2 = emitter.flush()

      await Promise.all([flush1, flush2])

      // Should only have made one fetch call (second flush had no events)
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
  })
})
