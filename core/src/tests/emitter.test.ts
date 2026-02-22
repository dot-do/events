/**
 * EventEmitter Tests
 *
 * Tests for pipeline-first EventEmitter functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { EventEmitter } from '../emitter.js'
import { EventBufferFullError, CircuitBreakerOpenError } from '../types.js'
import { createMockCtx } from './mocks.js'

// ============================================================================
// Tests
// ============================================================================

describe('EventEmitter', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn().mockResolvedValue(undefined) }

    // Use fake timers
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('emit()', () => {
    it('should add events to batch', () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: { value: 'hello' } })

      expect(emitter.pendingCount).toBe(1)
    })

    it('should add timestamp and DO identity to events', () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: { value: 'hello' } })

      // Access batch through pendingCount check and flush
      expect(emitter.pendingCount).toBe(1)
      expect(emitter.identity).toEqual({
        id: 'test-do-123',
        name: 'test-object',
      })
    })

    it('should batch multiple events', () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'event1', data: {} })
      emitter.emit({ type: 'test', event: 'event2', data: {} })
      emitter.emit({ type: 'test', event: 'event3', data: {} })

      expect(emitter.pendingCount).toBe(3)
    })
  })

  describe('auto-flush on batch size', () => {
    it('should auto-flush when batch reaches batchSize', async () => {
      const emitter = new EventEmitter(mockPipeline, { batchSize: 3 }, mockCtx)

      emitter.emit({ type: 'test', event: 'event1', data: {} })
      emitter.emit({ type: 'test', event: 'event2', data: {} })

      // Should not have flushed yet
      expect(mockPipeline.send).not.toHaveBeenCalled()

      emitter.emit({ type: 'test', event: 'event3', data: {} })

      // Should trigger flush
      await vi.runAllTimersAsync()

      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
      expect(emitter.pendingCount).toBe(0)
    })
  })

  describe('flush()', () => {
    it('should send events via pipeline', async () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: { value: 123 } })
      await emitter.flush()

      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
      const sentEvents = mockPipeline.send.mock.calls[0][0]
      expect(sentEvents).toHaveLength(1)
      expect(sentEvents[0].type).toBe('test')
      expect(sentEvents[0].event).toBe('test.event')
      expect(sentEvents[0].data).toEqual({ value: 123 })
    })

    it('should clear batch after successful flush', async () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      expect(emitter.pendingCount).toBe(1)

      await emitter.flush()

      expect(emitter.pendingCount).toBe(0)
    })

    it('should not send if batch is empty', async () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      await emitter.flush()

      expect(mockPipeline.send).not.toHaveBeenCalled()
    })
  })

  describe('flush() error handling and retries', () => {
    it('should schedule retry on network error', async () => {
      mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      // Should have stored events for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )

      // Should have scheduled alarm
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should combine events with existing retry queue', async () => {
      // Pre-populate retry queue
      const existingEvents: Record<string, unknown>[] = [
        { type: 'test', event: 'old.event', data: {}, ts: '2024-01-01T00:00:00Z', meta: { do: { id: 'test' } } },
      ]
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'new.event', data: {} })
      await emitter.flush()

      // Should have combined events
      const putCalls = mockCtx.storage.put.mock.calls
      const retryCall = putCalls.find((call: unknown[]) => call[0] === '_events:retry')
      expect(retryCall).toBeDefined()
      expect((retryCall?.[1] as Record<string, unknown>[]).length).toBe(2)
    })
  })

  describe('handleAlarm()', () => {
    it('should retry failed events', async () => {
      const failedEvents: Record<string, unknown>[] = [
        { type: 'test', event: 'failed.event', data: {}, ts: '2024-01-01T00:00:00Z', meta: { do: { id: 'test' } } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      await emitter.handleAlarm()

      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
      const sentEvents = mockPipeline.send.mock.calls[0][0]
      expect(sentEvents).toEqual(failedEvents)
    })

    it('should clear retry queue on success', async () => {
      const failedEvents: Record<string, unknown>[] = [
        { type: 'test', event: 'failed.event', data: {}, ts: '2024-01-01T00:00:00Z', meta: { do: { id: 'test' } } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      await emitter.handleAlarm()

      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retry')
    })

    it('should schedule another alarm on retry failure', async () => {
      const failedEvents: Record<string, unknown>[] = [
        { type: 'test', event: 'failed.event', data: {}, ts: '2024-01-01T00:00:00Z', meta: { do: { id: 'test' } } },
      ]
      mockCtx.storage._storage.set('_events:retry', failedEvents)
      mockPipeline.send.mockRejectedValueOnce(new Error('Still failing'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      await emitter.handleAlarm()

      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should do nothing if no events to retry', async () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      await emitter.handleAlarm()

      expect(mockPipeline.send).not.toHaveBeenCalled()
    })
  })

  describe('emitChange() - CDC events', () => {
    it('should not emit when cdc is disabled', () => {
      const emitter = new EventEmitter(mockPipeline, { cdc: false }, mockCtx)

      emitter.emitChange('created', 'users', 'user-1', { name: 'Alice' })

      expect(emitter.pendingCount).toBe(0)
    })

    it('should emit insert event with cdc enabled', async () => {
      const emitter = new EventEmitter(mockPipeline, { cdc: true }, mockCtx)

      emitter.emitChange('created', 'users', 'user-1', { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()

      expect(emitter.pendingCount).toBe(1)
    })

    it('should emit update event with previous value when trackPrevious enabled', async () => {
      const emitter = new EventEmitter(mockPipeline, {
        cdc: true,
        trackPrevious: true,
      }, mockCtx)

      emitter.emitChange('updated', 'users', 'user-1', { name: 'Bob' }, { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()

      expect(emitter.pendingCount).toBe(1)
    })

    it('should capture SQLite bookmark for PITR', async () => {
      const emitter = new EventEmitter(mockPipeline, { cdc: true }, mockCtx)

      emitter.emitChange('created', 'users', 'user-1', { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()
      await emitter.flush()

      const sentEvents = mockPipeline.send.mock.calls[0][0]
      expect(sentEvents[0].meta.bookmark).toBe('bookmark-123')
    })

    it('should emit delete event', async () => {
      const emitter = new EventEmitter(mockPipeline, { cdc: true }, mockCtx)

      emitter.emitChange('deleted', 'users', 'user-1', undefined, { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()

      expect(emitter.pendingCount).toBe(1)
    })
  })

  describe('flush interval timer', () => {
    it('should auto-flush after flushIntervalMs', async () => {
      const emitter = new EventEmitter(mockPipeline, { flushIntervalMs: 500 }, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      // Not flushed yet
      expect(mockPipeline.send).not.toHaveBeenCalled()

      // Advance timer
      await vi.advanceTimersByTimeAsync(500)

      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
    })

    it('should clear timer on manual flush', async () => {
      const emitter = new EventEmitter(mockPipeline, { flushIntervalMs: 1000 }, mockCtx)

      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      // Advance timer past the flush interval
      await vi.advanceTimersByTimeAsync(1500)

      // Should only have flushed once (from manual flush)
      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
    })
  })

  describe('retry queue cap', () => {
    it('should cap retry queue at 10000 events and throw EventBufferFullError', async () => {
      // Pre-populate with 9999 events
      const existingEvents: Record<string, unknown>[] = Array.from({ length: 9999 }, (_, i) => ({
        type: 'test',
        event: `old.event.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      // Add 5 more events that will fail
      for (let i = 0; i < 5; i++) {
        emitter.emit({ type: 'test', event: `new.event.${i}`, data: {} })
      }

      // flush() should throw EventBufferFullError when events are dropped
      await expect(emitter.flush()).rejects.toThrow(EventBufferFullError)

      // Should have stored max 10000 events (truncating old ones)
      const putCalls = mockCtx.storage.put.mock.calls
      const retryCall = putCalls.find((call: unknown[]) => call[0] === '_events:retry')
      expect(retryCall).toBeDefined()
      expect((retryCall?.[1] as Record<string, unknown>[]).length).toBe(10000)
    })

    it('should use configurable maxRetryQueueSize', async () => {
      const customMax = 100
      const existingEvents: Record<string, unknown>[] = Array.from({ length: 95 }, (_, i) => ({
        type: 'test',
        event: `old.event.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockPipeline, { maxRetryQueueSize: customMax }, mockCtx)

      for (let i = 0; i < 10; i++) {
        emitter.emit({ type: 'test', event: `new.event.${i}`, data: {} })
      }

      await expect(emitter.flush()).rejects.toThrow(EventBufferFullError)

      const putCalls = mockCtx.storage.put.mock.calls
      const retryCall = putCalls.find((call: unknown[]) => call[0] === '_events:retry')
      expect(retryCall).toBeDefined()
      expect((retryCall?.[1] as Record<string, unknown>[]).length).toBe(customMax)
    })

    it('should report dropped count in EventBufferFullError', async () => {
      const existingEvents: Record<string, unknown>[] = Array.from({ length: 100 }, (_, i) => ({
        type: 'test',
        event: `old.event.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockPipeline, { maxRetryQueueSize: 100 }, mockCtx)

      for (let i = 0; i < 10; i++) {
        emitter.emit({ type: 'test', event: `new.event.${i}`, data: {} })
      }

      try {
        await emitter.flush()
      } catch (e) {
        expect(e).toBeInstanceOf(EventBufferFullError)
        expect((e as EventBufferFullError).droppedCount).toBe(10)
      }
    })
  })

  describe('circuit breaker', () => {
    it('should open circuit breaker after maxConsecutiveFailures', async () => {
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 3 }, mockCtx)

      // Fail 3 times
      for (let i = 0; i < 3; i++) {
        mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))
        emitter.emit({ type: 'test', event: `test.event.${i}`, data: {} })
        await emitter.flush()
      }

      // Circuit breaker should now be open — flush() throws
      emitter.emit({ type: 'test', event: 'blocked.event', data: {} })
      await expect(emitter.flush()).rejects.toThrow(CircuitBreakerOpenError)
    })

    it('should close circuit breaker on success', async () => {
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 3 }, mockCtx)

      // Fail 2 times (not enough to open)
      for (let i = 0; i < 2; i++) {
        mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))
        emitter.emit({ type: 'test', event: `test.event.${i}`, data: {} })
        await emitter.flush()
      }

      // Now succeed
      mockPipeline.send.mockResolvedValueOnce(undefined)
      emitter.emit({ type: 'test', event: 'success.event', data: {} })
      await emitter.flush()

      // Should be able to emit and flush again without error
      emitter.emit({ type: 'test', event: 'next.event', data: {} })
      await expect(emitter.flush()).resolves.toBeUndefined()
    })

    it('should respect circuitBreakerResetMs for half-open state', async () => {
      const resetMs = 5000
      const emitter = new EventEmitter(mockPipeline, {
        maxConsecutiveFailures: 2,
        circuitBreakerResetMs: resetMs,
      }, mockCtx)

      // Fail twice to open circuit
      for (let i = 0; i < 2; i++) {
        mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))
        emitter.emit({ type: 'test', event: `test.event.${i}`, data: {} })
        await emitter.flush()
      }

      // Should be open
      expect(emitter.circuitBreakerOpen).toBe(true)

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Should allow retry (half-open state)
      expect(emitter.circuitBreakerOpen).toBe(false)
    })

    it('should be disabled when maxConsecutiveFailures is 0', async () => {
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 0 }, mockCtx)

      // Fail many times
      for (let i = 0; i < 20; i++) {
        mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))
        emitter.emit({ type: 'test', event: `test.event.${i}`, data: {} })
        await emitter.flush()
      }

      // Should still allow emit and flush without CircuitBreakerOpenError
      emitter.emit({ type: 'test', event: 'still.working', data: {} })
      mockPipeline.send.mockResolvedValueOnce(undefined)
      await expect(emitter.flush()).resolves.toBeUndefined()
    })

    it('should cap circuitBreakerResetMs at 1 hour maximum', async () => {
      const twoHoursMs = 2 * 60 * 60 * 1000 // 2 hours
      const oneHourMs = 60 * 60 * 1000 // 1 hour (max)

      const emitter = new EventEmitter(mockPipeline, {
        maxConsecutiveFailures: 2,
        circuitBreakerResetMs: twoHoursMs, // Try to set 2 hours
      }, mockCtx)

      // Fail twice to open circuit
      for (let i = 0; i < 2; i++) {
        mockPipeline.send.mockRejectedValueOnce(new Error('Network error'))
        emitter.emit({ type: 'test', event: `test.event.${i}`, data: {} })
        await emitter.flush()
      }

      // Should be open
      expect(emitter.circuitBreakerOpen).toBe(true)

      // Advance time to just under 1 hour — should still be open (capped at 1hr, not 2hr)
      await vi.advanceTimersByTimeAsync(oneHourMs - 100)
      expect(emitter.circuitBreakerOpen).toBe(true)

      // Advance past 1 hour — should now be closed
      await vi.advanceTimersByTimeAsync(200)
      expect(emitter.circuitBreakerOpen).toBe(false)
    })
  })

  describe('default options', () => {
    it('should use default batchSize of 100', () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      // Emit 99 events
      for (let i = 0; i < 99; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
      }

      // Should not have flushed yet
      expect(mockPipeline.send).not.toHaveBeenCalled()
      expect(emitter.pendingCount).toBe(99)

      // Emit 100th event
      emitter.emit({ type: 'test', event: 'event.99', data: {} })

      // Should trigger auto-flush
      expect(emitter.pendingCount).toBe(0)
    })
  })

  describe('emitChange without trackPrevious', () => {
    it('should not include prev when trackPrevious is disabled', async () => {
      const emitter = new EventEmitter(mockPipeline, {
        cdc: true,
        trackPrevious: false,
      }, mockCtx)

      emitter.emitChange('updated', 'users', 'user-1', { name: 'Bob' }, { name: 'Alice' })
      // Wait for async bookmark retrieval to complete
      await Promise.resolve()
      await emitter.flush()

      const sentEvents = mockPipeline.send.mock.calls[0][0]
      expect(sentEvents[0].meta.prev).toBeUndefined()
    })
  })

  describe('edge cases', () => {
    it('should handle empty DO name', () => {
      const ctxWithNoName = createMockCtx({ id: 'test-id' })
      const emitter = new EventEmitter(mockPipeline, {}, ctxWithNoName)

      expect(emitter.identity.name).toBeUndefined()
    })

    it('should handle concurrent flush calls', async () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'event1', data: {} })

      // Start two flushes concurrently
      const flush1 = emitter.flush()
      const flush2 = emitter.flush()

      await Promise.all([flush1, flush2])

      // Should only have made one pipeline.send call (second flush had no events)
      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
    })
  })
})
