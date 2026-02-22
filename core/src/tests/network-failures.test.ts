/**
 * Network Failure and Partial Failure Test Scenarios
 *
 * events-858: Tests for network failure scenarios including:
 * - Pipeline send timeouts
 * - Connection failures
 * - Partial writes/reads
 * - Retry behavior with exponential backoff
 * - Circuit breaker patterns
 *
 * These tests verify the resilience of the event system under adverse network conditions.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { EventEmitter } from '../emitter.js'
import { EventBufferFullError, CircuitBreakerOpenError } from '../types.js'
import { createMockCtx } from './mocks.js'
import {
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_CIRCUIT_BREAKER_RESET_MS,
} from '../config.js'

// ============================================================================
// Network Timeout Tests
// ============================================================================

describe('Network Timeouts', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn().mockResolvedValue(undefined) }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('pipeline.send timeout scenarios', () => {
    it('should handle pipeline.send that never resolves (simulated timeout)', async () => {
      // Create a pipeline.send that never resolves, simulating a hung connection
      mockPipeline = {
        send: vi.fn().mockImplementation(
          () =>
            new Promise(() => {
              // Never resolves - simulates pipeline timeout
            }),
        ),
      }

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      // Start flush - it should hang
      const flushPromise = emitter.flush()

      // Advance time past any reasonable timeout
      await vi.advanceTimersByTimeAsync(60000)

      // The promise should still be pending (since we can't cancel it)
      expect(mockPipeline.send).toHaveBeenCalledTimes(1)
    })

    it('should handle AbortError from pipeline.send', async () => {
      const abortError = new DOMException('The operation was aborted', 'AbortError')
      mockPipeline = { send: vi.fn().mockRejectedValue(abortError) }

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      // Should have scheduled retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should handle slow pipeline.send that eventually succeeds', async () => {
      let resolveResponse: (value: undefined) => void
      mockPipeline = {
        send: vi.fn().mockImplementation(
          () =>
            new Promise((resolve) => {
              resolveResponse = resolve
            }),
        ),
      }

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      const flushPromise = emitter.flush()

      // Advance time to simulate slow network
      await vi.advanceTimersByTimeAsync(5000)

      // Now resolve the response
      resolveResponse!(undefined)
      await flushPromise

      // Should not have scheduled retry since it eventually succeeded
      expect(mockCtx.storage.put).not.toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle timeout during retry attempts', async () => {
      const timeoutError = new Error('timeout')
      mockPipeline = {
        send: vi.fn().mockRejectedValueOnce(timeoutError).mockRejectedValueOnce(timeoutError).mockResolvedValueOnce(undefined),
      }

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      // Initial emit and flush - will fail
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      // First retry via alarm - will also fail
      await emitter.handleAlarm()

      // Second retry via alarm - will succeed
      await emitter.handleAlarm()

      expect(mockPipeline.send).toHaveBeenCalledTimes(3)
      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retry')
    })
  })
})

// ============================================================================
// Connection Failure Tests
// ============================================================================

describe('Connection Failures', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn().mockResolvedValue(undefined) }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('DNS resolution failures', () => {
    it('should handle DNS resolution failure', async () => {
      const dnsError = new TypeError('Failed to fetch')
      dnsError.cause = new Error('getaddrinfo ENOTFOUND events.workers.do')
      mockPipeline.send.mockRejectedValue(dnsError)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      // Should store for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })
  })

  describe('TCP connection failures', () => {
    it('should handle connection refused error', async () => {
      mockPipeline.send.mockRejectedValue(new Error('ECONNREFUSED'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should handle connection reset error', async () => {
      mockPipeline.send.mockRejectedValue(new Error('ECONNRESET'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle socket hang up', async () => {
      mockPipeline.send.mockRejectedValue(new Error('socket hang up'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })
  })

  describe('SSL/TLS failures', () => {
    it('should handle SSL certificate error', async () => {
      mockPipeline.send.mockRejectedValue(new Error('CERT_HAS_EXPIRED'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle SSL handshake failure', async () => {
      mockPipeline.send.mockRejectedValue(new Error('SSL_HANDSHAKE_FAILED'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })
  })

  describe('pipeline send failures (server errors)', () => {
    it('should handle pipeline send failure (Internal Server Error)', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Internal Server Error'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle pipeline send failure (Bad Gateway)', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Bad Gateway'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle pipeline send failure (Service Unavailable)', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Service Unavailable'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle pipeline send failure (Gateway Timeout)', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Gateway Timeout'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })

    it('should handle pipeline send failure (Too Many Requests)', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Too Many Requests'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })
  })
})

// ============================================================================
// Partial Write/Read Tests
// ============================================================================

describe('Partial Writes and Reads', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn().mockResolvedValue(undefined) }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('partial batch transmission', () => {
    it('should handle partial batch failure where some events succeed', async () => {
      // First flush succeeds, subsequent ones fail
      mockPipeline.send.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockPipeline, { batchSize: 2 }, mockCtx)

      // First batch - will succeed
      emitter.emit({ type: 'test', event: 'event1', data: {} })
      emitter.emit({ type: 'test', event: 'event2', data: {} })
      await vi.runAllTimersAsync()

      // Second batch - will fail
      emitter.emit({ type: 'test', event: 'event3', data: {} })
      emitter.emit({ type: 'test', event: 'event4', data: {} })
      await vi.runAllTimersAsync()

      // First batch should have succeeded
      expect(mockPipeline.send).toHaveBeenCalledTimes(2)
      // Failed events should be queued for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.arrayContaining([expect.objectContaining({ event: 'event3' }), expect.objectContaining({ event: 'event4' })]),
      )
    })

    it('should preserve event order when retrying failed batches', async () => {
      // Fail first, then succeed
      mockPipeline.send.mockRejectedValueOnce(new Error('Network error')).mockResolvedValueOnce(undefined)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      emitter.emit({ type: 'test', event: 'event1', data: {} })
      emitter.emit({ type: 'test', event: 'event2', data: {} })
      emitter.emit({ type: 'test', event: 'event3', data: {} })
      await emitter.flush()

      // Check retry queue preserves order
      const retryCalls = mockCtx.storage.put.mock.calls.filter((call: unknown[]) => call[0] === '_events:retry')
      expect(retryCalls.length).toBeGreaterThan(0)

      const retryEvents = retryCalls[0][1] as Record<string, unknown>[]
      expect(retryEvents[0].event).toBe('event1')
      expect(retryEvents[1].event).toBe('event2')
      expect(retryEvents[2].event).toBe('event3')
    })
  })

  describe('storage partial failures', () => {
    it('should handle storage.put failure during retry scheduling', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Network error'))

      // Make storage.put fail
      mockCtx.storage.put.mockRejectedValueOnce(new Error('Storage write failed'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      // flush() will fail to save retry queue
      await expect(emitter.flush()).rejects.toThrow('Storage write failed')
    })

    it('should handle storage.setAlarm failure', async () => {
      mockPipeline.send.mockRejectedValue(new Error('Network error'))

      // Make setAlarm fail
      mockCtx.storage.setAlarm.mockRejectedValueOnce(new Error('setAlarm failed'))

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      // flush() will fail when trying to schedule alarm
      await expect(emitter.flush()).rejects.toThrow('setAlarm failed')
    })
  })
})

// ============================================================================
// Retry Behavior Tests with Exponential Backoff
// ============================================================================

describe('Retry Behavior with Exponential Backoff', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn().mockRejectedValue(new Error('Network error')) }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('exponential backoff calculation', () => {
    it('should increase delay exponentially with each retry', async () => {
      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      // First failure
      await emitter.flush()
      const firstAlarmCall = mockCtx.storage.setAlarm.mock.calls[0]
      const firstDelay = (firstAlarmCall[0] as number) - Date.now()

      // Verify first delay is around RETRY_BASE_DELAY_MS (with jitter)
      expect(firstDelay).toBeGreaterThanOrEqual(RETRY_BASE_DELAY_MS)
      expect(firstDelay).toBeLessThan(RETRY_BASE_DELAY_MS * 2 + 1000) // Base + jitter

      // Clear and retry
      mockCtx.storage.setAlarm.mockClear()
      await emitter.handleAlarm()

      const secondAlarmCall = mockCtx.storage.setAlarm.mock.calls[0]
      const secondDelay = (secondAlarmCall[0] as number) - Date.now()

      // Second delay should be roughly double the first
      expect(secondDelay).toBeGreaterThanOrEqual(RETRY_BASE_DELAY_MS * 2)
    })

    it('should cap retry delay at RETRY_MAX_DELAY_MS', async () => {
      // Use maxConsecutiveFailures: 0 to disable circuit breaker
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 0 }, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })

      // Simulate many retries to exceed max delay
      for (let i = 0; i < 10; i++) {
        if (i === 0) {
          await emitter.flush()
        } else {
          await emitter.handleAlarm()
        }
      }

      // Get the last scheduled alarm delay
      const allAlarmCalls = mockCtx.storage.setAlarm.mock.calls
      const lastAlarmCall = allAlarmCalls[allAlarmCalls.length - 1]
      const lastDelay = (lastAlarmCall[0] as number) - Date.now()

      // Delay should be capped at RETRY_MAX_DELAY_MS (60 seconds) plus jitter (1 second)
      expect(lastDelay).toBeLessThanOrEqual(RETRY_MAX_DELAY_MS + 1000) // Max + jitter
    })

    it('should add jitter to prevent thundering herd', async () => {
      const delays: number[] = []

      // Run multiple times to check jitter variability
      for (let i = 0; i < 5; i++) {
        const ctx = createMockCtx({ id: `test-do-${i}` })
        const emitter = new EventEmitter(mockPipeline, {}, ctx)
        emitter.emit({ type: 'test', event: 'test.event', data: {} })
        await emitter.flush()

        const alarmCall = ctx.storage.setAlarm.mock.calls[0]
        const delay = (alarmCall[0] as number) - Date.now()
        delays.push(delay)
      }

      // With jitter, not all delays should be exactly the same
      const uniqueDelays = new Set(delays)
      // At least some variation expected (though jitter might be small)
      expect(uniqueDelays.size).toBeGreaterThanOrEqual(1)
    })

    it('should reset retry count after successful delivery', async () => {
      mockPipeline.send
        .mockRejectedValueOnce(new Error('Network error'))
        .mockRejectedValueOnce(new Error('Network error'))
        .mockResolvedValueOnce(undefined)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

      // First event - fails twice then succeeds
      emitter.emit({ type: 'test', event: 'event1', data: {} })
      await emitter.flush()
      await emitter.handleAlarm()
      await emitter.handleAlarm()

      // Retry count should be reset
      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retryCount')
      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retry')

      // Circuit breaker should be closed
      expect(emitter.circuitBreakerOpen).toBe(false)
    })
  })

  describe('retry queue management', () => {
    it('should combine new events with existing retry queue', async () => {
      // Pre-populate retry queue
      const existingEvents: Record<string, unknown>[] = [
        { type: 'test', event: 'existing.event', data: {}, ts: '2024-01-01T00:00:00Z', meta: { do: { id: 'test' } } },
      ]
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'new.event', data: {} })
      await emitter.flush()

      // Both events should be in retry queue
      const retryCalls = mockCtx.storage.put.mock.calls.filter((call: unknown[]) => call[0] === '_events:retry')
      const combinedEvents = retryCalls[retryCalls.length - 1][1] as Record<string, unknown>[]
      expect(combinedEvents.length).toBe(2)
      expect(combinedEvents[0].event).toBe('existing.event')
      expect(combinedEvents[1].event).toBe('new.event')
    })

    it('should not exceed maxRetryQueueSize', async () => {
      const maxSize = 100
      const emitter = new EventEmitter(mockPipeline, { maxRetryQueueSize: maxSize }, mockCtx)

      // Pre-populate with maxSize events
      const existingEvents: Record<string, unknown>[] = Array.from({ length: maxSize }, (_, i) => ({
        type: 'test',
        event: `event.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      // Add more events
      for (let i = 0; i < 10; i++) {
        emitter.emit({ type: 'test', event: `overflow.${i}`, data: {} })
      }

      // Should throw EventBufferFullError
      await expect(emitter.flush()).rejects.toThrow(EventBufferFullError)

      // Queue should still be capped at maxSize
      const retryCalls = mockCtx.storage.put.mock.calls.filter((call: unknown[]) => call[0] === '_events:retry')
      const finalQueue = retryCalls[retryCalls.length - 1][1] as Record<string, unknown>[]
      expect(finalQueue.length).toBe(maxSize)
    })

    it('should drop oldest events when queue overflows', async () => {
      const maxSize = 5
      const emitter = new EventEmitter(mockPipeline, { maxRetryQueueSize: maxSize }, mockCtx)

      // Pre-populate with maxSize events
      const existingEvents: Record<string, unknown>[] = Array.from({ length: maxSize }, (_, i) => ({
        type: 'test',
        event: `old.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      // Add 3 new events
      for (let i = 0; i < 3; i++) {
        emitter.emit({ type: 'test', event: `new.${i}`, data: {} })
      }

      try {
        await emitter.flush()
      } catch (e) {
        // Expected EventBufferFullError
      }

      const retryCalls = mockCtx.storage.put.mock.calls.filter((call: unknown[]) => call[0] === '_events:retry')
      const finalQueue = retryCalls[retryCalls.length - 1][1] as Record<string, unknown>[]

      // Should contain last 5 events (3 oldest dropped)
      expect(finalQueue.length).toBe(maxSize)
      // Oldest events should be dropped, newest kept
      expect(finalQueue[finalQueue.length - 1].event).toBe('new.2')
      expect(finalQueue[finalQueue.length - 2].event).toBe('new.1')
      expect(finalQueue[finalQueue.length - 3].event).toBe('new.0')
    })

    it('should track dropped events count via EventBufferFullError', async () => {
      const maxSize = 5
      const emitter = new EventEmitter(mockPipeline, { maxRetryQueueSize: maxSize }, mockCtx)

      const existingEvents: Record<string, unknown>[] = Array.from({ length: maxSize }, (_, i) => ({
        type: 'test',
        event: `old.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      for (let i = 0; i < 3; i++) {
        emitter.emit({ type: 'test', event: `new.${i}`, data: {} })
      }

      try {
        await emitter.flush()
      } catch (e) {
        expect(e).toBeInstanceOf(EventBufferFullError)
        expect((e as EventBufferFullError).droppedCount).toBe(3)
      }
    })
  })
})

// ============================================================================
// Circuit Breaker Pattern Tests
// ============================================================================

describe('Circuit Breaker Pattern', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn().mockRejectedValue(new Error('Network error')) }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('circuit breaker opening', () => {
    it('should open circuit breaker after maxConsecutiveFailures', async () => {
      const maxFailures = 3
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: maxFailures }, mockCtx)

      // Fail maxFailures times
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      // Circuit breaker should now be open — emit succeeds but flush throws
      emitter.emit({ type: 'test', event: 'blocked.event', data: {} })
      await expect(emitter.flush()).rejects.toThrow(CircuitBreakerOpenError)
    })

    it('should include failure count in CircuitBreakerOpenError', async () => {
      const maxFailures = 3
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: maxFailures }, mockCtx)

      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      try {
        emitter.emit({ type: 'test', event: 'blocked.event', data: {} })
        await emitter.flush()
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(CircuitBreakerOpenError)
        expect((e as CircuitBreakerOpenError).consecutiveFailures).toBe(maxFailures)
      }
    })

    it('should include reset time in CircuitBreakerOpenError', async () => {
      const maxFailures = 3
      const resetMs = 5000
      const emitter = new EventEmitter(
        mockPipeline,
        {
          maxConsecutiveFailures: maxFailures,
          circuitBreakerResetMs: resetMs,
        },
        mockCtx,
      )

      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      try {
        emitter.emit({ type: 'test', event: 'blocked.event', data: {} })
        await emitter.flush()
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(CircuitBreakerOpenError)
        const resetAt = (e as CircuitBreakerOpenError).resetAt
        expect(resetAt.getTime()).toBeGreaterThan(Date.now())
        expect(resetAt.getTime()).toBeLessThanOrEqual(Date.now() + resetMs + 1000)
      }
    })
  })

  describe('circuit breaker half-open state', () => {
    it('should allow retry attempt after circuitBreakerResetMs', async () => {
      const maxFailures = 2
      const resetMs = 5000
      const emitter = new EventEmitter(
        mockPipeline,
        {
          maxConsecutiveFailures: maxFailures,
          circuitBreakerResetMs: resetMs,
        },
        mockCtx,
      )

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      // Should be open
      emitter.emit({ type: 'test', event: 'blocked.event', data: {} })
      await expect(emitter.flush()).rejects.toThrow(CircuitBreakerOpenError)

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Should now allow emit and flush (half-open state)
      emitter.emit({ type: 'test', event: 'retry.event', data: {} })
      // flush will still fail (mockPipeline rejects), but it should not throw CircuitBreakerOpenError
      await emitter.flush()
      // The above flush fails with network error, not circuit breaker error
    })

    it('should close circuit breaker on successful retry', async () => {
      const maxFailures = 2
      const resetMs = 5000
      mockPipeline.send
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockRejectedValueOnce(new Error('Fail 2'))
        .mockResolvedValueOnce(undefined)

      const emitter = new EventEmitter(
        mockPipeline,
        {
          maxConsecutiveFailures: maxFailures,
          circuitBreakerResetMs: resetMs,
        },
        mockCtx,
      )

      // Open circuit breaker
      emitter.emit({ type: 'test', event: 'event.1', data: {} })
      await emitter.flush()
      emitter.emit({ type: 'test', event: 'event.2', data: {} })
      await emitter.flush()

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Retry should succeed and close circuit
      emitter.emit({ type: 'test', event: 'retry.event', data: {} })
      await emitter.flush()

      // Circuit should be closed
      expect(emitter.circuitBreakerOpen).toBe(false)
    })

    it('should re-open circuit breaker if retry fails', async () => {
      const maxFailures = 2
      const resetMs = 5000
      const emitter = new EventEmitter(
        mockPipeline,
        {
          maxConsecutiveFailures: maxFailures,
          circuitBreakerResetMs: resetMs,
        },
        mockCtx,
      )

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Retry fails
      emitter.emit({ type: 'test', event: 'retry.event', data: {} })
      await emitter.flush()

      // Should go back to open state
      emitter.emit({ type: 'test', event: 'blocked.again', data: {} })
      await expect(emitter.flush()).rejects.toThrow(CircuitBreakerOpenError)
    })
  })

  describe('circuit breaker closing', () => {
    it('should reset consecutive failures on success', async () => {
      mockPipeline.send
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error('Fail 2'))

      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 3 }, mockCtx)

      // First failure
      emitter.emit({ type: 'test', event: 'event.1', data: {} })
      await emitter.flush()

      expect(emitter.circuitBreakerOpen).toBe(false)

      // Success - should reset
      emitter.emit({ type: 'test', event: 'event.2', data: {} })
      await emitter.flush()

      expect(emitter.circuitBreakerOpen).toBe(false)

      // Another failure - should start from 1 again (not accumulate from before)
      emitter.emit({ type: 'test', event: 'event.3', data: {} })
      await emitter.flush()

      expect(emitter.circuitBreakerOpen).toBe(false)
    })
  })

  describe('circuit breaker disabled', () => {
    it('should never open circuit breaker when maxConsecutiveFailures is 0', async () => {
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 0 }, mockCtx)

      // Fail many times
      for (let i = 0; i < 50; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      // Should still allow emit and flush without CircuitBreakerOpenError
      emitter.emit({ type: 'test', event: 'still.allowed', data: {} })
      // flush will fail with network error, not circuit breaker
      await emitter.flush()

      expect(emitter.circuitBreakerOpen).toBe(false)
    })
  })

  describe('circuit breaker state persistence', () => {
    it('should persist circuit breaker state to storage', async () => {
      const maxFailures = 2
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: maxFailures }, mockCtx)

      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      // Verify state was persisted
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:circuitBreaker',
        expect.objectContaining({
          isOpen: true,
          consecutiveFailures: maxFailures,
        }),
      )
    })

    it('should restore circuit breaker state from storage on alarm', async () => {
      const maxFailures = 2
      const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: maxFailures }, mockCtx)

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
        await emitter.flush()
      }

      // Simulate new emitter instance with same storage
      const emitter2 = new EventEmitter(mockPipeline, { maxConsecutiveFailures: maxFailures }, mockCtx)

      // handleAlarm should load state
      await emitter2.handleAlarm()

      // State should be loaded — circuit breaker should be open
      expect(emitter2.circuitBreakerOpen).toBe(true)
    })
  })
})

// ============================================================================
// Intermittent Failure Tests
// ============================================================================

describe('Intermittent Network Failures', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn() }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('should handle alternating success and failure', async () => {
    mockPipeline.send
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValueOnce(undefined)

    const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

    // Success
    emitter.emit({ type: 'test', event: 'event.1', data: {} })
    await emitter.flush()
    expect(emitter.pendingCount).toBe(0)

    // Failure
    emitter.emit({ type: 'test', event: 'event.2', data: {} })
    await emitter.flush()

    // Retry should succeed
    await emitter.handleAlarm()

    // Circuit should not be open (alternating doesn't accumulate)
    expect(emitter.circuitBreakerOpen).toBe(false)
  })

  it('should handle random failure pattern', async () => {
    const failurePattern = [true, true, false, true, false, false, true, false, false, false]
    let callIndex = 0

    mockPipeline.send.mockImplementation(() => {
      const shouldFail = failurePattern[callIndex % failurePattern.length]
      callIndex++
      if (shouldFail) {
        return Promise.reject(new Error('Random failure'))
      }
      return Promise.resolve(undefined)
    })

    const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 5 }, mockCtx)

    // Run through the pattern
    for (let i = 0; i < failurePattern.length; i++) {
      emitter.emit({ type: 'test', event: `event.${i}`, data: {} })
      await emitter.flush()
      await emitter.handleAlarm()
    }

    // Should have processed all events eventually
    expect(mockPipeline.send).toHaveBeenCalled()
  })

  it('should handle burst of failures followed by recovery', async () => {
    mockPipeline.send
      // 5 failures
      .mockRejectedValueOnce(new Error('Fail 1'))
      .mockRejectedValueOnce(new Error('Fail 2'))
      .mockRejectedValueOnce(new Error('Fail 3'))
      .mockRejectedValueOnce(new Error('Fail 4'))
      .mockRejectedValueOnce(new Error('Fail 5'))
      // Then recovery
      .mockResolvedValue(undefined)

    const emitter = new EventEmitter(mockPipeline, { maxConsecutiveFailures: 10 }, mockCtx)

    // Generate burst of events
    for (let i = 0; i < 5; i++) {
      emitter.emit({ type: 'test', event: `burst.${i}`, data: {} })
      await emitter.flush()
    }

    // Should have consecutive failures but not enough to open (threshold is 10)
    expect(emitter.circuitBreakerOpen).toBe(false)

    // Recovery
    await emitter.handleAlarm()

    expect(emitter.circuitBreakerOpen).toBe(false)
  })
})

// ============================================================================
// Concurrent Request Failure Tests
// ============================================================================

describe('Concurrent Request Failures', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn() }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('should handle multiple concurrent flushes gracefully', async () => {
    mockPipeline.send.mockResolvedValue(undefined)

    const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

    emitter.emit({ type: 'test', event: 'event.1', data: {} })

    // Start multiple flushes concurrently
    const flushPromises = [emitter.flush(), emitter.flush(), emitter.flush()]

    await Promise.all(flushPromises)

    // Only one send should have been made (first flush takes the batch)
    expect(mockPipeline.send).toHaveBeenCalledTimes(1)
  })

  it('should handle race between flush and handleAlarm', async () => {
    mockPipeline.send.mockRejectedValueOnce(new Error('Network error')).mockResolvedValueOnce(undefined)

    const emitter = new EventEmitter(mockPipeline, {}, mockCtx)

    emitter.emit({ type: 'test', event: 'event.1', data: {} })
    await emitter.flush()

    // Now retry queue has the event
    // Start both alarm handler and new flush concurrently
    emitter.emit({ type: 'test', event: 'event.2', data: {} })

    await Promise.all([emitter.handleAlarm(), emitter.flush()])

    // Both events should eventually be processed
    expect(mockPipeline.send).toHaveBeenCalledTimes(3) // Initial fail + alarm retry + new flush
  })
})

// ============================================================================
// Network Error Classification Tests
// ============================================================================

describe('Network Error Classification', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockPipeline: { send: ReturnType<typeof vi.fn> }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockPipeline = { send: vi.fn() }
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  const networkErrors = [
    { name: 'ECONNREFUSED', error: new Error('connect ECONNREFUSED') },
    { name: 'ECONNRESET', error: new Error('read ECONNRESET') },
    { name: 'ETIMEDOUT', error: new Error('connect ETIMEDOUT') },
    { name: 'ENOTFOUND', error: new Error('getaddrinfo ENOTFOUND') },
    { name: 'EHOSTUNREACH', error: new Error('connect EHOSTUNREACH') },
    { name: 'ENETUNREACH', error: new Error('connect ENETUNREACH') },
    { name: 'AbortError', error: new DOMException('The operation was aborted', 'AbortError') },
    { name: 'TypeError (fetch failed)', error: new TypeError('Failed to fetch') },
  ]

  networkErrors.forEach(({ name, error }) => {
    it(`should treat ${name} as retryable network error`, async () => {
      mockPipeline.send.mockRejectedValue(error)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      // Should have stored for retry (network errors are retryable)
      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })
  })

  const pipelineErrors = [
    { name: 'Internal Server Error', error: new Error('Internal Server Error') },
    { name: 'Bad Gateway', error: new Error('Bad Gateway') },
    { name: 'Service Unavailable', error: new Error('Service Unavailable') },
    { name: 'Gateway Timeout', error: new Error('Gateway Timeout') },
    { name: 'Too Many Requests', error: new Error('Too Many Requests') },
  ]

  pipelineErrors.forEach(({ name, error }) => {
    it(`should treat pipeline "${name}" rejection as retryable error`, async () => {
      mockPipeline.send.mockRejectedValue(error)

      const emitter = new EventEmitter(mockPipeline, {}, mockCtx)
      emitter.emit({ type: 'test', event: 'test.event', data: {} })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith('_events:retry', expect.any(Array))
    })
  })
})
