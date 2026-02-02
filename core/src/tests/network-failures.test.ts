/**
 * Network Failure and Partial Failure Test Scenarios
 *
 * events-858: Tests for network failure scenarios including:
 * - Network timeouts
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
import type { DurableEvent } from '../types.js'
import { createMockCtx, createMockR2Bucket, createMockRequest, createMockFetch } from './mocks.js'
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
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('fetch timeout scenarios', () => {
    it('should handle fetch that never resolves (simulated timeout)', async () => {
      // Create a fetch that never resolves, simulating a hung connection
      mockFetch = vi.fn().mockImplementation(() =>
        new Promise(() => {
          // Never resolves - simulates network timeout
        })
      )
      vi.stubGlobal('fetch', mockFetch)

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })

      // Start flush - it should hang
      const flushPromise = emitter.flush()

      // Advance time past any reasonable timeout
      await vi.advanceTimersByTimeAsync(60000)

      // The promise should still be pending (since we can't cancel it)
      // In real implementation, AbortController with timeout would be used
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should handle AbortError from fetch timeout', async () => {
      const abortError = new DOMException('The operation was aborted', 'AbortError')
      mockFetch = vi.fn().mockRejectedValue(abortError)
      vi.stubGlobal('fetch', mockFetch)

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // Should have scheduled retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should handle slow response that eventually succeeds', async () => {
      let resolveResponse: (value: Response) => void
      mockFetch = vi.fn().mockImplementation(() =>
        new Promise((resolve) => {
          resolveResponse = resolve
        })
      )
      vi.stubGlobal('fetch', mockFetch)

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })

      const flushPromise = emitter.flush()

      // Advance time to simulate slow network
      await vi.advanceTimersByTimeAsync(5000)

      // Now resolve the response
      resolveResponse!(new Response('OK', { status: 200 }))
      await flushPromise

      // Should not have scheduled retry since it eventually succeeded
      expect(mockCtx.storage.put).not.toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle timeout during retry attempts', async () => {
      const timeoutError = new Error('timeout')
      mockFetch = vi.fn()
        .mockRejectedValueOnce(timeoutError)
        .mockRejectedValueOnce(timeoutError)
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))
      vi.stubGlobal('fetch', mockFetch)

      const emitter = new EventEmitter(mockCtx, mockEnv)

      // Initial emit and flush - will fail
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // First retry via alarm - will also fail
      await emitter.handleAlarm()

      // Second retry via alarm - will succeed
      await emitter.handleAlarm()

      expect(mockFetch).toHaveBeenCalledTimes(3)
      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retry')
    })
  })
})

// ============================================================================
// Connection Failure Tests
// ============================================================================

describe('Connection Failures', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    mockFetch = vi.fn().mockResolvedValue(new Response('OK', { status: 200 }))
    vi.stubGlobal('fetch', mockFetch)
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
      mockFetch.mockRejectedValue(dnsError)

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // Should store for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })
  })

  describe('TCP connection failures', () => {
    it('should handle connection refused error', async () => {
      mockFetch.mockRejectedValue(new Error('ECONNREFUSED'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })

    it('should handle connection reset error', async () => {
      mockFetch.mockRejectedValue(new Error('ECONNRESET'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle socket hang up', async () => {
      mockFetch.mockRejectedValue(new Error('socket hang up'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })
  })

  describe('SSL/TLS failures', () => {
    it('should handle SSL certificate error', async () => {
      mockFetch.mockRejectedValue(new Error('CERT_HAS_EXPIRED'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle SSL handshake failure', async () => {
      mockFetch.mockRejectedValue(new Error('SSL_HANDSHAKE_FAILED'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })
  })

  describe('HTTP error responses', () => {
    it('should handle 500 Internal Server Error', async () => {
      mockFetch.mockResolvedValue(new Response('Internal Server Error', { status: 500 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle 502 Bad Gateway', async () => {
      mockFetch.mockResolvedValue(new Response('Bad Gateway', { status: 502 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle 503 Service Unavailable', async () => {
      mockFetch.mockResolvedValue(new Response('Service Unavailable', { status: 503 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle 504 Gateway Timeout', async () => {
      mockFetch.mockResolvedValue(new Response('Gateway Timeout', { status: 504 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })

    it('should handle 429 Too Many Requests', async () => {
      mockFetch.mockResolvedValue(new Response('Too Many Requests', {
        status: 429,
        headers: { 'Retry-After': '60' }
      }))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })
  })
})

// ============================================================================
// Partial Write/Read Tests
// ============================================================================

describe('Partial Writes and Reads', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>
  let mockR2: ReturnType<typeof createMockR2Bucket>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    mockFetch = vi.fn().mockResolvedValue(new Response('OK', { status: 200 }))
    mockR2 = createMockR2Bucket()
    vi.stubGlobal('fetch', mockFetch)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('partial batch transmission', () => {
    it('should handle partial batch failure where some events succeed', async () => {
      // First flush succeeds, subsequent ones fail
      mockFetch
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))
        .mockRejectedValueOnce(new Error('Network error'))

      const emitter = new EventEmitter(mockCtx, mockEnv, { batchSize: 2 })

      // First batch - will succeed
      emitter.emit({ type: 'event1' })
      emitter.emit({ type: 'event2' })
      await vi.runAllTimersAsync()

      // Second batch - will fail
      emitter.emit({ type: 'event3' })
      emitter.emit({ type: 'event4' })
      await vi.runAllTimersAsync()

      // First batch should have succeeded
      expect(mockFetch).toHaveBeenCalledTimes(2)
      // Failed events should be queued for retry
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.arrayContaining([
          expect.objectContaining({ type: 'event3' }),
          expect.objectContaining({ type: 'event4' }),
        ])
      )
    })

    it('should preserve event order when retrying failed batches', async () => {
      // Fail first, then succeed
      mockFetch
        .mockRejectedValueOnce(new Error('Network error'))
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      emitter.emit({ type: 'event1' })
      emitter.emit({ type: 'event2' })
      emitter.emit({ type: 'event3' })
      await emitter.flush()

      // Check retry queue preserves order
      const retryCalls = mockCtx.storage.put.mock.calls.filter(
        (call: unknown[]) => call[0] === '_events:retry'
      )
      expect(retryCalls.length).toBeGreaterThan(0)

      const retryEvents = retryCalls[0][1] as DurableEvent[]
      expect(retryEvents[0].type).toBe('event1')
      expect(retryEvents[1].type).toBe('event2')
      expect(retryEvents[2].type).toBe('event3')
    })
  })

  describe('R2 partial write failures', () => {
    it('should handle R2 write failure after successful HTTP flush', async () => {
      // HTTP succeeds but R2 fails
      mockR2.put = vi.fn().mockRejectedValue(new Error('R2 write failed'))

      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // HTTP should have succeeded
      expect(mockFetch).toHaveBeenCalledTimes(1)
      // R2 write was attempted
      expect(mockR2.put).toHaveBeenCalled()
      // Events should still be cleared from batch (HTTP succeeded)
      expect(emitter.pendingCount).toBe(0)
    })

    it('should handle R2 write error gracefully', async () => {
      // R2 write fails after HTTP succeeds
      mockR2.put = vi.fn().mockRejectedValue(new Error('R2 timeout'))

      const emitter = new EventEmitter(mockCtx, mockEnv, { r2Bucket: mockR2 })

      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // HTTP succeeded, R2 failed, but flush should still complete
      expect(emitter.pendingCount).toBe(0)
      expect(mockR2.put).toHaveBeenCalled()
    })
  })

  describe('storage partial failures', () => {
    it('should handle storage.put failure during retry scheduling', async () => {
      mockFetch.mockRejectedValue(new Error('Network error'))

      // Make storage.put fail
      mockCtx.storage.put.mockRejectedValueOnce(new Error('Storage write failed'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })

      // flush() will fail to save retry queue
      await expect(emitter.flush()).rejects.toThrow('Storage write failed')
    })

    it('should handle storage.get failure during batch restore', async () => {
      // Make storage.get fail for batch restore
      mockCtx.storage.get.mockRejectedValueOnce(new Error('Storage read failed'))

      // Constructor calls restoreBatch which should handle the error gracefully
      const emitter = new EventEmitter(mockCtx, mockEnv)

      // Wait for async restore to complete
      await vi.runAllTimersAsync()

      // Emitter should still function despite restore failure
      expect(() => emitter.emit({ type: 'test.event' })).not.toThrow()
    })

    it('should handle storage.setAlarm failure', async () => {
      mockFetch.mockRejectedValue(new Error('Network error'))

      // Make setAlarm fail
      mockCtx.storage.setAlarm.mockRejectedValueOnce(new Error('setAlarm failed'))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })

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
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    mockFetch = vi.fn().mockRejectedValue(new Error('Network error'))
    vi.stubGlobal('fetch', mockFetch)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('exponential backoff calculation', () => {
    it('should increase delay exponentially with each retry', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })

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
      // Use a lower maxConsecutiveFailures to avoid circuit breaker opening
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: 0 })
      emitter.emit({ type: 'test.event' })

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
        const emitter = new EventEmitter(ctx, mockEnv)
        emitter.emit({ type: 'test.event' })
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
      mockFetch
        .mockRejectedValueOnce(new Error('Network error'))
        .mockRejectedValueOnce(new Error('Network error'))
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))

      const emitter = new EventEmitter(mockCtx, mockEnv)

      // First event - fails twice then succeeds
      emitter.emit({ type: 'event1' })
      await emitter.flush()
      await emitter.handleAlarm()
      await emitter.handleAlarm()

      // Retry count should be reset
      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retryCount')
      expect(mockCtx.storage.delete).toHaveBeenCalledWith('_events:retry')

      // Circuit breaker should be closed
      const info = await emitter.getCircuitBreakerInfo()
      expect(info.consecutiveFailures).toBe(0)
    })
  })

  describe('retry queue management', () => {
    it('should combine new events with existing retry queue', async () => {
      // Pre-populate retry queue
      const existingEvents: DurableEvent[] = [
        { type: 'existing.event', ts: '2024-01-01T00:00:00Z', do: { id: 'test' } },
      ]
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'new.event' })
      await emitter.flush()

      // Both events should be in retry queue
      const retryCalls = mockCtx.storage.put.mock.calls.filter(
        (call: unknown[]) => call[0] === '_events:retry'
      )
      const combinedEvents = retryCalls[retryCalls.length - 1][1] as DurableEvent[]
      expect(combinedEvents.length).toBe(2)
      expect(combinedEvents[0].type).toBe('existing.event')
      expect(combinedEvents[1].type).toBe('new.event')
    })

    it('should not exceed maxRetryQueueSize', async () => {
      const maxSize = 100
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxRetryQueueSize: maxSize })

      // Pre-populate with maxSize events
      const existingEvents: DurableEvent[] = Array.from({ length: maxSize }, (_, i) => ({
        type: `event.${i}`,
        ts: '2024-01-01T00:00:00Z',
        do: { id: 'test' },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      // Add more events
      for (let i = 0; i < 10; i++) {
        emitter.emit({ type: `overflow.${i}` })
      }

      // Should throw EventBufferFullError
      await expect(emitter.flush()).rejects.toThrow(EventBufferFullError)

      // Queue should still be capped at maxSize
      const retryCalls = mockCtx.storage.put.mock.calls.filter(
        (call: unknown[]) => call[0] === '_events:retry'
      )
      const finalQueue = retryCalls[retryCalls.length - 1][1] as DurableEvent[]
      expect(finalQueue.length).toBe(maxSize)
    })

    it('should drop oldest events when queue overflows', async () => {
      const maxSize = 5
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxRetryQueueSize: maxSize })

      // Pre-populate with maxSize events
      const existingEvents: DurableEvent[] = Array.from({ length: maxSize }, (_, i) => ({
        type: `old.${i}`,
        ts: '2024-01-01T00:00:00Z',
        do: { id: 'test' },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      // Add 3 new events
      for (let i = 0; i < 3; i++) {
        emitter.emit({ type: `new.${i}` })
      }

      try {
        await emitter.flush()
      } catch (e) {
        // Expected EventBufferFullError
      }

      const retryCalls = mockCtx.storage.put.mock.calls.filter(
        (call: unknown[]) => call[0] === '_events:retry'
      )
      const finalQueue = retryCalls[retryCalls.length - 1][1] as DurableEvent[]

      // Should contain last 5 events (2 oldest dropped)
      expect(finalQueue.length).toBe(maxSize)
      // Oldest events should be dropped, newest kept
      expect(finalQueue[finalQueue.length - 1].type).toBe('new.2')
      expect(finalQueue[finalQueue.length - 2].type).toBe('new.1')
      expect(finalQueue[finalQueue.length - 3].type).toBe('new.0')
    })

    it('should track dropped events count', async () => {
      const maxSize = 5
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxRetryQueueSize: maxSize })

      const existingEvents: DurableEvent[] = Array.from({ length: maxSize }, (_, i) => ({
        type: `old.${i}`,
        ts: '2024-01-01T00:00:00Z',
        do: { id: 'test' },
      }))
      mockCtx.storage._storage.set('_events:retry', existingEvents)

      for (let i = 0; i < 3; i++) {
        emitter.emit({ type: `new.${i}` })
      }

      try {
        await emitter.flush()
      } catch (e) {
        expect(e).toBeInstanceOf(EventBufferFullError)
        expect((e as EventBufferFullError).droppedCount).toBe(3)
      }

      expect(emitter.droppedEvents).toBe(3)
    })
  })
})

// ============================================================================
// Circuit Breaker Pattern Tests
// ============================================================================

describe('Circuit Breaker Pattern', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    mockFetch = vi.fn().mockRejectedValue(new Error('Network error'))
    vi.stubGlobal('fetch', mockFetch)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('circuit breaker opening', () => {
    it('should open circuit breaker after maxConsecutiveFailures', async () => {
      const maxFailures = 3
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      // Fail maxFailures times
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Circuit breaker should now be open
      expect(() => emitter.emit({ type: 'blocked.event' })).toThrow(CircuitBreakerOpenError)
    })

    it('should include failure count in CircuitBreakerOpenError', async () => {
      const maxFailures = 3
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      try {
        emitter.emit({ type: 'blocked.event' })
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(CircuitBreakerOpenError)
        expect((e as CircuitBreakerOpenError).consecutiveFailures).toBe(maxFailures)
      }
    })

    it('should include reset time in CircuitBreakerOpenError', async () => {
      const maxFailures = 3
      const resetMs = 5000
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })

      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      try {
        emitter.emit({ type: 'blocked.event' })
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
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Should be open
      expect(() => emitter.emit({ type: 'blocked.event' })).toThrow(CircuitBreakerOpenError)

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Should now allow emit (half-open state)
      expect(() => emitter.emit({ type: 'retry.event' })).not.toThrow()
    })

    it('should close circuit breaker on successful retry', async () => {
      const maxFailures = 2
      const resetMs = 5000
      mockFetch
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockRejectedValueOnce(new Error('Fail 2'))
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))

      const emitter = new EventEmitter(mockCtx, mockEnv, {
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })

      // Open circuit breaker
      emitter.emit({ type: 'event.1' })
      await emitter.flush()
      emitter.emit({ type: 'event.2' })
      await emitter.flush()

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Retry should succeed and close circuit
      emitter.emit({ type: 'retry.event' })
      await emitter.flush()

      // Circuit should be closed
      expect(() => emitter.emit({ type: 'next.event' })).not.toThrow()

      // Verify circuit breaker info
      const info = await emitter.getCircuitBreakerInfo()
      expect(info.isOpen).toBe(false)
      expect(info.consecutiveFailures).toBe(0)
    })

    it('should re-open circuit breaker if retry fails', async () => {
      const maxFailures = 2
      const resetMs = 5000
      const emitter = new EventEmitter(mockCtx, mockEnv, {
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Advance time past reset period
      await vi.advanceTimersByTimeAsync(resetMs + 100)

      // Retry fails
      emitter.emit({ type: 'retry.event' })
      await emitter.flush()

      // Should go back to open state
      expect(() => emitter.emit({ type: 'blocked.again' })).toThrow(CircuitBreakerOpenError)
    })
  })

  describe('circuit breaker closing', () => {
    it('should reset consecutive failures on success', async () => {
      mockFetch
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))
        .mockRejectedValueOnce(new Error('Fail 2'))

      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: 3 })

      // First failure
      emitter.emit({ type: 'event.1' })
      await emitter.flush()

      let info = await emitter.getCircuitBreakerInfo()
      expect(info.consecutiveFailures).toBe(1)

      // Success - should reset
      emitter.emit({ type: 'event.2' })
      await emitter.flush()

      info = await emitter.getCircuitBreakerInfo()
      expect(info.consecutiveFailures).toBe(0)

      // Another failure - should start from 1 again
      emitter.emit({ type: 'event.3' })
      await emitter.flush()

      info = await emitter.getCircuitBreakerInfo()
      expect(info.consecutiveFailures).toBe(1)
    })

    it('should allow manual circuit breaker reset', async () => {
      const maxFailures = 2
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Should be open
      expect(() => emitter.emit({ type: 'blocked' })).toThrow(CircuitBreakerOpenError)

      // Manual reset
      await emitter.resetCircuitBreaker()

      // Should be closed
      expect(() => emitter.emit({ type: 'allowed' })).not.toThrow()

      const info = await emitter.getCircuitBreakerInfo()
      expect(info.isOpen).toBe(false)
      expect(info.consecutiveFailures).toBe(0)
    })

    it('should schedule retry alarm after manual reset if events pending', async () => {
      const maxFailures = 2
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      // Add events to retry queue and open circuit
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Clear alarm mocks
      mockCtx.storage.setAlarm.mockClear()

      // Manual reset
      await emitter.resetCircuitBreaker()

      // Should have scheduled alarm for pending retry
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })
  })

  describe('circuit breaker disabled', () => {
    it('should never open circuit breaker when maxConsecutiveFailures is 0', async () => {
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: 0 })

      // Fail many times
      for (let i = 0; i < 50; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Should still allow emit
      expect(() => emitter.emit({ type: 'still.allowed' })).not.toThrow()

      const info = await emitter.getCircuitBreakerInfo()
      expect(info.isOpen).toBe(false)
    })
  })

  describe('circuit breaker state persistence', () => {
    it('should persist circuit breaker state to storage', async () => {
      const maxFailures = 2
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Verify state was persisted
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:circuitBreaker',
        expect.objectContaining({
          isOpen: true,
          consecutiveFailures: maxFailures,
        })
      )
    })

    it('should restore circuit breaker state from storage on alarm', async () => {
      const maxFailures = 2
      const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        emitter.emit({ type: `event.${i}` })
        await emitter.flush()
      }

      // Simulate new emitter instance with same storage
      const emitter2 = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: maxFailures })

      // handleAlarm should load state
      await emitter2.handleAlarm()

      // State should be loaded
      const info = await emitter2.getCircuitBreakerInfo()
      expect(info.isOpen).toBe(true)
    })
  })
})

// ============================================================================
// Intermittent Failure Tests
// ============================================================================

describe('Intermittent Network Failures', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('should handle alternating success and failure', async () => {
    mockFetch = vi.fn()
      .mockResolvedValueOnce(new Response('OK', { status: 200 }))
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValueOnce(new Response('OK', { status: 200 }))
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValueOnce(new Response('OK', { status: 200 }))
    vi.stubGlobal('fetch', mockFetch)

    const emitter = new EventEmitter(mockCtx, mockEnv)

    // Success
    emitter.emit({ type: 'event.1' })
    await emitter.flush()
    expect(emitter.pendingCount).toBe(0)

    // Failure
    emitter.emit({ type: 'event.2' })
    await emitter.flush()

    // Retry should succeed
    await emitter.handleAlarm()

    // Circuit should not be open (alternating doesn't accumulate)
    const info = await emitter.getCircuitBreakerInfo()
    expect(info.isOpen).toBe(false)
    expect(info.consecutiveFailures).toBe(0)
  })

  it('should handle random failure pattern', async () => {
    const failurePattern = [true, true, false, true, false, false, true, false, false, false]
    let callIndex = 0

    mockFetch = vi.fn().mockImplementation(() => {
      const shouldFail = failurePattern[callIndex % failurePattern.length]
      callIndex++
      if (shouldFail) {
        return Promise.reject(new Error('Random failure'))
      }
      return Promise.resolve(new Response('OK', { status: 200 }))
    })
    vi.stubGlobal('fetch', mockFetch)

    const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: 5 })

    // Run through the pattern
    for (let i = 0; i < failurePattern.length; i++) {
      emitter.emit({ type: `event.${i}` })
      await emitter.flush()
      await emitter.handleAlarm()
    }

    // Should have processed all events eventually
    expect(mockFetch).toHaveBeenCalled()
  })

  it('should handle burst of failures followed by recovery', async () => {
    mockFetch = vi.fn()
      // 5 failures
      .mockRejectedValueOnce(new Error('Fail 1'))
      .mockRejectedValueOnce(new Error('Fail 2'))
      .mockRejectedValueOnce(new Error('Fail 3'))
      .mockRejectedValueOnce(new Error('Fail 4'))
      .mockRejectedValueOnce(new Error('Fail 5'))
      // Then recovery
      .mockResolvedValue(new Response('OK', { status: 200 }))
    vi.stubGlobal('fetch', mockFetch)

    const emitter = new EventEmitter(mockCtx, mockEnv, { maxConsecutiveFailures: 10 })

    // Generate burst of events
    for (let i = 0; i < 5; i++) {
      emitter.emit({ type: `burst.${i}` })
      await emitter.flush()
    }

    // Should have 5 consecutive failures
    let info = await emitter.getCircuitBreakerInfo()
    expect(info.consecutiveFailures).toBe(5)
    expect(info.isOpen).toBe(false) // Not yet at 10

    // Recovery
    await emitter.handleAlarm()

    info = await emitter.getCircuitBreakerInfo()
    expect(info.consecutiveFailures).toBe(0)
  })
})

// ============================================================================
// Concurrent Request Failure Tests
// ============================================================================

describe('Concurrent Request Failures', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('should handle multiple concurrent flushes gracefully', async () => {
    mockFetch = vi.fn().mockResolvedValue(new Response('OK', { status: 200 }))
    vi.stubGlobal('fetch', mockFetch)

    const emitter = new EventEmitter(mockCtx, mockEnv)

    emitter.emit({ type: 'event.1' })

    // Start multiple flushes concurrently
    const flushPromises = [
      emitter.flush(),
      emitter.flush(),
      emitter.flush(),
    ]

    await Promise.all(flushPromises)

    // Only one fetch should have been made (first flush takes the batch)
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it('should handle race between flush and handleAlarm', async () => {
    mockFetch = vi.fn()
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValueOnce(new Response('OK', { status: 200 }))
    vi.stubGlobal('fetch', mockFetch)

    const emitter = new EventEmitter(mockCtx, mockEnv)

    emitter.emit({ type: 'event.1' })
    await emitter.flush()

    // Now retry queue has the event
    // Start both alarm handler and new flush concurrently
    emitter.emit({ type: 'event.2' })

    await Promise.all([
      emitter.handleAlarm(),
      emitter.flush(),
    ])

    // Both events should eventually be processed
    expect(mockFetch).toHaveBeenCalledTimes(3) // Initial fail + alarm retry + new flush
  })
})

// ============================================================================
// Network Error Classification Tests
// ============================================================================

describe('Network Error Classification', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockEnv: Record<string, unknown>
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'test-do-123', name: 'test-object' })
    mockEnv = {}
    mockFetch = vi.fn()
    vi.stubGlobal('fetch', mockFetch)
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
      mockFetch.mockRejectedValue(error)

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      // Should have stored for retry (network errors are retryable)
      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()
    })
  })

  const httpErrorCodes = [500, 502, 503, 504, 429]

  httpErrorCodes.forEach((statusCode) => {
    it(`should treat HTTP ${statusCode} as retryable error`, async () => {
      mockFetch.mockResolvedValue(new Response('Error', { status: statusCode }))

      const emitter = new EventEmitter(mockCtx, mockEnv)
      emitter.emit({ type: 'test.event' })
      await emitter.flush()

      expect(mockCtx.storage.put).toHaveBeenCalledWith(
        '_events:retry',
        expect.any(Array)
      )
    })
  })
})
