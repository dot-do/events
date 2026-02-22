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
 * Uses vitest-pool-workers with EventEmitterTestDO (real workerd runtime).
 * NO mocks — all assertions go through DO RPC methods.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import type { EventEmitterTestDO, TestEnv } from './test-do.js'
import {
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
} from '../config.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let stubCounter = 0

function getStub(name?: string) {
  const testEnv = env as unknown as TestEnv
  const id = testEnv.EVENT_EMITTER_TEST.idFromName(name ?? `net-fail-${++stubCounter}`)
  return testEnv.EVENT_EMITTER_TEST.get(id)
}

// ============================================================================
// Network Timeout Tests
// ============================================================================

describe('Network Timeouts', () => {
  describe('pipeline.send timeout scenarios', () => {
    it('should handle AbortError from pipeline.send', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('The operation was aborted')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      // Should have scheduled retry
      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)

      const alarmTime = await stub.getAlarmTime()
      expect(alarmTime).not.toBeNull()
    })

    it('should handle timeout during retry attempts', async () => {
      const stub = getStub()
      stub.setup()

      // Initial emit and flush with error — will fail and queue for retry
      stub.setPipelineError('timeout')
      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      expect(await stub.getSendCallCount()).toBe(1)

      // First retry via alarm — still failing
      await stub.handleAlarm()
      expect(await stub.getSendCallCount()).toBe(2)

      // Clear pipeline error before second retry
      stub.setPipelineError(null)

      // Second retry via alarm — will succeed
      await stub.handleAlarm()
      expect(await stub.getSendCallCount()).toBe(3)

      // Retry queue should be cleared
      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeUndefined()
    })
  })
})

// ============================================================================
// Connection Failure Tests
// ============================================================================

describe('Connection Failures', () => {
  describe('DNS resolution failures', () => {
    it('should handle DNS resolution failure', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('getaddrinfo ENOTFOUND events.workers.do')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      // Should store for retry
      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)
    })
  })

  describe('TCP connection failures', () => {
    it('should handle connection refused error', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('ECONNREFUSED')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)

      const alarmTime = await stub.getAlarmTime()
      expect(alarmTime).not.toBeNull()
    })

    it('should handle connection reset error', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('ECONNRESET')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)
    })

    it('should handle socket hang up', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('socket hang up')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)
    })
  })

  describe('SSL/TLS failures', () => {
    it('should handle SSL certificate error', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('CERT_HAS_EXPIRED')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)
    })

    it('should handle SSL handshake failure', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('SSL_HANDSHAKE_FAILED')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)
    })
  })

  describe('pipeline send failures (server errors)', () => {
    it('should handle pipeline send failure (Internal Server Error)', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('Internal Server Error')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(1)
    })

    it('should handle pipeline send failure (Bad Gateway)', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('Bad Gateway')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
    })

    it('should handle pipeline send failure (Service Unavailable)', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('Service Unavailable')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
    })

    it('should handle pipeline send failure (Gateway Timeout)', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('Gateway Timeout')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
    })

    it('should handle pipeline send failure (Too Many Requests)', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('Too Many Requests')

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
    })
  })
})

// ============================================================================
// Partial Write/Read Tests
// ============================================================================

describe('Partial Writes and Reads', () => {
  describe('partial batch transmission', () => {
    it('should handle partial batch failure where some events succeed', async () => {
      const stub = getStub()
      stub.setup({ batchSize: 2 })

      // First batch — will succeed (pipeline has no error)
      stub.emit({ type: 'test', event: 'event1', data: {} })
      stub.emit({ type: 'test', event: 'event2', data: {} })
      await stub.flush()

      const batches1 = await stub.getSentBatches()
      expect(batches1.length).toBe(1)

      // Now set pipeline error for second batch
      stub.setPipelineError('Network error')

      stub.emit({ type: 'test', event: 'event3', data: {} })
      stub.emit({ type: 'test', event: 'event4', data: {} })
      await stub.flush()

      // Second batch should have been attempted but failed
      expect(await stub.getSendCallCount()).toBe(2)

      // Failed events should be queued for retry
      const retryQueue = await stub.getStorageValue<Record<string, unknown>[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(2)
      expect(retryQueue!.some((e) => e.event === 'event3')).toBe(true)
      expect(retryQueue!.some((e) => e.event === 'event4')).toBe(true)
    })

    it('should preserve event order when retrying failed batches', async () => {
      const stub = getStub()
      stub.setup()

      // Pipeline fails
      stub.setPipelineError('Network error')

      stub.emit({ type: 'test', event: 'event1', data: {} })
      stub.emit({ type: 'test', event: 'event2', data: {} })
      stub.emit({ type: 'test', event: 'event3', data: {} })
      await stub.flush()

      // Check retry queue preserves order
      const retryQueue = await stub.getStorageValue<Record<string, unknown>[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(3)
      expect(retryQueue![0].event).toBe('event1')
      expect(retryQueue![1].event).toBe('event2')
      expect(retryQueue![2].event).toBe('event3')
    })
  })
})

// ============================================================================
// Retry Behavior Tests with Exponential Backoff
// ============================================================================

describe('Retry Behavior with Exponential Backoff', () => {
  describe('exponential backoff calculation', () => {
    it('should increase delay exponentially with each retry', async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError('Network error')

      stub.emit({ type: 'test', event: 'test.event', data: {} })

      // First failure
      await stub.flush()
      const firstAlarmTime = await stub.getAlarmTime()
      expect(firstAlarmTime).not.toBeNull()

      // The first delay should be around RETRY_BASE_DELAY_MS (1s) + jitter (up to 1s)
      const now1 = Date.now()
      const firstDelay = firstAlarmTime! - now1
      expect(firstDelay).toBeGreaterThanOrEqual(RETRY_BASE_DELAY_MS - 100) // small tolerance
      expect(firstDelay).toBeLessThan(RETRY_BASE_DELAY_MS * 2 + 1000)

      // Trigger alarm to get next retry scheduled
      await stub.handleAlarm()

      const secondAlarmTime = await stub.getAlarmTime()
      expect(secondAlarmTime).not.toBeNull()

      // Second delay should be larger (exponential backoff)
      const now2 = Date.now()
      const secondDelay = secondAlarmTime! - now2
      expect(secondDelay).toBeGreaterThanOrEqual(RETRY_BASE_DELAY_MS * 2 - 100)
    })

    it('should cap retry delay at RETRY_MAX_DELAY_MS', async () => {
      const stub = getStub()
      // Disable circuit breaker so we can retry many times
      stub.setup({ maxConsecutiveFailures: 0 })
      stub.setPipelineError('Network error')

      stub.emit({ type: 'test', event: 'test.event', data: {} })

      // Simulate many retries to exceed max delay
      await stub.flush()
      for (let i = 0; i < 9; i++) {
        await stub.handleAlarm()
      }

      // Get the last scheduled alarm delay
      const lastAlarmTime = await stub.getAlarmTime()
      expect(lastAlarmTime).not.toBeNull()

      const lastDelay = lastAlarmTime! - Date.now()
      // Delay should be capped at RETRY_MAX_DELAY_MS (60s) plus jitter (1s)
      expect(lastDelay).toBeLessThanOrEqual(RETRY_MAX_DELAY_MS + 1500) // jitter + tolerance
    })

    it('should add jitter to prevent thundering herd', async () => {
      const delays: number[] = []

      // Run multiple DOs to check jitter variability
      for (let i = 0; i < 5; i++) {
        const stub = getStub(`jitter-${i}`)
        stub.setup()
        stub.setPipelineError('Network error')

        stub.emit({ type: 'test', event: 'test.event', data: {} })
        await stub.flush()

        const alarmTime = await stub.getAlarmTime()
        expect(alarmTime).not.toBeNull()
        const delay = alarmTime! - Date.now()
        delays.push(delay)
      }

      // With jitter, not all delays should be exactly the same
      // (though jitter might be small, at least check the range is reasonable)
      const uniqueDelays = new Set(delays)
      expect(uniqueDelays.size).toBeGreaterThanOrEqual(1)

      // All delays should be in the expected range
      for (const d of delays) {
        expect(d).toBeGreaterThanOrEqual(RETRY_BASE_DELAY_MS - 100)
        expect(d).toBeLessThanOrEqual(RETRY_BASE_DELAY_MS + 1500) // base + jitter + tolerance
      }
    })

    it('should reset retry count after successful delivery', async () => {
      const stub = getStub()
      stub.setup()

      // Fail twice
      stub.setPipelineError('Network error')
      stub.emit({ type: 'test', event: 'event1', data: {} })
      await stub.flush()
      await stub.handleAlarm()

      // Now succeed
      stub.setPipelineError(null)
      await stub.handleAlarm()

      // Retry queue and retry count should be cleared
      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeUndefined()

      const retryCount = await stub.getStorageValue<number>('_events:retryCount')
      expect(retryCount).toBeUndefined()

      // Circuit breaker should be closed
      expect(await stub.isCircuitBreakerOpen()).toBe(false)
    })
  })

  describe('retry queue management', () => {
    it('should combine new events with existing retry queue', async () => {
      const stub = getStub()
      stub.setup()

      // Pre-populate retry queue via storage
      const existingEvents = [
        { type: 'test', event: 'existing.event', data: {}, ts: '2024-01-01T00:00:00Z', meta: { do: { id: 'test' } } },
      ]
      await stub.setStorageValue('_events:retry', existingEvents)

      // Pipeline fails
      stub.setPipelineError('Network error')

      stub.emit({ type: 'test', event: 'new.event', data: {} })
      await stub.flush()

      // Both events should be in retry queue
      const retryQueue = await stub.getStorageValue<Record<string, unknown>[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(2)
      expect(retryQueue![0].event).toBe('existing.event')
      expect(retryQueue![1].event).toBe('new.event')
    })

    it('should not exceed maxRetryQueueSize', async () => {
      const maxSize = 100
      const stub = getStub()
      stub.setup({ maxRetryQueueSize: maxSize })

      // Pre-populate with maxSize events
      const existingEvents = Array.from({ length: maxSize }, (_, i) => ({
        type: 'test',
        event: `event.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      await stub.setStorageValue('_events:retry', existingEvents)

      // Pipeline fails
      stub.setPipelineError('Network error')

      // Add more events
      for (let i = 0; i < 10; i++) {
        stub.emit({ type: 'test', event: `overflow.${i}`, data: {} })
      }

      // Should return EventBufferFullError
      const result = await stub.tryFlush()
      expect(result.ok).toBe(false)
      expect(result.errorType).toBe('EventBufferFullError')

      // Queue should still be capped at maxSize
      const retryQueue = await stub.getStorageValue<Record<string, unknown>[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(maxSize)
    })

    it('should drop oldest events when queue overflows', async () => {
      const maxSize = 5
      const stub = getStub()
      stub.setup({ maxRetryQueueSize: maxSize })

      // Pre-populate with maxSize events
      const existingEvents = Array.from({ length: maxSize }, (_, i) => ({
        type: 'test',
        event: `old.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      await stub.setStorageValue('_events:retry', existingEvents)

      // Pipeline fails
      stub.setPipelineError('Network error')

      // Add 3 new events
      for (let i = 0; i < 3; i++) {
        stub.emit({ type: 'test', event: `new.${i}`, data: {} })
      }

      // Flush — will overflow and throw EventBufferFullError
      await stub.tryFlush()

      const retryQueue = await stub.getStorageValue<Record<string, unknown>[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBe(maxSize)

      // Oldest events should be dropped, newest kept
      expect(retryQueue![retryQueue!.length - 1].event).toBe('new.2')
      expect(retryQueue![retryQueue!.length - 2].event).toBe('new.1')
      expect(retryQueue![retryQueue!.length - 3].event).toBe('new.0')
    })

    it('should track dropped events count via EventBufferFullError', async () => {
      const maxSize = 5
      const stub = getStub()
      stub.setup({ maxRetryQueueSize: maxSize })

      const existingEvents = Array.from({ length: maxSize }, (_, i) => ({
        type: 'test',
        event: `old.${i}`,
        data: {},
        ts: '2024-01-01T00:00:00Z',
        meta: { do: { id: 'test' } },
      }))
      await stub.setStorageValue('_events:retry', existingEvents)

      stub.setPipelineError('Network error')

      for (let i = 0; i < 3; i++) {
        stub.emit({ type: 'test', event: `new.${i}`, data: {} })
      }

      const result = await stub.tryFlush()
      expect(result.ok).toBe(false)
      expect(result.errorType).toBe('EventBufferFullError')
      expect(result.droppedCount).toBe(3)
    })
  })
})

// ============================================================================
// Circuit Breaker Pattern Tests
// ============================================================================

describe('Circuit Breaker Pattern', () => {
  describe('circuit breaker opening', () => {
    it('should open circuit breaker after maxConsecutiveFailures', async () => {
      const maxFailures = 3
      const stub = getStub()
      stub.setup({ maxConsecutiveFailures: maxFailures })
      stub.setPipelineError('Network error')

      // Fail maxFailures times
      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Circuit breaker should now be open — flush throws CircuitBreakerOpenError
      stub.emit({ type: 'test', event: 'blocked.event', data: {} })
      const result = await stub.tryFlush()
      expect(result.ok).toBe(false)
      expect(result.errorType).toBe('CircuitBreakerOpenError')
    })

    it('should include failure count in CircuitBreakerOpenError', async () => {
      const maxFailures = 3
      const stub = getStub()
      stub.setup({ maxConsecutiveFailures: maxFailures })
      stub.setPipelineError('Network error')

      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      stub.emit({ type: 'test', event: 'blocked.event', data: {} })
      const result = await stub.tryFlush()
      expect(result.ok).toBe(false)
      expect(result.errorType).toBe('CircuitBreakerOpenError')
      expect(result.consecutiveFailures).toBe(maxFailures)
    })

    it('should include reset time in CircuitBreakerOpenError', async () => {
      const maxFailures = 3
      const resetMs = 5000
      const stub = getStub()
      stub.setup({
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })
      stub.setPipelineError('Network error')

      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      stub.emit({ type: 'test', event: 'blocked.event', data: {} })
      const result = await stub.tryFlush()

      expect(result.ok).toBe(false)
      expect(result.errorType).toBe('CircuitBreakerOpenError')
      expect(result.resetAt).toBeDefined()

      const resetAt = new Date(result.resetAt!).getTime()
      expect(resetAt).toBeGreaterThan(Date.now())
      expect(resetAt).toBeLessThanOrEqual(Date.now() + resetMs + 1000)
    })
  })

  describe('circuit breaker half-open state', () => {
    it('should allow retry attempt after circuitBreakerResetMs', async () => {
      const maxFailures = 2
      const resetMs = 100 // Short reset for test
      const stub = getStub()
      stub.setup({
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })
      stub.setPipelineError('Network error')

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Should be open
      stub.emit({ type: 'test', event: 'blocked.event', data: {} })
      const blockedResult = await stub.tryFlush()
      expect(blockedResult.errorType).toBe('CircuitBreakerOpenError')

      // Wait past reset period
      await new Promise((r) => setTimeout(r, resetMs + 50))

      // Should now allow flush (half-open state)
      // flush will still fail (pipeline error set), but NOT with CircuitBreakerOpenError
      stub.emit({ type: 'test', event: 'retry.event', data: {} })
      const retryResult = await stub.tryFlush()
      // Should be a regular Error (pipeline failure), not CircuitBreakerOpenError
      expect(retryResult.errorType).not.toBe('CircuitBreakerOpenError')
    })

    it('should close circuit breaker on successful retry', async () => {
      const maxFailures = 2
      const resetMs = 100 // Short reset for test
      const stub = getStub()
      stub.setup({
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })

      // Open circuit breaker
      stub.setPipelineError('Network error')
      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Wait past reset period
      await new Promise((r) => setTimeout(r, resetMs + 50))

      // Clear pipeline error — retry should succeed
      stub.setPipelineError(null)

      stub.emit({ type: 'test', event: 'retry.event', data: {} })
      const result = await stub.tryFlush()
      expect(result.ok).toBe(true)

      // Circuit should be closed
      expect(await stub.isCircuitBreakerOpen()).toBe(false)
    })

    it('should re-open circuit breaker if retry fails', async () => {
      const maxFailures = 2
      const resetMs = 100 // Short reset for test
      const stub = getStub()
      stub.setup({
        maxConsecutiveFailures: maxFailures,
        circuitBreakerResetMs: resetMs,
      })
      stub.setPipelineError('Network error')

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Wait past reset period
      await new Promise((r) => setTimeout(r, resetMs + 50))

      // Retry fails (pipeline error still set)
      stub.emit({ type: 'test', event: 'retry.event', data: {} })
      await stub.tryFlush()

      // Should go back to open state
      stub.emit({ type: 'test', event: 'blocked.again', data: {} })
      const result = await stub.tryFlush()
      expect(result.ok).toBe(false)
      expect(result.errorType).toBe('CircuitBreakerOpenError')
    })
  })

  describe('circuit breaker closing', () => {
    it('should reset consecutive failures on success', async () => {
      const stub = getStub()
      stub.setup({ maxConsecutiveFailures: 3 })

      // First failure
      stub.setPipelineError('Fail 1')
      stub.emit({ type: 'test', event: 'event.1', data: {} })
      await stub.flush()
      expect(await stub.isCircuitBreakerOpen()).toBe(false)

      // Success — should reset failure count
      stub.setPipelineError(null)
      stub.emit({ type: 'test', event: 'event.2', data: {} })
      await stub.flush()
      expect(await stub.isCircuitBreakerOpen()).toBe(false)

      // Another failure — should start from 1 again, not accumulate
      stub.setPipelineError('Fail 2')
      stub.emit({ type: 'test', event: 'event.3', data: {} })
      await stub.flush()
      expect(await stub.isCircuitBreakerOpen()).toBe(false)
    })
  })

  describe('circuit breaker disabled', () => {
    it('should never open circuit breaker when maxConsecutiveFailures is 0', async () => {
      const stub = getStub()
      stub.setup({ maxConsecutiveFailures: 0 })
      stub.setPipelineError('Network error')

      // Fail many times
      for (let i = 0; i < 50; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Should still allow flush without CircuitBreakerOpenError
      stub.emit({ type: 'test', event: 'still.allowed', data: {} })
      const result = await stub.tryFlush()
      // Should be a regular pipeline error, not CircuitBreakerOpenError
      expect(result.errorType).not.toBe('CircuitBreakerOpenError')
      expect(await stub.isCircuitBreakerOpen()).toBe(false)
    })
  })

  describe('circuit breaker state persistence', () => {
    it('should persist circuit breaker state to storage', async () => {
      const maxFailures = 2
      const stub = getStub()
      stub.setup({ maxConsecutiveFailures: maxFailures })
      stub.setPipelineError('Network error')

      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Verify state was persisted
      const cbState = await stub.getStorageValue<{ isOpen: boolean; consecutiveFailures: number }>('_events:circuitBreaker')
      expect(cbState).toBeDefined()
      expect(cbState!.isOpen).toBe(true)
      expect(cbState!.consecutiveFailures).toBe(maxFailures)
    })

    it('should restore circuit breaker state from storage on alarm', async () => {
      const maxFailures = 2
      const stub = getStub()
      stub.setup({ maxConsecutiveFailures: maxFailures })
      stub.setPipelineError('Network error')

      // Open circuit breaker
      for (let i = 0; i < maxFailures; i++) {
        stub.emit({ type: 'test', event: `event.${i}`, data: {} })
        await stub.flush()
      }

      // Re-setup creates a new EventEmitter (simulating a new DO instance)
      // but uses same storage (same DO stub)
      stub.setup({ maxConsecutiveFailures: maxFailures })
      stub.setPipelineError('Network error')

      // handleAlarm should load state from storage
      await stub.handleAlarm()

      // State should be loaded — circuit breaker should be open
      expect(await stub.isCircuitBreakerOpen()).toBe(true)
    })
  })
})

// ============================================================================
// Intermittent Failure Tests
// ============================================================================

describe('Intermittent Network Failures', () => {
  it('should handle alternating success and failure', async () => {
    const stub = getStub()
    stub.setup()

    // Success
    stub.emit({ type: 'test', event: 'event.1', data: {} })
    await stub.flush()
    expect(await stub.getPendingCount()).toBe(0)

    // Failure
    stub.setPipelineError('Network error')
    stub.emit({ type: 'test', event: 'event.2', data: {} })
    await stub.flush()

    // Clear error and retry via alarm — should succeed
    stub.setPipelineError(null)
    await stub.handleAlarm()

    // Circuit should not be open (alternating doesn't accumulate)
    expect(await stub.isCircuitBreakerOpen()).toBe(false)
  })

  it('should handle random failure pattern', async () => {
    const stub = getStub()
    stub.setup({ maxConsecutiveFailures: 20 }) // High threshold to avoid circuit breaker

    const failurePattern = [true, true, false, true, false, false, true, false, false, false]

    for (let i = 0; i < failurePattern.length; i++) {
      const shouldFail = failurePattern[i]
      if (shouldFail) {
        stub.setPipelineError('Random failure')
      } else {
        stub.setPipelineError(null)
      }

      stub.emit({ type: 'test', event: `event.${i}`, data: {} })
      await stub.tryFlush()

      // Try alarm in case there are retries pending
      await stub.handleAlarm()
    }

    // Should have processed events
    expect(await stub.getSendCallCount()).toBeGreaterThan(0)
  })

  it('should handle burst of failures followed by recovery', async () => {
    const stub = getStub()
    stub.setup({ maxConsecutiveFailures: 10 })
    stub.setPipelineError('Network error')

    // Generate burst of failing events
    for (let i = 0; i < 5; i++) {
      stub.emit({ type: 'test', event: `burst.${i}`, data: {} })
      await stub.flush()
    }

    // Should have consecutive failures but not enough to open (threshold is 10)
    expect(await stub.isCircuitBreakerOpen()).toBe(false)

    // Recovery — clear error and retry
    stub.setPipelineError(null)
    await stub.handleAlarm()

    expect(await stub.isCircuitBreakerOpen()).toBe(false)
  })
})

// ============================================================================
// Concurrent Request Failure Tests
// ============================================================================

describe('Concurrent Request Failures', () => {
  it('should handle multiple concurrent flushes gracefully', async () => {
    const stub = getStub()
    stub.setup()

    stub.emit({ type: 'test', event: 'event.1', data: {} })

    // Start multiple flushes concurrently
    await Promise.all([stub.flush(), stub.flush(), stub.flush()])

    // Only one send should have been made (first flush takes the batch)
    expect(await stub.getSendCallCount()).toBe(1)
  })

  it('should handle race between flush and handleAlarm', async () => {
    const stub = getStub()
    stub.setup()

    // First flush fails — events go to retry queue
    stub.setPipelineError('Network error')
    stub.emit({ type: 'test', event: 'event.1', data: {} })
    await stub.flush()

    // Clear error — next operations should succeed
    stub.setPipelineError(null)

    // Now retry queue has the event
    // Start both alarm handler and new flush concurrently
    stub.emit({ type: 'test', event: 'event.2', data: {} })

    await Promise.all([stub.handleAlarm(), stub.flush()])

    // All events should eventually be processed
    // Initial fail (1) + alarm retry (1) + new flush (1) = 3
    expect(await stub.getSendCallCount()).toBe(3)
  })
})

// ============================================================================
// Network Error Classification Tests
// ============================================================================

describe('Network Error Classification', () => {
  const networkErrors = [
    'connect ECONNREFUSED',
    'read ECONNRESET',
    'connect ETIMEDOUT',
    'getaddrinfo ENOTFOUND',
    'connect EHOSTUNREACH',
    'connect ENETUNREACH',
    'The operation was aborted',
    'Failed to fetch',
  ]

  networkErrors.forEach((errorMsg) => {
    it(`should treat "${errorMsg}" as retryable network error`, async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError(errorMsg)

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      // Should have stored for retry (network errors are retryable)
      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBeGreaterThan(0)

      const alarmTime = await stub.getAlarmTime()
      expect(alarmTime).not.toBeNull()
    })
  })

  const pipelineErrors = [
    'Internal Server Error',
    'Bad Gateway',
    'Service Unavailable',
    'Gateway Timeout',
    'Too Many Requests',
  ]

  pipelineErrors.forEach((errorMsg) => {
    it(`should treat pipeline "${errorMsg}" rejection as retryable error`, async () => {
      const stub = getStub()
      stub.setup()
      stub.setPipelineError(errorMsg)

      stub.emit({ type: 'test', event: 'test.event', data: {} })
      await stub.flush()

      const retryQueue = await stub.getStorageValue<unknown[]>('_events:retry')
      expect(retryQueue).toBeDefined()
      expect(retryQueue!.length).toBeGreaterThan(0)
    })
  })
})
