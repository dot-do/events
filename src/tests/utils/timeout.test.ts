/**
 * Timeout and Circuit Breaker Utility Tests
 *
 * Unit tests for src/utils.ts timeout and circuit breaker functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  withTimeout,
  TimeoutError,
  CircuitBreaker,
  CircuitBreakerOpenError,
  protectedCall,
} from '../../utils'

// ============================================================================
// withTimeout Tests
// ============================================================================

describe('withTimeout', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('when promise resolves before timeout', () => {
    it('returns the resolved value', async () => {
      const promise = Promise.resolve('success')

      const result = await withTimeout(promise, 1000, 'Operation timed out')

      expect(result).toBe('success')
    })

    it('returns the resolved value for slow but not timed out operations', async () => {
      const promise = new Promise<string>((resolve) => {
        setTimeout(() => resolve('delayed success'), 500)
      })

      const resultPromise = withTimeout(promise, 1000, 'Operation timed out')
      vi.advanceTimersByTime(500)
      const result = await resultPromise

      expect(result).toBe('delayed success')
    })
  })

  describe('when promise times out', () => {
    it('throws TimeoutError with correct message', async () => {
      const promise = new Promise<string>((resolve) => {
        setTimeout(() => resolve('never'), 5000)
      })

      const resultPromise = withTimeout(promise, 100, 'Custom timeout message')
      vi.advanceTimersByTime(100)

      await expect(resultPromise).rejects.toThrow(TimeoutError)
      await expect(resultPromise).rejects.toThrow('Custom timeout message')
    })

    it('includes timeout duration in error', async () => {
      const promise = new Promise<string>(() => {})

      const resultPromise = withTimeout(promise, 250, 'Timed out')
      vi.advanceTimersByTime(250)

      try {
        await resultPromise
      } catch (err) {
        expect(err).toBeInstanceOf(TimeoutError)
        expect((err as TimeoutError).timeoutMs).toBe(250)
      }
    })
  })

  describe('when promise rejects before timeout', () => {
    it('propagates the rejection', async () => {
      const error = new Error('Original error')
      const promise = Promise.reject(error)

      await expect(withTimeout(promise, 1000, 'Timed out')).rejects.toThrow('Original error')
    })
  })

  describe('edge cases', () => {
    it('handles zero timeout by returning promise directly', async () => {
      const promise = Promise.resolve('immediate')

      const result = await withTimeout(promise, 0, 'Timed out')

      expect(result).toBe('immediate')
    })

    it('handles negative timeout by returning promise directly', async () => {
      const promise = Promise.resolve('immediate')

      const result = await withTimeout(promise, -100, 'Timed out')

      expect(result).toBe('immediate')
    })

    it('clears timeout when promise resolves', async () => {
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')
      const promise = Promise.resolve('success')

      await withTimeout(promise, 1000, 'Timed out')

      expect(clearTimeoutSpy).toHaveBeenCalled()
      clearTimeoutSpy.mockRestore()
    })

    it('clears timeout when promise rejects', async () => {
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')
      const promise = Promise.reject(new Error('fail'))

      await withTimeout(promise, 1000, 'Timed out').catch(() => {})

      expect(clearTimeoutSpy).toHaveBeenCalled()
      clearTimeoutSpy.mockRestore()
    })
  })
})

// ============================================================================
// CircuitBreaker Tests
// ============================================================================

describe('CircuitBreaker', () => {
  describe('initial state', () => {
    it('starts in closed state', () => {
      const breaker = new CircuitBreaker('test')

      expect(breaker.getState()).toBe('closed')
    })

    it('has zero failures and successes', () => {
      const breaker = new CircuitBreaker('test')

      const status = breaker.getStatus()
      expect(status.failures).toBe(0)
      expect(status.successes).toBe(0)
    })
  })

  describe('closed state', () => {
    it('executes function and returns result', async () => {
      const breaker = new CircuitBreaker('test')
      const fn = vi.fn().mockResolvedValue('result')

      const result = await breaker.execute(fn)

      expect(result).toBe('result')
      expect(fn).toHaveBeenCalled()
    })

    it('increments failure count on error', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 5 })
      const fn = vi.fn().mockRejectedValue(new Error('fail'))

      await breaker.execute(fn).catch(() => {})

      expect(breaker.getStatus().failures).toBe(1)
    })

    it('resets failure count on success', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 5 })
      const failFn = vi.fn().mockRejectedValue(new Error('fail'))
      const successFn = vi.fn().mockResolvedValue('success')

      await breaker.execute(failFn).catch(() => {})
      await breaker.execute(failFn).catch(() => {})
      expect(breaker.getStatus().failures).toBe(2)

      await breaker.execute(successFn)
      expect(breaker.getStatus().failures).toBe(0)
    })

    it('opens circuit after threshold failures', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 3 })
      const fn = vi.fn().mockRejectedValue(new Error('fail'))

      for (let i = 0; i < 3; i++) {
        await breaker.execute(fn).catch(() => {})
      }

      expect(breaker.getState()).toBe('open')
    })
  })

  describe('open state', () => {
    it('throws CircuitBreakerOpenError without calling function', async () => {
      const breaker = new CircuitBreaker('test-circuit', { failureThreshold: 1 })
      const fn = vi.fn().mockRejectedValue(new Error('fail'))

      await breaker.execute(fn).catch(() => {})
      expect(breaker.getState()).toBe('open')

      const fn2 = vi.fn().mockResolvedValue('success')
      await expect(breaker.execute(fn2)).rejects.toThrow(CircuitBreakerOpenError)
      expect(fn2).not.toHaveBeenCalled()
    })

    it('includes circuit name in error', async () => {
      const breaker = new CircuitBreaker('my-service', { failureThreshold: 1 })
      await breaker.execute(() => Promise.reject(new Error('fail'))).catch(() => {})

      try {
        await breaker.execute(() => Promise.resolve())
      } catch (err) {
        expect(err).toBeInstanceOf(CircuitBreakerOpenError)
        expect((err as CircuitBreakerOpenError).circuitName).toBe('my-service')
        expect((err as CircuitBreakerOpenError).message).toContain('my-service')
      }
    })

    it('includes last error in CircuitBreakerOpenError', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })
      const originalError = new Error('original failure')
      await breaker.execute(() => Promise.reject(originalError)).catch(() => {})

      try {
        await breaker.execute(() => Promise.resolve())
      } catch (err) {
        expect((err as CircuitBreakerOpenError).lastError).toBe(originalError)
      }
    })

    it('transitions to half-open after reset timeout', async () => {
      vi.useFakeTimers()
      const breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 1000,
      })

      await breaker.execute(() => Promise.reject(new Error())).catch(() => {})
      expect(breaker.getState()).toBe('open')

      vi.advanceTimersByTime(1000)
      expect(breaker.getState()).toBe('half-open')

      vi.useRealTimers()
    })
  })

  describe('half-open state', () => {
    let breaker: CircuitBreaker

    beforeEach(async () => {
      vi.useFakeTimers()
      breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 1000,
        successThreshold: 2,
      })

      // Trip the breaker
      await breaker.execute(() => Promise.reject(new Error())).catch(() => {})
      // Advance to half-open
      vi.advanceTimersByTime(1000)
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it('allows test request through', async () => {
      const fn = vi.fn().mockResolvedValue('success')

      const result = await breaker.execute(fn)

      expect(result).toBe('success')
      expect(fn).toHaveBeenCalled()
    })

    it('reopens on failure', async () => {
      const fn = vi.fn().mockRejectedValue(new Error())

      await breaker.execute(fn).catch(() => {})

      expect(breaker.getState()).toBe('open')
    })

    it('closes after success threshold', async () => {
      const fn = vi.fn().mockResolvedValue('success')

      await breaker.execute(fn)
      expect(breaker.getState()).toBe('half-open')

      await breaker.execute(fn)
      expect(breaker.getState()).toBe('closed')
    })

    it('resets failure and success counts when closing', async () => {
      const fn = vi.fn().mockResolvedValue('success')

      await breaker.execute(fn)
      await breaker.execute(fn)

      const status = breaker.getStatus()
      expect(status.state).toBe('closed')
      expect(status.failures).toBe(0)
      expect(status.successes).toBe(0)
    })
  })

  describe('manual controls', () => {
    it('reset() closes the circuit', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })
      await breaker.execute(() => Promise.reject(new Error())).catch(() => {})
      expect(breaker.getState()).toBe('open')

      breaker.reset()

      expect(breaker.getState()).toBe('closed')
      expect(breaker.getStatus().failures).toBe(0)
    })

    it('trip() opens the circuit', () => {
      const breaker = new CircuitBreaker('test')
      expect(breaker.getState()).toBe('closed')

      breaker.trip(new Error('manual trip'))

      expect(breaker.getState()).toBe('open')
      expect(breaker.getStatus().lastError).toBe('manual trip')
    })
  })

  describe('configuration', () => {
    it('uses default values when not specified', () => {
      const breaker = new CircuitBreaker('test')

      // Default failure threshold is 5
      const fn = vi.fn().mockRejectedValue(new Error())
      for (let i = 0; i < 4; i++) {
        breaker.execute(fn).catch(() => {})
      }

      // Should still be closed after 4 failures
      expect(breaker.getState()).toBe('closed')
    })

    it('accepts custom failure threshold', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 2 })
      const fn = vi.fn().mockRejectedValue(new Error())

      await breaker.execute(fn).catch(() => {})
      expect(breaker.getState()).toBe('closed')

      await breaker.execute(fn).catch(() => {})
      expect(breaker.getState()).toBe('open')
    })

    it('accepts custom reset timeout', async () => {
      vi.useFakeTimers()
      const breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 5000,
      })

      await breaker.execute(() => Promise.reject(new Error())).catch(() => {})

      vi.advanceTimersByTime(4999)
      expect(breaker.getState()).toBe('open')

      vi.advanceTimersByTime(1)
      expect(breaker.getState()).toBe('half-open')

      vi.useRealTimers()
    })

    it('accepts custom success threshold', async () => {
      vi.useFakeTimers()
      const breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 1000,
        successThreshold: 3,
      })

      await breaker.execute(() => Promise.reject(new Error())).catch(() => {})
      vi.advanceTimersByTime(1000)

      const fn = vi.fn().mockResolvedValue('success')
      await breaker.execute(fn)
      expect(breaker.getState()).toBe('half-open')
      await breaker.execute(fn)
      expect(breaker.getState()).toBe('half-open')
      await breaker.execute(fn)
      expect(breaker.getState()).toBe('closed')

      vi.useRealTimers()
    })
  })
})

// ============================================================================
// protectedCall Tests
// ============================================================================

describe('protectedCall', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('without circuit breaker', () => {
    it('returns result on success', async () => {
      const fn = vi.fn().mockResolvedValue('result')

      const result = await protectedCall(fn, { timeoutMs: 1000 })

      expect(result).toBe('result')
    })

    it('throws TimeoutError on timeout', async () => {
      const fn = vi.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve('late'), 5000))
      )

      const resultPromise = protectedCall(fn, { timeoutMs: 100, errorMsg: 'DO call' })
      vi.advanceTimersByTime(100)

      await expect(resultPromise).rejects.toThrow(TimeoutError)
      await expect(resultPromise).rejects.toThrow('DO call timed out after 100ms')
    })

    it('uses default timeout of 5000ms', async () => {
      const fn = vi.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve('late'), 10000))
      )

      const resultPromise = protectedCall(fn)
      vi.advanceTimersByTime(5000)

      await expect(resultPromise).rejects.toThrow(TimeoutError)
    })

    it('propagates other errors', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('Custom error'))

      await expect(protectedCall(fn, { timeoutMs: 1000 })).rejects.toThrow('Custom error')
    })
  })

  describe('with circuit breaker', () => {
    it('uses circuit breaker for protection', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })
      const fn = vi.fn().mockRejectedValue(new Error('fail'))

      await protectedCall(fn, { circuitBreaker: breaker }).catch(() => {})

      expect(breaker.getState()).toBe('open')
    })

    it('throws CircuitBreakerOpenError when circuit is open', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })
      breaker.trip()

      const fn = vi.fn().mockResolvedValue('success')

      await expect(
        protectedCall(fn, { circuitBreaker: breaker })
      ).rejects.toThrow(CircuitBreakerOpenError)
      expect(fn).not.toHaveBeenCalled()
    })

    it('records timeout as circuit breaker failure', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 2 })
      const slowFn = vi.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve('late'), 5000))
      )

      // First timeout
      const promise1 = protectedCall(slowFn, { timeoutMs: 100, circuitBreaker: breaker })
      vi.advanceTimersByTime(100)
      await promise1.catch(() => {})

      expect(breaker.getStatus().failures).toBe(1)

      // Second timeout should open circuit
      const promise2 = protectedCall(slowFn, { timeoutMs: 100, circuitBreaker: breaker })
      vi.advanceTimersByTime(100)
      await promise2.catch(() => {})

      expect(breaker.getState()).toBe('open')
    })

    it('successful call resets circuit breaker failures', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 3 })
      const fn = vi.fn().mockRejectedValue(new Error('fail'))

      await protectedCall(fn, { circuitBreaker: breaker }).catch(() => {})
      await protectedCall(fn, { circuitBreaker: breaker }).catch(() => {})
      expect(breaker.getStatus().failures).toBe(2)

      const successFn = vi.fn().mockResolvedValue('success')
      await protectedCall(successFn, { circuitBreaker: breaker })

      expect(breaker.getStatus().failures).toBe(0)
    })
  })
})

// ============================================================================
// Error Type Tests
// ============================================================================

describe('TimeoutError', () => {
  it('has correct name', () => {
    const error = new TimeoutError('message', 1000)
    expect(error.name).toBe('TimeoutError')
  })

  it('stores timeout duration', () => {
    const error = new TimeoutError('message', 5000)
    expect(error.timeoutMs).toBe(5000)
  })

  it('is instanceof Error', () => {
    const error = new TimeoutError('message', 1000)
    expect(error).toBeInstanceOf(Error)
  })
})

describe('CircuitBreakerOpenError', () => {
  it('has correct name', () => {
    const error = new CircuitBreakerOpenError('message', 'circuit-name')
    expect(error.name).toBe('CircuitBreakerOpenError')
  })

  it('stores circuit name', () => {
    const error = new CircuitBreakerOpenError('message', 'my-circuit')
    expect(error.circuitName).toBe('my-circuit')
  })

  it('stores last error if provided', () => {
    const lastError = new Error('original')
    const error = new CircuitBreakerOpenError('message', 'circuit', lastError)
    expect(error.lastError).toBe(lastError)
  })

  it('is instanceof Error', () => {
    const error = new CircuitBreakerOpenError('message', 'circuit')
    expect(error).toBeInstanceOf(Error)
  })
})
