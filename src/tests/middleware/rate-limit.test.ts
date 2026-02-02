/**
 * Rate Limit Middleware Tests
 *
 * Unit tests for src/middleware/rate-limit.ts
 * Tests getRateLimitKey, checkRateLimit, and addRateLimitHeaders
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  getRateLimitKey,
  checkRateLimit,
  addRateLimitHeaders,
  getRateLimiterCircuitBreaker,
  type RateLimitEnv,
  type RateLimitResult,
} from '../../middleware/rate-limit'

// ============================================================================
// Mock Types
// ============================================================================

interface MockRateLimiterDO {
  checkAndIncrement: ReturnType<typeof vi.fn>
  getStatus: ReturnType<typeof vi.fn>
  reset: ReturnType<typeof vi.fn>
}

interface MockDurableObjectId {
  toString(): string
}

interface MockDurableObjectNamespace {
  idFromName: ReturnType<typeof vi.fn<[string], MockDurableObjectId>>
  get: ReturnType<typeof vi.fn<[MockDurableObjectId], MockRateLimiterDO>>
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockRateLimiter(overrides: Partial<MockRateLimiterDO> = {}): MockRateLimiterDO {
  return {
    checkAndIncrement: vi.fn().mockResolvedValue({
      allowed: true,
      requestsRemaining: 999,
      eventsRemaining: 99999,
      resetAt: Date.now() + 60000,
    }),
    getStatus: vi.fn().mockResolvedValue({
      allowed: true,
      requestsRemaining: 1000,
      eventsRemaining: 100000,
      resetAt: Date.now() + 60000,
    }),
    reset: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

function createMockNamespace(rateLimiter: MockRateLimiterDO): MockDurableObjectNamespace {
  return {
    idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
    get: vi.fn().mockReturnValue(rateLimiter),
  }
}

function createMockEnv(
  namespace?: MockDurableObjectNamespace,
  overrides: Partial<RateLimitEnv> = {}
): RateLimitEnv {
  return {
    RATE_LIMITER: namespace as unknown as RateLimitEnv['RATE_LIMITER'],
    RATE_LIMIT_REQUESTS_PER_MINUTE: undefined,
    RATE_LIMIT_EVENTS_PER_MINUTE: undefined,
    ...overrides,
  }
}

function createRequest(options: {
  authorization?: string
  cfConnectingIp?: string
  xForwardedFor?: string
} = {}): Request {
  const headers = new Headers()
  if (options.authorization) {
    headers.set('Authorization', options.authorization)
  }
  if (options.cfConnectingIp) {
    headers.set('CF-Connecting-IP', options.cfConnectingIp)
  }
  if (options.xForwardedFor) {
    headers.set('X-Forwarded-For', options.xForwardedFor)
  }
  return new Request('https://events.do/ingest', { method: 'POST', headers })
}

// ============================================================================
// getRateLimitKey Tests
// ============================================================================

describe('getRateLimitKey', () => {
  describe('API key identification', () => {
    it('uses API key from Bearer token', () => {
      const request = createRequest({ authorization: 'Bearer my-api-key-123' })

      const key = getRateLimitKey(request)

      expect(key).toBe('key:my-api-key-123')
    })

    it('handles complex API keys', () => {
      const request = createRequest({ authorization: 'Bearer ns_acme_abc123xyz' })

      const key = getRateLimitKey(request)

      expect(key).toBe('key:ns_acme_abc123xyz')
    })

    it('prefers API key over IP address', () => {
      const request = createRequest({
        authorization: 'Bearer my-token',
        cfConnectingIp: '1.2.3.4',
      })

      const key = getRateLimitKey(request)

      expect(key).toBe('key:my-token')
    })
  })

  describe('IP address identification', () => {
    it('uses CF-Connecting-IP header', () => {
      const request = createRequest({ cfConnectingIp: '192.168.1.100' })

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:192.168.1.100')
    })

    it('falls back to X-Forwarded-For first IP', () => {
      const request = createRequest({ xForwardedFor: '10.0.0.1, 172.16.0.1, 192.168.1.1' })

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:10.0.0.1')
    })

    it('handles single IP in X-Forwarded-For', () => {
      const request = createRequest({ xForwardedFor: '203.0.113.50' })

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:203.0.113.50')
    })

    it('prefers CF-Connecting-IP over X-Forwarded-For', () => {
      const request = createRequest({
        cfConnectingIp: '1.1.1.1',
        xForwardedFor: '2.2.2.2, 3.3.3.3',
      })

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:1.1.1.1')
    })

    it('returns ip:unknown when no identification available', () => {
      const request = createRequest()

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:unknown')
    })
  })

  describe('edge cases', () => {
    it('handles "Bearer " (with trailing space) by falling back to IP', () => {
      // Note: HTTP Headers API trims trailing whitespace, so 'Bearer ' becomes 'Bearer'
      // which doesn't match 'Bearer ' (with space), so it falls back to IP
      const request = createRequest({
        authorization: 'Bearer ',
        cfConnectingIp: '1.2.3.4',
      })

      const key = getRateLimitKey(request)

      // Falls back to IP because header becomes 'Bearer' (trimmed)
      expect(key).toBe('ip:1.2.3.4')
    })

    it('ignores non-Bearer authorization', () => {
      const headers = new Headers()
      headers.set('Authorization', 'Basic dXNlcjpwYXNz')
      headers.set('CF-Connecting-IP', '5.5.5.5')
      const request = new Request('https://events.do/ingest', { method: 'POST', headers })

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:5.5.5.5')
    })

    it('handles IPv6 addresses', () => {
      const request = createRequest({ cfConnectingIp: '2001:db8::1' })

      const key = getRateLimitKey(request)

      expect(key).toBe('ip:2001:db8::1')
    })
  })
})

// ============================================================================
// checkRateLimit Tests
// ============================================================================

describe('checkRateLimit', () => {
  describe('when RATE_LIMITER is not configured', () => {
    it('allows all requests through', async () => {
      const env = createMockEnv(undefined)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)

      expect(result).toBeNull()
    })
  })

  describe('when rate limit is not exceeded', () => {
    it('allows request through (returns null)', async () => {
      const rateLimiter = createMockRateLimiter()
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 50)

      expect(result).toBeNull()
    })

    it('calls checkAndIncrement with correct parameters', async () => {
      const rateLimiter = createMockRateLimiter()
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      await checkRateLimit(request, env, 75)

      expect(rateLimiter.checkAndIncrement).toHaveBeenCalledWith(1000, 100000, 75)
    })

    it('uses custom rate limits from env', async () => {
      const rateLimiter = createMockRateLimiter()
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace, {
        RATE_LIMIT_REQUESTS_PER_MINUTE: '500',
        RATE_LIMIT_EVENTS_PER_MINUTE: '50000',
      })
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      await checkRateLimit(request, env, 25)

      expect(rateLimiter.checkAndIncrement).toHaveBeenCalledWith(500, 50000, 25)
    })

    it('uses correct rate limit key based on request', async () => {
      const rateLimiter = createMockRateLimiter()
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ authorization: 'Bearer my-key' })

      await checkRateLimit(request, env, 10)

      expect(namespace.idFromName).toHaveBeenCalledWith('key:my-key')
    })
  })

  describe('when rate limit is exceeded', () => {
    it('returns 429 response', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockResolvedValue({
          allowed: false,
          requestsRemaining: 0,
          eventsRemaining: 1000,
          resetAt: Date.now() + 30000,
          retryAfterSeconds: 30,
        }),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)

      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(429)
    })

    it('includes rate limit info in response body', async () => {
      const resetAt = Date.now() + 45000
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockResolvedValue({
          allowed: false,
          requestsRemaining: 0,
          eventsRemaining: 5000,
          resetAt,
          retryAfterSeconds: 45,
        }),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)
      const json = await result?.json()

      expect(json.error).toBe('Too Many Requests')
      expect(json.limits.requestsPerMinute).toBe(1000)
      expect(json.limits.eventsPerMinute).toBe(100000)
      expect(json.current.requestsRemaining).toBe(0)
      expect(json.current.eventsRemaining).toBe(5000)
      expect(json.retryAfterSeconds).toBe(45)
    })

    it('includes rate limit headers in response', async () => {
      const resetAt = Date.now() + 60000
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockResolvedValue({
          allowed: false,
          requestsRemaining: 10,
          eventsRemaining: 500,
          resetAt,
          retryAfterSeconds: 60,
        }),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)

      expect(result?.headers.get('Retry-After')).toBe('60')
      expect(result?.headers.get('X-RateLimit-Limit-Requests')).toBe('1000')
      expect(result?.headers.get('X-RateLimit-Limit-Events')).toBe('100000')
      expect(result?.headers.get('X-RateLimit-Remaining-Requests')).toBe('10')
      expect(result?.headers.get('X-RateLimit-Remaining-Events')).toBe('500')
      expect(result?.headers.get('X-RateLimit-Reset')).toBe(String(resetAt))
    })

    it('includes CORS headers in response', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockResolvedValue({
          allowed: false,
          requestsRemaining: 0,
          eventsRemaining: 0,
          resetAt: Date.now() + 60000,
          retryAfterSeconds: 60,
        }),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)

      expect(result?.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })

    it('uses default retry after when not provided', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockResolvedValue({
          allowed: false,
          requestsRemaining: 0,
          eventsRemaining: 0,
          resetAt: Date.now() + 60000,
          // retryAfterSeconds not provided
        }),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)

      expect(result?.headers.get('Retry-After')).toBe('60')
    })
  })

  describe('error handling', () => {
    it('allows request through when rate limiter throws', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockRejectedValue(new Error('DO unavailable')),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      const result = await checkRateLimit(request, env, 100)

      expect(result).toBeNull()
    })

    it('logs error when rate limiter throws', async () => {
      // The implementation uses structured logging via logError/logger
      // which outputs JSON to console.error
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockRejectedValue(new Error('Test error')),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      await checkRateLimit(request, env, 100)

      // Structured logger outputs JSON containing the error message
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Error checking rate limit')
      )
      consoleSpy.mockRestore()
    })
  })

  describe('timeout protection', () => {
    beforeEach(() => {
      vi.useFakeTimers()
      getRateLimiterCircuitBreaker().reset()
    })

    afterEach(() => {
      vi.useRealTimers()
      getRateLimiterCircuitBreaker().reset()
    })

    it('allows request through when rate limit check times out', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockImplementation(
          () => new Promise((resolve) => setTimeout(() => resolve({
            allowed: true,
            requestsRemaining: 999,
            eventsRemaining: 99999,
            resetAt: Date.now() + 60000,
          }), 5000))
        ),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      // Call with a 100ms timeout
      const resultPromise = checkRateLimit(request, env, 100, 100)
      vi.advanceTimersByTime(100)
      const result = await resultPromise

      expect(result).toBeNull()
      consoleSpy.mockRestore()
    })

    it('uses custom timeout when provided', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockImplementation(
          () => new Promise((resolve) => setTimeout(() => resolve({
            allowed: true,
            requestsRemaining: 999,
            eventsRemaining: 99999,
            resetAt: Date.now() + 60000,
          }), 200))
        ),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })

      // Call with a 500ms timeout - should succeed
      const resultPromise = checkRateLimit(request, env, 100, 500)
      vi.advanceTimersByTime(200)
      const result = await resultPromise

      expect(result).toBeNull() // null means request allowed
    })
  })

  describe('circuit breaker protection', () => {
    beforeEach(() => {
      getRateLimiterCircuitBreaker().reset()
    })

    afterEach(() => {
      getRateLimiterCircuitBreaker().reset()
    })

    it('allows request when circuit breaker is open', async () => {
      // Trip the circuit breaker
      getRateLimiterCircuitBreaker().trip(new Error('test'))

      const rateLimiter = createMockRateLimiter()
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

      const result = await checkRateLimit(request, env, 100)

      expect(result).toBeNull()
      expect(rateLimiter.checkAndIncrement).not.toHaveBeenCalled()
      consoleSpy.mockRestore()
    })

    it('opens circuit after repeated failures', async () => {
      const rateLimiter = createMockRateLimiter({
        checkAndIncrement: vi.fn().mockRejectedValue(new Error('DO error')),
      })
      const namespace = createMockNamespace(rateLimiter)
      const env = createMockEnv(namespace)
      const request = createRequest({ cfConnectingIp: '1.2.3.4' })
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      // Call 5 times to trip the circuit breaker (default threshold)
      for (let i = 0; i < 5; i++) {
        await checkRateLimit(request, env, 100)
      }

      expect(getRateLimiterCircuitBreaker().getState()).toBe('open')
      consoleSpy.mockRestore()
    })
  })
})

// ============================================================================
// addRateLimitHeaders Tests
// ============================================================================

describe('addRateLimitHeaders', () => {
  it('does nothing when result is undefined', () => {
    const headers = new Headers()
    const env = createMockEnv()

    addRateLimitHeaders(headers, env, undefined)

    expect(headers.get('X-RateLimit-Limit-Requests')).toBeNull()
  })

  it('adds rate limit headers from result', () => {
    const headers = new Headers()
    const env = createMockEnv()
    const result: RateLimitResult = {
      allowed: true,
      requestsRemaining: 800,
      eventsRemaining: 90000,
      resetAt: Date.now() + 30000,
    }

    addRateLimitHeaders(headers, env, result)

    expect(headers.get('X-RateLimit-Limit-Requests')).toBe('1000')
    expect(headers.get('X-RateLimit-Limit-Events')).toBe('100000')
    expect(headers.get('X-RateLimit-Remaining-Requests')).toBe('800')
    expect(headers.get('X-RateLimit-Remaining-Events')).toBe('90000')
  })

  it('uses custom limits from env', () => {
    const headers = new Headers()
    const env = createMockEnv(undefined, {
      RATE_LIMIT_REQUESTS_PER_MINUTE: '2000',
      RATE_LIMIT_EVENTS_PER_MINUTE: '200000',
    })
    const result: RateLimitResult = {
      allowed: true,
      requestsRemaining: 1500,
      eventsRemaining: 150000,
      resetAt: Date.now() + 30000,
    }

    addRateLimitHeaders(headers, env, result)

    expect(headers.get('X-RateLimit-Limit-Requests')).toBe('2000')
    expect(headers.get('X-RateLimit-Limit-Events')).toBe('200000')
  })

  it('handles negative remaining values', () => {
    const headers = new Headers()
    const env = createMockEnv()
    const result: RateLimitResult = {
      allowed: false,
      requestsRemaining: -5,
      eventsRemaining: -100,
      resetAt: Date.now() + 30000,
    }

    addRateLimitHeaders(headers, env, result)

    expect(headers.get('X-RateLimit-Remaining-Requests')).toBe('0')
    expect(headers.get('X-RateLimit-Remaining-Events')).toBe('0')
  })
})
