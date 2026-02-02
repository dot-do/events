/**
 * Body Size Limit Tests
 *
 * Unit tests for request body size limiting middleware.
 * Tests the size limit checking for ingest, webhook, and query endpoints.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  checkContentLength,
  readBodyWithLimit,
  getMaxBodySize,
} from '../../middleware/ingest/validate'
import { PayloadTooLargeError } from '../../../core/src/errors'
import {
  DEFAULT_MAX_BODY_SIZE,
  MAX_WEBHOOK_BODY_SIZE,
  MAX_QUERY_BODY_SIZE,
} from '../../middleware/ingest/types'

// ============================================================================
// Test Utilities
// ============================================================================

/**
 * Create a mock request with specified body and headers
 */
function createMockRequest(body: string, headers: Record<string, string> = {}): Request {
  return new Request('https://events.do/test', {
    method: 'POST',
    body,
    headers: {
      'content-type': 'application/json',
      ...headers,
    },
  })
}

/**
 * Create a large body of specified size
 */
function createLargeBody(sizeBytes: number): string {
  // Create a JSON object that is approximately the requested size
  const padding = 'x'.repeat(Math.max(0, sizeBytes - 20))
  return JSON.stringify({ data: padding })
}

// ============================================================================
// Constants Tests
// ============================================================================

describe('Body Size Limit Constants', () => {
  it('has correct default max body size (1MB)', () => {
    expect(DEFAULT_MAX_BODY_SIZE).toBe(1 * 1024 * 1024)
  })

  it('has correct webhook max body size (1MB)', () => {
    expect(MAX_WEBHOOK_BODY_SIZE).toBe(1 * 1024 * 1024)
  })

  it('has correct query max body size (64KB)', () => {
    expect(MAX_QUERY_BODY_SIZE).toBe(64 * 1024)
  })
})

// ============================================================================
// getMaxBodySize Tests
// ============================================================================

describe('getMaxBodySize', () => {
  it('returns default size when env not set', () => {
    const result = getMaxBodySize({})
    expect(result).toBe(DEFAULT_MAX_BODY_SIZE)
  })

  it('returns custom default when provided', () => {
    const customDefault = 500000
    const result = getMaxBodySize({}, customDefault)
    expect(result).toBe(customDefault)
  })

  it('parses MAX_INGEST_BODY_SIZE from env', () => {
    const result = getMaxBodySize({ MAX_INGEST_BODY_SIZE: '2097152' }) // 2MB
    expect(result).toBe(2097152)
  })

  it('falls back to default for invalid env value', () => {
    const result = getMaxBodySize({ MAX_INGEST_BODY_SIZE: 'invalid' })
    expect(result).toBe(DEFAULT_MAX_BODY_SIZE)
  })

  it('falls back to default for negative env value', () => {
    const result = getMaxBodySize({ MAX_INGEST_BODY_SIZE: '-1000' })
    expect(result).toBe(DEFAULT_MAX_BODY_SIZE)
  })

  it('falls back to default for zero env value', () => {
    const result = getMaxBodySize({ MAX_INGEST_BODY_SIZE: '0' })
    expect(result).toBe(DEFAULT_MAX_BODY_SIZE)
  })
})

// ============================================================================
// checkContentLength Tests
// ============================================================================

describe('checkContentLength', () => {
  it('returns undefined when no Content-Length header', () => {
    const request = createMockRequest('{"test": true}')
    // Remove content-length header for this test
    const headers = new Headers(request.headers)
    headers.delete('content-length')
    const newRequest = new Request(request.url, {
      method: 'POST',
      headers,
      body: '{"test": true}',
    })

    const result = checkContentLength(newRequest, DEFAULT_MAX_BODY_SIZE)
    expect(result).toBeUndefined()
  })

  it('returns content length when within limit', () => {
    const body = '{"test": true}'
    const request = createMockRequest(body, {
      'content-length': String(body.length),
    })

    const result = checkContentLength(request, DEFAULT_MAX_BODY_SIZE)
    expect(result).toBe(body.length)
  })

  it('throws PayloadTooLargeError when Content-Length exceeds limit', () => {
    const maxSize = 100
    const request = createMockRequest('x', {
      'content-length': '200',
    })

    expect(() => checkContentLength(request, maxSize)).toThrow(PayloadTooLargeError)
  })

  it('includes details in PayloadTooLargeError', () => {
    const maxSize = 100
    const request = createMockRequest('x', {
      'content-length': '200',
    })

    try {
      checkContentLength(request, maxSize)
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(PayloadTooLargeError)
      const ptle = error as PayloadTooLargeError
      expect(ptle.details?.maxSize).toBe(100)
      expect(ptle.details?.contentLength).toBe(200)
    }
  })

  it('returns content length for edge case at exact limit', () => {
    const maxSize = 100
    const request = createMockRequest('x', {
      'content-length': '100',
    })

    const result = checkContentLength(request, maxSize)
    expect(result).toBe(100)
  })
})

// ============================================================================
// readBodyWithLimit Tests
// ============================================================================

describe('readBodyWithLimit', () => {
  it('reads body within size limit', async () => {
    const body = JSON.stringify({ events: [] })
    const request = createMockRequest(body)

    const result = await readBodyWithLimit(request, DEFAULT_MAX_BODY_SIZE)
    expect(result).toBe(body)
  })

  it('reads empty body', async () => {
    const request = new Request('https://events.do/test', {
      method: 'POST',
    })

    const result = await readBodyWithLimit(request, DEFAULT_MAX_BODY_SIZE)
    expect(result).toBe('')
  })

  it('rejects body exceeding limit via Content-Length', async () => {
    const maxSize = 100
    const body = createLargeBody(200)
    const request = createMockRequest(body, {
      'content-length': String(body.length),
    })

    await expect(readBodyWithLimit(request, maxSize)).rejects.toThrow(PayloadTooLargeError)
  })

  it('rejects body exceeding limit during streaming', async () => {
    const maxSize = 100
    const body = createLargeBody(200)
    // Create request without Content-Length to force streaming check
    const request = new Request('https://events.do/test', {
      method: 'POST',
      body: body,
      headers: {
        'content-type': 'application/json',
      },
    })

    await expect(readBodyWithLimit(request, maxSize)).rejects.toThrow(PayloadTooLargeError)
  })

  it('reads body at exact size limit', async () => {
    const maxSize = 100
    // Create a body that is exactly maxSize bytes
    const body = 'x'.repeat(maxSize)
    const request = createMockRequest(body, {
      'content-length': String(body.length),
    })

    const result = await readBodyWithLimit(request, maxSize)
    expect(result).toBe(body)
  })

  it('includes max size in error details', async () => {
    const maxSize = 100
    const body = createLargeBody(200)
    const request = createMockRequest(body, {
      'content-length': String(body.length),
    })

    try {
      await readBodyWithLimit(request, maxSize)
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(PayloadTooLargeError)
      const ptle = error as PayloadTooLargeError
      expect(ptle.details?.maxSize).toBe(maxSize)
    }
  })
})

// ============================================================================
// Integration-style Tests
// ============================================================================

describe('Body Size Limit Integration', () => {
  describe('Ingest endpoint limits', () => {
    it('uses 1MB default limit', () => {
      const maxSize = getMaxBodySize({})
      expect(maxSize).toBe(1 * 1024 * 1024)
    })

    it('accepts configurable limit via environment', () => {
      const maxSize = getMaxBodySize({ MAX_INGEST_BODY_SIZE: '5242880' }) // 5MB
      expect(maxSize).toBe(5 * 1024 * 1024)
    })
  })

  describe('Webhook endpoint limits', () => {
    it('uses 1MB limit for webhooks', () => {
      expect(MAX_WEBHOOK_BODY_SIZE).toBe(1 * 1024 * 1024)
    })
  })

  describe('Query endpoint limits', () => {
    it('uses 64KB limit for query requests', () => {
      expect(MAX_QUERY_BODY_SIZE).toBe(64 * 1024)
    })
  })
})

// ============================================================================
// Error Response Format Tests
// ============================================================================

describe('PayloadTooLargeError', () => {
  it('has correct error code', () => {
    const error = new PayloadTooLargeError()
    expect(error.code).toBe('PAYLOAD_TOO_LARGE')
  })

  it('has 413 status code', () => {
    const error = new PayloadTooLargeError()
    expect(error.status).toBe(413)
  })

  it('uses default message when none provided', () => {
    const error = new PayloadTooLargeError()
    expect(error.message).toBe('Payload Too Large')
  })

  it('uses custom message when provided', () => {
    const error = new PayloadTooLargeError('Custom error message')
    expect(error.message).toBe('Custom error message')
  })

  it('includes details in toJSON output', () => {
    const error = new PayloadTooLargeError('Test', { maxSize: 1000, contentLength: 2000 })
    const json = error.toJSON()

    expect(json.error).toBe('Test')
    expect(json.code).toBe('PAYLOAD_TOO_LARGE')
    expect(json.details?.maxSize).toBe(1000)
    expect(json.details?.contentLength).toBe(2000)
  })
})
