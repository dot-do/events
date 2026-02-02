/**
 * Worker ID Validation Tests
 *
 * Tests for the SSRF prevention validation in subscription delivery.
 */

import { describe, it, expect } from 'vitest'
import {
  validateWorkerId,
  validateRpcMethod,
  assertValidDeliveryTarget,
  buildSafeDeliveryUrl,
  WORKER_ID_PATTERN,
  RPC_METHOD_PATTERN,
  MAX_WORKER_ID_LENGTH,
  MAX_RPC_METHOD_LENGTH,
  SSRF_BLOCKLIST_PATTERNS,
} from '../worker-id-validation.js'

// ============================================================================
// Worker ID Validation Tests
// ============================================================================

describe('validateWorkerId', () => {
  describe('valid worker IDs', () => {
    it('accepts standard worker names', () => {
      expect(validateWorkerId('my-worker')).toEqual({ valid: true })
      expect(validateWorkerId('github-webhook-handler')).toEqual({ valid: true })
      expect(validateWorkerId('events-do')).toEqual({ valid: true })
      expect(validateWorkerId('api-v2')).toEqual({ valid: true })
    })

    it('accepts single letter worker IDs', () => {
      expect(validateWorkerId('a')).toEqual({ valid: true })
      expect(validateWorkerId('z')).toEqual({ valid: true })
    })

    it('accepts two character worker IDs', () => {
      expect(validateWorkerId('ab')).toEqual({ valid: true })
      expect(validateWorkerId('a1')).toEqual({ valid: true })
    })

    it('accepts worker names with numbers', () => {
      expect(validateWorkerId('worker1')).toEqual({ valid: true })
      expect(validateWorkerId('my-worker-123')).toEqual({ valid: true })
      expect(validateWorkerId('v2-api')).toEqual({ valid: true })
    })

    it('accepts maximum length worker IDs (63 chars)', () => {
      const maxLengthId = 'a' + 'b'.repeat(61) + 'c'
      expect(maxLengthId.length).toBe(63)
      expect(validateWorkerId(maxLengthId)).toEqual({ valid: true })
    })
  })

  describe('invalid worker IDs - type errors', () => {
    it('rejects non-string values', () => {
      expect(validateWorkerId(123)).toEqual({
        valid: false,
        error: 'Worker ID must be a string',
      })
      expect(validateWorkerId(null)).toEqual({
        valid: false,
        error: 'Worker ID must be a string',
      })
      expect(validateWorkerId(undefined)).toEqual({
        valid: false,
        error: 'Worker ID must be a string',
      })
      expect(validateWorkerId({})).toEqual({
        valid: false,
        error: 'Worker ID must be a string',
      })
      expect(validateWorkerId(['my-worker'])).toEqual({
        valid: false,
        error: 'Worker ID must be a string',
      })
    })

    it('rejects empty string', () => {
      expect(validateWorkerId('')).toEqual({
        valid: false,
        error: 'Worker ID cannot be empty',
      })
    })
  })

  describe('invalid worker IDs - length errors', () => {
    it('rejects worker IDs exceeding max length', () => {
      const tooLong = 'a'.repeat(64)
      expect(validateWorkerId(tooLong)).toEqual({
        valid: false,
        error: `Worker ID exceeds maximum length of ${MAX_WORKER_ID_LENGTH} characters`,
      })
    })
  })

  describe('invalid worker IDs - format errors', () => {
    it('rejects worker IDs starting with number', () => {
      expect(validateWorkerId('123-worker').valid).toBe(false)
      expect(validateWorkerId('1worker').valid).toBe(false)
    })

    it('rejects worker IDs starting with hyphen', () => {
      expect(validateWorkerId('-worker').valid).toBe(false)
    })

    it('rejects worker IDs ending with hyphen', () => {
      expect(validateWorkerId('worker-').valid).toBe(false)
    })

    it('rejects worker IDs with consecutive hyphens', () => {
      expect(validateWorkerId('my--worker')).toEqual({
        valid: false,
        error: 'Worker ID cannot contain consecutive hyphens',
      })
    })

    it('rejects worker IDs with uppercase letters', () => {
      expect(validateWorkerId('MyWorker').valid).toBe(false)
      expect(validateWorkerId('WORKER').valid).toBe(false)
    })

    it('rejects worker IDs with underscores', () => {
      expect(validateWorkerId('my_worker').valid).toBe(false)
    })

    it('rejects worker IDs with dots', () => {
      expect(validateWorkerId('my.worker').valid).toBe(false)
    })
  })

  describe('SSRF attack prevention - URL injection', () => {
    it('rejects worker IDs containing http://', () => {
      const result = validateWorkerId('http://evil.com')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('blocked pattern')
    })

    it('rejects worker IDs containing https://', () => {
      const result = validateWorkerId('https://evil.com')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('blocked pattern')
    })

    it('rejects worker IDs with protocol-like patterns', () => {
      expect(validateWorkerId('ftp://server').valid).toBe(false)
      expect(validateWorkerId('file://local').valid).toBe(false)
      expect(validateWorkerId('data:text').valid).toBe(false)
      expect(validateWorkerId('javascript:alert').valid).toBe(false)
    })
  })

  describe('SSRF attack prevention - path traversal', () => {
    it('rejects worker IDs with path traversal patterns', () => {
      expect(validateWorkerId('../etc/passwd').valid).toBe(false)
      expect(validateWorkerId('..%2F..%2F').valid).toBe(false)
      expect(validateWorkerId('./local').valid).toBe(false)
      expect(validateWorkerId('a/../../b').valid).toBe(false)
    })
  })

  describe('SSRF attack prevention - URL components', () => {
    it('rejects worker IDs with query string markers', () => {
      expect(validateWorkerId('worker?param=value').valid).toBe(false)
    })

    it('rejects worker IDs with fragment markers', () => {
      expect(validateWorkerId('worker#section').valid).toBe(false)
    })

    it('rejects worker IDs with @ (credentials)', () => {
      expect(validateWorkerId('user@host').valid).toBe(false)
    })

    it('rejects worker IDs with port specifiers', () => {
      expect(validateWorkerId('host:8080').valid).toBe(false)
    })

    it('rejects worker IDs with slashes', () => {
      expect(validateWorkerId('path/to/something').valid).toBe(false)
      expect(validateWorkerId('path\\to\\something').valid).toBe(false)
    })

    it('rejects worker IDs with encoded characters', () => {
      expect(validateWorkerId('worker%20name').valid).toBe(false)
      expect(validateWorkerId('%2e%2e').valid).toBe(false)
    })
  })

  describe('SSRF attack prevention - special characters', () => {
    it('rejects worker IDs with null bytes', () => {
      expect(validateWorkerId('worker\x00name').valid).toBe(false)
      expect(validateWorkerId('worker\0name').valid).toBe(false)
    })

    it('rejects worker IDs with newlines (header injection)', () => {
      expect(validateWorkerId('worker\nheader').valid).toBe(false)
      expect(validateWorkerId('worker\r\nheader').valid).toBe(false)
    })
  })

  describe('SSRF attack prevention - internal network access', () => {
    it('rejects localhost patterns', () => {
      expect(validateWorkerId('localhost').valid).toBe(false)
      expect(validateWorkerId('127.0.0.1').valid).toBe(false)
      expect(validateWorkerId('0.0.0.0').valid).toBe(false)
      expect(validateWorkerId('[::1]').valid).toBe(false)
    })

    it('rejects cloud metadata endpoints', () => {
      expect(validateWorkerId('169.254.169.254').valid).toBe(false)
      expect(validateWorkerId('metadata.google.internal').valid).toBe(false)
    })

    it('rejects common internal hostnames', () => {
      expect(validateWorkerId('internal').valid).toBe(false)
      expect(validateWorkerId('internal-service').valid).toBe(false)
      expect(validateWorkerId('private').valid).toBe(false)
      expect(validateWorkerId('admin').valid).toBe(false)
      expect(validateWorkerId('kubernetes').valid).toBe(false)
      expect(validateWorkerId('k8s').valid).toBe(false)
    })
  })
})

// ============================================================================
// RPC Method Validation Tests
// ============================================================================

describe('validateRpcMethod', () => {
  describe('valid RPC methods', () => {
    it('accepts camelCase method names', () => {
      expect(validateRpcMethod('handleEvent')).toEqual({ valid: true })
      expect(validateRpcMethod('processWebhook')).toEqual({ valid: true })
      expect(validateRpcMethod('getUserById')).toEqual({ valid: true })
    })

    it('accepts PascalCase method names', () => {
      expect(validateRpcMethod('HandleEvent')).toEqual({ valid: true })
      expect(validateRpcMethod('ProcessWebhook')).toEqual({ valid: true })
    })

    it('accepts snake_case method names', () => {
      expect(validateRpcMethod('handle_event')).toEqual({ valid: true })
      expect(validateRpcMethod('process_webhook')).toEqual({ valid: true })
    })

    it('accepts method names with numbers', () => {
      expect(validateRpcMethod('handleEventV2')).toEqual({ valid: true })
      expect(validateRpcMethod('process2')).toEqual({ valid: true })
    })

    it('accepts single letter method names', () => {
      expect(validateRpcMethod('a')).toEqual({ valid: true })
      expect(validateRpcMethod('Z')).toEqual({ valid: true })
    })
  })

  describe('invalid RPC methods - type errors', () => {
    it('rejects non-string values', () => {
      expect(validateRpcMethod(123)).toEqual({
        valid: false,
        error: 'RPC method must be a string',
      })
      expect(validateRpcMethod(null)).toEqual({
        valid: false,
        error: 'RPC method must be a string',
      })
    })

    it('rejects empty string', () => {
      expect(validateRpcMethod('')).toEqual({
        valid: false,
        error: 'RPC method cannot be empty',
      })
    })
  })

  describe('invalid RPC methods - length errors', () => {
    it('rejects methods exceeding max length', () => {
      const tooLong = 'a'.repeat(129)
      expect(validateRpcMethod(tooLong)).toEqual({
        valid: false,
        error: `RPC method exceeds maximum length of ${MAX_RPC_METHOD_LENGTH} characters`,
      })
    })
  })

  describe('invalid RPC methods - format errors', () => {
    it('rejects methods starting with number', () => {
      expect(validateRpcMethod('123method').valid).toBe(false)
    })

    it('rejects methods starting with underscore', () => {
      expect(validateRpcMethod('_method').valid).toBe(false)
    })

    it('rejects methods with hyphens', () => {
      expect(validateRpcMethod('handle-event').valid).toBe(false)
    })

    it('rejects methods with dots', () => {
      expect(validateRpcMethod('handle.event').valid).toBe(false)
    })
  })

  describe('SSRF attack prevention in RPC methods', () => {
    it('rejects path traversal patterns', () => {
      expect(validateRpcMethod('../etc/passwd').valid).toBe(false)
      expect(validateRpcMethod('..%2Fetc').valid).toBe(false)
    })

    it('rejects URL components', () => {
      expect(validateRpcMethod('method?param=value').valid).toBe(false)
      expect(validateRpcMethod('method#section').valid).toBe(false)
      expect(validateRpcMethod('path/method').valid).toBe(false)
    })

    it('rejects special characters', () => {
      expect(validateRpcMethod('method\nheader').valid).toBe(false)
      expect(validateRpcMethod('method\x00null').valid).toBe(false)
    })
  })
})

// ============================================================================
// assertValidDeliveryTarget Tests
// ============================================================================

describe('assertValidDeliveryTarget', () => {
  it('does not throw for valid inputs', () => {
    expect(() => assertValidDeliveryTarget('my-worker', 'handleEvent')).not.toThrow()
    expect(() => assertValidDeliveryTarget('api-v2', 'processWebhook')).not.toThrow()
  })

  it('throws for invalid worker ID', () => {
    expect(() => assertValidDeliveryTarget('http://evil.com', 'handleEvent')).toThrow(
      'Invalid worker ID'
    )
    expect(() => assertValidDeliveryTarget('../etc', 'handleEvent')).toThrow(
      'Invalid worker ID'
    )
    expect(() => assertValidDeliveryTarget('localhost', 'handleEvent')).toThrow(
      'Invalid worker ID'
    )
  })

  it('throws for invalid RPC method', () => {
    expect(() => assertValidDeliveryTarget('my-worker', '../etc/passwd')).toThrow(
      'Invalid RPC method'
    )
    expect(() => assertValidDeliveryTarget('my-worker', 'method?param')).toThrow(
      'Invalid RPC method'
    )
  })

  it('error message contains reason', () => {
    try {
      assertValidDeliveryTarget('localhost', 'handleEvent')
    } catch (e) {
      expect((e as Error).message).toContain('localhost')
    }
  })
})

// ============================================================================
// buildSafeDeliveryUrl Tests
// ============================================================================

describe('buildSafeDeliveryUrl', () => {
  it('builds correct URL for valid inputs', () => {
    expect(buildSafeDeliveryUrl('my-worker', 'handleEvent')).toBe(
      'https://my-worker.workers.dev/rpc/handleEvent'
    )
    expect(buildSafeDeliveryUrl('github-webhook-handler', 'processWebhook')).toBe(
      'https://github-webhook-handler.workers.dev/rpc/processWebhook'
    )
    expect(buildSafeDeliveryUrl('api', 'get')).toBe(
      'https://api.workers.dev/rpc/get'
    )
  })

  it('throws for invalid worker ID', () => {
    expect(() => buildSafeDeliveryUrl('http://evil.com', 'method')).toThrow()
    expect(() => buildSafeDeliveryUrl('localhost', 'method')).toThrow()
    expect(() => buildSafeDeliveryUrl('../etc', 'method')).toThrow()
  })

  it('throws for invalid RPC method', () => {
    expect(() => buildSafeDeliveryUrl('my-worker', '../etc')).toThrow()
    expect(() => buildSafeDeliveryUrl('my-worker', 'method?x=1')).toThrow()
  })

  it('prevents URL manipulation attacks', () => {
    // Attempt to escape to different host
    expect(() => buildSafeDeliveryUrl('evil.com/path', 'method')).toThrow()

    // Attempt credential injection
    expect(() => buildSafeDeliveryUrl('user:pass@evil.com', 'method')).toThrow()

    // Attempt port injection
    expect(() => buildSafeDeliveryUrl('evil.com:8080', 'method')).toThrow()
  })
})

// ============================================================================
// Integration Tests - Real-world Attack Scenarios
// ============================================================================

describe('SSRF Attack Scenarios', () => {
  describe('Cloud metadata service access attempts', () => {
    it('blocks AWS metadata endpoint', () => {
      expect(validateWorkerId('169.254.169.254').valid).toBe(false)
    })

    it('blocks GCP metadata endpoint', () => {
      expect(validateWorkerId('metadata.google.internal').valid).toBe(false)
    })

    it('blocks attempts to reach metadata via encoding', () => {
      expect(validateWorkerId('169%2e254%2e169%2e254').valid).toBe(false)
    })
  })

  describe('Internal network scanning attempts', () => {
    it('blocks RFC 1918 private addresses', () => {
      // These would fail pattern validation anyway, but blocklist catches them first
      expect(validateWorkerId('10.0.0.1').valid).toBe(false)
      expect(validateWorkerId('192.168.1.1').valid).toBe(false)
      expect(validateWorkerId('172.16.0.1').valid).toBe(false)
    })

    it('blocks Kubernetes service discovery', () => {
      expect(validateWorkerId('kubernetes').valid).toBe(false)
      expect(validateWorkerId('k8s').valid).toBe(false)
    })
  })

  describe('URL manipulation attempts', () => {
    it('blocks host header injection', () => {
      expect(validateWorkerId('evil.com\r\nHost: internal').valid).toBe(false)
    })

    it('blocks path injection via double encoding', () => {
      expect(validateWorkerId('%252e%252e%252f').valid).toBe(false)
    })
  })
})

// ============================================================================
// Blocklist Coverage Tests
// ============================================================================

describe('SSRF_BLOCKLIST_PATTERNS coverage', () => {
  it('contains all critical patterns', () => {
    // Protocol schemes
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('http://')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('https://')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('file://')

    // Path traversal
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('..')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('./')

    // URL components
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('?')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('#')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('@')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain(':')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('/')

    // Encoding
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('%')

    // Special chars
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('\n')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('\r')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('\x00')

    // Internal hosts
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('localhost')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('127.0.0.1')
    expect(SSRF_BLOCKLIST_PATTERNS).toContain('169.254.169.254')
  })
})
