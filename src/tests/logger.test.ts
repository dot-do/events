/**
 * Logger Tests
 *
 * Unit tests for the structured logging utility with correlation IDs.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  createLogger,
  createRequestLogger,
  generateRequestId,
  getRequestId,
  generateCorrelationId,
  getOrCreateCorrelationId,
  extractRequestContext,
  correlationIdHeaders,
  withCorrelationId,
  createLoggerFromRequest,
  logError,
  sanitize,
  type LogLevel,
  type LogContext,
  type RequestContext,
} from '../logger'

// ============================================================================
// Mock Console
// ============================================================================

describe('Logger', () => {
  let consoleSpy: {
    debug: ReturnType<typeof vi.spyOn>
    info: ReturnType<typeof vi.spyOn>
    warn: ReturnType<typeof vi.spyOn>
    error: ReturnType<typeof vi.spyOn>
  }

  beforeEach(() => {
    consoleSpy = {
      debug: vi.spyOn(console, 'debug').mockImplementation(() => {}),
      info: vi.spyOn(console, 'info').mockImplementation(() => {}),
      warn: vi.spyOn(console, 'warn').mockImplementation(() => {}),
      error: vi.spyOn(console, 'error').mockImplementation(() => {}),
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // ============================================================================
  // Basic Logger Tests
  // ============================================================================

  describe('createLogger', () => {
    it('creates a logger with default context', () => {
      const log = createLogger()
      log.info('Test message')

      expect(consoleSpy.info).toHaveBeenCalledTimes(1)
      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.level).toBe('info')
      expect(output.message).toBe('Test message')
      expect(output.timestamp).toBeDefined()
    })

    it('includes base context in all log entries', () => {
      const log = createLogger({ component: 'test', requestId: 'abc123' })
      log.info('Test message')

      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.context.component).toBe('test')
      expect(output.context.requestId).toBe('abc123')
    })

    it('merges call-specific context with base context', () => {
      const log = createLogger({ component: 'test' })
      log.info('Test message', { userId: 'user1' })

      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.context.component).toBe('test')
      expect(output.context.userId).toBe('user1')
    })

    it('extracts correlationId to top level', () => {
      const log = createLogger({ correlationId: 'corr-123', component: 'test' })
      log.info('Test message')

      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.correlationId).toBe('corr-123')
      expect(output.context.component).toBe('test')
      // correlationId should not be in context
      expect(output.context.correlationId).toBeUndefined()
    })

    it('supports all log levels', () => {
      const log = createLogger({}, 'debug')

      log.debug('Debug message')
      log.info('Info message')
      log.warn('Warn message')
      log.error('Error message')

      expect(consoleSpy.debug).toHaveBeenCalledTimes(1)
      expect(consoleSpy.info).toHaveBeenCalledTimes(1)
      expect(consoleSpy.warn).toHaveBeenCalledTimes(1)
      expect(consoleSpy.error).toHaveBeenCalledTimes(1)
    })

    it('respects minimum log level', () => {
      const log = createLogger({}, 'warn')

      log.debug('Debug message')
      log.info('Info message')
      log.warn('Warn message')
      log.error('Error message')

      expect(consoleSpy.debug).not.toHaveBeenCalled()
      expect(consoleSpy.info).not.toHaveBeenCalled()
      expect(consoleSpy.warn).toHaveBeenCalledTimes(1)
      expect(consoleSpy.error).toHaveBeenCalledTimes(1)
    })

    it('creates child logger with merged context', () => {
      const parentLog = createLogger({ correlationId: 'corr-123' })
      const childLog = parentLog.child({ component: 'child', namespace: 'ns1' })

      childLog.info('Child message')

      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.correlationId).toBe('corr-123')
      expect(output.context.component).toBe('child')
      expect(output.context.namespace).toBe('ns1')
    })
  })

  // ============================================================================
  // Correlation ID Tests
  // ============================================================================

  describe('generateCorrelationId', () => {
    it('generates unique IDs', () => {
      const id1 = generateCorrelationId()
      const id2 = generateCorrelationId()

      expect(id1).not.toBe(id2)
    })

    it('generates IDs with timestamp prefix', () => {
      const id = generateCorrelationId()
      // Format: {timestamp_base36}-{uuid_prefix}
      expect(id).toMatch(/^[a-z0-9]+-[a-z0-9]+$/)
    })

    it('generates IDs of reasonable length', () => {
      const id = generateCorrelationId()
      expect(id.length).toBeGreaterThan(10)
      expect(id.length).toBeLessThan(30)
    })
  })

  describe('getOrCreateCorrelationId', () => {
    it('extracts x-correlation-id header', () => {
      const request = new Request('https://example.com', {
        headers: { 'x-correlation-id': 'existing-corr-id-123' },
      })

      const id = getOrCreateCorrelationId(request)
      expect(id).toBe('existing-corr-id-123')
    })

    it('extracts x-request-id header as fallback', () => {
      const request = new Request('https://example.com', {
        headers: { 'x-request-id': 'request-id-456' },
      })

      const id = getOrCreateCorrelationId(request)
      expect(id).toBe('request-id-456')
    })

    it('extracts trace-id from traceparent header', () => {
      const request = new Request('https://example.com', {
        headers: { 'traceparent': '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' },
      })

      const id = getOrCreateCorrelationId(request)
      // Should extract first 16 chars of trace-id
      expect(id).toBe('0af7651916cd43dd')
    })

    it('sanitizes invalid characters from header', () => {
      const request = new Request('https://example.com', {
        headers: { 'x-correlation-id': 'corr<script>alert(1)</script>id' },
      })

      const id = getOrCreateCorrelationId(request)
      expect(id).toBe('corrscriptalert1scriptid')
    })

    it('generates new ID when header is too short', () => {
      const request = new Request('https://example.com', {
        headers: { 'x-correlation-id': 'short' },
      })

      const id = getOrCreateCorrelationId(request)
      // Should generate new ID since 'short' is less than 8 chars
      expect(id).not.toBe('short')
      expect(id.length).toBeGreaterThan(8)
    })

    it('generates new ID when no header present', () => {
      const request = new Request('https://example.com')

      const id = getOrCreateCorrelationId(request)
      expect(id.length).toBeGreaterThan(8)
    })
  })

  // ============================================================================
  // Request Context Tests
  // ============================================================================

  describe('extractRequestContext', () => {
    it('extracts basic request information', () => {
      const request = new Request('https://example.com/api/test?foo=bar', {
        method: 'POST',
      })

      const ctx = extractRequestContext(request)

      expect(ctx.method).toBe('POST')
      expect(ctx.path).toBe('/api/test')
      expect(ctx.query).toBe('foo=bar')
      expect(ctx.correlationId).toBeDefined()
      expect(ctx.requestId).toBeDefined()
    })

    it('extracts user agent (truncated)', () => {
      const longUserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 ' + 'x'.repeat(100)
      const request = new Request('https://example.com', {
        headers: { 'user-agent': longUserAgent },
      })

      const ctx = extractRequestContext(request)

      expect(ctx.userAgent).toBeDefined()
      expect(ctx.userAgent!.length).toBeLessThanOrEqual(100)
    })

    it('extracts Cloudflare headers', () => {
      const request = new Request('https://example.com', {
        headers: {
          'cf-ray': '1234567890abcdef-SJC',
          'cf-connecting-ip': '192.168.1.1',
          'cf-ipcolo': 'SJC',
        },
      })

      const ctx = extractRequestContext(request)

      expect(ctx.rayId).toBe('1234567890abcdef')
      expect(ctx.clientIp).toBe('192.168.1.1')
      expect(ctx.colo).toBe('SJC')
    })

    it('uses correlation ID from headers when available', () => {
      const request = new Request('https://example.com', {
        headers: { 'x-correlation-id': 'my-correlation-id' },
      })

      const ctx = extractRequestContext(request)

      expect(ctx.correlationId).toBe('my-correlation-id')
    })
  })

  // ============================================================================
  // Response Header Tests
  // ============================================================================

  describe('correlationIdHeaders', () => {
    it('returns headers with correlation ID', () => {
      const headers = correlationIdHeaders('test-corr-id')

      expect(headers).toEqual({ 'x-correlation-id': 'test-corr-id' })
    })
  })

  describe('withCorrelationId', () => {
    it('merges correlation ID with other headers', () => {
      const headers = withCorrelationId('test-corr-id', {
        'content-type': 'application/json',
        'cache-control': 'no-cache',
      })

      expect(headers).toEqual({
        'content-type': 'application/json',
        'cache-control': 'no-cache',
        'x-correlation-id': 'test-corr-id',
      })
    })

    it('works with empty additional headers', () => {
      const headers = withCorrelationId('test-corr-id')

      expect(headers).toEqual({ 'x-correlation-id': 'test-corr-id' })
    })
  })

  // ============================================================================
  // createLoggerFromRequest Tests
  // ============================================================================

  describe('createLoggerFromRequest', () => {
    it('creates logger with request context', () => {
      const request = new Request('https://example.com/api/test', {
        method: 'POST',
        headers: { 'x-correlation-id': 'req-corr-123' },
      })

      const { logger, correlationId, requestContext } = createLoggerFromRequest(request)

      expect(correlationId).toBe('req-corr-123')
      expect(requestContext.method).toBe('POST')
      expect(requestContext.path).toBe('/api/test')

      logger.info('Test message')
      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.correlationId).toBe('req-corr-123')
    })

    it('includes additional context', () => {
      const request = new Request('https://example.com')

      const { logger } = createLoggerFromRequest(request, { namespace: 'test-ns' })

      logger.info('Test message')
      const output = JSON.parse(consoleSpy.info.mock.calls[0][0] as string)
      expect(output.context.namespace).toBe('test-ns')
    })
  })

  // ============================================================================
  // Legacy Function Tests
  // ============================================================================

  describe('generateRequestId', () => {
    it('generates unique request IDs', () => {
      const id1 = generateRequestId()
      const id2 = generateRequestId()

      expect(id1).not.toBe(id2)
    })

    it('generates short IDs (8 chars)', () => {
      const id = generateRequestId()
      expect(id.length).toBe(8)
    })
  })

  describe('getRequestId', () => {
    it('extracts x-request-id header', () => {
      const request = new Request('https://example.com', {
        headers: { 'x-request-id': 'my-request-id' },
      })

      const id = getRequestId(request)
      expect(id).toBe('my-request-id')
    })

    it('extracts cf-ray header', () => {
      const request = new Request('https://example.com', {
        headers: { 'cf-ray': '1234567890abcdef-SJC' },
      })

      const id = getRequestId(request)
      expect(id).toBe('1234567890abcdef')
    })

    it('generates ID when no header present', () => {
      const request = new Request('https://example.com')

      const id = getRequestId(request)
      expect(id.length).toBe(8)
    })
  })

  // ============================================================================
  // Error Logging Tests
  // ============================================================================

  describe('logError', () => {
    it('logs error with name and message', () => {
      const log = createLogger({ correlationId: 'err-test' })
      const error = new Error('Test error message')

      logError(log, 'Operation failed', error)

      const output = JSON.parse(consoleSpy.error.mock.calls[0][0] as string)
      expect(output.message).toBe('Operation failed')
      expect(output.correlationId).toBe('err-test')
      expect(output.context.error.name).toBe('Error')
      expect(output.context.error.message).toContain('Test error')
    })

    it('logs non-Error values as strings', () => {
      const log = createLogger()

      logError(log, 'Operation failed', 'string error')

      const output = JSON.parse(consoleSpy.error.mock.calls[0][0] as string)
      expect(output.context.error).toContain('string error')
    })

    it('includes additional context', () => {
      const log = createLogger()
      const error = new Error('Test error')

      logError(log, 'Operation failed', error, { userId: 'user123', action: 'delete' })

      const output = JSON.parse(consoleSpy.error.mock.calls[0][0] as string)
      expect(output.context.userId).toBe('user123')
      expect(output.context.action).toBe('delete')
    })
  })

  // ============================================================================
  // Sanitization Tests
  // ============================================================================

  describe('sanitize', () => {
    describe('token', () => {
      it('redacts middle of token', () => {
        expect(sanitize.token('sk_live_abc123def456')).toBe('sk_l***f456')
      })

      it('handles short tokens', () => {
        expect(sanitize.token('short')).toBe('***')
      })

      it('handles null/undefined', () => {
        expect(sanitize.token(null)).toBe('[empty]')
        expect(sanitize.token(undefined)).toBe('[empty]')
      })
    })

    describe('bearer', () => {
      it('redacts Bearer token', () => {
        expect(sanitize.bearer('Bearer abc123')).toBe('Bearer ***')
      })

      it('falls back to token sanitization for non-Bearer', () => {
        expect(sanitize.bearer('sk_live_abc123def456')).toBe('sk_l***f456')
      })
    })

    describe('email', () => {
      it('redacts email local part', () => {
        expect(sanitize.email('user@example.com')).toBe('u***@example.com')
      })

      it('handles short local parts', () => {
        expect(sanitize.email('a@example.com')).toBe('*@example.com')
      })
    })

    describe('url', () => {
      it('removes query parameters', () => {
        expect(sanitize.url('https://api.example.com/path?token=secret')).toBe('https://api.example.com/path')
      })

      it('removes hash fragments', () => {
        expect(sanitize.url('https://example.com/path#secret')).toBe('https://example.com/path')
      })
    })

    describe('errorMessage', () => {
      it('redacts tokens in error messages', () => {
        const result = sanitize.errorMessage('Failed with token=abc123def456')
        expect(result).toBe('Failed with token=[redacted]')
      })

      it('redacts JWT-like patterns', () => {
        const jwt = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c'
        const result = sanitize.errorMessage(`Token validation failed: ${jwt}`)
        expect(result).toContain('[jwt-redacted]')
        expect(result).not.toContain('eyJ')
      })
    })

    describe('payload', () => {
      it('redacts sensitive keys', () => {
        const obj = {
          username: 'john',
          password: 'secret123',
          apiKey: 'key_abc123',
        }

        const result = sanitize.payload(obj) as Record<string, unknown>
        expect(result.username).toBe('john')
        expect(result.password).toBe('secr***t123')
        expect(result.apiKey).toBe('key_***c123')
      })

      it('handles nested objects', () => {
        const obj = {
          user: {
            name: 'john',
            secretKey: 'my_secret_key_value',
          },
        }

        const result = sanitize.payload(obj) as Record<string, unknown>
        const user = result.user as Record<string, unknown>
        expect(user.name).toBe('john')
        // secretKey should be redacted because it matches the 'secret' pattern
        expect(user.secretKey).toBe('my_s***alue')
      })

      it('truncates large arrays', () => {
        const obj = {
          items: Array(20).fill('item'),
        }

        const result = sanitize.payload(obj) as Record<string, unknown>
        const items = result.items as unknown[]
        expect(items.length).toBe(11) // 10 items + "...(10 more)"
        expect(items[10]).toBe('...(10 more)')
      })
    })
  })
})

// ============================================================================
// Request Logger Tests
// ============================================================================

describe('createRequestLogger', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'info').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('creates logger with request context', () => {
    const log = createRequestLogger({
      requestId: 'req-123',
      namespace: 'test-ns',
      correlationId: 'corr-456',
    })

    log.info('Test message')

    const output = JSON.parse(consoleSpy.mock.calls[0][0] as string)
    expect(output.correlationId).toBe('corr-456')
    expect(output.context.requestId).toBe('req-123')
    expect(output.context.namespace).toBe('test-ns')
  })
})
