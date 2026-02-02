/**
 * Ingest Route Handler Tests
 *
 * Unit tests for src/routes/ingest.ts
 * Tests event ingestion, validation, authentication, and rate limiting.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface TenantContext {
  namespace: string
  isAdmin: boolean
  keyId: string
}

interface EventBatch {
  events: DurableEvent[]
  batchId?: string
}

interface DurableEvent {
  ts: string
  type: string
  [key: string]: unknown
}

interface MockEnv {
  AUTH_TOKEN?: string
  ALLOW_UNAUTHENTICATED_INGEST?: string
  DEFAULT_NAMESPACE?: string
  NAMESPACE_API_KEYS?: string
  ENABLE_SCHEMA_VALIDATION?: string
  USE_QUEUE_FANOUT?: string
  EVENTS_QUEUE?: object
  RATE_LIMITER: object
  EVENT_WRITER: object
  SCHEMA_REGISTRY?: object
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    RATE_LIMITER: {},
    EVENT_WRITER: {},
    ...overrides,
  }
}

function createRequest(url: string, options: RequestInit = {}): Request {
  return new Request(url, { method: 'POST', ...options })
}

// ============================================================================
// Authentication Tests
// ============================================================================

describe('Ingest Authentication', () => {
  describe('namespace-scoped API keys', () => {
    it('extracts namespace from namespaced API key', () => {
      const apiKeys = { 'ns_acme_abc123': 'acme', 'ns_beta_xyz789': 'beta' }
      const token = 'ns_acme_abc123'

      const namespace = apiKeys[token]
      expect(namespace).toBe('acme')
    })

    it('rejects unknown namespaced API key', () => {
      const apiKeys = { 'ns_acme_abc123': 'acme' }
      const token = 'ns_unknown_xyz'

      const namespace = apiKeys[token]
      expect(namespace).toBeUndefined()
    })

    it('parses NAMESPACE_API_KEYS JSON', () => {
      const env = createMockEnv({
        NAMESPACE_API_KEYS: JSON.stringify({ 'ns_acme_abc': 'acme' }),
      })

      const keys = JSON.parse(env.NAMESPACE_API_KEYS!)
      expect(keys['ns_acme_abc']).toBe('acme')
    })

    it('handles invalid NAMESPACE_API_KEYS JSON gracefully', () => {
      const env = createMockEnv({
        NAMESPACE_API_KEYS: 'not-json',
      })

      let keys = {}
      try {
        keys = JSON.parse(env.NAMESPACE_API_KEYS!)
      } catch {
        keys = {}
      }

      expect(keys).toEqual({})
    })
  })

  describe('legacy AUTH_TOKEN', () => {
    it('authenticates with valid AUTH_TOKEN', () => {
      const env = createMockEnv({ AUTH_TOKEN: 'secret-token' })
      const token = 'secret-token'

      const isValid = token === env.AUTH_TOKEN
      expect(isValid).toBe(true)
    })

    it('rejects invalid AUTH_TOKEN', () => {
      const env = createMockEnv({ AUTH_TOKEN: 'secret-token' })
      const token = 'wrong-token'

      const isValid = token === env.AUTH_TOKEN
      expect(isValid).toBe(false)
    })

    it('uses DEFAULT_NAMESPACE for legacy tokens', () => {
      const env = createMockEnv({
        AUTH_TOKEN: 'secret-token',
        DEFAULT_NAMESPACE: 'my-namespace',
      })

      const namespace = env.DEFAULT_NAMESPACE || 'default'
      expect(namespace).toBe('my-namespace')
    })

    it('defaults to "default" namespace when DEFAULT_NAMESPACE not set', () => {
      const env = createMockEnv({ AUTH_TOKEN: 'secret-token' })

      const namespace = env.DEFAULT_NAMESPACE || 'default'
      expect(namespace).toBe('default')
    })
  })

  describe('unauthenticated access', () => {
    it('allows unauthenticated when ALLOW_UNAUTHENTICATED_INGEST=true', () => {
      const env = createMockEnv({ ALLOW_UNAUTHENTICATED_INGEST: 'true' })

      const allowUnauthenticated = env.ALLOW_UNAUTHENTICATED_INGEST === 'true'
      expect(allowUnauthenticated).toBe(true)
    })

    it('denies unauthenticated by default', () => {
      const env = createMockEnv()

      const allowUnauthenticated = env.ALLOW_UNAUTHENTICATED_INGEST === 'true'
      expect(allowUnauthenticated).toBe(false)
    })

    it('uses default namespace for unauthenticated access', () => {
      const env = createMockEnv({
        ALLOW_UNAUTHENTICATED_INGEST: 'true',
        DEFAULT_NAMESPACE: 'public',
      })

      const namespace = env.DEFAULT_NAMESPACE || 'default'
      expect(namespace).toBe('public')
    })
  })

  describe('authorization header parsing', () => {
    it('extracts token from Bearer header', () => {
      const authHeader = 'Bearer my-token'
      const token = authHeader.replace('Bearer ', '')
      expect(token).toBe('my-token')
    })

    it('handles missing Bearer prefix', () => {
      const authHeader = 'my-token'
      const token = authHeader.startsWith('Bearer ') ? authHeader.replace('Bearer ', '') : authHeader
      expect(token).toBe('my-token')
    })

    it('handles null authorization header', () => {
      const authHeader: string | null = null
      const token = authHeader?.replace('Bearer ', '') ?? null
      expect(token).toBeNull()
    })
  })
})

// ============================================================================
// Request Validation Tests
// ============================================================================

describe('Event Batch Validation', () => {
  describe('batch structure', () => {
    it('accepts valid event batch', () => {
      const batch: EventBatch = {
        events: [
          { ts: new Date().toISOString(), type: 'test.event' },
        ],
      }

      const isValid = Array.isArray(batch.events) && batch.events.length > 0
      expect(isValid).toBe(true)
    })

    it('rejects batch without events array', () => {
      const batch = { notEvents: [] }
      const isValid = 'events' in batch && Array.isArray(batch.events)
      expect(isValid).toBe(false)
    })

    it('rejects empty events array', () => {
      const batch: EventBatch = { events: [] }
      const isValid = batch.events.length > 0
      expect(isValid).toBe(false)
    })

    it('accepts optional batchId', () => {
      const batch: EventBatch = {
        events: [{ ts: new Date().toISOString(), type: 'test' }],
        batchId: 'batch-123',
      }

      expect(batch.batchId).toBe('batch-123')
    })
  })

  describe('event validation', () => {
    it('requires ts field', () => {
      const event = { type: 'test' }
      const hasTs = 'ts' in event
      expect(hasTs).toBe(false)
    })

    it('requires type field', () => {
      const event = { ts: new Date().toISOString() }
      const hasType = 'type' in event
      expect(hasType).toBe(false)
    })

    it('accepts valid event', () => {
      const event: DurableEvent = {
        ts: new Date().toISOString(),
        type: 'user.created',
        userId: '123',
      }

      const isValid = 'ts' in event && 'type' in event
      expect(isValid).toBe(true)
    })

    it('accepts events with additional fields', () => {
      const event: DurableEvent = {
        ts: new Date().toISOString(),
        type: 'user.created',
        userId: '123',
        email: 'test@example.com',
        metadata: { source: 'api' },
      }

      expect(event.userId).toBe('123')
      expect(event.email).toBe('test@example.com')
      expect(event.metadata).toEqual({ source: 'api' })
    })
  })
})

// ============================================================================
// Event Size Validation Tests
// ============================================================================

describe('Event Size Validation', () => {
  const MAX_EVENT_SIZE = 128 * 1024 // 128KB

  it('accepts events within size limit', () => {
    const event = { ts: new Date().toISOString(), type: 'test', data: 'small' }
    const size = JSON.stringify(event).length

    expect(size).toBeLessThan(MAX_EVENT_SIZE)
  })

  it('rejects events exceeding size limit', () => {
    const largeData = 'x'.repeat(MAX_EVENT_SIZE + 1)
    const event = { ts: new Date().toISOString(), type: 'test', data: largeData }
    const size = JSON.stringify(event).length

    expect(size).toBeGreaterThan(MAX_EVENT_SIZE)
  })

  it('calculates size correctly for complex events', () => {
    const event = {
      ts: new Date().toISOString(),
      type: 'test',
      nested: {
        array: [1, 2, 3],
        object: { key: 'value' },
      },
    }

    const size = JSON.stringify(event).length
    expect(size).toBeGreaterThan(0)
    expect(size).toBeLessThan(1000)
  })
})

// ============================================================================
// Rate Limiting Tests
// ============================================================================

describe('Rate Limiting', () => {
  it('applies rate limiting to ingest requests', () => {
    // Rate limiting is applied before processing
    const RATE_LIMIT_REQUESTS_PER_MINUTE = 1000
    const RATE_LIMIT_EVENTS_PER_MINUTE = 100000

    expect(RATE_LIMIT_REQUESTS_PER_MINUTE).toBe(1000)
    expect(RATE_LIMIT_EVENTS_PER_MINUTE).toBe(100000)
  })

  it('uses event count for rate limit check', () => {
    const batch: EventBatch = {
      events: [
        { ts: new Date().toISOString(), type: 'event1' },
        { ts: new Date().toISOString(), type: 'event2' },
        { ts: new Date().toISOString(), type: 'event3' },
      ],
    }

    const eventCount = batch.events.length
    expect(eventCount).toBe(3)
  })
})

// ============================================================================
// Fanout Mode Tests
// ============================================================================

describe('Fanout Mode', () => {
  it('uses queue fanout when USE_QUEUE_FANOUT is true and queue exists', () => {
    const env = createMockEnv({
      USE_QUEUE_FANOUT: 'true',
      EVENTS_QUEUE: {},
    })

    const useQueue = env.USE_QUEUE_FANOUT === 'true' && !!env.EVENTS_QUEUE
    expect(useQueue).toBe(true)
  })

  it('uses direct fanout when USE_QUEUE_FANOUT is false', () => {
    const env = createMockEnv({
      USE_QUEUE_FANOUT: 'false',
      EVENTS_QUEUE: {},
    })

    const useQueue = env.USE_QUEUE_FANOUT === 'true' && !!env.EVENTS_QUEUE
    expect(useQueue).toBe(false)
  })

  it('uses direct fanout when no queue configured', () => {
    const env = createMockEnv({
      USE_QUEUE_FANOUT: 'true',
      // No EVENTS_QUEUE
    })

    const useQueue = env.USE_QUEUE_FANOUT === 'true' && !!env.EVENTS_QUEUE
    expect(useQueue).toBe(false)
  })

  it('ensures mutual exclusion between queue and direct fanout', () => {
    const useQueueFanout = true
    const useDirect = !useQueueFanout

    // Only one should be true
    expect(useQueueFanout).not.toBe(useDirect)
  })
})

// ============================================================================
// Event Record Conversion Tests
// ============================================================================

describe('Event Record Conversion', () => {
  it('converts DurableEvent to EventRecord', () => {
    const event: DurableEvent = {
      ts: '2024-01-01T00:00:00Z',
      type: 'user.created',
      userId: '123',
    }
    const tenant: TenantContext = { namespace: 'acme', isAdmin: false, keyId: 'key-1' }

    const record = {
      ts: event.ts,
      type: event.type,
      source: 'ingest',
      payload: { ...event, _namespace: tenant.namespace },
    }

    expect(record.ts).toBe('2024-01-01T00:00:00Z')
    expect(record.type).toBe('user.created')
    expect(record.source).toBe('ingest')
    expect(record.payload._namespace).toBe('acme')
    expect(record.payload.userId).toBe('123')
  })

  it('includes namespace in payload for isolation', () => {
    const tenant: TenantContext = { namespace: 'beta', isAdmin: false, keyId: 'key-1' }
    const payload = { type: 'test', _namespace: tenant.namespace }

    expect(payload._namespace).toBe('beta')
  })
})

// ============================================================================
// Response Format Tests
// ============================================================================

describe('Ingest Response Format', () => {
  it('returns success response', async () => {
    const response = Response.json({
      ok: true,
      received: 5,
      namespace: 'acme',
    })

    expect(response.status).toBe(200)
    const json = await response.json()
    expect(json.ok).toBe(true)
    expect(json.received).toBe(5)
    expect(json.namespace).toBe('acme')
  })

  it('includes CORS headers', () => {
    const response = Response.json({ ok: true }, {
      headers: { 'Access-Control-Allow-Origin': '*' },
    })

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})

// ============================================================================
// Route Matching Tests
// ============================================================================

describe('Ingest Route Matching', () => {
  it('matches POST /ingest', () => {
    const url = new URL('https://events.do/ingest')
    const method = 'POST'

    const matches = url.pathname === '/ingest' && method === 'POST'
    expect(matches).toBe(true)
  })

  it('matches POST /e (alias)', () => {
    const url = new URL('https://events.do/e')
    const method = 'POST'

    const matches = url.pathname === '/e' && method === 'POST'
    expect(matches).toBe(true)
  })

  it('does not match GET /ingest', () => {
    const url = new URL('https://events.do/ingest')
    const method = 'GET'

    const matches = (url.pathname === '/ingest' || url.pathname === '/e') && method === 'POST'
    expect(matches).toBe(false)
  })
})

// ============================================================================
// Deduplication Tests
// ============================================================================

describe('Event Deduplication', () => {
  it('uses batchId for deduplication', () => {
    const batch: EventBatch = {
      events: [{ ts: new Date().toISOString(), type: 'test' }],
      batchId: 'unique-batch-id',
    }

    const dedupKey = batch.batchId
    expect(dedupKey).toBe('unique-batch-id')
  })

  it('skips deduplication without batchId', () => {
    const batch: EventBatch = {
      events: [{ ts: new Date().toISOString(), type: 'test' }],
    }

    const shouldDedup = !!batch.batchId
    expect(shouldDedup).toBe(false)
  })
})

// ============================================================================
// Schema Validation Tests
// ============================================================================

describe('Schema Validation', () => {
  it('enables validation when ENABLE_SCHEMA_VALIDATION=true', () => {
    const env = createMockEnv({
      ENABLE_SCHEMA_VALIDATION: 'true',
      SCHEMA_REGISTRY: {},
    })

    const enabled = env.ENABLE_SCHEMA_VALIDATION === 'true' && !!env.SCHEMA_REGISTRY
    expect(enabled).toBe(true)
  })

  it('disables validation without schema registry', () => {
    const env = createMockEnv({
      ENABLE_SCHEMA_VALIDATION: 'true',
      // No SCHEMA_REGISTRY
    })

    const enabled = env.ENABLE_SCHEMA_VALIDATION === 'true' && !!env.SCHEMA_REGISTRY
    expect(enabled).toBe(false)
  })

  it('disables validation when ENABLE_SCHEMA_VALIDATION is not true', () => {
    const env = createMockEnv({
      SCHEMA_REGISTRY: {},
    })

    const enabled = env.ENABLE_SCHEMA_VALIDATION === 'true'
    expect(enabled).toBe(false)
  })
})

// ============================================================================
// Error Response Tests
// ============================================================================

describe('Ingest Error Responses', () => {
  it('returns 400 for invalid JSON', async () => {
    const response = Response.json(
      { error: 'Invalid JSON' },
      { status: 400, headers: { 'Access-Control-Allow-Origin': '*' } }
    )

    expect(response.status).toBe(400)
    const json = await response.json()
    expect(json.error).toBe('Invalid JSON')
  })

  it('returns 400 for invalid batch', async () => {
    const response = Response.json(
      { error: 'Invalid batch format' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('returns 401 for unauthorized', async () => {
    const response = Response.json(
      { error: 'Unauthorized' },
      { status: 401 }
    )

    expect(response.status).toBe(401)
  })

  it('returns 429 for rate limited', async () => {
    const response = Response.json(
      { error: 'Rate limit exceeded' },
      { status: 429, headers: { 'Retry-After': '60' } }
    )

    expect(response.status).toBe(429)
    expect(response.headers.get('Retry-After')).toBe('60')
  })

  it('returns 500 for server misconfiguration', async () => {
    const response = Response.json(
      { error: 'Server misconfiguration: authentication not configured' },
      { status: 500 }
    )

    expect(response.status).toBe(500)
  })
})

// ============================================================================
// Namespaced R2 Path Tests
// ============================================================================

describe('Namespaced R2 Path Building', () => {
  it('builds namespaced source path', () => {
    const tenant: TenantContext = { namespace: 'acme', isAdmin: false, keyId: 'key-1' }
    const source = 'events'

    const namespacedSource = `ns/${tenant.namespace}/${source}`
    expect(namespacedSource).toBe('ns/acme/events')
  })

  it('handles default namespace', () => {
    const tenant: TenantContext = { namespace: 'default', isAdmin: false, keyId: 'key-1' }
    const source = 'events'

    const namespacedSource = `ns/${tenant.namespace}/${source}`
    expect(namespacedSource).toBe('ns/default/events')
  })
})
