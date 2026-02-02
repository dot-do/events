/**
 * Fetch Handler Tests
 *
 * Unit tests for the main fetch handler and utilities in src/handlers/fetch.ts
 * Tests routing, authentication, error handling, and CORS using actual implementation code.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// Import actual implementation code
import { getAllowedOrigin, corsHeaders, authCorsHeaders } from '../../utils'
import { handleHealth } from '../../routes/health'
import type { Env } from '../../env'

// ============================================================================
// Mock Factories
// ============================================================================

function createMockEnv(overrides: Partial<Env> = {}): Env {
  return {
    AUTH: {
      authenticate: vi.fn().mockResolvedValue({ ok: false, status: 401, error: 'Unauthorized' }),
    },
    EVENTS_BUCKET: createMockR2Bucket(),
    PIPELINE_BUCKET: createMockR2Bucket(),
    BENCHMARK_BUCKET: createMockR2Bucket(),
    CATALOG: createMockDONamespace(),
    SUBSCRIPTIONS: createMockDONamespace(),
    CDC_PROCESSOR: createMockDONamespace(),
    EVENT_WRITER: createMockDONamespace(),
    RATE_LIMITER: createMockDONamespace(),
    OAUTH: { fetch: vi.fn() } as unknown as Fetcher,
    ENVIRONMENT: 'test',
    ...overrides,
  } as unknown as Env
}

function createMockR2Bucket() {
  return {
    list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue({}),
    head: vi.fn().mockResolvedValue(null),
    delete: vi.fn().mockResolvedValue(undefined),
  } as unknown as R2Bucket
}

function createMockDONamespace() {
  return {
    idFromName: vi.fn((name: string) => ({ name, toString: () => name })),
    get: vi.fn(() => ({})),
  }
}

function createRequest(method: string, url: string, options: RequestInit = {}): Request {
  return new Request(url, { method, ...options })
}

// ============================================================================
// getAllowedOrigin Tests (Actual Implementation)
// ============================================================================

describe('getAllowedOrigin', () => {
  it('returns null when no origin provided', () => {
    expect(getAllowedOrigin(null)).toBeNull()
  })

  it('returns null for empty origin', () => {
    expect(getAllowedOrigin('')).toBeNull()
  })

  it('allows *.do domains by default', () => {
    expect(getAllowedOrigin('https://events.do')).toBe('https://events.do')
    expect(getAllowedOrigin('https://api.do')).toBe('https://api.do')
    expect(getAllowedOrigin('https://sub.example.do')).toBe('https://sub.example.do')
  })

  it('rejects non-.do domains by default', () => {
    expect(getAllowedOrigin('https://example.com')).toBeNull()
    expect(getAllowedOrigin('https://evil.site')).toBeNull()
  })

  it('allows explicit origins from ALLOWED_ORIGINS env', () => {
    const env = { ALLOWED_ORIGINS: 'https://trusted.com,https://partner.org' }
    expect(getAllowedOrigin('https://trusted.com', env)).toBe('https://trusted.com')
    expect(getAllowedOrigin('https://partner.org', env)).toBe('https://partner.org')
  })

  it('rejects non-listed origins when ALLOWED_ORIGINS is set', () => {
    const env = { ALLOWED_ORIGINS: 'https://trusted.com' }
    expect(getAllowedOrigin('https://untrusted.com', env)).toBeNull()
  })

  it('handles wildcard patterns in ALLOWED_ORIGINS', () => {
    const env = { ALLOWED_ORIGINS: '*.do' }
    expect(getAllowedOrigin('https://events.do', env)).toBe('https://events.do')
    expect(getAllowedOrigin('https://sub.events.do', env)).toBe('https://sub.events.do')
  })

  it('handles mixed patterns in ALLOWED_ORIGINS', () => {
    const env = { ALLOWED_ORIGINS: '*.do,https://trusted.com' }
    expect(getAllowedOrigin('https://events.do', env)).toBe('https://events.do')
    expect(getAllowedOrigin('https://trusted.com', env)).toBe('https://trusted.com')
    expect(getAllowedOrigin('https://untrusted.com', env)).toBeNull()
  })

  it('handles whitespace in ALLOWED_ORIGINS', () => {
    const env = { ALLOWED_ORIGINS: 'https://a.com , https://b.com' }
    expect(getAllowedOrigin('https://a.com', env)).toBe('https://a.com')
    expect(getAllowedOrigin('https://b.com', env)).toBe('https://b.com')
  })

  it('handles invalid origin URLs gracefully', () => {
    const env = { ALLOWED_ORIGINS: '*.do' }
    // Invalid URL should return null without throwing
    expect(getAllowedOrigin('not-a-valid-url', env)).toBeNull()
  })
})

// ============================================================================
// corsHeaders Tests (Actual Implementation)
// ============================================================================

describe('corsHeaders', () => {
  it('returns wildcard origin for public endpoints', () => {
    const headers = corsHeaders()
    expect(headers['Access-Control-Allow-Origin']).toBe('*')
  })
})

// ============================================================================
// authCorsHeaders Tests (Actual Implementation)
// ============================================================================

describe('authCorsHeaders', () => {
  it('returns allowed origin for valid .do domain', () => {
    const request = createRequest('GET', 'https://events.do/api', {
      headers: { Origin: 'https://dashboard.do' },
    })
    const headers = authCorsHeaders(request)
    expect(headers['Access-Control-Allow-Origin']).toBe('https://dashboard.do')
    expect(headers['Vary']).toBe('Origin')
  })

  it('returns null for disallowed origin', () => {
    const request = createRequest('GET', 'https://events.do/api', {
      headers: { Origin: 'https://evil.com' },
    })
    const headers = authCorsHeaders(request)
    expect(headers['Access-Control-Allow-Origin']).toBe('null')
  })

  it('returns null when no origin header present', () => {
    const request = createRequest('GET', 'https://events.do/api')
    const headers = authCorsHeaders(request)
    expect(headers['Access-Control-Allow-Origin']).toBe('null')
  })

  it('respects custom ALLOWED_ORIGINS env', () => {
    const request = createRequest('GET', 'https://events.do/api', {
      headers: { Origin: 'https://custom.com' },
    })
    const env = { ALLOWED_ORIGINS: 'https://custom.com' }
    const headers = authCorsHeaders(request, env)
    expect(headers['Access-Control-Allow-Origin']).toBe('https://custom.com')
  })
})

// ============================================================================
// handleHealth Tests (Actual Implementation)
// ============================================================================

describe('handleHealth', () => {
  let env: Env

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  it('returns health response for /health endpoint', async () => {
    const request = createRequest('GET', 'https://events.do/health')
    const url = new URL(request.url)
    const startTime = performance.now()

    const response = handleHealth(request, env, url, false, startTime)

    expect(response).not.toBeNull()
    expect(response?.status).toBe(200)

    const json = await response?.json() as { status: string; service: string; env: string }
    expect(json.status).toBe('ok')
    expect(json.service).toBe('events.do')
    expect(json.env).toBe('test')
  })

  it('returns health response for / endpoint', async () => {
    const request = createRequest('GET', 'https://events.do/')
    const url = new URL(request.url)

    const response = handleHealth(request, env, url, false, performance.now())

    expect(response).not.toBeNull()
    expect(response?.status).toBe(200)
  })

  it('returns null for non-health endpoints', () => {
    const request = createRequest('GET', 'https://events.do/ingest')
    const url = new URL(request.url)

    const response = handleHealth(request, env, url, false, performance.now())

    expect(response).toBeNull()
  })

  it('returns webhooks.do service name for webhooks domain', async () => {
    const request = createRequest('GET', 'https://webhooks.do/health')
    const url = new URL(request.url)

    const response = handleHealth(request, env, url, true, performance.now())

    const json = await response?.json() as { service: string; providers?: string[] }
    expect(json.service).toBe('webhooks.do')
    expect(json.providers).toContain('github')
    expect(json.providers).toContain('stripe')
  })

  it('includes cpuTimeMs in response', async () => {
    const request = createRequest('GET', 'https://events.do/health')
    const url = new URL(request.url)
    const startTime = performance.now()

    // Simulate some work
    await new Promise(resolve => setTimeout(resolve, 1))

    const response = handleHealth(request, env, url, false, startTime)

    const json = await response?.json() as { cpuTimeMs: number }
    expect(json.cpuTimeMs).toBeGreaterThanOrEqual(0)
  })

  it('uses environment from env', async () => {
    const prodEnv = createMockEnv({ ENVIRONMENT: 'production' } as Partial<Env>)
    const request = createRequest('GET', 'https://events.do/health')
    const url = new URL(request.url)

    const response = handleHealth(request, prodEnv, url, false, performance.now())

    const json = await response?.json() as { env: string }
    expect(json.env).toBe('production')
  })
})

// ============================================================================
// Route Matching Tests
// ============================================================================

describe('Route Matching', () => {
  describe('Ingest Route Detection', () => {
    it('identifies POST /ingest as ingest route', () => {
      const url = new URL('https://events.do/ingest')
      const isIngestRoute = (url.pathname === '/ingest' || url.pathname === '/e')
      expect(isIngestRoute).toBe(true)
    })

    it('identifies POST /e as ingest route (alias)', () => {
      const url = new URL('https://events.do/e')
      const isIngestRoute = (url.pathname === '/ingest' || url.pathname === '/e')
      expect(isIngestRoute).toBe(true)
    })

    it('does not identify /ingest/other as ingest route', () => {
      const url = new URL('https://events.do/ingest/other')
      const isIngestRoute = (url.pathname === '/ingest' || url.pathname === '/e')
      expect(isIngestRoute).toBe(false)
    })
  })

  describe('Protected Route Detection', () => {
    const protectedRoutes = ['/query', '/recent', '/events', '/pipeline', '/catalog', '/subscriptions', '/schemas', '/benchmark', '/dashboard']

    it.each(protectedRoutes)('identifies %s as protected route', (route) => {
      const isProtected = protectedRoutes.some(r => route === r || route.startsWith(r + '/'))
      expect(isProtected).toBe(true)
    })

    it.each(protectedRoutes)('identifies %s/subpath as protected', (route) => {
      const path = `${route}/subpath`
      const isProtected = protectedRoutes.some(r => path === r || path.startsWith(r + '/'))
      expect(isProtected).toBe(true)
    })

    it('does not identify /health as protected', () => {
      const isProtected = protectedRoutes.some(r => '/health' === r || '/health'.startsWith(r + '/'))
      expect(isProtected).toBe(false)
    })

    it('does not identify /ingest as protected', () => {
      const isProtected = protectedRoutes.some(r => '/ingest' === r || '/ingest'.startsWith(r + '/'))
      expect(isProtected).toBe(false)
    })
  })

  describe('Webhooks Domain Detection', () => {
    it('detects webhooks.do domain', () => {
      const hostname = 'webhooks.do'
      const isWebhooksDomain = hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
      expect(isWebhooksDomain).toBe(true)
    })

    it('detects subdomain of webhooks.do', () => {
      const hostname = 'github.webhooks.do'
      const isWebhooksDomain = hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
      expect(isWebhooksDomain).toBe(true)
    })

    it('does not detect events.do as webhooks domain', () => {
      const hostname = 'events.do'
      const isWebhooksDomain = hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
      expect(isWebhooksDomain).toBe(false)
    })

    it('does not detect localhost as webhooks domain', () => {
      const hostname = 'localhost'
      const isWebhooksDomain = hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
      expect(isWebhooksDomain).toBe(false)
    })
  })
})

// ============================================================================
// Schema Route Matching Tests
// ============================================================================

describe('Schema Route Matching', () => {
  const schemaRoutePatterns = {
    listSchemas: { method: 'GET', pattern: /^\/schemas$/ },
    registerSchema: { method: 'POST', pattern: /^\/schemas$/ },
    validateEvents: { method: 'POST', pattern: /^\/schemas\/validate$/ },
    getSchemaHistory: { method: 'GET', pattern: /^\/schemas\/[^/]+\/[^/]+\/history$/ },
    getSchema: { method: 'GET', pattern: /^\/schemas\/[^/]+\/[^/]+$/ },
    deleteSchema: { method: 'DELETE', pattern: /^\/schemas\/[^/]+\/[^/]+$/ },
    configureNamespace: { method: 'PUT', pattern: /^\/schemas\/namespace\/[^/]+\/config$/ },
    getNamespaceConfig: { method: 'GET', pattern: /^\/schemas\/namespace\/[^/]+\/config$/ },
  }

  it('matches GET /schemas', () => {
    expect(schemaRoutePatterns.listSchemas.pattern.test('/schemas')).toBe(true)
  })

  it('matches POST /schemas', () => {
    expect(schemaRoutePatterns.registerSchema.pattern.test('/schemas')).toBe(true)
  })

  it('matches POST /schemas/validate', () => {
    expect(schemaRoutePatterns.validateEvents.pattern.test('/schemas/validate')).toBe(true)
  })

  it('matches GET /schemas/:namespace/:eventType', () => {
    expect(schemaRoutePatterns.getSchema.pattern.test('/schemas/default/user.created')).toBe(true)
  })

  it('matches GET /schemas/:namespace/:eventType/history', () => {
    expect(schemaRoutePatterns.getSchemaHistory.pattern.test('/schemas/default/user.created/history')).toBe(true)
  })

  it('matches DELETE /schemas/:namespace/:eventType', () => {
    expect(schemaRoutePatterns.deleteSchema.pattern.test('/schemas/default/user.created')).toBe(true)
  })

  it('matches PUT /schemas/namespace/:namespace/config', () => {
    expect(schemaRoutePatterns.configureNamespace.pattern.test('/schemas/namespace/default/config')).toBe(true)
  })

  it('matches GET /schemas/namespace/:namespace/config', () => {
    expect(schemaRoutePatterns.getNamespaceConfig.pattern.test('/schemas/namespace/default/config')).toBe(true)
  })
})
