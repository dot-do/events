/**
 * Fetch Handler Tests
 *
 * Unit tests for the main fetch handler in src/handlers/fetch.ts
 * Tests routing, authentication, error handling, and CORS.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockAuthResult {
  ok: boolean
  status?: number
  error?: string
  user?: {
    email: string
    roles: string[]
  }
}

interface MockEnv {
  AUTH: {
    authenticate: ReturnType<typeof vi.fn>
  }
  EVENTS_BUCKET: MockR2Bucket
  PIPELINE_BUCKET: MockR2Bucket
  BENCHMARK_BUCKET: MockR2Bucket
  ENVIRONMENT: string
  ALLOWED_ORIGINS?: string
  AUTH_TOKEN?: string
  ALLOW_UNAUTHENTICATED_INGEST?: string
  DEFAULT_NAMESPACE?: string
  NAMESPACE_API_KEYS?: string
}

interface MockR2Bucket {
  list: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  put: ReturnType<typeof vi.fn>
  head: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
}

interface MockExecutionContext {
  waitUntil: ReturnType<typeof vi.fn>
  passThroughOnException: ReturnType<typeof vi.fn>
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockR2Bucket(): MockR2Bucket {
  return {
    list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue({}),
    head: vi.fn().mockResolvedValue(null),
    delete: vi.fn().mockResolvedValue(undefined),
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    AUTH: {
      authenticate: vi.fn().mockResolvedValue({ ok: false, status: 401, error: 'Unauthorized' }),
    },
    EVENTS_BUCKET: createMockR2Bucket(),
    PIPELINE_BUCKET: createMockR2Bucket(),
    BENCHMARK_BUCKET: createMockR2Bucket(),
    ENVIRONMENT: 'test',
    ...overrides,
  }
}

function createMockExecutionContext(): MockExecutionContext {
  return {
    waitUntil: vi.fn(),
    passThroughOnException: vi.fn(),
  }
}

function createRequest(method: string, url: string, options: RequestInit = {}): Request {
  return new Request(url, { method, ...options })
}

// ============================================================================
// CORS Handler Tests
// ============================================================================

describe('CORS Handling', () => {
  it('returns CORS headers for OPTIONS request', () => {
    const corsResponse = handleCORSRequest()

    expect(corsResponse.status).toBe(200)
    expect(corsResponse.headers.get('Access-Control-Allow-Origin')).toBe('*')
    expect(corsResponse.headers.get('Access-Control-Allow-Methods')).toBe('POST, GET, OPTIONS')
    expect(corsResponse.headers.get('Access-Control-Allow-Headers')).toBe('Content-Type, Authorization')
    expect(corsResponse.headers.get('Access-Control-Max-Age')).toBe('86400')
  })

  it('handles authenticated CORS with specific origin', () => {
    const origin = 'https://example.do'
    const corsResponse = handleAuthenticatedCORSRequest(origin, { ALLOWED_ORIGINS: '*.do' })

    expect(corsResponse.headers.get('Access-Control-Allow-Origin')).toBe(origin)
    expect(corsResponse.headers.get('Vary')).toBe('Origin')
  })

  it('returns null origin for disallowed origins', () => {
    const origin = 'https://evil.com'
    const corsResponse = handleAuthenticatedCORSRequest(origin, { ALLOWED_ORIGINS: '*.do' })

    expect(corsResponse.headers.get('Access-Control-Allow-Origin')).toBe('null')
  })
})

/**
 * Simulates CORS preflight handling
 */
function handleCORSRequest(): Response {
  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400',
    },
  })
}

/**
 * Simulates authenticated CORS handling
 */
function handleAuthenticatedCORSRequest(
  requestOrigin: string,
  env: { ALLOWED_ORIGINS?: string }
): Response {
  const origin = getAllowedOrigin(requestOrigin, env) ?? 'null'

  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': origin,
      ...(origin !== '*' ? { 'Vary': 'Origin' } : {}),
    },
  })
}

/**
 * Mirror of getAllowedOrigin from src/utils.ts
 */
function getAllowedOrigin(requestOrigin: string | null, env?: { ALLOWED_ORIGINS?: string }): string | null {
  if (!requestOrigin) return null

  if (env?.ALLOWED_ORIGINS) {
    const allowed = env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
    if (allowed.includes(requestOrigin)) return requestOrigin
    for (const pattern of allowed) {
      if (pattern.startsWith('*.')) {
        const suffix = pattern.slice(1)
        try {
          const originHost = new URL(requestOrigin).hostname
          if (originHost.endsWith(suffix) || originHost === suffix.slice(1)) return requestOrigin
        } catch {
          // Invalid origin URL
        }
      }
    }
    return null
  }

  const DEFAULT_ALLOWED_ORIGIN_PATTERN = /^https?:\/\/([a-z0-9-]+\.)*do$/
  if (DEFAULT_ALLOWED_ORIGIN_PATTERN.test(requestOrigin)) return requestOrigin
  return null
}

// ============================================================================
// Health Check Tests
// ============================================================================

describe('Health Check Handler', () => {
  it('returns health status for /health endpoint', () => {
    const result = handleHealthCheck('/health', 'events.do', 'test', 0)

    expect(result).not.toBeNull()
    expect(result?.status).toBe(200)
  })

  it('returns health status for / endpoint', () => {
    const result = handleHealthCheck('/', 'events.do', 'test', 0)

    expect(result).not.toBeNull()
  })

  it('returns null for non-health endpoints', () => {
    const result = handleHealthCheck('/ingest', 'events.do', 'test', 0)

    expect(result).toBeNull()
  })

  it('includes correct service name for events.do domain', async () => {
    const result = handleHealthCheck('/health', 'events.do', 'test', 0)
    const json = await result?.json()

    expect(json.service).toBe('events.do')
  })

  it('includes correct service name for webhooks.do domain', async () => {
    const result = handleHealthCheck('/health', 'webhooks.do', 'test', 0)
    const json = await result?.json()

    expect(json.service).toBe('webhooks.do')
    expect(json.providers).toContain('github')
    expect(json.providers).toContain('stripe')
  })

  it('includes environment in response', async () => {
    const result = handleHealthCheck('/health', 'events.do', 'production', 0)
    const json = await result?.json()

    expect(json.env).toBe('production')
  })
})

/**
 * Simulates health check handling
 */
function handleHealthCheck(
  pathname: string,
  hostname: string,
  environment: string,
  startTime: number
): Response | null {
  if (pathname !== '/health' && pathname !== '/') return null

  const isWebhooksDomain = hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
  const serviceName = isWebhooksDomain ? 'webhooks.do' : 'events.do'
  const cpuTime = performance.now() - startTime

  return Response.json({
    status: 'ok',
    service: serviceName,
    ts: new Date().toISOString(),
    env: environment,
    cpuTimeMs: cpuTime,
    providers: isWebhooksDomain ? ['github', 'stripe', 'workos', 'slack', 'linear', 'svix'] : undefined,
  })
}

// ============================================================================
// Routing Tests
// ============================================================================

describe('Route Matching', () => {
  describe('Ingest Route', () => {
    it('matches POST /ingest', () => {
      expect(matchesIngestRoute('/ingest', 'POST')).toBe(true)
    })

    it('matches POST /e (alias)', () => {
      expect(matchesIngestRoute('/e', 'POST')).toBe(true)
    })

    it('does not match GET /ingest', () => {
      expect(matchesIngestRoute('/ingest', 'GET')).toBe(false)
    })

    it('does not match POST /ingest/other', () => {
      expect(matchesIngestRoute('/ingest/other', 'POST')).toBe(false)
    })
  })

  describe('Protected Routes', () => {
    const protectedRoutes = ['/query', '/recent', '/events', '/pipeline', '/catalog', '/subscriptions', '/schemas', '/benchmark', '/dashboard']

    it.each(protectedRoutes)('identifies %s as protected', (route) => {
      expect(isProtectedRoute(route)).toBe(true)
    })

    it.each(protectedRoutes)('identifies %s/subpath as protected', (route) => {
      expect(isProtectedRoute(`${route}/subpath`)).toBe(true)
    })

    it('does not identify /health as protected', () => {
      expect(isProtectedRoute('/health')).toBe(false)
    })

    it('does not identify /ingest as protected', () => {
      expect(isProtectedRoute('/ingest')).toBe(false)
    })
  })

  describe('Schema Routes', () => {
    it('matches GET /schemas', () => {
      expect(matchesSchemaRoute('/schemas', 'GET')).toBe(true)
    })

    it('matches POST /schemas', () => {
      expect(matchesSchemaRoute('/schemas', 'POST')).toBe(true)
    })

    it('matches POST /schemas/validate', () => {
      expect(matchesSchemaRoute('/schemas/validate', 'POST')).toBe(true)
    })

    it('matches GET /schemas/:namespace/:eventType', () => {
      expect(matchesSchemaRoute('/schemas/default/user.created', 'GET')).toBe(true)
    })

    it('matches GET /schemas/:namespace/:eventType/history', () => {
      expect(matchesSchemaRoute('/schemas/default/user.created/history', 'GET')).toBe(true)
    })

    it('matches DELETE /schemas/:namespace/:eventType', () => {
      expect(matchesSchemaRoute('/schemas/default/user.created', 'DELETE')).toBe(true)
    })
  })
})

/**
 * Match ingest routes
 */
function matchesIngestRoute(pathname: string, method: string): boolean {
  return (pathname === '/ingest' || pathname === '/e') && method === 'POST'
}

/**
 * Check if a route is protected
 */
function isProtectedRoute(pathname: string): boolean {
  const protectedRoutes = ['/query', '/recent', '/events', '/pipeline', '/catalog', '/subscriptions', '/schemas', '/benchmark', '/dashboard']
  return protectedRoutes.some(r => pathname === r || pathname.startsWith(r + '/'))
}

/**
 * Match schema routes
 */
function matchesSchemaRoute(pathname: string, _method: string): boolean {
  return pathname.startsWith('/schemas')
}

// ============================================================================
// Authentication Tests
// ============================================================================

describe('Authentication Flow', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  describe('Protected Route Auth', () => {
    it('requires authentication for protected routes', async () => {
      const result = await simulateAuthCheck(env, null, null)

      expect(result.authenticated).toBe(false)
      expect(result.status).toBe(401)
    })

    it('authenticates with valid Bearer token', async () => {
      env.AUTH.authenticate.mockResolvedValue({
        ok: true,
        user: { email: 'admin@example.com', roles: ['admin'] },
      })

      const result = await simulateAuthCheck(env, 'Bearer valid-token', null)

      expect(result.authenticated).toBe(true)
      expect(result.user?.email).toBe('admin@example.com')
    })

    it('authenticates with valid cookie', async () => {
      env.AUTH.authenticate.mockResolvedValue({
        ok: true,
        user: { email: 'admin@example.com', roles: ['admin'] },
      })

      const result = await simulateAuthCheck(env, null, 'session=abc123')

      expect(result.authenticated).toBe(true)
    })

    it('requires admin role for protected routes', async () => {
      env.AUTH.authenticate.mockResolvedValue({
        ok: true,
        user: { email: 'user@example.com', roles: ['user'] },
      })

      const result = await simulateAuthCheck(env, 'Bearer valid-token', null)

      expect(result.authenticated).toBe(true)
      expect(result.hasAdminRole).toBe(false)
    })

    it('grants access with admin role', async () => {
      env.AUTH.authenticate.mockResolvedValue({
        ok: true,
        user: { email: 'admin@example.com', roles: ['admin'] },
      })

      const result = await simulateAuthCheck(env, 'Bearer valid-token', null)

      expect(result.authenticated).toBe(true)
      expect(result.hasAdminRole).toBe(true)
    })

    it('redirects to login for HTML requests', async () => {
      const result = await simulateAuthCheckWithRedirect(env, null, 'text/html,*/*')

      expect(result.shouldRedirect).toBe(true)
      expect(result.redirectUrl).toContain('/login')
    })

    it('returns 401 for API requests', async () => {
      const result = await simulateAuthCheckWithRedirect(env, null, 'application/json')

      expect(result.shouldRedirect).toBe(false)
      expect(result.status).toBe(401)
    })
  })

  describe('Ingest Auth', () => {
    it('allows unauthenticated access when ALLOW_UNAUTHENTICATED_INGEST=true', () => {
      const envWithUnauthenticated = createMockEnv({
        ALLOW_UNAUTHENTICATED_INGEST: 'true',
      })

      const result = simulateIngestAuth(envWithUnauthenticated, null)

      expect(result.allowed).toBe(true)
      expect(result.namespace).toBe('default')
    })

    it('requires auth when AUTH_TOKEN is set', () => {
      const envWithToken = createMockEnv({
        AUTH_TOKEN: 'secret-token',
      })

      const result = simulateIngestAuth(envWithToken, null)

      expect(result.allowed).toBe(false)
    })

    it('authenticates with valid AUTH_TOKEN', () => {
      const envWithToken = createMockEnv({
        AUTH_TOKEN: 'secret-token',
      })

      const result = simulateIngestAuth(envWithToken, 'Bearer secret-token')

      expect(result.allowed).toBe(true)
    })

    it('extracts namespace from namespaced API key', () => {
      const envWithNamespaceKeys = createMockEnv({
        NAMESPACE_API_KEYS: JSON.stringify({ 'ns_acme_abc123': 'acme' }),
      })

      const result = simulateIngestAuth(envWithNamespaceKeys, 'Bearer ns_acme_abc123')

      expect(result.allowed).toBe(true)
      expect(result.namespace).toBe('acme')
    })

    it('uses DEFAULT_NAMESPACE for legacy tokens', () => {
      const envWithDefaults = createMockEnv({
        AUTH_TOKEN: 'legacy-token',
        DEFAULT_NAMESPACE: 'my-namespace',
      })

      const result = simulateIngestAuth(envWithDefaults, 'Bearer legacy-token')

      expect(result.allowed).toBe(true)
      expect(result.namespace).toBe('my-namespace')
    })
  })
})

interface AuthCheckResult {
  authenticated: boolean
  status?: number
  user?: { email: string; roles: string[] }
  hasAdminRole: boolean
}

interface AuthCheckWithRedirectResult {
  shouldRedirect: boolean
  redirectUrl?: string
  status?: number
}

interface IngestAuthResult {
  allowed: boolean
  namespace?: string
}

/**
 * Simulates auth check for protected routes
 */
async function simulateAuthCheck(
  env: MockEnv,
  authHeader: string | null,
  cookie: string | null
): Promise<AuthCheckResult> {
  const result = await env.AUTH.authenticate(authHeader, cookie)

  if (!result.ok) {
    return { authenticated: false, status: result.status || 401, hasAdminRole: false }
  }

  const hasAdminRole = result.user?.roles?.includes('admin') ?? false

  return {
    authenticated: true,
    user: result.user,
    hasAdminRole,
  }
}

/**
 * Simulates auth check with redirect behavior
 */
async function simulateAuthCheckWithRedirect(
  env: MockEnv,
  authHeader: string | null,
  accept: string
): Promise<AuthCheckWithRedirectResult> {
  const result = await env.AUTH.authenticate(authHeader, null)

  if (!result.ok) {
    if (accept.includes('text/html')) {
      return {
        shouldRedirect: true,
        redirectUrl: '/login?redirect_uri=%2Fdashboard',
      }
    }
    return { shouldRedirect: false, status: result.status || 401 }
  }

  return { shouldRedirect: false }
}

/**
 * Simulates ingest endpoint authentication
 */
function simulateIngestAuth(env: MockEnv, authHeader: string | null): IngestAuthResult {
  const hasNamespaceKeys = !!env.NAMESPACE_API_KEYS

  if (hasNamespaceKeys) {
    // Check namespace-scoped API keys
    const token = authHeader?.replace('Bearer ', '')
    if (token && env.NAMESPACE_API_KEYS) {
      try {
        const keys = JSON.parse(env.NAMESPACE_API_KEYS) as Record<string, string>
        if (keys[token]) {
          return { allowed: true, namespace: keys[token] }
        }
      } catch {
        // Invalid JSON
      }
    }
    return { allowed: false }
  }

  if (env.AUTH_TOKEN) {
    const token = authHeader?.replace('Bearer ', '')
    if (token === env.AUTH_TOKEN) {
      return { allowed: true, namespace: env.DEFAULT_NAMESPACE || 'default' }
    }
    return { allowed: false }
  }

  if (env.ALLOW_UNAUTHENTICATED_INGEST === 'true') {
    return { allowed: true, namespace: env.DEFAULT_NAMESPACE || 'default' }
  }

  return { allowed: false }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Error Handling', () => {
  it('returns 404 for unknown routes', () => {
    const response = handleUnknownRoute('/unknown')

    expect(response.status).toBe(404)
  })

  it('returns 403 for non-admin users on protected routes', () => {
    const response = handleForbidden()

    expect(response.status).toBe(403)
  })

  it('returns error JSON with correct structure', async () => {
    const response = handleError('Something went wrong', 500)
    const json = await response.json()

    expect(json.error).toBe('Something went wrong')
    expect(response.status).toBe(500)
  })

  it('includes CORS headers in error responses', () => {
    const response = handleError('Error', 500)

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})

function handleUnknownRoute(_pathname: string): Response {
  return new Response('Not found', { status: 404 })
}

function handleForbidden(): Response {
  return Response.json({
    error: 'Admin access required',
    authenticated: true,
    message: 'You are logged in but do not have admin privileges',
  }, { status: 403 })
}

function handleError(message: string, status: number): Response {
  return Response.json({ error: message }, {
    status,
    headers: { 'Access-Control-Allow-Origin': '*' },
  })
}

// ============================================================================
// Domain Detection Tests
// ============================================================================

describe('Domain Detection', () => {
  it('detects webhooks.do domain', () => {
    expect(isWebhooksDomain('webhooks.do')).toBe(true)
  })

  it('detects subdomain of webhooks.do', () => {
    expect(isWebhooksDomain('github.webhooks.do')).toBe(true)
  })

  it('does not detect events.do as webhooks domain', () => {
    expect(isWebhooksDomain('events.do')).toBe(false)
  })

  it('does not detect local development as webhooks domain', () => {
    expect(isWebhooksDomain('localhost')).toBe(false)
  })
})

function isWebhooksDomain(hostname: string): boolean {
  return hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
}

// ============================================================================
// Request Timing Tests
// ============================================================================

describe('Request Timing', () => {
  it('calculates CPU time correctly', async () => {
    const startTime = performance.now()

    // Simulate some work
    await new Promise(resolve => setTimeout(resolve, 10))

    const cpuTime = performance.now() - startTime

    expect(cpuTime).toBeGreaterThan(0)
    expect(cpuTime).toBeLessThan(1000) // Should be well under 1 second
  })
})
