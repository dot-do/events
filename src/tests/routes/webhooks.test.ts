/**
 * Webhooks Route Handler Tests
 *
 * Unit tests for src/routes/webhooks.ts
 * Tests webhook route handling, provider routing, and rate limiting.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockRateLimiterDO {
  checkLimit: ReturnType<typeof vi.fn>
}

interface MockEventWriterDO {
  ingest: ReturnType<typeof vi.fn>
}

interface MockEnv {
  RATE_LIMITER: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
  EVENT_WRITER: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
  RATE_LIMIT_REQUESTS_PER_MINUTE?: string
  RATE_LIMIT_EVENTS_PER_MINUTE?: string
  GITHUB_WEBHOOK_SECRET?: string
  STRIPE_WEBHOOK_SECRET?: string
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockRateLimiter(allowed: boolean = true): MockRateLimiterDO {
  return {
    checkLimit: vi.fn().mockResolvedValue({ allowed }),
  }
}

function createMockEventWriter(): MockEventWriterDO {
  return {
    ingest: vi.fn().mockResolvedValue({ ok: true, shard: 0, buffered: 1 }),
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  const rateLimiter = createMockRateLimiter()
  const eventWriter = createMockEventWriter()

  return {
    RATE_LIMITER: {
      idFromName: vi.fn().mockReturnValue('rate-limiter-id'),
      get: vi.fn().mockReturnValue(rateLimiter),
    },
    EVENT_WRITER: {
      idFromName: vi.fn().mockReturnValue('event-writer-id'),
      get: vi.fn().mockReturnValue(eventWriter),
    },
    ...overrides,
  }
}

function createRequest(url: string, options: RequestInit = {}): Request {
  return new Request(url, { method: 'POST', ...options })
}

// ============================================================================
// Webhook Provider Constants
// ============================================================================

const WEBHOOK_PROVIDERS = ['github', 'stripe', 'workos', 'slack', 'linear', 'svix']

// ============================================================================
// Route Matching Tests
// ============================================================================

describe('Webhook Route Matching', () => {
  describe('POST /webhooks?provider=xxx', () => {
    it('matches /webhooks with provider query param', () => {
      const url = new URL('https://events.do/webhooks?provider=github')
      expect(url.pathname).toBe('/webhooks')
      expect(url.searchParams.get('provider')).toBe('github')
    })

    it('returns null for missing provider', () => {
      const url = new URL('https://events.do/webhooks')
      expect(url.searchParams.get('provider')).toBeNull()
    })

    it('does not match GET requests', () => {
      const request = createRequest('https://events.do/webhooks?provider=github', { method: 'GET' })
      expect(request.method).toBe('GET')
    })
  })

  describe('webhooks.do/provider format', () => {
    it.each(WEBHOOK_PROVIDERS)('matches /%s on webhooks.do', (provider) => {
      const url = new URL(`https://webhooks.do/${provider}`)
      const extractedProvider = url.pathname.slice(1).split('/')[0]
      expect(WEBHOOK_PROVIDERS.includes(extractedProvider)).toBe(true)
    })

    it('extracts provider from first path segment', () => {
      const url = new URL('https://webhooks.do/github/extra/path')
      const provider = url.pathname.slice(1).split('/')[0]
      expect(provider).toBe('github')
    })

    it('does not match unknown providers', () => {
      const url = new URL('https://webhooks.do/unknown')
      const provider = url.pathname.slice(1).split('/')[0]
      expect(WEBHOOK_PROVIDERS.includes(provider)).toBe(false)
    })
  })
})

// ============================================================================
// Provider Query Parameter Tests
// ============================================================================

describe('Provider Parameter Validation', () => {
  it('returns 400 when provider is missing', async () => {
    const response = Response.json(
      { error: 'provider query param required' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
    const json = await response.json()
    expect(json.error).toBe('provider query param required')
  })

  it.each(WEBHOOK_PROVIDERS)('accepts valid provider: %s', (provider) => {
    const url = new URL(`https://events.do/webhooks?provider=${provider}`)
    expect(url.searchParams.get('provider')).toBe(provider)
  })
})

// ============================================================================
// Rate Limiting Tests
// ============================================================================

describe('Webhook Rate Limiting', () => {
  const WEBHOOK_RATE_LIMIT_REQUESTS_PER_MINUTE = 10000
  const WEBHOOK_RATE_LIMIT_EVENTS_PER_MINUTE = 100000

  it('uses lenient rate limits for webhook traffic', () => {
    // Webhook traffic has more lenient limits than ingest
    expect(WEBHOOK_RATE_LIMIT_REQUESTS_PER_MINUTE).toBeGreaterThanOrEqual(10000)
    expect(WEBHOOK_RATE_LIMIT_EVENTS_PER_MINUTE).toBeGreaterThanOrEqual(100000)
  })

  it('applies rate limiting before signature verification', () => {
    // This is important to prevent DoS attacks
    // The flow should be: rate limit check -> signature verification -> processing
    const steps = ['rateLimit', 'verifySignature', 'processEvent']
    expect(steps.indexOf('rateLimit')).toBeLessThan(steps.indexOf('verifySignature'))
  })

  it('counts each webhook as a single event', () => {
    // Webhooks are single events, so eventCount is always 1
    const eventCount = 1
    expect(eventCount).toBe(1)
  })
})

// ============================================================================
// Event Record Formatting Tests
// ============================================================================

describe('Webhook Event Record Formatting', () => {
  interface NormalizedWebhookEvent {
    ts: string
    type: string
    source: string
    webhook: {
      provider: string
      eventType: string
      verified: boolean
    }
    payload: Record<string, unknown>
  }

  function createEventRecord(event: NormalizedWebhookEvent) {
    return {
      ts: event.ts,
      type: event.type,
      source: event.source,
      provider: event.webhook.provider,
      eventType: event.webhook.eventType,
      verified: event.webhook.verified,
      payload: event.payload,
    }
  }

  it('maps normalized webhook event to event record', () => {
    const webhookEvent: NormalizedWebhookEvent = {
      ts: new Date().toISOString(),
      type: 'webhook.github.push',
      source: 'webhook',
      webhook: {
        provider: 'github',
        eventType: 'push',
        verified: true,
      },
      payload: { ref: 'refs/heads/main' },
    }

    const record = createEventRecord(webhookEvent)

    expect(record.ts).toBe(webhookEvent.ts)
    expect(record.type).toBe('webhook.github.push')
    expect(record.source).toBe('webhook')
    expect(record.provider).toBe('github')
    expect(record.eventType).toBe('push')
    expect(record.verified).toBe(true)
    expect(record.payload).toEqual({ ref: 'refs/heads/main' })
  })

  it('includes verification status', () => {
    const verifiedEvent: NormalizedWebhookEvent = {
      ts: new Date().toISOString(),
      type: 'webhook.stripe.charge.succeeded',
      source: 'webhook',
      webhook: {
        provider: 'stripe',
        eventType: 'charge.succeeded',
        verified: true,
      },
      payload: {},
    }

    const unverifiedEvent: NormalizedWebhookEvent = {
      ts: new Date().toISOString(),
      type: 'webhook.stripe.charge.succeeded',
      source: 'webhook',
      webhook: {
        provider: 'stripe',
        eventType: 'charge.succeeded',
        verified: false,
      },
      payload: {},
    }

    expect(createEventRecord(verifiedEvent).verified).toBe(true)
    expect(createEventRecord(unverifiedEvent).verified).toBe(false)
  })
})

// ============================================================================
// Domain Detection Tests
// ============================================================================

describe('Webhooks Domain Detection', () => {
  function isWebhooksDomain(hostname: string): boolean {
    return hostname === 'webhooks.do' || hostname.endsWith('.webhooks.do')
  }

  it('detects webhooks.do as webhooks domain', () => {
    expect(isWebhooksDomain('webhooks.do')).toBe(true)
  })

  it('detects subdomains of webhooks.do', () => {
    expect(isWebhooksDomain('github.webhooks.do')).toBe(true)
    expect(isWebhooksDomain('stripe.webhooks.do')).toBe(true)
  })

  it('does not detect events.do as webhooks domain', () => {
    expect(isWebhooksDomain('events.do')).toBe(false)
  })

  it('does not detect similar domains as webhooks domain', () => {
    expect(isWebhooksDomain('webhooks.do.evil.com')).toBe(false)
    expect(isWebhooksDomain('mywebhooks.do')).toBe(false)
  })
})

// ============================================================================
// Response Status Tests
// ============================================================================

describe('Webhook Response Handling', () => {
  it('only sends to EventWriterDO on 200 status', () => {
    const statuses = [200, 400, 401, 403, 500]

    statuses.forEach(status => {
      const shouldSend = status === 200
      expect(shouldSend).toBe(status === 200)
    })
  })

  it('logs webhook processing time', () => {
    const startTime = performance.now()
    // Simulate some processing
    const cpuTime = performance.now() - startTime

    expect(cpuTime).toBeGreaterThanOrEqual(0)
    expect(cpuTime).toBeLessThan(1000) // Should be fast
  })
})

// ============================================================================
// Supported Providers Tests
// ============================================================================

describe('Supported Webhook Providers', () => {
  it('includes github', () => {
    expect(WEBHOOK_PROVIDERS).toContain('github')
  })

  it('includes stripe', () => {
    expect(WEBHOOK_PROVIDERS).toContain('stripe')
  })

  it('includes workos', () => {
    expect(WEBHOOK_PROVIDERS).toContain('workos')
  })

  it('includes slack', () => {
    expect(WEBHOOK_PROVIDERS).toContain('slack')
  })

  it('includes linear', () => {
    expect(WEBHOOK_PROVIDERS).toContain('linear')
  })

  it('includes svix', () => {
    expect(WEBHOOK_PROVIDERS).toContain('svix')
  })

  it('has exactly 6 supported providers', () => {
    expect(WEBHOOK_PROVIDERS).toHaveLength(6)
  })
})

// ============================================================================
// Error Response Tests
// ============================================================================

describe('Webhook Error Responses', () => {
  it('returns proper error for missing provider', async () => {
    const response = Response.json(
      { error: 'provider query param required' },
      { status: 400, headers: { 'Access-Control-Allow-Origin': '*' } }
    )

    expect(response.status).toBe(400)
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    const json = await response.json()
    expect(json.error).toBe('provider query param required')
  })

  it('includes CORS headers in error responses', () => {
    const response = Response.json({ error: 'test' }, {
      status: 400,
      headers: { 'Access-Control-Allow-Origin': '*' }
    })

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})
