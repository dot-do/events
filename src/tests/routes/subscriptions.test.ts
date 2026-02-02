/**
 * Subscription Routes Handler Tests
 *
 * Unit tests for src/subscription-routes.ts
 * Tests subscription REST API endpoints, pattern validation, shard routing,
 * namespace isolation, and error handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  validatePattern,
  getSubscriptionShard,
  handleSubscriptionRoutes,
  KNOWN_SUBSCRIPTION_SHARDS,
} from '../../subscription-routes'
import { MAX_PATTERN_LENGTH } from '../../config'
import type { TenantContext } from '../../middleware/tenant'

// ============================================================================
// Mock Types
// ============================================================================

interface MockSubscriptionDO {
  subscribe: ReturnType<typeof vi.fn>
  unsubscribe: ReturnType<typeof vi.fn>
  reactivate: ReturnType<typeof vi.fn>
  listSubscriptions: ReturnType<typeof vi.fn>
  findMatchingSubscriptions: ReturnType<typeof vi.fn>
  getSubscription: ReturnType<typeof vi.fn>
  getSubscriptionStatus: ReturnType<typeof vi.fn>
  updateSubscription: ReturnType<typeof vi.fn>
  deleteSubscription: ReturnType<typeof vi.fn>
  getDeadLetters: ReturnType<typeof vi.fn>
  retryDeadLetter: ReturnType<typeof vi.fn>
  getDeliveryLogs: ReturnType<typeof vi.fn>
}

interface MockShardCoordinatorDO {
  getActiveShards: ReturnType<typeof vi.fn>
  getAllActiveShards: ReturnType<typeof vi.fn>
  getRoutingShard: ReturnType<typeof vi.fn>
  getStats: ReturnType<typeof vi.fn>
  getConfig: ReturnType<typeof vi.fn>
  updateConfig: ReturnType<typeof vi.fn>
  forceScale: ReturnType<typeof vi.fn>
  startHealthChecks: ReturnType<typeof vi.fn>
}

interface MockEnv {
  SUBSCRIPTIONS: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
  SUBSCRIPTION_SHARD_COORDINATOR?: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockSubscriptionDO(overrides: Partial<MockSubscriptionDO> = {}): MockSubscriptionDO {
  return {
    subscribe: vi.fn().mockResolvedValue({ ok: true, id: 'SUB123' }),
    unsubscribe: vi.fn().mockResolvedValue({ ok: true }),
    reactivate: vi.fn().mockResolvedValue({ ok: true }),
    listSubscriptions: vi.fn().mockResolvedValue([]),
    findMatchingSubscriptions: vi.fn().mockResolvedValue([]),
    getSubscription: vi.fn().mockResolvedValue(null),
    getSubscriptionStatus: vi.fn().mockResolvedValue({
      subscription: null,
      stats: { pendingDeliveries: 0, failedDeliveries: 0, deadLetters: 0, successRate: 0, totalDelivered: 0, totalAttempts: 0 },
    }),
    updateSubscription: vi.fn().mockResolvedValue({ ok: true }),
    deleteSubscription: vi.fn().mockResolvedValue({ ok: true }),
    getDeadLetters: vi.fn().mockResolvedValue([]),
    retryDeadLetter: vi.fn().mockResolvedValue({ ok: false }),
    getDeliveryLogs: vi.fn().mockResolvedValue([]),
    ...overrides,
  }
}

function createMockShardCoordinator(overrides: Partial<MockShardCoordinatorDO> = {}): MockShardCoordinatorDO {
  return {
    getActiveShards: vi.fn().mockResolvedValue(['collection:0']),
    getAllActiveShards: vi.fn().mockResolvedValue(['collection:0', 'webhook:0', 'default:0']),
    getRoutingShard: vi.fn().mockResolvedValue('collection:0'),
    getStats: vi.fn().mockResolvedValue({ totalShards: 6, prefixStates: {} }),
    getConfig: vi.fn().mockResolvedValue({ minShardsPerPrefix: 1, maxShardsPerPrefix: 16 }),
    updateConfig: vi.fn().mockResolvedValue({ minShardsPerPrefix: 1, maxShardsPerPrefix: 16 }),
    forceScale: vi.fn().mockResolvedValue({ scaled: true, previousCount: 1, newCount: 2, prefix: 'collection' }),
    startHealthChecks: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

function createMockEnv(
  subscriptionDO?: MockSubscriptionDO,
  shardCoordinator?: MockShardCoordinatorDO | null
): MockEnv {
  const mockDO = subscriptionDO || createMockSubscriptionDO()

  const env: MockEnv = {
    SUBSCRIPTIONS: {
      idFromName: vi.fn().mockReturnValue('subscription-do-id'),
      get: vi.fn().mockReturnValue(mockDO),
    },
  }

  if (shardCoordinator !== null) {
    const mockCoordinator = shardCoordinator || createMockShardCoordinator()
    env.SUBSCRIPTION_SHARD_COORDINATOR = {
      idFromName: vi.fn().mockReturnValue('coordinator-id'),
      get: vi.fn().mockReturnValue(mockCoordinator),
    }
  }

  return env
}

function createRequest(
  url: string,
  options: RequestInit & { body?: unknown } = {}
): Request {
  const { body, ...rest } = options
  return new Request(url, {
    ...rest,
    body: body ? JSON.stringify(body) : undefined,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })
}

function createTenantContext(namespace: string = 'test-tenant', isAdmin: boolean = false): TenantContext {
  return {
    namespace,
    isAdmin,
    keyId: 'test-key',
  }
}

// ============================================================================
// validatePattern Tests
// ============================================================================

describe('validatePattern', () => {
  describe('valid patterns', () => {
    it('accepts simple event types', () => {
      expect(validatePattern('collection.insert')).toBeNull()
      expect(validatePattern('webhook.github.push')).toBeNull()
      expect(validatePattern('rpc.call')).toBeNull()
    })

    it('accepts single wildcard patterns', () => {
      expect(validatePattern('collection.*')).toBeNull()
      expect(validatePattern('*.insert')).toBeNull()
      expect(validatePattern('collection.*.users')).toBeNull()
    })

    it('accepts double wildcard patterns', () => {
      expect(validatePattern('collection.**')).toBeNull()
      expect(validatePattern('**')).toBeNull()
      expect(validatePattern('**.error')).toBeNull()
    })

    it('accepts patterns with underscores', () => {
      expect(validatePattern('user_events.created')).toBeNull()
      expect(validatePattern('my_collection.insert')).toBeNull()
    })

    it('accepts patterns with hyphens', () => {
      expect(validatePattern('my-service.event')).toBeNull()
      expect(validatePattern('webhook.my-provider.push')).toBeNull()
    })

    it('accepts patterns with alphanumeric characters', () => {
      expect(validatePattern('service1.event2')).toBeNull()
      expect(validatePattern('v2.collection.insert')).toBeNull()
    })

    it('accepts maximum length pattern', () => {
      const maxPattern = 'a'.repeat(MAX_PATTERN_LENGTH)
      expect(validatePattern(maxPattern)).toBeNull()
    })
  })

  describe('invalid patterns - length', () => {
    it('rejects patterns exceeding max length', () => {
      const longPattern = 'a'.repeat(MAX_PATTERN_LENGTH + 1)
      const result = validatePattern(longPattern)
      expect(result).toContain('maximum length')
      expect(result).toContain(String(MAX_PATTERN_LENGTH))
    })
  })

  describe('invalid patterns - characters', () => {
    it('rejects patterns with spaces', () => {
      const result = validatePattern('collection insert')
      expect(result).toContain('invalid characters')
    })

    it('rejects patterns with special characters', () => {
      expect(validatePattern('collection@insert')).toContain('invalid characters')
      expect(validatePattern('collection#insert')).toContain('invalid characters')
      expect(validatePattern('collection$insert')).toContain('invalid characters')
      expect(validatePattern('collection%insert')).toContain('invalid characters')
    })

    it('rejects patterns with slashes', () => {
      expect(validatePattern('collection/insert')).toContain('invalid characters')
      expect(validatePattern('collection\\insert')).toContain('invalid characters')
    })

    it('rejects patterns with brackets', () => {
      expect(validatePattern('collection[0]')).toContain('invalid characters')
      expect(validatePattern('collection(test)')).toContain('invalid characters')
    })
  })

  describe('invalid patterns - consecutive asterisks', () => {
    it('rejects patterns with three or more consecutive asterisks', () => {
      expect(validatePattern('collection.***')).toContain('more than two consecutive asterisks')
      expect(validatePattern('****')).toContain('more than two consecutive asterisks')
      expect(validatePattern('collection.*****.insert')).toContain('more than two consecutive asterisks')
    })
  })

  describe('invalid patterns - dots', () => {
    it('rejects patterns starting with a dot', () => {
      const result = validatePattern('.collection.insert')
      expect(result).toContain('cannot start or end with a dot')
    })

    it('rejects patterns ending with a dot', () => {
      const result = validatePattern('collection.insert.')
      expect(result).toContain('cannot start or end with a dot')
    })

    it('rejects patterns with consecutive dots', () => {
      const result = validatePattern('collection..insert')
      expect(result).toContain('consecutive dots')
    })

    it('rejects patterns with multiple consecutive dots', () => {
      expect(validatePattern('collection...insert')).toContain('consecutive dots')
      expect(validatePattern('a....b')).toContain('consecutive dots')
    })
  })

  describe('invalid patterns - excessive wildcards', () => {
    it('rejects patterns with too many wildcard segments', () => {
      // Pattern with more than 5 wildcard segments and more than half total segments
      const result = validatePattern('*.*.*.*.*.*')
      expect(result).toContain('too many wildcard segments')
    })
  })
})

// ============================================================================
// getSubscriptionShard Tests
// ============================================================================

describe('getSubscriptionShard', () => {
  describe('extracts prefix from patterns', () => {
    it('extracts first segment as shard key', () => {
      expect(getSubscriptionShard('collection.insert')).toBe('collection')
      expect(getSubscriptionShard('webhook.github.push')).toBe('webhook')
      expect(getSubscriptionShard('rpc.call')).toBe('rpc')
      expect(getSubscriptionShard('do.create')).toBe('do')
      expect(getSubscriptionShard('ws.connect')).toBe('ws')
    })

    it('handles patterns with wildcards', () => {
      expect(getSubscriptionShard('collection.*')).toBe('collection')
      expect(getSubscriptionShard('webhook.**')).toBe('webhook')
      expect(getSubscriptionShard('rpc.call.*')).toBe('rpc')
    })

    it('handles single-segment patterns', () => {
      expect(getSubscriptionShard('collection')).toBe('collection')
      expect(getSubscriptionShard('webhook')).toBe('webhook')
    })
  })

  describe('handles wildcard-only patterns', () => {
    it('returns "default" for single wildcard', () => {
      expect(getSubscriptionShard('*')).toBe('default')
    })

    it('returns "default" for double wildcard', () => {
      expect(getSubscriptionShard('**')).toBe('default')
    })

    it('returns "default" for wildcard prefix patterns', () => {
      // Note: This test verifies the current behavior
      expect(getSubscriptionShard('*.insert')).toBe('default')
      expect(getSubscriptionShard('**.error')).toBe('default')
    })
  })

  describe('handles edge cases', () => {
    it('returns "default" for empty string', () => {
      expect(getSubscriptionShard('')).toBe('default')
    })
  })
})

// ============================================================================
// KNOWN_SUBSCRIPTION_SHARDS Tests
// ============================================================================

describe('KNOWN_SUBSCRIPTION_SHARDS', () => {
  it('includes core event type prefixes', () => {
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('collection')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('rpc')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('do')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('ws')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('webhook')
    expect(KNOWN_SUBSCRIPTION_SHARDS).toContain('default')
  })

  it('has at least 6 known shards', () => {
    expect(KNOWN_SUBSCRIPTION_SHARDS.length).toBeGreaterThanOrEqual(6)
  })
})

// ============================================================================
// handleSubscriptionRoutes - Route Matching Tests
// ============================================================================

describe('handleSubscriptionRoutes - Route Matching', () => {
  let mockEnv: MockEnv

  beforeEach(() => {
    mockEnv = createMockEnv()
  })

  it('returns null for non-subscription routes', async () => {
    const request = createRequest('https://events.do/ingest', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)
    expect(result).toBeNull()
  })

  it('returns null for /events route', async () => {
    const request = createRequest('https://events.do/events', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)
    expect(result).toBeNull()
  })

  it('handles /subscriptions prefix routes', async () => {
    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)
    expect(result).not.toBeNull()
  })

  it('returns 404 for unknown subscription endpoints', async () => {
    const request = createRequest('https://events.do/subscriptions/unknown-endpoint', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)
    expect(result?.status).toBe(404)

    const json = await result?.json()
    expect(json.error).toBe('Unknown subscription endpoint')
  })
})

// ============================================================================
// POST /subscriptions/subscribe Tests
// ============================================================================

describe('POST /subscriptions/subscribe', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      subscribe: vi.fn().mockResolvedValue({ ok: true, id: 'SUB123ABC' }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  describe('successful subscription', () => {
    it('creates subscription with required fields', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          pattern: 'collection.insert',
          rpcMethod: 'handleEvent',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.ok).toBe(true)
      expect(json.id).toBe('SUB123ABC')
    })

    it('creates subscription with optional fields', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          workerBinding: 'MY_SERVICE',
          pattern: 'collection.*',
          rpcMethod: 'handleEvent',
          maxRetries: 3,
          timeoutMs: 5000,
        },
      })
      const url = new URL(request.url)

      await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(mockDO.subscribe).toHaveBeenCalledWith({
        workerId: 'my-worker',
        workerBinding: 'MY_SERVICE',
        pattern: 'collection.*',
        rpcMethod: 'handleEvent',
        maxRetries: 3,
        timeoutMs: 5000,
      })
    })

    it('routes to correct shard based on pattern', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          pattern: 'webhook.github.*',
          rpcMethod: 'handleWebhook',
        },
      })
      const url = new URL(request.url)

      await handleSubscriptionRoutes(request, mockEnv as any, url)

      // Should route to webhook shard
      expect(mockEnv.SUBSCRIPTIONS.idFromName).toHaveBeenCalled()
    })
  })

  describe('validation errors', () => {
    it('returns 400 for invalid JSON body', async () => {
      const request = new Request('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: 'not-json',
        headers: { 'Content-Type': 'application/json' },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toBe('Invalid JSON body')
    })

    it('returns 400 for missing workerId', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          pattern: 'collection.insert',
          rpcMethod: 'handleEvent',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('workerId')
    })

    it('returns 400 for missing pattern', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          rpcMethod: 'handleEvent',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('pattern')
    })

    it('returns 400 for missing rpcMethod', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          pattern: 'collection.insert',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('rpcMethod')
    })

    it('returns 400 for invalid pattern', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          pattern: 'collection@invalid',
          rpcMethod: 'handleEvent',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('Invalid pattern')
    })

    it('returns 400 for invalid maxRetries type', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          pattern: 'collection.insert',
          rpcMethod: 'handleEvent',
          maxRetries: 'three',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('maxRetries')
    })

    it('returns 400 for invalid timeoutMs type', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: {
          workerId: 'my-worker',
          pattern: 'collection.insert',
          rpcMethod: 'handleEvent',
          timeoutMs: 'fast',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('timeoutMs')
    })

    it('returns 400 for array body instead of object', async () => {
      const request = createRequest('https://events.do/subscriptions/subscribe', {
        method: 'POST',
        body: ['not', 'an', 'object'],
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toBe('Invalid JSON body')
    })
  })
})

// ============================================================================
// POST /subscriptions/unsubscribe Tests
// ============================================================================

describe('POST /subscriptions/unsubscribe', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      unsubscribe: vi.fn().mockResolvedValue({ ok: true }),
      getSubscription: vi.fn().mockResolvedValue({ id: 'SUB123', active: true }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('unsubscribes with provided shard', async () => {
    const request = createRequest('https://events.do/subscriptions/unsubscribe', {
      method: 'POST',
      body: {
        subscriptionId: 'SUB123',
        shard: 'collection',
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.ok).toBe(true)
    expect(mockDO.unsubscribe).toHaveBeenCalledWith('SUB123')
  })

  it('searches all shards when shard not provided', async () => {
    const request = createRequest('https://events.do/subscriptions/unsubscribe', {
      method: 'POST',
      body: {
        subscriptionId: 'SUB123',
      },
    })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.getSubscription).toHaveBeenCalledWith('SUB123')
  })

  it('returns 404 when subscription not found', async () => {
    mockDO.getSubscription = vi.fn().mockResolvedValue(null)
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/unsubscribe', {
      method: 'POST',
      body: {
        subscriptionId: 'NOTFOUND',
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(404)
    const json = await result?.json()
    expect(json.error).toBe('Subscription not found')
  })

  it('returns 400 for missing subscriptionId', async () => {
    const request = createRequest('https://events.do/subscriptions/unsubscribe', {
      method: 'POST',
      body: {},
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(400)
    const json = await result?.json()
    expect(json.error).toContain('subscriptionId')
  })
})

// ============================================================================
// POST /subscriptions/reactivate Tests
// ============================================================================

describe('POST /subscriptions/reactivate', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      reactivate: vi.fn().mockResolvedValue({ ok: true }),
      getSubscription: vi.fn().mockResolvedValue({ id: 'SUB123', active: false }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('reactivates subscription with provided shard', async () => {
    const request = createRequest('https://events.do/subscriptions/reactivate', {
      method: 'POST',
      body: {
        subscriptionId: 'SUB123',
        shard: 'collection',
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.ok).toBe(true)
  })

  it('returns 404 when subscription not found', async () => {
    mockDO.getSubscription = vi.fn().mockResolvedValue(null)
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/reactivate', {
      method: 'POST',
      body: {
        subscriptionId: 'NOTFOUND',
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(404)
  })
})

// ============================================================================
// GET /subscriptions/list Tests
// ============================================================================

describe('GET /subscriptions/list', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      listSubscriptions: vi.fn().mockResolvedValue([
        { id: 'SUB1', pattern: 'collection.*', createdAt: 1000 },
        { id: 'SUB2', pattern: 'webhook.*', createdAt: 2000 },
      ]),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('lists all subscriptions without filters', async () => {
    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.subscriptions).toBeDefined()
  })

  it('filters by active status', async () => {
    const request = createRequest('https://events.do/subscriptions/list?active=true', { method: 'GET' })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.listSubscriptions).toHaveBeenCalledWith(
      expect.objectContaining({ active: true })
    )
  })

  it('filters by workerId', async () => {
    const request = createRequest('https://events.do/subscriptions/list?workerId=my-worker', { method: 'GET' })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.listSubscriptions).toHaveBeenCalledWith(
      expect.objectContaining({ workerId: 'my-worker' })
    )
  })

  it('filters by patternPrefix', async () => {
    const request = createRequest('https://events.do/subscriptions/list?patternPrefix=collection', { method: 'GET' })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.listSubscriptions).toHaveBeenCalledWith(
      expect.objectContaining({ patternPrefix: 'collection' })
    )
  })

  it('applies pagination with limit and offset', async () => {
    const request = createRequest('https://events.do/subscriptions/list?limit=10&offset=5', { method: 'GET' })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.listSubscriptions).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 10, offset: 5 })
    )
  })

  it('queries specific shard when provided', async () => {
    const request = createRequest('https://events.do/subscriptions/list?shard=collection', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)
    const json = await result?.json()

    expect(json.shard).toBeDefined()
  })

  it('sorts results by createdAt descending', async () => {
    mockDO.listSubscriptions = vi.fn().mockResolvedValue([
      { id: 'SUB1', pattern: 'a.*', createdAt: 1000 },
      { id: 'SUB2', pattern: 'b.*', createdAt: 3000 },
      { id: 'SUB3', pattern: 'c.*', createdAt: 2000 },
    ])
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)
    const json = await result?.json()

    // Should be sorted by createdAt descending
    const subscriptions = json.subscriptions
    for (let i = 0; i < subscriptions.length - 1; i++) {
      expect(subscriptions[i].createdAt).toBeGreaterThanOrEqual(subscriptions[i + 1].createdAt)
    }
  })
})

// ============================================================================
// GET /subscriptions/match Tests
// ============================================================================

describe('GET /subscriptions/match', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      findMatchingSubscriptions: vi.fn().mockResolvedValue([
        { id: 'SUB1', pattern: 'collection.*' },
      ]),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('returns matching subscriptions for event type', async () => {
    const request = createRequest('https://events.do/subscriptions/match?eventType=collection.insert', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.subscriptions).toBeDefined()
    expect(mockDO.findMatchingSubscriptions).toHaveBeenCalledWith('collection.insert')
  })

  it('returns 400 when eventType is missing', async () => {
    const request = createRequest('https://events.do/subscriptions/match', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(400)
    const json = await result?.json()
    expect(json.error).toContain('eventType')
  })
})

// ============================================================================
// GET /subscriptions/status/:id Tests
// ============================================================================

describe('GET /subscriptions/status/:id', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      getSubscriptionStatus: vi.fn().mockResolvedValue({
        subscription: { id: 'SUB123', pattern: 'collection.*', active: true },
        stats: {
          pendingDeliveries: 5,
          failedDeliveries: 1,
          deadLetters: 0,
          successRate: 95,
          totalDelivered: 100,
          totalAttempts: 105,
        },
      }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('returns subscription status with stats', async () => {
    const request = createRequest('https://events.do/subscriptions/status/SUB123', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.subscription).toBeDefined()
    expect(json.stats).toBeDefined()
    expect(json.stats.pendingDeliveries).toBe(5)
  })

  it('returns empty stats when subscription not found', async () => {
    mockDO.getSubscriptionStatus = vi.fn().mockResolvedValue({
      subscription: null,
      stats: { pendingDeliveries: 0, failedDeliveries: 0, deadLetters: 0, successRate: 0, totalDelivered: 0, totalAttempts: 0 },
    })
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/status/NOTFOUND', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.subscription).toBeNull()
    expect(json.stats.pendingDeliveries).toBe(0)
  })
})

// ============================================================================
// GET /subscriptions/:id Tests
// ============================================================================

describe('GET /subscriptions/:id', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      getSubscription: vi.fn().mockResolvedValue({
        id: 'SUB123',
        pattern: 'collection.*',
        workerId: 'my-worker',
        rpcMethod: 'handleEvent',
        active: true,
      }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('returns subscription by ID', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.subscription).toBeDefined()
    expect(json.subscription.id).toBe('SUB123')
  })

  it('returns 404 when subscription not found', async () => {
    mockDO.getSubscription = vi.fn().mockResolvedValue(null)
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/NOTFOUND', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(404)
    const json = await result?.json()
    expect(json.error).toBe('Subscription not found')
  })
})

// ============================================================================
// PUT /subscriptions/:id Tests
// ============================================================================

describe('PUT /subscriptions/:id', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      getSubscription: vi.fn().mockResolvedValue({
        id: 'SUB123',
        pattern: 'collection.*',
        active: true,
      }),
      updateSubscription: vi.fn().mockResolvedValue({ ok: true }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('updates subscription settings', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', {
      method: 'PUT',
      body: {
        maxRetries: 5,
        timeoutMs: 10000,
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    expect(mockDO.updateSubscription).toHaveBeenCalledWith('SUB123', {
      maxRetries: 5,
      timeoutMs: 10000,
      rpcMethod: undefined,
    })
  })

  it('updates rpcMethod', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', {
      method: 'PUT',
      body: {
        rpcMethod: 'newHandler',
      },
    })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.updateSubscription).toHaveBeenCalledWith('SUB123', {
      maxRetries: undefined,
      timeoutMs: undefined,
      rpcMethod: 'newHandler',
    })
  })

  it('returns 404 when subscription not found', async () => {
    mockDO.getSubscription = vi.fn().mockResolvedValue(null)
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/NOTFOUND', {
      method: 'PUT',
      body: { maxRetries: 5 },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(404)
  })

  it('returns 400 for invalid maxRetries type', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', {
      method: 'PUT',
      body: { maxRetries: 'invalid' },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(400)
    const json = await result?.json()
    expect(json.error).toContain('maxRetries')
  })

  it('returns 400 for invalid timeoutMs type', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', {
      method: 'PUT',
      body: { timeoutMs: 'fast' },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(400)
    const json = await result?.json()
    expect(json.error).toContain('timeoutMs')
  })

  it('returns 400 for invalid rpcMethod type', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', {
      method: 'PUT',
      body: { rpcMethod: 123 },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(400)
    const json = await result?.json()
    expect(json.error).toContain('rpcMethod')
  })
})

// ============================================================================
// DELETE /subscriptions/:id Tests
// ============================================================================

describe('DELETE /subscriptions/:id', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      getSubscription: vi.fn().mockResolvedValue({
        id: 'SUB123',
        pattern: 'collection.*',
      }),
      deleteSubscription: vi.fn().mockResolvedValue({ ok: true }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('deletes subscription by ID', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123', { method: 'DELETE' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    expect(mockDO.deleteSubscription).toHaveBeenCalledWith('SUB123')
  })

  it('returns 404 when subscription not found', async () => {
    mockDO.getSubscription = vi.fn().mockResolvedValue(null)
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/NOTFOUND', { method: 'DELETE' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(404)
    const json = await result?.json()
    expect(json.error).toBe('Subscription not found')
  })
})

// ============================================================================
// GET /subscriptions/:id/dead-letters Tests
// ============================================================================

describe('GET /subscriptions/:id/dead-letters', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      getSubscription: vi.fn().mockResolvedValue({ id: 'SUB123' }),
      getDeadLetters: vi.fn().mockResolvedValue([
        { id: 'DL1', subscriptionId: 'SUB123', eventType: 'collection.insert', error: 'Timeout' },
        { id: 'DL2', subscriptionId: 'SUB123', eventType: 'collection.update', error: 'Connection refused' },
      ]),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('returns dead letters for subscription', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123/dead-letters', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.deadLetters).toHaveLength(2)
  })

  it('applies limit parameter', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123/dead-letters?limit=10', { method: 'GET' })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockDO.getDeadLetters).toHaveBeenCalledWith('SUB123', 10)
  })

  it('returns empty array when subscription not found', async () => {
    mockDO.getSubscription = vi.fn().mockResolvedValue(null)
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/NOTFOUND/dead-letters', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.deadLetters).toEqual([])
  })
})

// ============================================================================
// POST /subscriptions/:id/dead-letters/:deadLetterId/retry Tests
// ============================================================================

describe('POST /subscriptions/:id/dead-letters/:deadLetterId/retry', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      retryDeadLetter: vi.fn().mockResolvedValue({ ok: true }),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('retries dead letter delivery', async () => {
    const request = createRequest('https://events.do/subscriptions/SUB123/dead-letters/DL456/retry', { method: 'POST' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    expect(mockDO.retryDeadLetter).toHaveBeenCalledWith('DL456')
  })

  it('returns 404 when dead letter not found', async () => {
    mockDO.retryDeadLetter = vi.fn().mockResolvedValue({ ok: false })
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/SUB123/dead-letters/NOTFOUND/retry', { method: 'POST' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(404)
    const json = await result?.json()
    expect(json.ok).toBe(false)
  })
})

// ============================================================================
// GET /subscriptions/deliveries/:id/logs Tests
// ============================================================================

describe('GET /subscriptions/deliveries/:id/logs', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv

  beforeEach(() => {
    mockDO = createMockSubscriptionDO({
      getDeliveryLogs: vi.fn().mockResolvedValue([
        { attempt: 1, status: 'failed', error: 'Timeout', timestamp: 1000 },
        { attempt: 2, status: 'success', timestamp: 2000 },
      ]),
    })
    mockEnv = createMockEnv(mockDO)
  })

  it('returns delivery logs', async () => {
    const request = createRequest('https://events.do/subscriptions/deliveries/DEL123/logs', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.logs).toHaveLength(2)
  })

  it('returns empty logs when delivery not found', async () => {
    mockDO.getDeliveryLogs = vi.fn().mockResolvedValue([])
    mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/deliveries/NOTFOUND/logs', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(200)
    const json = await result?.json()
    expect(json.logs).toEqual([])
  })
})

// ============================================================================
// Namespace Isolation Tests
// ============================================================================

describe('Namespace Isolation', () => {
  let mockDO: MockSubscriptionDO
  let mockEnv: MockEnv
  let tenant: TenantContext

  beforeEach(() => {
    mockDO = createMockSubscriptionDO()
    mockEnv = createMockEnv(mockDO)
    tenant = createTenantContext('acme', false)
  })

  it('includes namespace in subscribe response', async () => {
    const request = createRequest('https://events.do/subscriptions/subscribe', {
      method: 'POST',
      body: {
        workerId: 'my-worker',
        pattern: 'collection.insert',
        rpcMethod: 'handleEvent',
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url, tenant)

    const json = await result?.json()
    expect(json.namespace).toBe('acme')
  })

  it('includes namespace in list response', async () => {
    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url, tenant)

    const json = await result?.json()
    expect(json.namespace).toBe('acme')
  })

  it('includes namespace in match response', async () => {
    const request = createRequest('https://events.do/subscriptions/match?eventType=collection.insert', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url, tenant)

    const json = await result?.json()
    expect(json.namespace).toBe('acme')
  })

  it('admin tenant returns namespace when provided', async () => {
    const adminTenant = createTenantContext('admin', true)
    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url, adminTenant)

    const json = await result?.json()
    // Admin tenant still has their namespace in the response
    expect(json.namespace).toBe('admin')
  })

  it('returns null namespace when no tenant provided', async () => {
    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url, undefined)

    const json = await result?.json()
    expect(json.namespace).toBeNull()
  })
})

// ============================================================================
// Shard Management Routes Tests
// ============================================================================

describe('Shard Management Routes', () => {
  describe('GET /subscriptions/shards', () => {
    it('returns shard statistics when coordinator is available', async () => {
      const mockCoordinator = createMockShardCoordinator({
        getStats: vi.fn().mockResolvedValue({
          totalShards: 8,
          prefixStates: {
            collection: { activeShardCount: 2 },
            webhook: { activeShardCount: 1 },
          },
        }),
      })
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards', { method: 'GET' })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.dynamicShardingEnabled).toBe(true)
      expect(json.totalShards).toBe(8)
    })

    it('returns fallback info when coordinator not available', async () => {
      const mockEnv = createMockEnv(undefined, null)

      const request = createRequest('https://events.do/subscriptions/shards', { method: 'GET' })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.dynamicShardingEnabled).toBe(false)
      expect(json.fallbackShards).toBeDefined()
    })
  })

  describe('GET /subscriptions/shards/config', () => {
    it('returns coordinator configuration', async () => {
      const mockCoordinator = createMockShardCoordinator({
        getConfig: vi.fn().mockResolvedValue({
          minShardsPerPrefix: 1,
          maxShardsPerPrefix: 16,
          scaleUpThreshold: 1000,
          scaleDownThreshold: 100,
        }),
      })
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/config', { method: 'GET' })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.dynamicShardingEnabled).toBe(true)
      expect(json.config.minShardsPerPrefix).toBe(1)
    })

    it('returns disabled status when coordinator not available', async () => {
      const mockEnv = createMockEnv(undefined, null)

      const request = createRequest('https://events.do/subscriptions/shards/config', { method: 'GET' })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.dynamicShardingEnabled).toBe(false)
    })
  })

  describe('PUT /subscriptions/shards/config', () => {
    it('updates coordinator configuration', async () => {
      const mockCoordinator = createMockShardCoordinator({
        updateConfig: vi.fn().mockResolvedValue({
          minShardsPerPrefix: 2,
          maxShardsPerPrefix: 32,
        }),
      })
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/config', {
        method: 'PUT',
        body: {
          minShardsPerPrefix: 2,
          maxShardsPerPrefix: 32,
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.ok).toBe(true)
    })

    it('returns 400 for invalid config keys', async () => {
      const mockCoordinator = createMockShardCoordinator()
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/config', {
        method: 'PUT',
        body: {
          invalidKey: 'value',
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.invalidKeys).toContain('invalidKey')
    })

    it('returns 400 when coordinator not available', async () => {
      const mockEnv = createMockEnv(undefined, null)

      const request = createRequest('https://events.do/subscriptions/shards/config', {
        method: 'PUT',
        body: { minShardsPerPrefix: 2 },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
    })
  })

  describe('POST /subscriptions/shards/scale', () => {
    it('force scales a prefix', async () => {
      const mockCoordinator = createMockShardCoordinator({
        forceScale: vi.fn().mockResolvedValue({
          scaled: true,
          direction: 'up',
          previousCount: 1,
          newCount: 4,
          prefix: 'collection',
        }),
      })
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/scale', {
        method: 'POST',
        body: {
          prefix: 'collection',
          targetCount: 4,
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.ok).toBe(true)
      expect(json.newCount).toBe(4)
    })

    it('returns 400 for missing prefix', async () => {
      const mockCoordinator = createMockShardCoordinator()
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/scale', {
        method: 'POST',
        body: { targetCount: 4 },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('prefix')
    })

    it('returns 400 for invalid targetCount', async () => {
      const mockCoordinator = createMockShardCoordinator()
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/scale', {
        method: 'POST',
        body: {
          prefix: 'collection',
          targetCount: 0,
        },
      })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(400)
      const json = await result?.json()
      expect(json.error).toContain('targetCount')
    })
  })

  describe('POST /subscriptions/shards/health-check', () => {
    it('starts health checks', async () => {
      const mockCoordinator = createMockShardCoordinator()
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/health-check', { method: 'POST' })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.ok).toBe(true)
      expect(mockCoordinator.startHealthChecks).toHaveBeenCalled()
    })
  })

  describe('GET /subscriptions/shards/:prefix', () => {
    it('returns shards for a specific prefix', async () => {
      const mockCoordinator = createMockShardCoordinator({
        getActiveShards: vi.fn().mockResolvedValue(['collection:0', 'collection:1']),
      })
      const mockEnv = createMockEnv(undefined, mockCoordinator)

      const request = createRequest('https://events.do/subscriptions/shards/collection', { method: 'GET' })
      const url = new URL(request.url)

      const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

      expect(result?.status).toBe(200)
      const json = await result?.json()
      expect(json.prefix).toBe('collection')
      expect(json.shards).toBeDefined()
    })
  })
})

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Error Handling', () => {
  it('returns 500 for internal errors', async () => {
    const mockDO = createMockSubscriptionDO({
      listSubscriptions: vi.fn().mockRejectedValue(new Error('Database connection failed')),
    })
    const mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(500)
    const json = await result?.json()
    expect(json.ok).toBe(false)
    expect(json.error).toBe('Database connection failed')
  })

  it('handles unknown errors gracefully', async () => {
    const mockDO = createMockSubscriptionDO({
      listSubscriptions: vi.fn().mockRejectedValue('Unknown error string'),
    })
    const mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.status).toBe(500)
    const json = await result?.json()
    expect(json.error).toBe('Unknown error')
  })
})

// ============================================================================
// CORS Headers Tests
// ============================================================================

describe('CORS Headers', () => {
  it('includes CORS headers in successful responses', async () => {
    const mockDO = createMockSubscriptionDO()
    const mockEnv = createMockEnv(mockDO)

    const request = createRequest('https://events.do/subscriptions/list', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.headers.get('Access-Control-Allow-Origin')).toBeDefined()
    expect(result?.headers.get('Content-Type')).toBe('application/json')
  })

  it('includes CORS headers in error responses', async () => {
    const mockEnv = createMockEnv()

    const request = createRequest('https://events.do/subscriptions/subscribe', {
      method: 'POST',
      body: {},
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(result?.headers.get('Access-Control-Allow-Origin')).toBeDefined()
  })
})

// ============================================================================
// Dynamic Sharding Integration Tests
// ============================================================================

describe('Dynamic Sharding Integration', () => {
  it('includes dynamicSharding flag in subscribe response', async () => {
    const mockCoordinator = createMockShardCoordinator()
    const mockDO = createMockSubscriptionDO()
    const mockEnv = createMockEnv(mockDO, mockCoordinator)

    const request = createRequest('https://events.do/subscriptions/subscribe', {
      method: 'POST',
      body: {
        workerId: 'my-worker',
        pattern: 'collection.insert',
        rpcMethod: 'handleEvent',
      },
    })
    const url = new URL(request.url)

    const result = await handleSubscriptionRoutes(request, mockEnv as any, url)

    const json = await result?.json()
    expect(json.dynamicSharding).toBe(true)
  })

  it('uses coordinator for shard routing when available', async () => {
    const mockCoordinator = createMockShardCoordinator({
      getRoutingShard: vi.fn().mockResolvedValue('collection:2'),
    })
    const mockDO = createMockSubscriptionDO()
    const mockEnv = createMockEnv(mockDO, mockCoordinator)

    const request = createRequest('https://events.do/subscriptions/subscribe', {
      method: 'POST',
      body: {
        workerId: 'my-worker',
        pattern: 'collection.insert',
        rpcMethod: 'handleEvent',
      },
    })
    const url = new URL(request.url)

    await handleSubscriptionRoutes(request, mockEnv as any, url)

    expect(mockCoordinator.getRoutingShard).toHaveBeenCalledWith('collection', undefined, undefined)
  })
})
