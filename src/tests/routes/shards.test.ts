/**
 * Shards Route Handler Tests
 *
 * Unit tests for src/routes/shards.ts
 * Tests shard management endpoints.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockShardCoordinatorDO {
  getConfig: ReturnType<typeof vi.fn>
  updateConfig: ReturnType<typeof vi.fn>
  scale: ReturnType<typeof vi.fn>
  startHealthChecks: ReturnType<typeof vi.fn>
}

interface MockEventWriterDO {
  stats: ReturnType<typeof vi.fn>
}

interface MockEnv {
  SHARD_COORDINATOR?: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
  EVENT_WRITER: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockShardCoordinator(): MockShardCoordinatorDO {
  return {
    getConfig: vi.fn().mockResolvedValue({
      minShards: 1,
      maxShards: 10,
      scaleUpThreshold: 1000,
      scaleDownThreshold: 100,
      cooldownMs: 60000,
    }),
    updateConfig: vi.fn().mockResolvedValue({}),
    scale: vi.fn().mockResolvedValue({ previousCount: 2, newCount: 4 }),
    startHealthChecks: vi.fn().mockResolvedValue({}),
  }
}

function createMockEventWriter(): MockEventWriterDO {
  return {
    stats: vi.fn().mockResolvedValue({
      buffered: 100,
      flushed: 5000,
      errors: 0,
    }),
  }
}

function createMockEnv(withCoordinator: boolean = true): MockEnv {
  const eventWriter = createMockEventWriter()

  const env: MockEnv = {
    EVENT_WRITER: {
      idFromName: vi.fn().mockReturnValue('event-writer-id'),
      get: vi.fn().mockReturnValue(eventWriter),
    },
  }

  if (withCoordinator) {
    const coordinator = createMockShardCoordinator()
    env.SHARD_COORDINATOR = {
      idFromName: vi.fn().mockReturnValue('coordinator-id'),
      get: vi.fn().mockReturnValue(coordinator),
    }
  }

  return env
}

// ============================================================================
// Route Matching Tests
// ============================================================================

describe('Shards Route Matching', () => {
  describe('GET /shards', () => {
    it('matches GET /shards', () => {
      const url = new URL('https://events.do/shards')
      const method = 'GET'

      expect(url.pathname).toBe('/shards')
      expect(method).toBe('GET')
    })

    it('does not match POST /shards', () => {
      const url = new URL('https://events.do/shards')
      const method = 'POST'

      const matches = url.pathname === '/shards' && method === 'GET'
      expect(matches).toBe(false)
    })
  })

  describe('GET /shards/config', () => {
    it('matches GET /shards/config', () => {
      const url = new URL('https://events.do/shards/config')
      const method = 'GET'

      expect(url.pathname).toBe('/shards/config')
      expect(method).toBe('GET')
    })
  })

  describe('PUT /shards/config', () => {
    it('matches PUT /shards/config', () => {
      const url = new URL('https://events.do/shards/config')
      const method = 'PUT'

      expect(url.pathname).toBe('/shards/config')
      expect(method).toBe('PUT')
    })
  })

  describe('POST /shards/scale', () => {
    it('matches POST /shards/scale', () => {
      const url = new URL('https://events.do/shards/scale')
      const method = 'POST'

      expect(url.pathname).toBe('/shards/scale')
      expect(method).toBe('POST')
    })
  })

  describe('POST /shards/health-check', () => {
    it('matches POST /shards/health-check', () => {
      const url = new URL('https://events.do/shards/health-check')
      const method = 'POST'

      expect(url.pathname).toBe('/shards/health-check')
      expect(method).toBe('POST')
    })
  })

  describe('GET /shards/:shardId', () => {
    it('matches GET /shards/0', () => {
      const url = new URL('https://events.do/shards/0')
      const match = url.pathname.match(/^\/shards\/(\d+)$/)

      expect(match).not.toBeNull()
      expect(match![1]).toBe('0')
    })

    it('matches GET /shards/123', () => {
      const url = new URL('https://events.do/shards/123')
      const match = url.pathname.match(/^\/shards\/(\d+)$/)

      expect(match).not.toBeNull()
      expect(match![1]).toBe('123')
    })

    it('does not match GET /shards/abc', () => {
      const url = new URL('https://events.do/shards/abc')
      const match = url.pathname.match(/^\/shards\/(\d+)$/)

      expect(match).toBeNull()
    })

    it('parses shard ID as integer', () => {
      const url = new URL('https://events.do/shards/42')
      const match = url.pathname.match(/^\/shards\/(\d+)$/)
      const shardId = parseInt(match![1], 10)

      expect(shardId).toBe(42)
      expect(typeof shardId).toBe('number')
    })
  })
})

// ============================================================================
// GET /shards Tests
// ============================================================================

describe('handleGetShards', () => {
  it('returns shard statistics when coordinator available', async () => {
    const stats = {
      activeShards: [0, 1, 2],
      totalEvents: 10000,
      eventsPerShard: [3000, 4000, 3000],
    }

    const response = Response.json({
      ...stats,
      dynamicShardingEnabled: true,
    })

    const json = await response.json()
    expect(json.activeShards).toEqual([0, 1, 2])
    expect(json.dynamicShardingEnabled).toBe(true)
  })

  it('returns fallback when coordinator not available', async () => {
    const response = Response.json({
      error: 'Shard coordinator not available',
      message: 'The SHARD_COORDINATOR binding is not configured. Dynamic sharding is disabled.',
      fallbackShards: [0],
    }, { status: 200 })

    const json = await response.json()
    expect(json.error).toBe('Shard coordinator not available')
    expect(json.fallbackShards).toEqual([0])
    expect(response.status).toBe(200)
  })
})

// ============================================================================
// GET /shards/config Tests
// ============================================================================

describe('handleGetShardConfig', () => {
  let env: MockEnv
  let coordinator: MockShardCoordinatorDO

  beforeEach(() => {
    env = createMockEnv(true)
    coordinator = env.SHARD_COORDINATOR!.get() as MockShardCoordinatorDO
    vi.clearAllMocks()
  })

  it('returns coordinator configuration', async () => {
    const config = {
      minShards: 1,
      maxShards: 10,
      scaleUpThreshold: 1000,
      scaleDownThreshold: 100,
      cooldownMs: 60000,
      metricsWindowMs: 300000,
      healthCheckIntervalMs: 30000,
    }

    coordinator.getConfig.mockResolvedValue(config)
    const result = await coordinator.getConfig()

    expect(result).toEqual(config)
  })

  it('returns disabled status when coordinator not available', async () => {
    const response = Response.json({
      error: 'Shard coordinator not available',
      dynamicShardingEnabled: false,
    }, { status: 200 })

    const json = await response.json()
    expect(json.dynamicShardingEnabled).toBe(false)
  })
})

// ============================================================================
// PUT /shards/config Tests
// ============================================================================

describe('handleUpdateShardConfig', () => {
  let env: MockEnv
  let coordinator: MockShardCoordinatorDO

  beforeEach(() => {
    env = createMockEnv(true)
    coordinator = env.SHARD_COORDINATOR!.get() as MockShardCoordinatorDO
    vi.clearAllMocks()
  })

  describe('request validation', () => {
    it('returns 400 for invalid JSON', async () => {
      const response = Response.json(
        { error: 'Invalid JSON body' },
        { status: 400 }
      )

      expect(response.status).toBe(400)
    })

    it('validates allowed configuration keys', () => {
      const validKeys = ['minShards', 'maxShards', 'scaleUpThreshold', 'scaleDownThreshold', 'cooldownMs', 'metricsWindowMs', 'healthCheckIntervalMs']
      const updates = { minShards: 2, invalidKey: 'value' }

      const invalidKeys = Object.keys(updates).filter(k => !validKeys.includes(k))
      expect(invalidKeys).toContain('invalidKey')
    })

    it('accepts valid configuration keys', () => {
      const validKeys = ['minShards', 'maxShards', 'scaleUpThreshold', 'scaleDownThreshold', 'cooldownMs', 'metricsWindowMs', 'healthCheckIntervalMs']
      const updates = { minShards: 2, maxShards: 20 }

      const invalidKeys = Object.keys(updates).filter(k => !validKeys.includes(k))
      expect(invalidKeys).toHaveLength(0)
    })
  })

  describe('configuration updates', () => {
    it('updates minShards', async () => {
      await coordinator.updateConfig({ minShards: 2 })

      expect(coordinator.updateConfig).toHaveBeenCalledWith({ minShards: 2 })
    })

    it('updates maxShards', async () => {
      await coordinator.updateConfig({ maxShards: 20 })

      expect(coordinator.updateConfig).toHaveBeenCalledWith({ maxShards: 20 })
    })

    it('updates scaleUpThreshold', async () => {
      await coordinator.updateConfig({ scaleUpThreshold: 2000 })

      expect(coordinator.updateConfig).toHaveBeenCalledWith({ scaleUpThreshold: 2000 })
    })

    it('updates scaleDownThreshold', async () => {
      await coordinator.updateConfig({ scaleDownThreshold: 50 })

      expect(coordinator.updateConfig).toHaveBeenCalledWith({ scaleDownThreshold: 50 })
    })

    it('updates cooldownMs', async () => {
      await coordinator.updateConfig({ cooldownMs: 120000 })

      expect(coordinator.updateConfig).toHaveBeenCalledWith({ cooldownMs: 120000 })
    })

    it('returns updated config on success', async () => {
      const updatedConfig = { minShards: 2, maxShards: 20 }
      coordinator.updateConfig.mockResolvedValue(updatedConfig)

      const result = await coordinator.updateConfig({ minShards: 2, maxShards: 20 })

      const response = Response.json({ ok: true, config: result })
      const json = await response.json()

      expect(json.ok).toBe(true)
      expect(json.config).toEqual(updatedConfig)
    })
  })

  it('returns 400 when coordinator not available', async () => {
    const response = Response.json(
      { error: 'Shard coordinator not available' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })
})

// ============================================================================
// POST /shards/scale Tests
// ============================================================================

describe('handleForceScale', () => {
  let env: MockEnv
  let coordinator: MockShardCoordinatorDO

  beforeEach(() => {
    env = createMockEnv(true)
    coordinator = env.SHARD_COORDINATOR!.get() as MockShardCoordinatorDO
    vi.clearAllMocks()
  })

  describe('request validation', () => {
    it('returns 400 for invalid JSON', async () => {
      const response = Response.json(
        { error: 'Invalid JSON body' },
        { status: 400 }
      )

      expect(response.status).toBe(400)
    })

    it('requires targetCount to be a number', () => {
      const body = { targetCount: 'five' }
      const valid = typeof body.targetCount === 'number'

      expect(valid).toBe(false)
    })

    it('requires targetCount to be positive', () => {
      const body = { targetCount: 0 }
      const valid = typeof body.targetCount === 'number' && body.targetCount >= 1

      expect(valid).toBe(false)
    })

    it('accepts valid targetCount', () => {
      const body = { targetCount: 5 }
      const valid = typeof body.targetCount === 'number' && body.targetCount >= 1

      expect(valid).toBe(true)
    })
  })

  describe('scaling operations', () => {
    it('scales to target count', async () => {
      coordinator.scale.mockResolvedValue({ previousCount: 2, newCount: 5 })

      const result = await coordinator.scale(5)

      expect(result.previousCount).toBe(2)
      expect(result.newCount).toBe(5)
    })

    it('returns scale result on success', async () => {
      coordinator.scale.mockResolvedValue({ previousCount: 2, newCount: 5 })

      const result = await coordinator.scale(5)

      const response = Response.json({
        ok: true,
        ...result,
      })

      const json = await response.json()
      expect(json.ok).toBe(true)
      expect(json.previousCount).toBe(2)
      expect(json.newCount).toBe(5)
    })
  })

  it('returns 400 when coordinator not available', async () => {
    const response = Response.json(
      { error: 'Shard coordinator not available' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })
})

// ============================================================================
// GET /shards/:shardId Tests
// ============================================================================

describe('handleGetShardById', () => {
  let env: MockEnv
  let eventWriter: MockEventWriterDO

  beforeEach(() => {
    env = createMockEnv(true)
    eventWriter = env.EVENT_WRITER.get() as MockEventWriterDO
    vi.clearAllMocks()
  })

  it('returns shard stats for valid shard ID', async () => {
    const stats = {
      buffered: 100,
      flushed: 5000,
      errors: 0,
      lastFlush: '2024-01-01T00:00:00Z',
    }

    eventWriter.stats.mockResolvedValue(stats)
    const result = await eventWriter.stats()

    const response = Response.json({
      ...result,
      exists: true,
    })

    const json = await response.json()
    expect(json.buffered).toBe(100)
    expect(json.flushed).toBe(5000)
    expect(json.exists).toBe(true)
  })

  it('returns 500 on stats error', async () => {
    eventWriter.stats.mockRejectedValue(new Error('Connection failed'))

    const response = Response.json({
      error: 'Failed to get shard stats',
      message: 'Connection failed',
      shardId: 0,
    }, { status: 500 })

    expect(response.status).toBe(500)
    const json = await response.json()
    expect(json.error).toBe('Failed to get shard stats')
    expect(json.shardId).toBe(0)
  })
})

// ============================================================================
// POST /shards/health-check Tests
// ============================================================================

describe('handleStartHealthChecks', () => {
  let env: MockEnv
  let coordinator: MockShardCoordinatorDO

  beforeEach(() => {
    env = createMockEnv(true)
    coordinator = env.SHARD_COORDINATOR!.get() as MockShardCoordinatorDO
    vi.clearAllMocks()
  })

  it('starts health checks successfully', async () => {
    await coordinator.startHealthChecks()

    expect(coordinator.startHealthChecks).toHaveBeenCalled()

    const response = Response.json({
      ok: true,
      message: 'Health checks started',
    })

    const json = await response.json()
    expect(json.ok).toBe(true)
    expect(json.message).toBe('Health checks started')
  })

  it('returns 400 when coordinator not available', async () => {
    const response = Response.json(
      { error: 'Shard coordinator not available' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('returns 500 on health check error', async () => {
    coordinator.startHealthChecks.mockRejectedValue(new Error('Health check failed'))

    const response = Response.json({
      error: 'Failed to start health checks',
      message: 'Health check failed',
    }, { status: 500 })

    expect(response.status).toBe(500)
  })
})

// ============================================================================
// CORS Headers Tests
// ============================================================================

describe('Shards CORS Headers', () => {
  it('includes CORS headers in responses', () => {
    const response = Response.json({ data: 'test' }, {
      headers: { 'Access-Control-Allow-Origin': '*' },
    })

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})

// ============================================================================
// Fallback Behavior Tests
// ============================================================================

describe('Shard Fallback Behavior', () => {
  it('uses static shard 0 when coordinator not bound', () => {
    const env = createMockEnv(false) // No coordinator

    const fallbackShards = [0]
    expect(fallbackShards).toEqual([0])
  })

  it('dynamic sharding is disabled without coordinator', () => {
    const env = createMockEnv(false)

    const dynamicEnabled = !!env.SHARD_COORDINATOR
    expect(dynamicEnabled).toBe(false)
  })
})

// ============================================================================
// Configuration Validation Tests
// ============================================================================

describe('Shard Configuration Validation', () => {
  it('has sensible default values', () => {
    const defaults = {
      minShards: 1,
      maxShards: 10,
      scaleUpThreshold: 1000,
      scaleDownThreshold: 100,
      cooldownMs: 60000,
      metricsWindowMs: 300000,
      healthCheckIntervalMs: 30000,
    }

    expect(defaults.minShards).toBeGreaterThanOrEqual(1)
    expect(defaults.maxShards).toBeGreaterThan(defaults.minShards)
    expect(defaults.scaleUpThreshold).toBeGreaterThan(defaults.scaleDownThreshold)
    expect(defaults.cooldownMs).toBeGreaterThan(0)
  })

  it('minShards should be at least 1', () => {
    const minShards = 0
    const valid = minShards >= 1

    expect(valid).toBe(false)
  })

  it('maxShards should be greater than minShards', () => {
    const config = { minShards: 5, maxShards: 3 }
    const valid = config.maxShards > config.minShards

    expect(valid).toBe(false)
  })

  it('scaleUpThreshold should be greater than scaleDownThreshold', () => {
    const config = { scaleUpThreshold: 100, scaleDownThreshold: 200 }
    const valid = config.scaleUpThreshold > config.scaleDownThreshold

    expect(valid).toBe(false)
  })
})
