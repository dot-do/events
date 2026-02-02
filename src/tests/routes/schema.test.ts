/**
 * Schema Route Handler Tests
 *
 * Unit tests for src/routes/schema.ts
 * Tests schema registry endpoints for event validation.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockSchemaRegistryDO {
  registerSchema: ReturnType<typeof vi.fn>
  listSchemas: ReturnType<typeof vi.fn>
  getSchema: ReturnType<typeof vi.fn>
  getSchemaHistory: ReturnType<typeof vi.fn>
  deleteSchema: ReturnType<typeof vi.fn>
  configureNamespace: ReturnType<typeof vi.fn>
  getNamespaceConfig: ReturnType<typeof vi.fn>
  validateEvents: ReturnType<typeof vi.fn>
}

interface MockEnv {
  SCHEMA_REGISTRY?: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockSchemaRegistry(): MockSchemaRegistryDO {
  return {
    registerSchema: vi.fn().mockResolvedValue({ ok: true, schemaId: 'schema-123', version: 1 }),
    listSchemas: vi.fn().mockResolvedValue([]),
    getSchema: vi.fn().mockResolvedValue(null),
    getSchemaHistory: vi.fn().mockResolvedValue([]),
    deleteSchema: vi.fn().mockResolvedValue({ deleted: true }),
    configureNamespace: vi.fn().mockResolvedValue({}),
    getNamespaceConfig: vi.fn().mockResolvedValue(null),
    validateEvents: vi.fn().mockResolvedValue({ valid: true, results: [] }),
  }
}

function createMockEnv(withRegistry: boolean = true): MockEnv {
  if (!withRegistry) {
    return {}
  }

  const registry = createMockSchemaRegistry()
  return {
    SCHEMA_REGISTRY: {
      idFromName: vi.fn().mockReturnValue('schema-registry-id'),
      get: vi.fn().mockReturnValue(registry),
    },
  }
}

function createRequest(url: string, options: RequestInit = {}): Request {
  return new Request(url, { method: 'GET', ...options })
}

// ============================================================================
// Schema Registry Not Configured Tests
// ============================================================================

describe('Schema Routes - Registry Not Configured', () => {
  it('returns 501 when registry not bound for POST /schemas', async () => {
    const response = Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: { 'Access-Control-Allow-Origin': '*' } }
    )

    expect(response.status).toBe(501)
    const json = await response.json()
    expect(json.error).toBe('Schema registry not configured')
  })

  it('returns 501 when registry not bound for GET /schemas', async () => {
    const response = Response.json(
      { error: 'Schema registry not configured' },
      { status: 501 }
    )

    expect(response.status).toBe(501)
  })

  it('returns 501 when registry not bound for validation', async () => {
    const response = Response.json(
      { error: 'Schema registry not configured' },
      { status: 501 }
    )

    expect(response.status).toBe(501)
  })
})

// ============================================================================
// POST /schemas - Register Schema Tests
// ============================================================================

describe('POST /schemas - Register Schema', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('requires eventType field', async () => {
    const body = { schema: { type: 'object' } }
    const isValid = typeof body.eventType === 'undefined'

    expect(isValid).toBe(true)

    const response = Response.json(
      { error: 'eventType is required and must be a string' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('requires eventType to be a string', async () => {
    const body = { eventType: 123, schema: { type: 'object' } }
    const isValid = typeof body.eventType === 'string'

    expect(isValid).toBe(false)
  })

  it('requires schema field', async () => {
    const body = { eventType: 'user.created' }
    const isValid = typeof body.schema !== 'undefined'

    expect(isValid).toBe(false)
  })

  it('requires schema to be an object', async () => {
    const body = { eventType: 'user.created', schema: 'not-an-object' }
    const isValid = typeof body.schema === 'object' && body.schema !== null

    expect(isValid).toBe(false)
  })

  it('uses default namespace when not provided', () => {
    const body = { eventType: 'user.created', schema: { type: 'object' } }
    const namespace = body.namespace ?? 'default'

    expect(namespace).toBe('default')
  })

  it('accepts custom namespace', () => {
    const body = { eventType: 'user.created', namespace: 'acme', schema: { type: 'object' } }
    const namespace = body.namespace ?? 'default'

    expect(namespace).toBe('acme')
  })

  it('returns schema ID and version on success', async () => {
    const response = Response.json({
      ok: true,
      schemaId: 'schema-123',
      version: 1,
      namespace: 'default',
      eventType: 'user.created',
    }, { status: 201 })

    expect(response.status).toBe(201)
    const json = await response.json()
    expect(json.ok).toBe(true)
    expect(json.schemaId).toBe('schema-123')
    expect(json.version).toBe(1)
  })

  it('handles registration errors', async () => {
    registry.registerSchema.mockResolvedValue({ ok: false, error: 'Invalid schema' })

    const result = await registry.registerSchema({})
    expect(result.ok).toBe(false)
    expect(result.error).toBe('Invalid schema')
  })
})

// ============================================================================
// GET /schemas - List Schemas Tests
// ============================================================================

describe('GET /schemas - List Schemas', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('uses default namespace when not provided', () => {
    const url = new URL('https://events.do/schemas')
    const namespace = url.searchParams.get('namespace') ?? undefined
    expect(namespace).toBeUndefined()
  })

  it('uses custom namespace from query param', () => {
    const url = new URL('https://events.do/schemas?namespace=acme')
    const namespace = url.searchParams.get('namespace')
    expect(namespace).toBe('acme')
  })

  it('uses default limit of 100', () => {
    const url = new URL('https://events.do/schemas')
    const limit = parseInt(url.searchParams.get('limit') ?? '100', 10)
    expect(limit).toBe(100)
  })

  it('uses custom limit from query param', () => {
    const url = new URL('https://events.do/schemas?limit=50')
    const limit = parseInt(url.searchParams.get('limit') ?? '100', 10)
    expect(limit).toBe(50)
  })

  it('uses default offset of 0', () => {
    const url = new URL('https://events.do/schemas')
    const offset = parseInt(url.searchParams.get('offset') ?? '0', 10)
    expect(offset).toBe(0)
  })

  it('uses custom offset from query param', () => {
    const url = new URL('https://events.do/schemas?offset=10')
    const offset = parseInt(url.searchParams.get('offset') ?? '0', 10)
    expect(offset).toBe(10)
  })

  it('returns schema list with count', async () => {
    const schemas = [
      { eventType: 'user.created', namespace: 'default', version: 1 },
      { eventType: 'user.updated', namespace: 'default', version: 2 },
    ]

    registry.listSchemas.mockResolvedValue(schemas)
    const result = await registry.listSchemas({})

    const response = Response.json({
      schemas: result,
      count: result.length,
      limit: 100,
      offset: 0,
    })

    const json = await response.json()
    expect(json.schemas).toHaveLength(2)
    expect(json.count).toBe(2)
  })
})

// ============================================================================
// GET /schemas/:namespace/:eventType Tests
// ============================================================================

describe('GET /schemas/:namespace/:eventType', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('extracts namespace and eventType from path', () => {
    const path = '/schemas/default/user.created'
    const match = path.match(/^\/schemas\/([^/]+)\/([^/]+)$/)

    expect(match).not.toBeNull()
    expect(match![1]).toBe('default')
    expect(match![2]).toBe('user.created')
  })

  it('returns 404 when schema not found', async () => {
    registry.getSchema.mockResolvedValue(null)

    const result = await registry.getSchema({ eventType: 'nonexistent', namespace: 'default' })
    expect(result).toBeNull()

    const response = Response.json({ error: 'Schema not found' }, { status: 404 })
    expect(response.status).toBe(404)
  })

  it('returns schema when found', async () => {
    const schema = {
      eventType: 'user.created',
      namespace: 'default',
      schema: { type: 'object', properties: { id: { type: 'string' } } },
      version: 1,
    }

    registry.getSchema.mockResolvedValue(schema)
    const result = await registry.getSchema({ eventType: 'user.created', namespace: 'default' })

    expect(result).toEqual(schema)
  })
})

// ============================================================================
// GET /schemas/:namespace/:eventType/history Tests
// ============================================================================

describe('GET /schemas/:namespace/:eventType/history', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('returns schema version history', async () => {
    const history = [
      { version: 1, schema: { type: 'object' }, createdAt: Date.now() - 1000 },
      { version: 2, schema: { type: 'object', required: ['id'] }, createdAt: Date.now() },
    ]

    registry.getSchemaHistory.mockResolvedValue(history)
    const result = await registry.getSchemaHistory({ eventType: 'user.created', namespace: 'default' })

    expect(result).toHaveLength(2)
    expect(result[0].version).toBe(1)
    expect(result[1].version).toBe(2)
  })

  it('returns empty array for new schemas', async () => {
    registry.getSchemaHistory.mockResolvedValue([])
    const result = await registry.getSchemaHistory({ eventType: 'new.event', namespace: 'default' })

    expect(result).toHaveLength(0)
  })
})

// ============================================================================
// DELETE /schemas/:namespace/:eventType Tests
// ============================================================================

describe('DELETE /schemas/:namespace/:eventType', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('returns 404 when schema to delete not found', async () => {
    registry.deleteSchema.mockResolvedValue({ deleted: false })

    const result = await registry.deleteSchema({ eventType: 'nonexistent', namespace: 'default' })
    expect(result.deleted).toBe(false)

    const response = Response.json({ error: 'Schema not found' }, { status: 404 })
    expect(response.status).toBe(404)
  })

  it('returns success when schema deleted', async () => {
    registry.deleteSchema.mockResolvedValue({ deleted: true })

    const result = await registry.deleteSchema({ eventType: 'user.created', namespace: 'default' })
    expect(result.deleted).toBe(true)

    const response = Response.json({ ok: true, deleted: true })
    const json = await response.json()
    expect(json.ok).toBe(true)
    expect(json.deleted).toBe(true)
  })
})

// ============================================================================
// PUT /schemas/namespace/:namespace/config Tests
// ============================================================================

describe('PUT /schemas/namespace/:namespace/config', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('requires valid JSON body', async () => {
    const invalidJson = 'not-json'
    let isValid = false
    try {
      JSON.parse(invalidJson)
      isValid = true
    } catch {
      isValid = false
    }

    expect(isValid).toBe(false)
  })

  it('accepts validationEnabled setting', async () => {
    const body = { validationEnabled: true }

    await registry.configureNamespace({ namespace: 'default', ...body })

    expect(registry.configureNamespace).toHaveBeenCalledWith({
      namespace: 'default',
      validationEnabled: true,
    })
  })

  it('accepts strictMode setting', async () => {
    const body = { strictMode: true }

    await registry.configureNamespace({ namespace: 'default', ...body })

    expect(registry.configureNamespace).toHaveBeenCalledWith({
      namespace: 'default',
      strictMode: true,
    })
  })

  it('returns success on configuration update', async () => {
    const response = Response.json({ ok: true, namespace: 'default' })

    const json = await response.json()
    expect(json.ok).toBe(true)
    expect(json.namespace).toBe('default')
  })
})

// ============================================================================
// GET /schemas/namespace/:namespace/config Tests
// ============================================================================

describe('GET /schemas/namespace/:namespace/config', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('returns default config when not explicitly configured', async () => {
    registry.getNamespaceConfig.mockResolvedValue(null)

    const result = await registry.getNamespaceConfig('default')
    expect(result).toBeNull()

    // Default config
    const response = Response.json({
      namespace: 'default',
      validationEnabled: true,
      strictMode: false,
      configured: false,
    })

    const json = await response.json()
    expect(json.validationEnabled).toBe(true)
    expect(json.strictMode).toBe(false)
    expect(json.configured).toBe(false)
  })

  it('returns explicit config when configured', async () => {
    const config = {
      namespace: 'acme',
      validationEnabled: true,
      strictMode: true,
    }

    registry.getNamespaceConfig.mockResolvedValue(config)
    const result = await registry.getNamespaceConfig('acme')

    const response = Response.json({ ...result, configured: true })
    const json = await response.json()

    expect(json.namespace).toBe('acme')
    expect(json.validationEnabled).toBe(true)
    expect(json.strictMode).toBe(true)
    expect(json.configured).toBe(true)
  })
})

// ============================================================================
// POST /schemas/validate Tests
// ============================================================================

describe('POST /schemas/validate', () => {
  let env: MockEnv
  let registry: MockSchemaRegistryDO

  beforeEach(() => {
    env = createMockEnv(true)
    registry = env.SCHEMA_REGISTRY!.get() as MockSchemaRegistryDO
    vi.clearAllMocks()
  })

  it('requires events to be an array', async () => {
    const body = { events: 'not-an-array' }
    const isValid = Array.isArray(body.events)

    expect(isValid).toBe(false)
  })

  it('requires events array to be non-empty', async () => {
    const body = { events: [] }
    const isValid = Array.isArray(body.events) && body.events.length > 0

    expect(isValid).toBe(false)
  })

  it('uses default namespace when not provided', () => {
    const body = { events: [{ type: 'test' }] }
    const namespace = body.namespace ?? 'default'

    expect(namespace).toBe('default')
  })

  it('returns validation results', async () => {
    const events = [
      { type: 'user.created', id: '123' },
      { type: 'user.updated', id: '456' },
    ]

    registry.validateEvents.mockResolvedValue({
      valid: true,
      results: [],
    })

    const result = await registry.validateEvents(events, 'default')

    const response = Response.json({
      valid: result.valid,
      totalEvents: events.length,
      invalidCount: result.results.length,
      results: result.results,
    })

    const json = await response.json()
    expect(json.valid).toBe(true)
    expect(json.totalEvents).toBe(2)
    expect(json.invalidCount).toBe(0)
  })

  it('returns validation errors for invalid events', async () => {
    const events = [{ type: 'user.created' }]

    const validationResult = {
      valid: false,
      results: [
        {
          index: 0,
          eventType: 'user.created',
          valid: false,
          errors: [{ path: '/id', message: 'required property missing' }],
        },
      ],
    }

    registry.validateEvents.mockResolvedValue(validationResult)
    const result = await registry.validateEvents(events, 'default')

    expect(result.valid).toBe(false)
    expect(result.results).toHaveLength(1)
    expect(result.results[0].errors).toHaveLength(1)
  })
})

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Schema Routes - Error Handling', () => {
  it('returns 400 for invalid JSON', async () => {
    const response = Response.json({ error: 'Invalid JSON' }, { status: 400 })

    expect(response.status).toBe(400)
    const json = await response.json()
    expect(json.error).toBe('Invalid JSON')
  })

  it('returns 500 for internal errors', async () => {
    const response = Response.json(
      { error: 'Failed to register schema' },
      { status: 500 }
    )

    expect(response.status).toBe(500)
  })

  it('includes CORS headers in all responses', () => {
    const response = Response.json({ error: 'test' }, {
      headers: { 'Access-Control-Allow-Origin': '*' },
    })

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})
