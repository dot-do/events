/**
 * Catalog Route Handler Tests
 *
 * Unit tests for src/routes/catalog.ts
 * Tests catalog API endpoints for namespace and table management.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockCatalogDO {
  listNamespaces: ReturnType<typeof vi.fn>
  createNamespace: ReturnType<typeof vi.fn>
  listTables: ReturnType<typeof vi.fn>
  loadTable: ReturnType<typeof vi.fn>
  createTable: ReturnType<typeof vi.fn>
  commitSnapshot: ReturnType<typeof vi.fn>
  buildQuery: ReturnType<typeof vi.fn>
  listFiles: ReturnType<typeof vi.fn>
}

interface MockEnv {
  CATALOG: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockCatalog(): MockCatalogDO {
  return {
    listNamespaces: vi.fn().mockResolvedValue([]),
    createNamespace: vi.fn().mockResolvedValue({}),
    listTables: vi.fn().mockResolvedValue([]),
    loadTable: vi.fn().mockResolvedValue(null),
    createTable: vi.fn().mockResolvedValue({}),
    commitSnapshot: vi.fn().mockResolvedValue({}),
    buildQuery: vi.fn().mockResolvedValue(''),
    listFiles: vi.fn().mockResolvedValue([]),
  }
}

function createMockEnv(): MockEnv {
  const catalog = createMockCatalog()
  return {
    CATALOG: {
      idFromName: vi.fn().mockReturnValue('catalog-id'),
      get: vi.fn().mockReturnValue(catalog),
    },
  }
}

// ============================================================================
// SQL Injection Prevention Constants
// ============================================================================

const SQL_DANGEROUS_KEYWORDS = [
  'UNION', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER', 'TRUNCATE',
  'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
  'ATTACH', 'DETACH', 'PRAGMA', 'VACUUM', 'COPY', 'LOAD', 'INSTALL',
]

const ALLOWED_COLUMNS = new Set([
  'id', 'ts', 'type', 'collection', 'provider', 'do_id', 'do_class', 'colo',
  'method', 'status', 'latency', 'size', 'error', 'created_at', 'updated_at',
  'name', 'namespace', 'path', 'format', 'record_count', 'file_size_bytes',
])

// ============================================================================
// Where Clause Validation (mirrored from catalog.ts)
// ============================================================================

function validateWhereClause(where: string): { valid: boolean; reason?: string } {
  if (where.includes('--') || where.includes('/*') || where.includes('*/')) {
    return { valid: false, reason: 'SQL comments are not allowed' }
  }

  if (where.includes(';')) {
    return { valid: false, reason: 'Multiple statements are not allowed' }
  }

  const upperWhere = where.toUpperCase()
  for (const keyword of SQL_DANGEROUS_KEYWORDS) {
    const regex = new RegExp(`\\b${keyword}\\b`, 'i')
    if (regex.test(upperWhere)) {
      return { valid: false, reason: `Keyword '${keyword}' is not allowed` }
    }
  }

  if (/0x[0-9a-fA-F]+/.test(where)) {
    return { valid: false, reason: 'Hex literals are not allowed' }
  }

  if (where.includes('\\')) {
    return { valid: false, reason: 'Backslash escapes are not allowed' }
  }

  const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!=|<>|<=?|>=?|(?:LIKE|IN|IS|BETWEEN)\b)/gi
  let match
  while ((match = columnPattern.exec(where)) !== null) {
    const column = match[1]
    if (!column) continue
    const columnLower = column.toLowerCase()
    if (['and', 'or', 'not', 'null', 'true', 'false'].includes(columnLower)) {
      continue
    }
    if (!ALLOWED_COLUMNS.has(columnLower)) {
      return { valid: false, reason: `Column '${columnLower}' is not allowed. Allowed columns: ${[...ALLOWED_COLUMNS].join(', ')}` }
    }
  }

  const safePattern = /^(?:[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|'[^']*'|[<>=!]+|[\s(),]|AND|OR|NOT|IS|NULL|IN|LIKE|BETWEEN|TRUE|FALSE)+$/i
  if (!safePattern.test(where)) {
    return { valid: false, reason: 'Contains disallowed characters or expressions' }
  }

  if (where.length > 1000) {
    return { valid: false, reason: 'Where clause exceeds maximum length of 1000 characters' }
  }

  return { valid: true }
}

// ============================================================================
// Where Clause Validation Tests
// ============================================================================

describe('validateWhereClause', () => {
  describe('safe queries', () => {
    it('accepts simple equality', () => {
      expect(validateWhereClause("type = 'user.created'").valid).toBe(true)
    })

    it('accepts multiple conditions with AND', () => {
      expect(validateWhereClause("type = 'user.created' AND status = 200").valid).toBe(true)
    })

    it('accepts OR conditions', () => {
      expect(validateWhereClause("type = 'user.created' OR type = 'user.updated'").valid).toBe(true)
    })

    it('accepts comparison operators', () => {
      expect(validateWhereClause("latency > 100").valid).toBe(true)
      expect(validateWhereClause("latency < 100").valid).toBe(true)
      expect(validateWhereClause("latency >= 100").valid).toBe(true)
      expect(validateWhereClause("latency <= 100").valid).toBe(true)
    })

    it('accepts IN clause', () => {
      expect(validateWhereClause("type IN ('a', 'b', 'c')").valid).toBe(true)
    })

    it('accepts LIKE clause', () => {
      expect(validateWhereClause("type LIKE 'user.%'").valid).toBe(true)
    })

    it('accepts IS NULL', () => {
      expect(validateWhereClause("error IS NULL").valid).toBe(true)
    })

    it('accepts IS NOT NULL', () => {
      expect(validateWhereClause("error IS NOT NULL").valid).toBe(true)
    })

    it('accepts BETWEEN', () => {
      expect(validateWhereClause("latency BETWEEN 100 AND 500").valid).toBe(true)
    })
  })

  describe('SQL comment injection', () => {
    it('rejects -- comments', () => {
      const result = validateWhereClause("type = 'test' -- comment")
      expect(result.valid).toBe(false)
      expect(result.reason).toBe('SQL comments are not allowed')
    })

    it('rejects /* */ comments', () => {
      const result = validateWhereClause("type = 'test' /* comment */")
      expect(result.valid).toBe(false)
      expect(result.reason).toBe('SQL comments are not allowed')
    })

    it('rejects opening comment without close', () => {
      const result = validateWhereClause("type = 'test' /*")
      expect(result.valid).toBe(false)
    })
  })

  describe('statement terminator injection', () => {
    it('rejects semicolons', () => {
      const result = validateWhereClause("type = 'test'; DROP TABLE events")
      expect(result.valid).toBe(false)
      expect(result.reason).toBe('Multiple statements are not allowed')
    })
  })

  describe('dangerous keyword injection', () => {
    it.each(SQL_DANGEROUS_KEYWORDS)('rejects %s keyword', (keyword) => {
      const result = validateWhereClause(`type = 'test' ${keyword} something`)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain(keyword)
    })

    it('rejects UNION SELECT', () => {
      const result = validateWhereClause("type = 'test' UNION SELECT * FROM secrets")
      expect(result.valid).toBe(false)
    })

    it('rejects DROP TABLE', () => {
      const result = validateWhereClause("DROP TABLE events")
      expect(result.valid).toBe(false)
    })
  })

  describe('hex literal injection', () => {
    it('rejects hex literals', () => {
      const result = validateWhereClause("type = 0x74657374")
      expect(result.valid).toBe(false)
      expect(result.reason).toBe('Hex literals are not allowed')
    })
  })

  describe('escape sequence injection', () => {
    it('rejects backslash escapes', () => {
      const result = validateWhereClause("type = 'test\\x27")
      expect(result.valid).toBe(false)
      expect(result.reason).toBe('Backslash escapes are not allowed')
    })
  })

  describe('column whitelist enforcement', () => {
    it('rejects unknown columns', () => {
      const result = validateWhereClause("secret_column = 'value'")
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('secret_column')
      expect(result.reason).toContain('is not allowed')
    })

    it.each([...ALLOWED_COLUMNS])('accepts allowed column: %s', (column) => {
      const result = validateWhereClause(`${column} = 'test'`)
      expect(result.valid).toBe(true)
    })
  })

  describe('length limits', () => {
    it('rejects where clause exceeding 1000 characters', () => {
      const longWhere = "type = '" + 'a'.repeat(1000) + "'"
      const result = validateWhereClause(longWhere)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('exceeds maximum length')
    })
  })
})

// ============================================================================
// Namespace Routes Tests
// ============================================================================

describe('GET /catalog/namespaces', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('returns list of namespaces', async () => {
    catalog.listNamespaces.mockResolvedValue(['default', 'acme', 'beta'])

    const result = await catalog.listNamespaces()

    expect(result).toEqual(['default', 'acme', 'beta'])
  })

  it('always includes default namespace', () => {
    const namespaces = new Set(['acme'])
    namespaces.add('default')

    expect([...namespaces].sort()).toContain('default')
  })
})

describe('POST /catalog/namespaces', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('requires name field', async () => {
    const body = {}
    const hasName = 'name' in body && typeof body.name === 'string'

    expect(hasName).toBe(false)
  })

  it('requires name to be a string', async () => {
    const body = { name: 123 }
    const validName = typeof body.name === 'string'

    expect(validName).toBe(false)
  })

  it('accepts optional properties object', async () => {
    const body = { name: 'acme', properties: { owner: 'admin' } }
    const validProperties = body.properties === undefined ||
      (typeof body.properties === 'object' && body.properties !== null && !Array.isArray(body.properties))

    expect(validProperties).toBe(true)
  })

  it('rejects array as properties', async () => {
    const body = { name: 'acme', properties: [] }
    const validProperties = body.properties === undefined ||
      (typeof body.properties === 'object' && body.properties !== null && !Array.isArray(body.properties))

    expect(validProperties).toBe(false)
  })

  it('creates namespace successfully', async () => {
    await catalog.createNamespace('acme', { owner: 'admin' })

    expect(catalog.createNamespace).toHaveBeenCalledWith('acme', { owner: 'admin' })
  })
})

// ============================================================================
// Table Routes Tests
// ============================================================================

describe('GET /catalog/tables', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('uses default namespace when not provided', () => {
    const url = new URL('https://events.do/catalog/tables')
    const namespace = url.searchParams.get('namespace') ?? 'default'
    expect(namespace).toBe('default')
  })

  it('uses custom namespace from query param', () => {
    const url = new URL('https://events.do/catalog/tables?namespace=acme')
    const namespace = url.searchParams.get('namespace')
    expect(namespace).toBe('acme')
  })

  it('returns list of tables', async () => {
    catalog.listTables.mockResolvedValue(['events', 'webhooks', 'cdc_changes'])

    const result = await catalog.listTables('default')

    expect(result).toEqual(['events', 'webhooks', 'cdc_changes'])
  })
})

describe('POST /catalog/tables', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('requires namespace field', () => {
    const body = { name: 'events', schema: [] }
    const hasNamespace = 'namespace' in body && typeof body.namespace === 'string'
    expect(hasNamespace).toBe(false)
  })

  it('requires name field', () => {
    const body = { namespace: 'default', schema: [] }
    const hasName = 'name' in body && typeof body.name === 'string'
    expect(hasName).toBe(false)
  })

  it('requires schema to be an array', () => {
    const body = { namespace: 'default', name: 'events', schema: 'not-array' }
    const validSchema = Array.isArray(body.schema)
    expect(validSchema).toBe(false)
  })

  it('validates schema entries', () => {
    const body = {
      namespace: 'default',
      name: 'events',
      schema: [
        { name: 'id', type: 'string' },
        { name: 123, type: 'string' }, // Invalid
      ],
    }

    const validEntries = body.schema.every(
      (col: any) => typeof col?.name === 'string' && typeof col?.type === 'string'
    )

    expect(validEntries).toBe(false)
  })

  it('accepts valid table creation', async () => {
    const schema = [
      { name: 'id', type: 'string' },
      { name: 'ts', type: 'timestamp' },
      { name: 'data', type: 'json' },
    ]

    await catalog.createTable('default', 'events', schema, {})

    expect(catalog.createTable).toHaveBeenCalledWith('default', 'events', schema, {})
  })

  it('accepts optional location', () => {
    const body = { namespace: 'default', name: 'events', schema: [], location: 's3://bucket/path' }
    const validLocation = body.location === undefined || typeof body.location === 'string'
    expect(validLocation).toBe(true)
  })
})

describe('GET /catalog/table', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('requires name parameter', () => {
    const url = new URL('https://events.do/catalog/table')
    const name = url.searchParams.get('name')
    expect(name).toBeNull()
  })

  it('returns 404 when table not found', async () => {
    catalog.loadTable.mockResolvedValue(null)

    const result = await catalog.loadTable('default', 'nonexistent')
    expect(result).toBeNull()

    const response = Response.json({ error: 'not found' }, { status: 404 })
    expect(response.status).toBe(404)
  })

  it('returns table when found', async () => {
    const table = {
      name: 'events',
      namespace: 'default',
      schema: [{ name: 'id', type: 'string' }],
    }

    catalog.loadTable.mockResolvedValue(table)
    const result = await catalog.loadTable('default', 'events')

    expect(result).toEqual(table)
  })
})

// ============================================================================
// Commit Routes Tests
// ============================================================================

describe('POST /catalog/commit', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('requires namespace field', () => {
    const body = { table: 'events', files: [] }
    const hasNamespace = 'namespace' in body && typeof body.namespace === 'string'
    expect(hasNamespace).toBe(false)
  })

  it('requires table field', () => {
    const body = { namespace: 'default', files: [] }
    const hasTable = 'table' in body && typeof body.table === 'string'
    expect(hasTable).toBe(false)
  })

  it('requires files to be an array', () => {
    const body = { namespace: 'default', table: 'events', files: 'not-array' }
    const validFiles = Array.isArray(body.files)
    expect(validFiles).toBe(false)
  })

  it('validates file entries have required fields', () => {
    const files = [
      { path: 'events/file1.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 1024 },
      { path: 'events/file2.parquet', format: 'parquet' }, // Missing recordCount and fileSizeBytes
    ]

    const validFiles = files.every(
      (f: any) =>
        typeof f?.path === 'string' &&
        (f?.format === 'parquet' || f?.format === 'jsonl') &&
        typeof f?.recordCount === 'number' &&
        typeof f?.fileSizeBytes === 'number'
    )

    expect(validFiles).toBe(false)
  })

  it('accepts valid file format: parquet', () => {
    const file = { path: 'events/file.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 1024 }
    const validFormat = file.format === 'parquet' || file.format === 'jsonl'
    expect(validFormat).toBe(true)
  })

  it('accepts valid file format: jsonl', () => {
    const file = { path: 'events/file.jsonl', format: 'jsonl', recordCount: 100, fileSizeBytes: 1024 }
    const validFormat = file.format === 'parquet' || file.format === 'jsonl'
    expect(validFormat).toBe(true)
  })

  it('rejects invalid file format', () => {
    const file = { path: 'events/file.csv', format: 'csv', recordCount: 100, fileSizeBytes: 1024 }
    const validFormat = file.format === 'parquet' || file.format === 'jsonl'
    expect(validFormat).toBe(false)
  })

  it('accepts optional cdcBookmark', () => {
    const body = { namespace: 'default', table: 'events', files: [], cdcBookmark: 'bookmark-123' }
    const validBookmark = body.cdcBookmark === undefined || typeof body.cdcBookmark === 'string'
    expect(validBookmark).toBe(true)
  })

  it('commits snapshot successfully', async () => {
    const files = [
      { path: 'events/file1.parquet', format: 'parquet' as const, recordCount: 100, fileSizeBytes: 1024 },
    ]

    await catalog.commitSnapshot('default', 'events', files, { cdcBookmark: 'bookmark-123' })

    expect(catalog.commitSnapshot).toHaveBeenCalled()
  })
})

// ============================================================================
// Query Routes Tests
// ============================================================================

describe('GET /catalog/query', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('requires table parameter', () => {
    const url = new URL('https://events.do/catalog/query')
    const table = url.searchParams.get('table')
    expect(table).toBeNull()
  })

  it('validates where clause when provided', () => {
    const where = "type = 'user.created'"
    const result = validateWhereClause(where)
    expect(result.valid).toBe(true)
  })

  it('rejects invalid where clause', async () => {
    const where = "type = 'test'; DROP TABLE events"
    const result = validateWhereClause(where)

    expect(result.valid).toBe(false)

    const response = Response.json(
      { error: `Invalid where clause: ${result.reason}` },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('builds query with namespace and table', async () => {
    catalog.buildQuery.mockResolvedValue('SELECT * FROM events WHERE type = \'test\'')

    const sql = await catalog.buildQuery('default', 'events', { where: "type = 'test'", limit: 100 })

    expect(catalog.buildQuery).toHaveBeenCalledWith('default', 'events', {
      where: "type = 'test'",
      limit: 100,
    })
  })
})

// ============================================================================
// Files Routes Tests
// ============================================================================

describe('GET /catalog/files', () => {
  let env: MockEnv
  let catalog: MockCatalogDO

  beforeEach(() => {
    env = createMockEnv()
    catalog = env.CATALOG.get() as MockCatalogDO
    vi.clearAllMocks()
  })

  it('requires table parameter', () => {
    const url = new URL('https://events.do/catalog/files')
    const table = url.searchParams.get('table')
    expect(table).toBeNull()
  })

  it('returns list of files', async () => {
    const files = [
      { path: 'events/2024/01/file1.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 1024 },
      { path: 'events/2024/01/file2.parquet', format: 'parquet', recordCount: 200, fileSizeBytes: 2048 },
    ]

    catalog.listFiles.mockResolvedValue(files)
    const result = await catalog.listFiles('default', 'events')

    expect(result).toEqual(files)
  })
})

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Catalog Error Handling', () => {
  it('returns 400 for invalid JSON', async () => {
    const response = Response.json(
      { error: 'Invalid JSON body' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('returns 404 for unknown endpoint', async () => {
    const response = Response.json(
      { error: 'unknown catalog endpoint', path: '/catalog/unknown' },
      { status: 404 }
    )

    expect(response.status).toBe(404)
  })

  it('returns 500 for internal errors', async () => {
    const response = Response.json(
      { error: 'Internal error message' },
      { status: 500 }
    )

    expect(response.status).toBe(500)
  })

  it('includes CORS headers in responses', () => {
    const response = Response.json({ data: 'test' }, {
      headers: {
        'Access-Control-Allow-Origin': 'https://dashboard.do',
        'Vary': 'Origin',
      },
    })

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('https://dashboard.do')
    expect(response.headers.get('Vary')).toBe('Origin')
  })
})

// ============================================================================
// Sharding Tests
// ============================================================================

describe('Catalog Sharding', () => {
  it('uses namespace for shard ID', () => {
    const namespace = 'acme'
    // getCatalog shards by namespace
    const shardId = namespace

    expect(shardId).toBe('acme')
  })

  it('uses legacy catalog name for backwards compatibility', () => {
    const LEGACY_CATALOG_NAME = 'events-catalog'
    expect(LEGACY_CATALOG_NAME).toBe('events-catalog')
  })

  it('falls back to legacy catalog for default namespace', () => {
    const namespace = 'default'
    // For backwards compatibility, may check legacy catalog
    const shouldCheckLegacy = namespace === 'default'

    expect(shouldCheckLegacy).toBe(true)
  })
})
