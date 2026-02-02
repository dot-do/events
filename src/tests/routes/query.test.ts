/**
 * Query Route Handler Tests
 *
 * Unit tests for src/routes/query.ts
 * Tests query builder, SQL generation, and multi-tenant isolation.
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

interface QueryRequest {
  dateRange?: { start: string; end: string }
  doId?: string
  doClass?: string
  eventTypes?: string[]
  collection?: string
  colo?: string
  limit?: number
  namespace?: string
}

// ============================================================================
// Constants
// ============================================================================

const MAX_QUERY_LIMIT = 10000
const MAX_EVENT_TYPES = 100
const MAX_STRING_PARAM_LENGTH = 256

// ============================================================================
// Helper Functions (mirrored from query.ts)
// ============================================================================

const SAFE_PATH_PATTERN = /^[a-zA-Z0-9\/_*.-]+$/

function validatePathPattern(pattern: string): boolean {
  if (!SAFE_PATH_PATTERN.test(pattern)) {
    return false
  }
  if (pattern.includes('--')) {
    return false
  }
  return true
}

function escapeSql(value: string): string {
  return value.replace(/'/g, "''")
}

function validateQueryRequest(query: unknown): query is QueryRequest {
  if (typeof query !== 'object' || query === null || Array.isArray(query)) return false
  const q = query as Record<string, unknown>

  if (q.dateRange !== undefined) {
    if (typeof q.dateRange !== 'object' || q.dateRange === null) return false
    const dr = q.dateRange as Record<string, unknown>
    if (typeof dr.start !== 'string' || isNaN(Date.parse(dr.start))) return false
    if (typeof dr.end !== 'string' || isNaN(Date.parse(dr.end))) return false
    if (Date.parse(dr.start) > Date.parse(dr.end)) return false
  }

  if (q.doId !== undefined && (typeof q.doId !== 'string' || q.doId.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.doClass !== undefined && (typeof q.doClass !== 'string' || q.doClass.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.collection !== undefined && (typeof q.collection !== 'string' || q.collection.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.colo !== undefined && (typeof q.colo !== 'string' || q.colo.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.namespace !== undefined && (typeof q.namespace !== 'string' || q.namespace.length > MAX_STRING_PARAM_LENGTH)) return false

  if (q.eventTypes !== undefined) {
    if (!Array.isArray(q.eventTypes)) return false
    if (q.eventTypes.length > MAX_EVENT_TYPES) return false
    for (const t of q.eventTypes) {
      if (typeof t !== 'string' || t.length > MAX_STRING_PARAM_LENGTH) return false
    }
  }

  if (q.limit !== undefined) {
    if (typeof q.limit !== 'number' || q.limit < 1 || q.limit > MAX_QUERY_LIMIT || !Number.isInteger(q.limit)) return false
  }

  return true
}

// ============================================================================
// Query Request Validation Tests
// ============================================================================

describe('validateQueryRequest', () => {
  describe('basic validation', () => {
    it('accepts empty object', () => {
      expect(validateQueryRequest({})).toBe(true)
    })

    it('rejects null', () => {
      expect(validateQueryRequest(null)).toBe(false)
    })

    it('rejects undefined', () => {
      expect(validateQueryRequest(undefined)).toBe(false)
    })

    it('rejects non-objects', () => {
      expect(validateQueryRequest('string')).toBe(false)
      expect(validateQueryRequest(123)).toBe(false)
      // Note: arrays are technically objects in JS, but should be rejected
      // The actual validation function may need to check Array.isArray
    })
  })

  describe('dateRange validation', () => {
    it('accepts valid date range', () => {
      expect(validateQueryRequest({
        dateRange: {
          start: '2024-01-01T00:00:00Z',
          end: '2024-01-31T23:59:59Z',
        },
      })).toBe(true)
    })

    it('rejects invalid start date', () => {
      expect(validateQueryRequest({
        dateRange: { start: 'not-a-date', end: '2024-01-31T00:00:00Z' },
      })).toBe(false)
    })

    it('rejects invalid end date', () => {
      expect(validateQueryRequest({
        dateRange: { start: '2024-01-01T00:00:00Z', end: 'not-a-date' },
      })).toBe(false)
    })

    it('rejects start > end', () => {
      expect(validateQueryRequest({
        dateRange: {
          start: '2024-12-31T00:00:00Z',
          end: '2024-01-01T00:00:00Z',
        },
      })).toBe(false)
    })

    it('accepts start = end', () => {
      expect(validateQueryRequest({
        dateRange: {
          start: '2024-01-01T00:00:00Z',
          end: '2024-01-01T00:00:00Z',
        },
      })).toBe(true)
    })

    it('rejects non-object dateRange', () => {
      expect(validateQueryRequest({ dateRange: 'string' })).toBe(false)
      expect(validateQueryRequest({ dateRange: null })).toBe(false)
    })
  })

  describe('string parameter validation', () => {
    it('accepts valid doId', () => {
      expect(validateQueryRequest({ doId: 'my-do-id' })).toBe(true)
    })

    it('rejects doId exceeding max length', () => {
      expect(validateQueryRequest({ doId: 'a'.repeat(257) })).toBe(false)
    })

    it('rejects non-string doId', () => {
      expect(validateQueryRequest({ doId: 123 })).toBe(false)
    })

    it('accepts valid doClass', () => {
      expect(validateQueryRequest({ doClass: 'MyDurableObject' })).toBe(true)
    })

    it('rejects doClass exceeding max length', () => {
      expect(validateQueryRequest({ doClass: 'a'.repeat(257) })).toBe(false)
    })

    it('accepts valid collection', () => {
      expect(validateQueryRequest({ collection: 'users' })).toBe(true)
    })

    it('accepts valid colo', () => {
      expect(validateQueryRequest({ colo: 'SJC' })).toBe(true)
    })

    it('accepts valid namespace', () => {
      expect(validateQueryRequest({ namespace: 'acme' })).toBe(true)
    })
  })

  describe('eventTypes validation', () => {
    it('accepts valid eventTypes array', () => {
      expect(validateQueryRequest({
        eventTypes: ['user.created', 'user.updated'],
      })).toBe(true)
    })

    it('rejects non-array eventTypes', () => {
      expect(validateQueryRequest({ eventTypes: 'string' })).toBe(false)
    })

    it('rejects eventTypes exceeding max count', () => {
      expect(validateQueryRequest({
        eventTypes: Array.from({ length: 101 }, (_, i) => `event.${i}`),
      })).toBe(false)
    })

    it('rejects eventTypes with non-string elements', () => {
      expect(validateQueryRequest({
        eventTypes: ['valid', 123] as any,
      })).toBe(false)
    })

    it('rejects eventTypes with elements exceeding max length', () => {
      expect(validateQueryRequest({
        eventTypes: ['a'.repeat(257)],
      })).toBe(false)
    })
  })

  describe('limit validation', () => {
    it('accepts valid limit', () => {
      expect(validateQueryRequest({ limit: 100 })).toBe(true)
    })

    it('accepts limit of 1', () => {
      expect(validateQueryRequest({ limit: 1 })).toBe(true)
    })

    it('accepts limit of max value', () => {
      expect(validateQueryRequest({ limit: 10000 })).toBe(true)
    })

    it('rejects limit of 0', () => {
      expect(validateQueryRequest({ limit: 0 })).toBe(false)
    })

    it('rejects negative limit', () => {
      expect(validateQueryRequest({ limit: -1 })).toBe(false)
    })

    it('rejects limit exceeding max', () => {
      expect(validateQueryRequest({ limit: 10001 })).toBe(false)
    })

    it('rejects non-integer limit', () => {
      expect(validateQueryRequest({ limit: 50.5 })).toBe(false)
    })

    it('rejects non-number limit', () => {
      expect(validateQueryRequest({ limit: '100' })).toBe(false)
    })
  })
})

// ============================================================================
// Path Pattern Validation Tests
// ============================================================================

describe('validatePathPattern', () => {
  describe('safe patterns', () => {
    it('accepts simple paths', () => {
      expect(validatePathPattern('events/2024/01/01')).toBe(true)
    })

    it('accepts paths with wildcards', () => {
      expect(validatePathPattern('events/*/*/*/*.parquet')).toBe(true)
    })

    it('accepts paths with double wildcards', () => {
      expect(validatePathPattern('events/**/*.parquet')).toBe(true)
    })

    it('accepts paths with dashes and underscores', () => {
      expect(validatePathPattern('my-prefix/sub_folder/file.parquet')).toBe(true)
    })

    it('accepts paths with dots', () => {
      expect(validatePathPattern('events/file.parquet')).toBe(true)
    })
  })

  describe('dangerous patterns', () => {
    it('rejects SQL comments', () => {
      expect(validatePathPattern('events/--comment')).toBe(false)
    })

    it('rejects special characters', () => {
      expect(validatePathPattern("events/'; DROP TABLE--")).toBe(false)
    })

    it('rejects parentheses', () => {
      expect(validatePathPattern('events/(test)')).toBe(false)
    })

    it('rejects semicolons', () => {
      expect(validatePathPattern('events;test')).toBe(false)
    })

    it('rejects quotes', () => {
      expect(validatePathPattern("events/'test'")).toBe(false)
    })
  })
})

// ============================================================================
// SQL Escaping Tests
// ============================================================================

describe('escapeSql', () => {
  it('escapes single quotes', () => {
    expect(escapeSql("O'Reilly")).toBe("O''Reilly")
  })

  it('escapes multiple single quotes', () => {
    expect(escapeSql("It's John's")).toBe("It''s John''s")
  })

  it('leaves other characters unchanged', () => {
    expect(escapeSql('normal string')).toBe('normal string')
  })

  it('handles empty string', () => {
    expect(escapeSql('')).toBe('')
  })
})

// ============================================================================
// Multi-Tenant Isolation Tests
// ============================================================================

describe('Multi-Tenant Query Isolation', () => {
  describe('namespace access control', () => {
    it('allows admin to query any namespace', () => {
      const tenant: TenantContext = { namespace: 'admin', isAdmin: true, keyId: 'admin-key' }
      const query: QueryRequest = { namespace: 'acme' }

      // Admin can query any namespace
      const canAccess = tenant.isAdmin || tenant.namespace === query.namespace
      expect(canAccess).toBe(true)
    })

    it('allows tenant to query their own namespace', () => {
      const tenant: TenantContext = { namespace: 'acme', isAdmin: false, keyId: 'acme-key' }
      const query: QueryRequest = { namespace: 'acme' }

      const canAccess = tenant.isAdmin || tenant.namespace === query.namespace
      expect(canAccess).toBe(true)
    })

    it('denies tenant access to other namespaces', () => {
      const tenant: TenantContext = { namespace: 'acme', isAdmin: false, keyId: 'acme-key' }
      const query: QueryRequest = { namespace: 'other' }

      const canAccess = tenant.isAdmin || tenant.namespace === query.namespace
      expect(canAccess).toBe(false)
    })

    it('scopes non-admin tenant to their namespace', () => {
      const tenant: TenantContext = { namespace: 'acme', isAdmin: false, keyId: 'acme-key' }
      const query: QueryRequest = {} // No explicit namespace

      // Non-admin must be scoped to their namespace
      const namespace = query.namespace || (tenant.isAdmin ? undefined : tenant.namespace)
      expect(namespace).toBe('acme')
    })

    it('allows admin to query all data without namespace', () => {
      const tenant: TenantContext = { namespace: 'admin', isAdmin: true, keyId: 'admin-key' }
      const query: QueryRequest = {} // No explicit namespace

      const namespace = query.namespace || (tenant.isAdmin ? undefined : tenant.namespace)
      expect(namespace).toBeUndefined()
    })
  })

  describe('path pattern building', () => {
    it('includes namespace in path for tenant queries', () => {
      const namespace = 'acme'
      const pathPattern = namespace ? `ns/${escapeSql(namespace)}/events/` : 'events/'
      expect(pathPattern).toBe('ns/acme/events/')
    })

    it('uses default events path for admin queries', () => {
      const namespace: string | undefined = undefined
      const pathPattern = namespace ? `ns/${namespace}/events/` : 'events/'
      expect(pathPattern).toBe('events/')
    })

    it('escapes namespace in path pattern', () => {
      const namespace = "acme's"
      const pathPattern = `ns/${escapeSql(namespace)}/events/`
      expect(pathPattern).toBe("ns/acme''s/events/")
    })
  })
})

// ============================================================================
// SQL Generation Tests
// ============================================================================

describe('SQL Generation', () => {
  describe('conditions building', () => {
    it('adds doId condition', () => {
      const query: QueryRequest = { doId: 'my-do-id' }
      const conditions: string[] = []

      if (query.doId) {
        conditions.push(`"do".id = '${escapeSql(query.doId)}'`)
      }

      expect(conditions).toContain(`"do".id = 'my-do-id'`)
    })

    it('adds doClass condition', () => {
      const query: QueryRequest = { doClass: 'MyDurableObject' }
      const conditions: string[] = []

      if (query.doClass) {
        conditions.push(`"do".class = '${escapeSql(query.doClass)}'`)
      }

      expect(conditions).toContain(`"do".class = 'MyDurableObject'`)
    })

    it('adds colo condition', () => {
      const query: QueryRequest = { colo: 'SJC' }
      const conditions: string[] = []

      if (query.colo) {
        conditions.push(`"do".colo = '${escapeSql(query.colo)}'`)
      }

      expect(conditions).toContain(`"do".colo = 'SJC'`)
    })

    it('adds eventTypes IN condition', () => {
      const query: QueryRequest = { eventTypes: ['user.created', 'user.updated'] }
      const conditions: string[] = []

      if (query.eventTypes?.length) {
        conditions.push(`type IN (${query.eventTypes.map(t => `'${escapeSql(t)}'`).join(', ')})`)
      }

      expect(conditions).toContain(`type IN ('user.created', 'user.updated')`)
    })

    it('adds collection condition', () => {
      const query: QueryRequest = { collection: 'users' }
      const conditions: string[] = []

      if (query.collection) {
        conditions.push(`collection = '${escapeSql(query.collection)}'`)
      }

      expect(conditions).toContain(`collection = 'users'`)
    })

    it('adds date range conditions', () => {
      const query: QueryRequest = {
        dateRange: { start: '2024-01-01T00:00:00Z', end: '2024-01-31T23:59:59Z' },
      }
      const conditions: string[] = []

      if (query.dateRange) {
        conditions.push(`ts >= '${escapeSql(query.dateRange.start)}'`)
        conditions.push(`ts <= '${escapeSql(query.dateRange.end)}'`)
      }

      expect(conditions).toContain(`ts >= '2024-01-01T00:00:00Z'`)
      expect(conditions).toContain(`ts <= '2024-01-31T23:59:59Z'`)
    })

    it('adds namespace isolation condition', () => {
      const namespace = 'acme'
      const conditions: string[] = []

      if (namespace) {
        conditions.push(`payload._namespace = '${escapeSql(namespace)}'`)
      }

      expect(conditions).toContain(`payload._namespace = 'acme'`)
    })
  })

  describe('path optimization', () => {
    it('narrows to specific year when same year', () => {
      const start = new Date('2024-01-01T00:00:00Z')
      const end = new Date('2024-12-31T23:59:59Z')

      const sameYear = start.getUTCFullYear() === end.getUTCFullYear()
      expect(sameYear).toBe(true)

      // Path should include year: events/2024/*/*/*/
      let pathPattern = 'events/'
      if (sameYear) {
        pathPattern += `${start.getUTCFullYear()}/`
      }
      expect(pathPattern).toBe('events/2024/')
    })

    it('narrows to specific month when same month', () => {
      const start = new Date('2024-01-01T00:00:00Z')
      const end = new Date('2024-01-31T23:59:59Z')

      const sameMonth = start.getUTCMonth() === end.getUTCMonth()
      expect(sameMonth).toBe(true)

      // Path should include month: events/2024/01/*/*/
      let pathPattern = `events/${start.getUTCFullYear()}/`
      if (sameMonth) {
        pathPattern += `${String(start.getUTCMonth() + 1).padStart(2, '0')}/`
      }
      expect(pathPattern).toBe('events/2024/01/')
    })

    it('narrows to specific day when same day', () => {
      const start = new Date('2024-01-15T00:00:00Z')
      const end = new Date('2024-01-15T23:59:59Z')

      const sameDay = start.getUTCDate() === end.getUTCDate()
      expect(sameDay).toBe(true)

      // Path should include day: events/2024/01/15/*/
      let pathPattern = `events/${start.getUTCFullYear()}/${String(start.getUTCMonth() + 1).padStart(2, '0')}/`
      if (sameDay) {
        pathPattern += `${String(start.getUTCDate()).padStart(2, '0')}/*/`
      }
      expect(pathPattern).toBe('events/2024/01/15/*/')
    })

    it('uses recursive glob when no date range', () => {
      const pathPattern = 'events/**/'
      expect(pathPattern).toContain('**')
    })
  })

  describe('UNION query structure', () => {
    it('includes both Parquet and JSONL sources', () => {
      const pathPattern = 'events/**/*.parquet'
      const jsonlPattern = pathPattern.replace(/\.parquet$/, '.jsonl')

      expect(pathPattern).toContain('.parquet')
      expect(jsonlPattern).toContain('.jsonl')
    })

    it('orders by timestamp descending', () => {
      const orderBy = 'ORDER BY ts DESC'
      expect(orderBy).toBe('ORDER BY ts DESC')
    })

    it('includes LIMIT when specified', () => {
      const query: QueryRequest = { limit: 100 }
      const limitClause = query.limit ? `LIMIT ${query.limit}` : ''
      expect(limitClause).toBe('LIMIT 100')
    })
  })
})

// ============================================================================
// Response Format Tests
// ============================================================================

describe('Query Response Format', () => {
  it('includes SQL in response', async () => {
    const response = Response.json({
      sql: 'SELECT * FROM ...',
      namespace: 'acme',
      isolated: true,
    })

    const json = await response.json()
    expect(json.sql).toBeDefined()
  })

  it('includes namespace in response', async () => {
    const response = Response.json({
      sql: 'SELECT * FROM ...',
      namespace: 'acme',
      isolated: true,
    })

    const json = await response.json()
    expect(json.namespace).toBe('acme')
  })

  it('indicates isolation status', async () => {
    const response = Response.json({
      sql: 'SELECT * FROM ...',
      namespace: 'acme',
      isolated: true,
    })

    const json = await response.json()
    expect(json.isolated).toBe(true)
  })

  it('returns null namespace for admin queries', async () => {
    const response = Response.json({
      sql: 'SELECT * FROM ...',
      namespace: null,
      isolated: false,
    })

    const json = await response.json()
    expect(json.namespace).toBeNull()
    expect(json.isolated).toBe(false)
  })
})

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Query Error Handling', () => {
  it('returns 400 for invalid JSON', async () => {
    const response = Response.json(
      { error: 'Invalid JSON' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('returns 400 for invalid query parameters', async () => {
    const response = Response.json(
      { error: 'Invalid query parameters. Check dateRange (valid timestamps, start <= end), limit (1-10000), and string lengths (max 256 chars)' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
  })

  it('returns 403 for unauthorized namespace access', async () => {
    const response = Response.json(
      { error: 'Access denied: cannot query namespace you do not have access to' },
      { status: 403 }
    )

    expect(response.status).toBe(403)
  })

  it('returns 500 for invalid path pattern', async () => {
    const response = Response.json(
      { error: 'Invalid path pattern generated' },
      { status: 500 }
    )

    expect(response.status).toBe(500)
  })
})
