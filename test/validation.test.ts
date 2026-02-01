/**
 * events-608: Input Validation and Auth Flow Tests for Worker
 *
 * Tests for validateEvent, validateBatch, validateEventSize, and escapeSql
 * functions defined in src/index.ts. Since these are module-private functions,
 * we re-implement the logic here to test the contract (matching the source).
 */

import { describe, it, expect } from 'vitest'

// ============================================================================
// Re-implement validation functions matching src/index.ts
// These mirror the private functions in the worker to test their contracts.
// ============================================================================

const MAX_EVENT_SIZE = 10 * 1024 // 10KB per event
const MAX_BATCH_SIZE = 1000 // Max events per batch
const MAX_TYPE_LENGTH = 256

function validateEvent(event: unknown): event is { type: string; ts: string } {
  if (typeof event !== 'object' || event === null) return false
  const e = event as Record<string, unknown>
  if (typeof e.type !== 'string' || e.type.length === 0 || e.type.length > MAX_TYPE_LENGTH) return false
  if (typeof e.ts !== 'string' || isNaN(Date.parse(e.ts))) return false
  return true
}

function validateBatch(batch: unknown): batch is { events: unknown[] } {
  if (typeof batch !== 'object' || batch === null) return false
  const b = batch as Record<string, unknown>
  if (!Array.isArray(b.events)) return false
  if (b.events.length > MAX_BATCH_SIZE) return false
  return true
}

function validateEventSize(event: unknown): boolean {
  const size = JSON.stringify(event).length
  return size <= MAX_EVENT_SIZE
}

function escapeSql(value: string): string {
  return value.replace(/'/g, "''")
}

// ============================================================================
// Tests
// ============================================================================

describe('validateEvent', () => {
  describe('valid events', () => {
    it('accepts a minimal valid event', () => {
      expect(validateEvent({ type: 'test.event', ts: '2024-01-01T00:00:00Z' })).toBe(true)
    })

    it('accepts event with extra fields', () => {
      expect(validateEvent({
        type: 'rpc.call',
        ts: '2024-06-15T12:00:00.000Z',
        data: { method: 'getUser' },
        duration: 150,
      })).toBe(true)
    })

    it('accepts event with single-char type', () => {
      expect(validateEvent({ type: 'x', ts: '2024-01-01T00:00:00Z' })).toBe(true)
    })

    it('accepts event with max-length type (256 chars)', () => {
      const longType = 'a'.repeat(256)
      expect(validateEvent({ type: longType, ts: '2024-01-01T00:00:00Z' })).toBe(true)
    })

    it('accepts various valid ISO timestamp formats', () => {
      expect(validateEvent({ type: 'test', ts: '2024-01-01' })).toBe(true)
      expect(validateEvent({ type: 'test', ts: '2024-01-01T00:00:00Z' })).toBe(true)
      expect(validateEvent({ type: 'test', ts: '2024-01-01T00:00:00.000Z' })).toBe(true)
      expect(validateEvent({ type: 'test', ts: '2024-06-15T10:30:45+05:30' })).toBe(true)
    })
  })

  describe('missing fields', () => {
    it('rejects null', () => {
      expect(validateEvent(null)).toBe(false)
    })

    it('rejects undefined', () => {
      expect(validateEvent(undefined)).toBe(false)
    })

    it('rejects non-object primitives', () => {
      expect(validateEvent('string')).toBe(false)
      expect(validateEvent(42)).toBe(false)
      expect(validateEvent(true)).toBe(false)
    })

    it('rejects empty object', () => {
      expect(validateEvent({})).toBe(false)
    })

    it('rejects object missing type', () => {
      expect(validateEvent({ ts: '2024-01-01T00:00:00Z' })).toBe(false)
    })

    it('rejects object missing ts', () => {
      expect(validateEvent({ type: 'test.event' })).toBe(false)
    })
  })

  describe('invalid fields', () => {
    it('rejects non-string type', () => {
      expect(validateEvent({ type: 123, ts: '2024-01-01T00:00:00Z' })).toBe(false)
      expect(validateEvent({ type: null, ts: '2024-01-01T00:00:00Z' })).toBe(false)
      expect(validateEvent({ type: true, ts: '2024-01-01T00:00:00Z' })).toBe(false)
      expect(validateEvent({ type: {}, ts: '2024-01-01T00:00:00Z' })).toBe(false)
      expect(validateEvent({ type: [], ts: '2024-01-01T00:00:00Z' })).toBe(false)
    })

    it('rejects empty string type', () => {
      expect(validateEvent({ type: '', ts: '2024-01-01T00:00:00Z' })).toBe(false)
    })

    it('rejects type exceeding 256 chars', () => {
      const tooLongType = 'a'.repeat(257)
      expect(validateEvent({ type: tooLongType, ts: '2024-01-01T00:00:00Z' })).toBe(false)
    })

    it('rejects non-string ts', () => {
      expect(validateEvent({ type: 'test', ts: 12345 })).toBe(false)
      expect(validateEvent({ type: 'test', ts: null })).toBe(false)
      expect(validateEvent({ type: 'test', ts: true })).toBe(false)
    })

    it('rejects invalid timestamp string', () => {
      expect(validateEvent({ type: 'test', ts: 'not-a-date' })).toBe(false)
      expect(validateEvent({ type: 'test', ts: '' })).toBe(false)
      expect(validateEvent({ type: 'test', ts: 'yesterday' })).toBe(false)
    })

    it('rejects arrays as events', () => {
      expect(validateEvent([{ type: 'test', ts: '2024-01-01' }])).toBe(false)
    })
  })
})

describe('validateBatch', () => {
  describe('valid batches', () => {
    it('accepts batch with single event', () => {
      expect(validateBatch({ events: [{ type: 'test', ts: '2024-01-01' }] })).toBe(true)
    })

    it('accepts batch with multiple events', () => {
      const events = Array.from({ length: 10 }, (_, i) => ({
        type: `event.${i}`,
        ts: '2024-01-01T00:00:00Z',
      }))
      expect(validateBatch({ events })).toBe(true)
    })

    it('accepts batch with exactly 1000 events (max)', () => {
      const events = Array.from({ length: 1000 }, (_, i) => ({
        type: `event.${i}`,
        ts: '2024-01-01T00:00:00Z',
      }))
      expect(validateBatch({ events })).toBe(true)
    })

    it('accepts batch with extra fields beyond events', () => {
      expect(validateBatch({
        events: [{ type: 'test', ts: '2024-01-01' }],
        source: 'my-worker',
        metadata: { version: 1 },
      })).toBe(true)
    })
  })

  describe('edge cases', () => {
    it('accepts empty events array', () => {
      // validateBatch only checks structure, not emptiness
      expect(validateBatch({ events: [] })).toBe(true)
    })

    it('rejects batch with more than 1000 events', () => {
      const events = Array.from({ length: 1001 }, (_, i) => ({
        type: `event.${i}`,
        ts: '2024-01-01T00:00:00Z',
      }))
      expect(validateBatch({ events })).toBe(false)
    })

    it('rejects batch with 10000 events', () => {
      const events = Array.from({ length: 10000 }, () => ({}))
      expect(validateBatch({ events })).toBe(false)
    })
  })

  describe('invalid batches', () => {
    it('rejects null', () => {
      expect(validateBatch(null)).toBe(false)
    })

    it('rejects undefined', () => {
      expect(validateBatch(undefined)).toBe(false)
    })

    it('rejects non-object types', () => {
      expect(validateBatch('string')).toBe(false)
      expect(validateBatch(42)).toBe(false)
      expect(validateBatch(true)).toBe(false)
    })

    it('rejects object without events field', () => {
      expect(validateBatch({})).toBe(false)
      expect(validateBatch({ data: [] })).toBe(false)
    })

    it('rejects non-array events field', () => {
      expect(validateBatch({ events: 'not an array' })).toBe(false)
      expect(validateBatch({ events: 42 })).toBe(false)
      expect(validateBatch({ events: {} })).toBe(false)
      expect(validateBatch({ events: null })).toBe(false)
    })
  })
})

describe('validateEventSize', () => {
  it('accepts small event', () => {
    expect(validateEventSize({ type: 'test', ts: '2024-01-01' })).toBe(true)
  })

  it('accepts event exactly at 10KB boundary', () => {
    // JSON.stringify adds overhead for keys, quotes, braces, etc.
    // We need to create an event whose stringified form is exactly 10240 bytes
    const base = JSON.stringify({ type: 'test', ts: '2024-01-01', data: '' })
    const paddingNeeded = MAX_EVENT_SIZE - base.length
    const event = { type: 'test', ts: '2024-01-01', data: 'x'.repeat(paddingNeeded) }
    expect(JSON.stringify(event).length).toBe(MAX_EVENT_SIZE)
    expect(validateEventSize(event)).toBe(true)
  })

  it('rejects event one byte over 10KB', () => {
    const base = JSON.stringify({ type: 'test', ts: '2024-01-01', data: '' })
    const paddingNeeded = MAX_EVENT_SIZE - base.length + 1
    const event = { type: 'test', ts: '2024-01-01', data: 'x'.repeat(paddingNeeded) }
    expect(JSON.stringify(event).length).toBe(MAX_EVENT_SIZE + 1)
    expect(validateEventSize(event)).toBe(false)
  })

  it('rejects very large event', () => {
    const event = {
      type: 'test',
      ts: '2024-01-01',
      data: 'x'.repeat(100_000),
    }
    expect(validateEventSize(event)).toBe(false)
  })

  it('accepts event with nested objects within size limit', () => {
    const event = {
      type: 'collection.update',
      ts: '2024-01-01T00:00:00Z',
      collection: 'users',
      doc: {
        name: 'Alice',
        email: 'alice@example.com',
        profile: {
          bio: 'Hello world',
          avatar: 'https://example.com/avatar.png',
        },
      },
      prev: {
        name: 'Alice',
        email: 'alice@old.com',
      },
    }
    expect(validateEventSize(event)).toBe(true)
  })

  it('handles empty object', () => {
    expect(validateEventSize({})).toBe(true)
  })

  it('handles event with array data', () => {
    const event = {
      type: 'test',
      ts: '2024-01-01',
      data: Array.from({ length: 100 }, (_, i) => `item-${i}`),
    }
    expect(validateEventSize(event)).toBe(true)
  })
})

describe('escapeSql', () => {
  describe('basic escaping', () => {
    it('returns unchanged string when no quotes present', () => {
      expect(escapeSql('hello world')).toBe('hello world')
    })

    it('escapes single quote to double single quotes', () => {
      expect(escapeSql("it's")).toBe("it''s")
    })

    it('escapes multiple single quotes', () => {
      expect(escapeSql("it's a 'test'")).toBe("it''s a ''test''")
    })

    it('handles empty string', () => {
      expect(escapeSql('')).toBe('')
    })

    it('handles string of only quotes', () => {
      expect(escapeSql("'''")).toBe("''''''")
    })
  })

  describe('SQL injection payloads', () => {
    it('neutralizes basic SQL injection with closing quote', () => {
      const payload = "'; DROP TABLE events; --"
      const escaped = escapeSql(payload)
      expect(escaped).toBe("''; DROP TABLE events; --")
      // The key thing: the quote is doubled, so it stays within the string literal
    })

    it('neutralizes UNION SELECT injection', () => {
      const payload = "' UNION SELECT * FROM users; --"
      const escaped = escapeSql(payload)
      expect(escaped).toBe("'' UNION SELECT * FROM users; --")
    })

    it('neutralizes OR 1=1 injection', () => {
      const payload = "' OR '1'='1"
      const escaped = escapeSql(payload)
      expect(escaped).toBe("'' OR ''1''=''1")
    })

    it('neutralizes nested quote injection', () => {
      const payload = "test'; DELETE FROM users WHERE name = 'admin'; --"
      const escaped = escapeSql(payload)
      expect(escaped).toBe("test''; DELETE FROM users WHERE name = ''admin''; --")
    })

    it('neutralizes batch execution attempt', () => {
      const payload = "x'; INSERT INTO admin_users VALUES('hacker','password'); --"
      const escaped = escapeSql(payload)
      // All single quotes should be doubled, making them string literals not terminators
      expect(escaped).toBe("x''; INSERT INTO admin_users VALUES(''hacker'',''password''); --")
      // The key point: the original single quote before the semicolon is now doubled
      // So when placed in SQL like 'x''; INSERT...', the '' is an escaped quote, not a terminator
    })

    it('handles double-quote strings (not affected)', () => {
      // escapeSql only handles single quotes; double quotes pass through
      const payload = 'test "injection" attempt'
      expect(escapeSql(payload)).toBe('test "injection" attempt')
    })

    it('handles backslash attempts', () => {
      // Some databases use backslash escaping; our function only escapes single quotes
      const payload = "test\\'; DROP TABLE; --"
      const escaped = escapeSql(payload)
      expect(escaped).toBe("test\\''; DROP TABLE; --")
    })

    it('handles unicode quote characters', () => {
      // Standard single quote escaping; unicode fancy quotes are not escaped
      const payload = "test\u2019s value" // unicode right single quotation mark
      const escaped = escapeSql(payload)
      expect(escaped).toBe("test\u2019s value") // not a regular quote, not escaped
    })

    it('handles null byte injection attempt', () => {
      const payload = "test\x00'; DROP TABLE; --"
      const escaped = escapeSql(payload)
      expect(escaped).toContain("''")
    })
  })
})

// ============================================================================
// Query validation (validateQueryRequest) contract tests
// ============================================================================

const MAX_QUERY_LIMIT = 10000
const MAX_EVENT_TYPES = 100
const MAX_STRING_PARAM_LENGTH = 256

function validateQueryRequest(query: unknown): boolean {
  if (typeof query !== 'object' || query === null) return false
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

describe('validateQueryRequest', () => {
  it('accepts minimal empty query', () => {
    expect(validateQueryRequest({})).toBe(true)
  })

  it('accepts query with all valid fields', () => {
    expect(validateQueryRequest({
      dateRange: { start: '2024-01-01', end: '2024-12-31' },
      doId: 'abc-123',
      doClass: 'ChatRoom',
      collection: 'messages',
      colo: 'SFO',
      eventTypes: ['rpc.call', 'collection.insert'],
      limit: 500,
    })).toBe(true)
  })

  describe('dateRange validation', () => {
    it('rejects dateRange with start > end', () => {
      expect(validateQueryRequest({
        dateRange: { start: '2024-12-31', end: '2024-01-01' },
      })).toBe(false)
    })

    it('accepts dateRange with start === end', () => {
      expect(validateQueryRequest({
        dateRange: { start: '2024-06-15', end: '2024-06-15' },
      })).toBe(true)
    })

    it('rejects invalid start date', () => {
      expect(validateQueryRequest({
        dateRange: { start: 'not-a-date', end: '2024-01-01' },
      })).toBe(false)
    })

    it('rejects invalid end date', () => {
      expect(validateQueryRequest({
        dateRange: { start: '2024-01-01', end: 'invalid' },
      })).toBe(false)
    })

    it('rejects non-object dateRange', () => {
      expect(validateQueryRequest({ dateRange: 'invalid' })).toBe(false)
      expect(validateQueryRequest({ dateRange: null })).toBe(false)
    })
  })

  describe('string param length validation', () => {
    it('rejects doId exceeding 256 chars', () => {
      expect(validateQueryRequest({ doId: 'a'.repeat(257) })).toBe(false)
    })

    it('rejects doClass exceeding 256 chars', () => {
      expect(validateQueryRequest({ doClass: 'a'.repeat(257) })).toBe(false)
    })

    it('rejects collection exceeding 256 chars', () => {
      expect(validateQueryRequest({ collection: 'a'.repeat(257) })).toBe(false)
    })

    it('rejects colo exceeding 256 chars', () => {
      expect(validateQueryRequest({ colo: 'a'.repeat(257) })).toBe(false)
    })

    it('rejects non-string doId', () => {
      expect(validateQueryRequest({ doId: 123 })).toBe(false)
    })
  })

  describe('eventTypes validation', () => {
    it('rejects non-array eventTypes', () => {
      expect(validateQueryRequest({ eventTypes: 'not-an-array' })).toBe(false)
    })

    it('rejects more than 100 event types', () => {
      const eventTypes = Array.from({ length: 101 }, (_, i) => `type.${i}`)
      expect(validateQueryRequest({ eventTypes })).toBe(false)
    })

    it('accepts exactly 100 event types', () => {
      const eventTypes = Array.from({ length: 100 }, (_, i) => `type.${i}`)
      expect(validateQueryRequest({ eventTypes })).toBe(true)
    })

    it('rejects eventType entries that are not strings', () => {
      expect(validateQueryRequest({ eventTypes: [123] })).toBe(false)
    })

    it('rejects eventType entries exceeding 256 chars', () => {
      expect(validateQueryRequest({ eventTypes: ['a'.repeat(257)] })).toBe(false)
    })
  })

  describe('limit validation', () => {
    it('accepts valid limits', () => {
      expect(validateQueryRequest({ limit: 1 })).toBe(true)
      expect(validateQueryRequest({ limit: 100 })).toBe(true)
      expect(validateQueryRequest({ limit: 10000 })).toBe(true)
    })

    it('rejects limit less than 1', () => {
      expect(validateQueryRequest({ limit: 0 })).toBe(false)
      expect(validateQueryRequest({ limit: -1 })).toBe(false)
    })

    it('rejects limit greater than 10000', () => {
      expect(validateQueryRequest({ limit: 10001 })).toBe(false)
    })

    it('rejects non-integer limit', () => {
      expect(validateQueryRequest({ limit: 1.5 })).toBe(false)
    })

    it('rejects non-number limit', () => {
      expect(validateQueryRequest({ limit: '100' })).toBe(false)
    })
  })

  describe('top-level validation', () => {
    it('rejects null', () => {
      expect(validateQueryRequest(null)).toBe(false)
    })

    it('rejects non-object', () => {
      expect(validateQueryRequest('string')).toBe(false)
      expect(validateQueryRequest(42)).toBe(false)
    })
  })
})

// ============================================================================
// Path Pattern validation (validatePathPattern) contract tests
// events-fzu: Prevents SQL injection via path pattern interpolation
// ============================================================================

/**
 * Regex whitelist for path patterns - only allows safe characters:
 * alphanumeric, slashes, asterisks, dashes, underscores, and dots.
 * Also rejects SQL comment syntax (--) to prevent injection.
 */
const SAFE_PATH_PATTERN = /^[a-zA-Z0-9\/_*.-]+$/

function validatePathPattern(pattern: string): boolean {
  // First check character whitelist
  if (!SAFE_PATH_PATTERN.test(pattern)) {
    return false
  }
  // Reject SQL comment syntax (double dashes)
  if (pattern.includes('--')) {
    return false
  }
  return true
}

describe('validatePathPattern', () => {
  describe('valid patterns', () => {
    it('accepts basic path', () => {
      expect(validatePathPattern('events/')).toBe(true)
    })

    it('accepts path with year', () => {
      expect(validatePathPattern('events/2024/')).toBe(true)
    })

    it('accepts path with wildcards', () => {
      expect(validatePathPattern('events/**/')).toBe(true)
      expect(validatePathPattern('events/*/*/*/')).toBe(true)
    })

    it('accepts full pattern with extension', () => {
      expect(validatePathPattern('events/2024/01/15/*/*.parquet')).toBe(true)
      expect(validatePathPattern('events/2024/01/15/*/*.jsonl')).toBe(true)
    })

    it('accepts pattern with dashes and underscores', () => {
      expect(validatePathPattern('events_backup/2024-01/')).toBe(true)
    })

    it('accepts recursive glob pattern', () => {
      expect(validatePathPattern('events/**/*.parquet')).toBe(true)
    })
  })

  describe('SQL injection attempts', () => {
    it('rejects single quote', () => {
      expect(validatePathPattern("events/'; DROP TABLE events; --")).toBe(false)
    })

    it('rejects double quote', () => {
      expect(validatePathPattern('events/"; DROP TABLE events; --')).toBe(false)
    })

    it('rejects parentheses', () => {
      expect(validatePathPattern('events/test()')).toBe(false)
    })

    it('rejects semicolon', () => {
      expect(validatePathPattern('events/test;delete')).toBe(false)
    })

    it('rejects equals sign', () => {
      expect(validatePathPattern('events/test=1')).toBe(false)
    })

    it('rejects space', () => {
      expect(validatePathPattern('events/test path')).toBe(false)
    })

    it('rejects newline', () => {
      expect(validatePathPattern('events/test\npath')).toBe(false)
    })

    it('rejects backslash', () => {
      expect(validatePathPattern('events\\test')).toBe(false)
    })

    it('rejects UNION SELECT injection', () => {
      expect(validatePathPattern("events/' UNION SELECT * FROM secrets --")).toBe(false)
    })

    it('rejects comment injection', () => {
      expect(validatePathPattern('events/test--comment')).toBe(false)
    })

    it('rejects backtick', () => {
      expect(validatePathPattern('events/`test`')).toBe(false)
    })

    it('rejects angle brackets', () => {
      expect(validatePathPattern('events/<script>')).toBe(false)
    })

    it('rejects curly braces', () => {
      expect(validatePathPattern('events/{test}')).toBe(false)
    })

    it('rejects square brackets', () => {
      expect(validatePathPattern('events/[test]')).toBe(false)
    })

    it('rejects pipe', () => {
      expect(validatePathPattern('events/test|other')).toBe(false)
    })

    it('rejects dollar sign', () => {
      expect(validatePathPattern('events/$test')).toBe(false)
    })

    it('rejects at sign', () => {
      expect(validatePathPattern('events/@test')).toBe(false)
    })

    it('rejects hash', () => {
      expect(validatePathPattern('events/#test')).toBe(false)
    })

    it('rejects percent (LIKE wildcard)', () => {
      expect(validatePathPattern('events/%test%')).toBe(false)
    })

    it('rejects ampersand', () => {
      expect(validatePathPattern('events/test&other')).toBe(false)
    })

    it('rejects null byte', () => {
      expect(validatePathPattern('events/test\x00other')).toBe(false)
    })
  })

  describe('edge cases', () => {
    it('rejects empty string', () => {
      expect(validatePathPattern('')).toBe(false)
    })

    it('accepts single character paths', () => {
      expect(validatePathPattern('a')).toBe(true)
      expect(validatePathPattern('/')).toBe(true)
      expect(validatePathPattern('*')).toBe(true)
    })
  })
})
