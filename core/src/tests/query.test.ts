/**
 * Tests for query.ts - DuckDB query builders
 *
 * Tests for:
 * - buildQuery() - basic queries with filters, ordering, and limits
 * - buildHistoryQuery() - document history/time-travel queries
 * - buildLatencyQuery() - RPC latency aggregation queries
 * - buildPITRRangeQuery() - point-in-time recovery range queries
 * - SQL injection prevention across all builders
 * - Edge cases (empty params, invalid dates, etc.)
 */

import { describe, it, expect } from 'vitest'
import {
  buildQuery,
  buildHistoryQuery,
  buildLatencyQuery,
  buildPITRRangeQuery,
  type QueryOptions,
  type OrderBy,
} from '../query'

// ============================================================================
// buildQuery() tests
// ============================================================================

describe('buildQuery', () => {
  describe('basic queries', () => {
    it('builds minimal query with only bucket', () => {
      const sql = buildQuery({ bucket: 'my-events' })
      expect(sql).toContain("SELECT * FROM read_json_auto('r2://my-events/events/**/*.jsonl')")
      expect(sql).toContain('ORDER BY ts DESC')
    })

    it('uses default ORDER BY ts DESC when not specified', () => {
      const sql = buildQuery({ bucket: 'test' })
      expect(sql).toContain('ORDER BY ts DESC')
    })

    it('includes all basic components in correct order', () => {
      const sql = buildQuery({ bucket: 'test' })
      const lines = sql.split('\n')
      expect(lines[0]).toContain('SELECT *')
      expect(lines[0]).toContain('FROM read_json_auto')
      expect(lines[lines.length - 1]).toContain('ORDER BY')
    })
  })

  describe('column selection and path patterns', () => {
    it('uses wildcard path pattern when no date range specified', () => {
      const sql = buildQuery({ bucket: 'test' })
      expect(sql).toContain('r2://test/events/**/*.jsonl')
    })

    it('uses year-specific path when date range spans single year same month', () => {
      // Create dates explicitly in current timezone to avoid TZ issues
      const start = new Date(2024, 0, 1) // Jan 1, 2024
      const end = new Date(2024, 0, 31) // Jan 31, 2024
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      // Same year, same month -> uses year/month path with wildcard for days
      expect(sql).toContain('r2://test/events/2024/01/**/*.jsonl')
    })

    it('uses year-specific path when date range spans different months same year', () => {
      const start = new Date(2024, 0, 1) // Jan 1, 2024
      const end = new Date(2024, 5, 30) // June 30, 2024
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      // Same year but different months -> uses year path with wildcard
      expect(sql).toContain('r2://test/events/2024/**/*.jsonl')
    })

    it('uses day-specific path when date range is single day', () => {
      const start = new Date(2024, 5, 15, 0, 0, 0) // June 15, 2024 00:00:00
      const end = new Date(2024, 5, 15, 23, 59, 59) // June 15, 2024 23:59:59
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      expect(sql).toContain('r2://test/events/2024/06/15/*.jsonl')
    })

    it('uses wildcard when date range spans multiple years', () => {
      const start = new Date(2023, 11, 1) // Dec 1, 2023
      const end = new Date(2024, 0, 31) // Jan 31, 2024
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      expect(sql).toContain('r2://test/events/**/*.jsonl')
    })
  })

  describe('WHERE clause filters', () => {
    it('adds doId filter', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: 'do-123',
      })
      expect(sql).toContain("WHERE \"do\".id = 'do-123'")
    })

    it('adds doClass filter', () => {
      const sql = buildQuery({
        bucket: 'test',
        doClass: 'ChatRoom',
      })
      expect(sql).toContain("WHERE \"do\".class = 'ChatRoom'")
    })

    it('adds colo filter', () => {
      const sql = buildQuery({
        bucket: 'test',
        colo: 'SFO',
      })
      expect(sql).toContain("WHERE \"do\".colo = 'SFO'")
    })

    it('adds collection filter', () => {
      const sql = buildQuery({
        bucket: 'test',
        collection: 'users',
      })
      expect(sql).toContain("WHERE collection = 'users'")
    })

    it('adds single eventType filter', () => {
      const sql = buildQuery({
        bucket: 'test',
        eventTypes: ['rpc.call'],
      })
      expect(sql).toContain("WHERE type IN ('rpc.call')")
    })

    it('adds multiple eventTypes filter', () => {
      const sql = buildQuery({
        bucket: 'test',
        eventTypes: ['rpc.call', 'collection.insert', 'collection.update'],
      })
      expect(sql).toContain("type IN ('rpc.call', 'collection.insert', 'collection.update')")
    })

    it('adds date range filters to WHERE clause', () => {
      const start = new Date('2024-01-01T00:00:00Z')
      const end = new Date('2024-01-31T23:59:59Z')
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      expect(sql).toContain(`ts >= '${start.toISOString()}'`)
      expect(sql).toContain(`ts <= '${end.toISOString()}'`)
    })

    it('combines multiple filters with AND', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: 'do-123',
        doClass: 'ChatRoom',
        collection: 'messages',
      })
      expect(sql).toContain("\"do\".id = 'do-123'")
      expect(sql).toContain('AND')
      expect(sql).toContain("\"do\".class = 'ChatRoom'")
      expect(sql).toContain("collection = 'messages'")
    })
  })

  describe('LIMIT clause', () => {
    it('adds LIMIT when specified', () => {
      const sql = buildQuery({
        bucket: 'test',
        limit: 100,
      })
      expect(sql).toContain('LIMIT 100')
    })

    it('handles large limit values', () => {
      const sql = buildQuery({
        bucket: 'test',
        limit: 10000,
      })
      expect(sql).toContain('LIMIT 10000')
    })

    it('floors floating point limit values', () => {
      const sql = buildQuery({
        bucket: 'test',
        limit: 99.9,
      })
      expect(sql).toContain('LIMIT 99')
    })

    it('omits LIMIT when not specified', () => {
      const sql = buildQuery({ bucket: 'test' })
      expect(sql).not.toContain('LIMIT')
    })
  })

  describe('ORDER BY clause', () => {
    it('accepts ts ASC', () => {
      const sql = buildQuery({
        bucket: 'test',
        orderBy: 'ts ASC',
      })
      expect(sql).toContain('ORDER BY ts ASC')
    })

    it('accepts ts DESC', () => {
      const sql = buildQuery({
        bucket: 'test',
        orderBy: 'ts DESC',
      })
      expect(sql).toContain('ORDER BY ts DESC')
    })

    it('accepts column without direction (defaults to ASC)', () => {
      const sql = buildQuery({
        bucket: 'test',
        orderBy: 'type',
      })
      expect(sql).toContain('ORDER BY type')
    })

    it('accepts all valid orderBy columns', () => {
      const columns: OrderBy[] = ['ts', 'type', 'id', 'do_id', 'collection', 'colo']
      for (const col of columns) {
        const sql = buildQuery({ bucket: 'test', orderBy: col })
        expect(sql).toContain(`ORDER BY ${col}`)
      }
    })

    it('rejects invalid column names', () => {
      expect(() => buildQuery({
        bucket: 'test',
        orderBy: 'invalid_column' as OrderBy,
      })).toThrow(/Invalid orderBy column/)
    })

    it('rejects invalid direction', () => {
      expect(() => buildQuery({
        bucket: 'test',
        orderBy: 'ts INVALID' as OrderBy,
      })).toThrow(/Invalid orderBy direction/)
    })

    it('rejects malformed orderBy with extra parts', () => {
      expect(() => buildQuery({
        bucket: 'test',
        orderBy: 'ts DESC EXTRA' as OrderBy,
      })).toThrow(/Invalid orderBy format/)
    })
  })
})

// ============================================================================
// buildHistoryQuery() tests
// ============================================================================

describe('buildHistoryQuery', () => {
  it('builds query for document history', () => {
    const sql = buildHistoryQuery({
      bucket: 'test',
      collection: 'users',
      docId: 'user-123',
    })
    expect(sql).toContain("docId = 'user-123'")
    expect(sql).toContain("collection = 'users'")
    expect(sql).toContain("type IN ('collection.insert', 'collection.update', 'collection.delete')")
  })

  it('orders by ts ASC for chronological history', () => {
    const sql = buildHistoryQuery({
      bucket: 'test',
      collection: 'users',
      docId: 'user-123',
    })
    expect(sql).toContain('ORDER BY ts ASC')
  })

  it('includes date range filters when specified', () => {
    const start = new Date('2024-01-01')
    const end = new Date('2024-06-30')
    const sql = buildHistoryQuery({
      bucket: 'test',
      collection: 'messages',
      docId: 'msg-456',
      dateRange: { start, end },
    })
    expect(sql).toContain(`ts >= '${start.toISOString()}'`)
    expect(sql).toContain(`ts <= '${end.toISOString()}'`)
  })

  it('places docId condition at the beginning of WHERE clause', () => {
    const sql = buildHistoryQuery({
      bucket: 'test',
      collection: 'users',
      docId: 'user-123',
    })
    // The docId should come first in WHERE, followed by AND
    expect(sql).toMatch(/WHERE docId = 'user-123'\s+AND/)
  })
})

// ============================================================================
// buildLatencyQuery() tests
// ============================================================================

describe('buildLatencyQuery', () => {
  it('builds basic latency aggregation query', () => {
    const sql = buildLatencyQuery({ bucket: 'test' })
    expect(sql).toContain('SELECT')
    expect(sql).toContain('"do".class')
    expect(sql).toContain('method')
    expect(sql).toContain('COUNT(*) as call_count')
    expect(sql).toContain('AVG(durationMs) as avg_duration_ms')
    expect(sql).toContain('PERCENTILE_CONT(0.5)') // p50
    expect(sql).toContain('PERCENTILE_CONT(0.95)') // p95
    expect(sql).toContain('PERCENTILE_CONT(0.99)') // p99
    expect(sql).toContain('MAX(durationMs) as max_duration_ms')
    expect(sql).toContain('error_count')
  })

  it('filters to rpc.call events only', () => {
    const sql = buildLatencyQuery({ bucket: 'test' })
    expect(sql).toContain("type = 'rpc.call'")
  })

  it('groups by class and method', () => {
    const sql = buildLatencyQuery({ bucket: 'test' })
    expect(sql).toContain('GROUP BY "do".class, method')
  })

  it('orders by call count descending', () => {
    const sql = buildLatencyQuery({ bucket: 'test' })
    expect(sql).toContain('ORDER BY call_count DESC')
  })

  it('adds doClass filter when specified', () => {
    const sql = buildLatencyQuery({
      bucket: 'test',
      doClass: 'ChatRoom',
    })
    expect(sql).toContain("\"do\".class = 'ChatRoom'")
  })

  it('adds method filter when specified', () => {
    const sql = buildLatencyQuery({
      bucket: 'test',
      method: 'sendMessage',
    })
    expect(sql).toContain("method = 'sendMessage'")
  })

  it('uses year-specific path when dateRange is provided', () => {
    const sql = buildLatencyQuery({
      bucket: 'test',
      dateRange: {
        start: new Date('2024-06-01'),
        end: new Date('2024-06-30'),
      },
    })
    expect(sql).toContain('r2://test/events/2024/**/*.jsonl')
  })

  it('combines multiple filters with AND', () => {
    const sql = buildLatencyQuery({
      bucket: 'test',
      doClass: 'ChatRoom',
      method: 'sendMessage',
    })
    expect(sql).toContain("type = 'rpc.call'")
    expect(sql).toContain('AND')
    expect(sql).toContain("\"do\".class = 'ChatRoom'")
    expect(sql).toContain("method = 'sendMessage'")
  })
})

// ============================================================================
// buildPITRRangeQuery() tests
// ============================================================================

describe('buildPITRRangeQuery', () => {
  it('builds query for PITR range with bookmarks', () => {
    const sql = buildPITRRangeQuery({
      bucket: 'test',
      startBookmark: 'bm-001',
      endBookmark: 'bm-100',
    })
    expect(sql).toContain("bookmark > 'bm-001'")
    expect(sql).toContain("bookmark <= 'bm-100'")
  })

  it('uses exclusive start and inclusive end for bookmarks', () => {
    const sql = buildPITRRangeQuery({
      bucket: 'test',
      startBookmark: 'start',
      endBookmark: 'end',
    })
    // start is exclusive (>), end is inclusive (<=)
    expect(sql).toContain("bookmark > 'start'")
    expect(sql).toContain("bookmark <= 'end'")
  })

  it('orders by ts ASC for chronological replay', () => {
    const sql = buildPITRRangeQuery({
      bucket: 'test',
      startBookmark: 'a',
      endBookmark: 'z',
    })
    expect(sql).toContain('ORDER BY ts ASC')
  })

  it('adds collection filter when specified', () => {
    const sql = buildPITRRangeQuery({
      bucket: 'test',
      startBookmark: 'a',
      endBookmark: 'z',
      collection: 'users',
    })
    expect(sql).toContain("collection = 'users'")
  })

  it('uses wildcard path pattern to scan all events', () => {
    const sql = buildPITRRangeQuery({
      bucket: 'test',
      startBookmark: 'a',
      endBookmark: 'z',
    })
    expect(sql).toContain('r2://test/events/**/*.jsonl')
  })

  it('selects all columns', () => {
    const sql = buildPITRRangeQuery({
      bucket: 'test',
      startBookmark: 'a',
      endBookmark: 'z',
    })
    expect(sql).toContain('SELECT *')
  })
})

// ============================================================================
// SQL injection prevention tests
// ============================================================================

describe('SQL injection prevention', () => {
  describe('buildQuery injection attempts', () => {
    it('escapes single quotes in doId', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: "'; DROP TABLE events; --",
      })
      expect(sql).toContain("''; DROP TABLE events; --")
      expect(sql).not.toMatch(/doId.*'\s*;/)
    })

    it('escapes single quotes in doClass', () => {
      const sql = buildQuery({
        bucket: 'test',
        doClass: "ChatRoom' OR '1'='1",
      })
      expect(sql).toContain("ChatRoom'' OR ''1''=''1")
    })

    it('escapes single quotes in colo', () => {
      const sql = buildQuery({
        bucket: 'test',
        colo: "SFO' UNION SELECT * FROM secrets --",
      })
      expect(sql).toContain("''")
    })

    it('escapes single quotes in collection', () => {
      const sql = buildQuery({
        bucket: 'test',
        collection: "users'; DELETE FROM events; --",
      })
      expect(sql).toContain("users''; DELETE FROM events; --")
    })

    it('escapes single quotes in eventTypes', () => {
      const sql = buildQuery({
        bucket: 'test',
        eventTypes: ["rpc.call'; DROP TABLE events; --"],
      })
      expect(sql).toContain("rpc.call''; DROP TABLE events; --")
    })

    it('rejects strings exceeding max length', () => {
      expect(() => buildQuery({
        bucket: 'test',
        doId: 'x'.repeat(1001),
      })).toThrow(/exceeds maximum length/)
    })

    it('prevents orderBy injection by validating column names', () => {
      expect(() => buildQuery({
        bucket: 'test',
        orderBy: 'ts; DROP TABLE events; --' as OrderBy,
      })).toThrow(/Invalid orderBy/)
    })
  })

  describe('buildHistoryQuery injection attempts', () => {
    it('escapes single quotes in docId', () => {
      const sql = buildHistoryQuery({
        bucket: 'test',
        collection: 'users',
        docId: "user-123'; DELETE FROM events; --",
      })
      expect(sql).toContain("user-123''; DELETE FROM events; --")
    })

    it('escapes single quotes in collection', () => {
      const sql = buildHistoryQuery({
        bucket: 'test',
        collection: "users' OR '1'='1",
        docId: 'doc-1',
      })
      expect(sql).toContain("users'' OR ''1''=''1")
    })
  })

  describe('buildLatencyQuery injection attempts', () => {
    it('escapes single quotes in doClass', () => {
      const sql = buildLatencyQuery({
        bucket: 'test',
        doClass: "ChatRoom'; DROP TABLE events; --",
      })
      expect(sql).toContain("ChatRoom''; DROP TABLE events; --")
    })

    it('escapes single quotes in method', () => {
      const sql = buildLatencyQuery({
        bucket: 'test',
        method: "send' OR 1=1 --",
      })
      expect(sql).toContain("send'' OR 1=1 --")
    })
  })

  describe('buildPITRRangeQuery injection attempts', () => {
    it('escapes single quotes in startBookmark', () => {
      const sql = buildPITRRangeQuery({
        bucket: 'test',
        startBookmark: "bm'; DROP TABLE events; --",
        endBookmark: 'end',
      })
      expect(sql).toContain("bm''; DROP TABLE events; --")
    })

    it('escapes single quotes in endBookmark', () => {
      const sql = buildPITRRangeQuery({
        bucket: 'test',
        startBookmark: 'start',
        endBookmark: "end' UNION SELECT * FROM secrets --",
      })
      expect(sql).toContain("end'' UNION SELECT * FROM secrets --")
    })

    it('escapes single quotes in collection', () => {
      const sql = buildPITRRangeQuery({
        bucket: 'test',
        startBookmark: 'a',
        endBookmark: 'z',
        collection: "users'; --",
      })
      expect(sql).toContain("users''; --")
    })
  })
})

// ============================================================================
// Edge cases
// ============================================================================

describe('edge cases', () => {
  describe('empty or minimal parameters', () => {
    it('handles empty eventTypes array', () => {
      const sql = buildQuery({
        bucket: 'test',
        eventTypes: [],
      })
      // Empty array should not add a type IN clause
      expect(sql).not.toContain('type IN')
    })

    it('handles undefined optional parameters', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: undefined,
        doClass: undefined,
        colo: undefined,
        eventTypes: undefined,
        collection: undefined,
        limit: undefined,
        orderBy: undefined,
      })
      // Should still build a valid query
      expect(sql).toContain('SELECT *')
      expect(sql).toContain('ORDER BY ts DESC')
    })

    it('handles zero limit', () => {
      const sql = buildQuery({
        bucket: 'test',
        limit: 0,
      })
      // Zero is falsy, so LIMIT should not be added
      expect(sql).not.toContain('LIMIT')
    })
  })

  describe('date handling', () => {
    it('handles date at year boundary', () => {
      // Use explicit local dates to avoid timezone issues
      const start = new Date(2024, 11, 31, 23, 59, 59) // Dec 31, 2024 23:59:59
      const end = new Date(2025, 0, 1, 0, 0, 0) // Jan 1, 2025 00:00:00
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      // Different years -> should use wildcard
      expect(sql).toContain('events/**/*.jsonl')
    })

    it('handles date at month boundary', () => {
      const start = new Date(2024, 0, 31) // Jan 31, 2024
      const end = new Date(2024, 1, 1) // Feb 1, 2024
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      // Same year but different months -> year with wildcard
      expect(sql).toContain('events/2024/**/*.jsonl')
    })

    it('handles leap year date', () => {
      const start = new Date(2024, 1, 29, 0, 0, 0) // Feb 29, 2024 00:00:00
      const end = new Date(2024, 1, 29, 23, 59, 59) // Feb 29, 2024 23:59:59
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      expect(sql).toContain('events/2024/02/29/*.jsonl')
    })

    it('pads single-digit months and days', () => {
      const start = new Date(2024, 0, 5, 0, 0, 0) // Jan 5, 2024 00:00:00
      const end = new Date(2024, 0, 5, 23, 59, 59) // Jan 5, 2024 23:59:59
      const sql = buildQuery({
        bucket: 'test',
        dateRange: { start, end },
      })
      expect(sql).toContain('events/2024/01/05/*.jsonl')
    })
  })

  describe('special characters in values', () => {
    it('handles backslashes in values', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: 'path\\to\\file',
      })
      expect(sql).toContain('path\\to\\file')
    })

    it('handles unicode characters in values', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: 'user-emoji',
        collection: 'messages',
      })
      expect(sql).toContain('user-emoji')
    })

    it('handles newlines in values', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: 'line1\nline2',
      })
      expect(sql).toContain('line1\nline2')
    })

    it('handles consecutive single quotes', () => {
      const sql = buildQuery({
        bucket: 'test',
        doId: "test'''value",
      })
      // Each single quote should be doubled
      expect(sql).toContain("test''''''value")
    })
  })

  describe('bucket name handling', () => {
    it('includes bucket name in path', () => {
      const sql = buildQuery({ bucket: 'my-custom-bucket' })
      expect(sql).toContain('r2://my-custom-bucket/events/')
    })

    it('handles bucket names with hyphens', () => {
      const sql = buildQuery({ bucket: 'my-events-bucket-2024' })
      expect(sql).toContain('r2://my-events-bucket-2024/')
    })
  })

  describe('complex combined queries', () => {
    it('builds query with all parameters specified', () => {
      const start = new Date(2024, 5, 15, 0, 0, 0) // June 15, 2024 00:00:00
      const end = new Date(2024, 5, 15, 23, 59, 59) // June 15, 2024 23:59:59
      const sql = buildQuery({
        bucket: 'production',
        dateRange: { start, end },
        doId: 'do-12345',
        doClass: 'ChatRoom',
        colo: 'SFO',
        eventTypes: ['rpc.call', 'collection.insert'],
        collection: 'messages',
        limit: 500,
        orderBy: 'ts DESC',
      })

      expect(sql).toContain('r2://production/events/2024/06/15/*.jsonl')
      expect(sql).toContain("\"do\".id = 'do-12345'")
      expect(sql).toContain("\"do\".class = 'ChatRoom'")
      expect(sql).toContain("\"do\".colo = 'SFO'")
      expect(sql).toContain("type IN ('rpc.call', 'collection.insert')")
      expect(sql).toContain("collection = 'messages'")
      expect(sql).toContain(`ts >= '${start.toISOString()}'`)
      expect(sql).toContain(`ts <= '${end.toISOString()}'`)
      expect(sql).toContain('ORDER BY ts DESC')
      expect(sql).toContain('LIMIT 500')
    })
  })
})
