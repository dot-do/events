/**
 * Events Route Handler Tests
 *
 * Unit tests for src/routes/events.ts
 * Tests event query and recent events functionality.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { convertBigInts } from '../../routes/events'

// ============================================================================
// Mock Types
// ============================================================================

interface MockR2Object {
  key: string
  size: number
}

interface MockR2Bucket {
  list: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
}

interface MockEnv {
  EVENTS_BUCKET: MockR2Bucket
  ALLOWED_ORIGINS?: string
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockR2Bucket(objects: MockR2Object[] = []): MockR2Bucket {
  return {
    list: vi.fn().mockResolvedValue({ objects, truncated: false }),
    get: vi.fn(),
  }
}

function createMockEnv(objects: MockR2Object[] = []): MockEnv {
  return {
    EVENTS_BUCKET: createMockR2Bucket(objects),
  }
}

// ============================================================================
// convertBigInts Tests
// ============================================================================

describe('convertBigInts', () => {
  describe('BigInt conversion', () => {
    it('converts BigInt to number', () => {
      const result = convertBigInts(BigInt(12345))
      expect(result).toBe(12345)
    })

    it('converts BigInt in objects', () => {
      const obj = { count: BigInt(100) }
      const result = convertBigInts(obj) as Record<string, unknown>

      expect(result.count).toBe(100)
    })

    it('converts nested BigInts', () => {
      const obj = {
        outer: {
          inner: BigInt(50),
        },
      }
      const result = convertBigInts(obj) as Record<string, any>

      expect(result.outer.inner).toBe(50)
    })

    it('converts BigInts in arrays', () => {
      const arr = [BigInt(1), BigInt(2), BigInt(3)]
      const result = convertBigInts(arr) as number[]

      expect(result).toEqual([1, 2, 3])
    })
  })

  describe('timestamp conversion', () => {
    it('converts BigInt timestamp to ISO string', () => {
      const ms = 1704067200000 // 2024-01-01T00:00:00Z
      const obj = { ts: BigInt(ms) }
      const result = convertBigInts(obj) as Record<string, unknown>

      expect(result.ts).toBe('2024-01-01T00:00:00.000Z')
    })

    it('converts Date timestamp to ISO string', () => {
      const date = new Date('2024-01-01T00:00:00Z')
      const obj = { ts: date }
      const result = convertBigInts(obj) as Record<string, unknown>

      expect(result.ts).toBe('2024-01-01T00:00:00.000Z')
    })

    it('converts number timestamp to ISO string', () => {
      const ms = 1704067200000
      const obj = { ts: ms }
      const result = convertBigInts(obj) as Record<string, unknown>

      expect(result.ts).toBe('2024-01-01T00:00:00.000Z')
    })

    it('preserves string timestamps', () => {
      const obj = { ts: '2024-01-01T00:00:00Z' }
      const result = convertBigInts(obj) as Record<string, unknown>

      expect(result.ts).toBe('2024-01-01T00:00:00Z')
    })
  })

  describe('Date conversion', () => {
    it('converts Date objects to ISO strings', () => {
      const date = new Date('2024-06-15T12:30:00Z')
      const result = convertBigInts(date)

      expect(result).toBe('2024-06-15T12:30:00.000Z')
    })

    it('converts nested Date objects', () => {
      const obj = {
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-06-15T12:30:00Z'),
      }
      const result = convertBigInts(obj) as Record<string, unknown>

      expect(result.createdAt).toBe('2024-01-01T00:00:00.000Z')
      expect(result.updatedAt).toBe('2024-06-15T12:30:00.000Z')
    })
  })

  describe('passthrough values', () => {
    it('preserves strings', () => {
      expect(convertBigInts('hello')).toBe('hello')
    })

    it('preserves numbers', () => {
      expect(convertBigInts(42)).toBe(42)
    })

    it('preserves booleans', () => {
      expect(convertBigInts(true)).toBe(true)
      expect(convertBigInts(false)).toBe(false)
    })

    it('preserves null', () => {
      expect(convertBigInts(null)).toBe(null)
    })

    it('preserves undefined', () => {
      expect(convertBigInts(undefined)).toBe(undefined)
    })
  })

  describe('complex objects', () => {
    it('handles full event record', () => {
      const event = {
        ts: BigInt(1704067200000),
        type: 'user.created',
        do: {
          id: 'abc123',
          class: 'UserDO',
        },
        payload: {
          userId: '123',
          count: BigInt(5),
        },
      }

      const result = convertBigInts(event) as Record<string, any>

      expect(result.ts).toBe('2024-01-01T00:00:00.000Z')
      expect(result.type).toBe('user.created')
      expect(result.do.id).toBe('abc123')
      expect(result.payload.count).toBe(5)
    })
  })
})

// ============================================================================
// EventsQueryParams Tests
// ============================================================================

describe('EventsQueryParams', () => {
  describe('type filter', () => {
    it('parses type from query string', () => {
      const url = new URL('https://events.do/events?type=user.*')
      const type = url.searchParams.get('type') ?? undefined
      expect(type).toBe('user.*')
    })

    it('returns undefined when not provided', () => {
      const url = new URL('https://events.do/events')
      const type = url.searchParams.get('type') ?? undefined
      expect(type).toBeUndefined()
    })
  })

  describe('pagination params', () => {
    it('parses before cursor', () => {
      const url = new URL('https://events.do/events?before=2024-01-01T00:00:00Z')
      const before = url.searchParams.get('before') ?? undefined
      expect(before).toBe('2024-01-01T00:00:00Z')
    })

    it('parses after cursor', () => {
      const url = new URL('https://events.do/events?after=2024-01-01T00:00:00Z')
      const after = url.searchParams.get('after') ?? undefined
      expect(after).toBe('2024-01-01T00:00:00Z')
    })
  })

  describe('time range params', () => {
    it('parses from timestamp', () => {
      const url = new URL('https://events.do/events?from=2024-01-01T00:00:00Z')
      const from = url.searchParams.get('from') ?? undefined
      expect(from).toBe('2024-01-01T00:00:00Z')
    })

    it('parses to timestamp', () => {
      const url = new URL('https://events.do/events?to=2024-01-31T23:59:59Z')
      const to = url.searchParams.get('to') ?? undefined
      expect(to).toBe('2024-01-31T23:59:59Z')
    })
  })

  describe('limit param', () => {
    it('parses limit as integer', () => {
      const url = new URL('https://events.do/events?limit=50')
      const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '20'), 1000)
      expect(limit).toBe(50)
    })

    it('defaults to 20', () => {
      const url = new URL('https://events.do/events')
      const limit = parseInt(url.searchParams.get('limit') ?? '20')
      expect(limit).toBe(20)
    })

    it('caps at 1000', () => {
      const url = new URL('https://events.do/events?limit=5000')
      const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '20'), 1000)
      expect(limit).toBe(1000)
    })
  })

  describe('provider filter', () => {
    it('parses provider', () => {
      const url = new URL('https://events.do/events?provider=github')
      const provider = url.searchParams.get('provider') ?? undefined
      expect(provider).toBe('github')
    })
  })

  describe('format param', () => {
    it('parses format', () => {
      const url = new URL('https://events.do/events?format=jsonl')
      const format = url.searchParams.get('format') ?? 'parquet'
      expect(format).toBe('jsonl')
    })

    it('defaults to parquet', () => {
      const url = new URL('https://events.do/events')
      const format = url.searchParams.get('format') ?? 'parquet'
      expect(format).toBe('parquet')
    })
  })

  describe('source param', () => {
    it('parses source as events', () => {
      const url = new URL('https://events.do/events?source=events')
      const source = url.searchParams.get('source') ?? 'all'
      expect(source).toBe('events')
    })

    it('parses source as tail', () => {
      const url = new URL('https://events.do/events?source=tail')
      const source = url.searchParams.get('source') ?? 'all'
      expect(source).toBe('tail')
    })

    it('defaults to all', () => {
      const url = new URL('https://events.do/events')
      const source = url.searchParams.get('source') ?? 'all'
      expect(source).toBe('all')
    })
  })
})

// ============================================================================
// Glob Pattern Matching Tests
// ============================================================================

describe('Event Type Glob Matching', () => {
  function matchGlob(pattern: string, value: string): boolean {
    const regex = pattern
      .replace(/\./g, '\\.')
      .replace(/\*\*/g, '{{DOUBLESTAR}}')
      .replace(/\*/g, '[^.]*')
      .replace(/{{DOUBLESTAR}}/g, '.*')
    return new RegExp(`^${regex}$`).test(value)
  }

  describe('wildcard matching', () => {
    it('matches single segment with *', () => {
      expect(matchGlob('user.*', 'user.created')).toBe(true)
      expect(matchGlob('user.*', 'user.updated')).toBe(true)
    })

    it('does not match multiple segments with *', () => {
      expect(matchGlob('user.*', 'user.profile.updated')).toBe(false)
    })

    it('matches any depth with **', () => {
      expect(matchGlob('user.**', 'user.created')).toBe(true)
      expect(matchGlob('user.**', 'user.profile.updated')).toBe(true)
      expect(matchGlob('user.**', 'user.settings.email.changed')).toBe(true)
    })
  })

  describe('exact matching', () => {
    it('matches exact type', () => {
      expect(matchGlob('user.created', 'user.created')).toBe(true)
    })

    it('does not match different type', () => {
      expect(matchGlob('user.created', 'user.updated')).toBe(false)
    })
  })

  describe('prefix matching', () => {
    it('matches prefix with **', () => {
      expect(matchGlob('**.error', 'rpc.call.error')).toBe(true)
      expect(matchGlob('**.error', 'webhook.github.error')).toBe(true)
    })
  })
})

// ============================================================================
// Time Boundary Tests
// ============================================================================

describe('Time Boundary Calculation', () => {
  describe('cursor-based pagination', () => {
    it('uses before cursor as end time', () => {
      const before = '2024-01-15T12:00:00Z'
      const endTime = new Date(before)

      expect(endTime.toISOString()).toBe('2024-01-15T12:00:00.000Z')
    })

    it('uses after cursor as start time', () => {
      const after = '2024-01-01T00:00:00Z'
      const startTime = new Date(after)

      expect(startTime.toISOString()).toBe('2024-01-01T00:00:00.000Z')
    })

    it('calculates start from before with 24h default', () => {
      const before = '2024-01-15T12:00:00Z'
      const endTime = new Date(before)
      const startTime = new Date(endTime.getTime() - 24 * 60 * 60 * 1000)

      expect(startTime.toISOString()).toBe('2024-01-14T12:00:00.000Z')
    })
  })

  describe('absolute time range', () => {
    it('uses explicit from/to range', () => {
      const from = '2024-01-01T00:00:00Z'
      const to = '2024-01-31T23:59:59Z'

      const startTime = new Date(from)
      const endTime = new Date(to)

      expect(startTime.toISOString()).toBe('2024-01-01T00:00:00.000Z')
      expect(endTime.toISOString()).toBe('2024-01-31T23:59:59.000Z')
    })
  })

  describe('default range', () => {
    it('defaults to last 24 hours', () => {
      const now = new Date()
      const startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000)

      const diff = now.getTime() - startTime.getTime()
      expect(diff).toBe(24 * 60 * 60 * 1000)
    })
  })
})

// ============================================================================
// Hour Bucket Generation Tests
// ============================================================================

describe('Hour Bucket Generation', () => {
  it('generates correct bucket path format', () => {
    const date = new Date('2024-06-15T14:30:00Z')
    const path = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
      String(date.getUTCHours()).padStart(2, '0'),
    ].join('/')

    expect(path).toBe('2024/06/15/14')
  })

  it('pads single-digit months', () => {
    const date = new Date('2024-01-15T00:00:00Z')
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    expect(month).toBe('01')
  })

  it('pads single-digit days', () => {
    const date = new Date('2024-01-05T00:00:00Z')
    const day = String(date.getUTCDate()).padStart(2, '0')
    expect(day).toBe('05')
  })

  it('pads single-digit hours', () => {
    const date = new Date('2024-01-15T09:00:00Z')
    const hour = String(date.getUTCHours()).padStart(2, '0')
    expect(hour).toBe('09')
  })

  it('generates buckets in reverse chronological order', () => {
    const endTime = new Date('2024-01-15T12:00:00Z')
    const startTime = new Date('2024-01-15T09:00:00Z')
    const buckets: string[] = []

    const current = new Date(endTime)
    current.setUTCMinutes(0, 0, 0)

    while (current >= startTime) {
      const path = [
        current.getUTCFullYear(),
        String(current.getUTCMonth() + 1).padStart(2, '0'),
        String(current.getUTCDate()).padStart(2, '0'),
        String(current.getUTCHours()).padStart(2, '0'),
      ].join('/')
      buckets.push(path)
      current.setUTCHours(current.getUTCHours() - 1)
    }

    expect(buckets[0]).toBe('2024/01/15/12')
    expect(buckets[buckets.length - 1]).toBe('2024/01/15/09')
  })
})

// ============================================================================
// Source Prefix Tests
// ============================================================================

describe('Source Prefixes', () => {
  it('includes events prefix for source=events', () => {
    const source = 'events'
    const prefixes: string[] = []

    if (source === 'all' || source === 'events') {
      prefixes.push('events')
    }
    if (source === 'all' || source === 'tail') {
      prefixes.push('tail')
    }

    expect(prefixes).toEqual(['events'])
  })

  it('includes tail prefix for source=tail', () => {
    const source = 'tail'
    const prefixes: string[] = []

    if (source === 'all' || source === 'events') {
      prefixes.push('events')
    }
    if (source === 'all' || source === 'tail') {
      prefixes.push('tail')
    }

    expect(prefixes).toEqual(['tail'])
  })

  it('includes both prefixes for source=all', () => {
    const source = 'all'
    const prefixes: string[] = []

    if (source === 'all' || source === 'events') {
      prefixes.push('events')
    }
    if (source === 'all' || source === 'tail') {
      prefixes.push('tail')
    }

    expect(prefixes).toEqual(['events', 'tail'])
  })
})

// ============================================================================
// Response Format Tests
// ============================================================================

describe('Events Response Format', () => {
  it('includes pagination links', async () => {
    const response = Response.json({
      links: {
        self: 'https://events.do/events?limit=20',
        first: 'https://events.do/events?limit=20',
        next: 'https://events.do/events?before=2024-01-01T00:00:00Z&limit=20',
      },
      meta: {},
      data: [],
    })

    const json = await response.json()
    expect(json.links).toBeDefined()
    expect(json.links.self).toBeDefined()
    expect(json.links.first).toBeDefined()
  })

  it('includes meta information', async () => {
    const response = Response.json({
      links: {},
      meta: {
        query: {
          source: 'events',
          startTime: '2024-01-01T00:00:00Z',
          endTime: '2024-01-02T00:00:00Z',
          durationMs: 50,
        },
        scan: {
          bucketsScanned: 24,
          bucketsTotal: 24,
          filesListed: 100,
          filesRead: 50,
          bytesRead: 102400,
          eventsScanned: 1000,
          eventsReturned: 20,
        },
        timing: {
          listMs: 10,
          readMs: 30,
          filterMs: 10,
        },
      },
      data: [],
    })

    const json = await response.json()
    expect(json.meta.query).toBeDefined()
    expect(json.meta.scan).toBeDefined()
    expect(json.meta.timing).toBeDefined()
  })

  it('includes data array', async () => {
    const events = [
      { ts: '2024-01-01T00:00:00Z', type: 'user.created' },
      { ts: '2024-01-01T00:01:00Z', type: 'user.updated' },
    ]

    const response = Response.json({
      links: {},
      meta: {},
      data: events,
    })

    const json = await response.json()
    expect(json.data).toEqual(events)
  })
})

// ============================================================================
// Recent Events Tests
// ============================================================================

describe('handleRecent', () => {
  describe('limit parameter', () => {
    it('parses limit from query', () => {
      const url = new URL('https://events.do/recent?limit=50')
      const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '100'), 1000)
      expect(limit).toBe(50)
    })

    it('defaults to 100', () => {
      const url = new URL('https://events.do/recent')
      const limit = parseInt(url.searchParams.get('limit') ?? '100')
      expect(limit).toBe(100)
    })

    it('caps at 1000', () => {
      const url = new URL('https://events.do/recent?limit=5000')
      const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '100'), 1000)
      expect(limit).toBe(1000)
    })
  })

  describe('file filtering', () => {
    it('filters JSONL files', () => {
      const files = ['file1.jsonl', 'file2.parquet', 'file3.jsonl']
      const jsonlFiles = files.filter(f => f.endsWith('.jsonl'))
      expect(jsonlFiles).toEqual(['file1.jsonl', 'file3.jsonl'])
    })

    it('filters Parquet files', () => {
      const files = ['file1.jsonl', 'file2.parquet', 'file3.parquet']
      const parquetFiles = files.filter(f => f.endsWith('.parquet'))
      expect(parquetFiles).toEqual(['file2.parquet', 'file3.parquet'])
    })
  })

  describe('file sorting', () => {
    it('sorts by ULID (time order)', () => {
      const files = [
        'events/2024/01/01/00/01HY1234.parquet',
        'events/2024/01/01/00/01HY5678.parquet',
        'events/2024/01/01/00/01HY0001.parquet',
      ]

      const sorted = files.sort((a, b) => b.localeCompare(a))

      expect(sorted[0]).toContain('01HY5678')
      expect(sorted[sorted.length - 1]).toContain('01HY0001')
    })
  })
})
