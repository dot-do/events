/**
 * TDD Tests for Snapshot and Query Utilities
 *
 * RED Phase - These tests should FAIL initially, demonstrating TDD approach
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  createSnapshot,
  restoreSnapshot,
  listSnapshots,
  type SnapshotOptions,
  type SnapshotResult,
} from '../src/snapshot'
import {
  buildQuery,
  buildHistoryQuery,
  buildLatencyQuery,
  buildPITRRangeQuery,
  type QueryOptions,
  type OrderBy,
} from '../src/query'
import { deleteSnapshot } from '../src/snapshot'

// =============================================================================
// MOCKS
// =============================================================================

/**
 * Mock SqlStorage interface matching Cloudflare Durable Objects
 */
interface MockExecResult<T> {
  toArray(): T[]
}

interface MockSqlStorage {
  exec: <T>(query: string, ...params: unknown[]) => MockExecResult<T>
}

function createMockSqlStorage(data: Record<string, { id: string; data: string }[]> = {}): MockSqlStorage {
  const execFn = vi.fn(<T>(query: string, ...params: unknown[]): MockExecResult<T> => {
    // Handle DISTINCT collection query
    if (query.includes('SELECT DISTINCT collection FROM _collections')) {
      const collections = Object.keys(data).map(collection => ({ collection }))
      return { toArray: () => collections as T[] }
    }

    // Handle SELECT documents for a collection
    if (query.includes('SELECT id, data FROM _collections WHERE collection = ?')) {
      const collection = params[0] as string
      const docs = data[collection] || []
      return { toArray: () => docs as T[] }
    }

    // Handle DELETE
    if (query.includes('DELETE FROM _collections WHERE collection = ?')) {
      return { toArray: () => [] as T[] }
    }

    // Handle INSERT
    if (query.includes('INSERT INTO _collections')) {
      return { toArray: () => [] as T[] }
    }

    return { toArray: () => [] as T[] }
  })

  return {
    exec: execFn,
  }
}

/**
 * Mock R2Bucket interface matching Cloudflare R2
 */
interface MockR2Object {
  key: string
  size: number
  etag: string
  httpMetadata?: { contentType?: string }
  customMetadata?: Record<string, string>
  body?: ReadableStream
  json: <T>() => Promise<T>
  text: () => Promise<string>
}

interface MockR2ObjectList {
  objects: MockR2Object[]
  truncated: boolean
  cursor?: string
}

interface MockR2Bucket {
  put: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  list: ReturnType<typeof vi.fn>
  _storage: Map<string, { body: string; metadata: Record<string, string>; httpMetadata: Record<string, string> }>
}

function createMockR2Bucket(): MockR2Bucket {
  const storage = new Map<string, { body: string; metadata: Record<string, string>; httpMetadata: Record<string, string> }>()

  const bucket: MockR2Bucket = {
    _storage: storage,

    put: vi.fn(async (key: string, body: string, options?: { customMetadata?: Record<string, string>; httpMetadata?: Record<string, string> }) => {
      storage.set(key, {
        body,
        metadata: options?.customMetadata || {},
        httpMetadata: options?.httpMetadata || {},
      })
      return {
        key,
        size: body.length,
        etag: 'mock-etag',
      }
    }),

    get: vi.fn(async (key: string): Promise<MockR2Object | null> => {
      const item = storage.get(key)
      if (!item) return null

      return {
        key,
        size: item.body.length,
        etag: 'mock-etag',
        customMetadata: item.metadata,
        httpMetadata: item.httpMetadata,
        json: async <T>() => JSON.parse(item.body) as T,
        text: async () => item.body,
      }
    }),

    delete: vi.fn(async (key: string) => {
      storage.delete(key)
    }),

    list: vi.fn(async (options?: { prefix?: string; limit?: number; cursor?: string }): Promise<MockR2ObjectList> => {
      const prefix = options?.prefix || ''
      const limit = options?.limit || 1000

      const objects: MockR2Object[] = []
      for (const [key, item] of storage) {
        if (key.startsWith(prefix)) {
          objects.push({
            key,
            size: item.body.length,
            etag: 'mock-etag',
            customMetadata: item.metadata,
            httpMetadata: item.httpMetadata,
            json: async <T>() => JSON.parse(item.body) as T,
            text: async () => item.body,
          })
        }
      }

      return {
        objects: objects.slice(0, limit),
        truncated: objects.length > limit,
      }
    }),
  }

  return bucket
}

// =============================================================================
// SNAPSHOT TESTS
// =============================================================================

describe('createSnapshot', () => {
  let mockSql: MockSqlStorage
  let mockBucket: MockR2Bucket

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-06-15T10:30:45.123Z'))
    mockBucket = createMockR2Bucket()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('creates JSON snapshot in R2 with correct path', async () => {
    const testData = {
      users: [
        { id: 'user-1', data: JSON.stringify({ name: 'Alice', email: 'alice@example.com' }) },
        { id: 'user-2', data: JSON.stringify({ name: 'Bob', email: 'bob@example.com' }) },
      ],
    }
    mockSql = createMockSqlStorage(testData)

    const result = await createSnapshot(mockSql as unknown as SqlStorage, 'do-123', {
      bucket: mockBucket as unknown as R2Bucket,
    })

    // Verify correct R2 key format
    expect(result.key).toBe('snapshots/do-123/2024-06-15T10-30-45-123Z.json')

    // Verify put was called with correct key
    expect(mockBucket.put).toHaveBeenCalledWith(
      'snapshots/do-123/2024-06-15T10-30-45-123Z.json',
      expect.any(String),
      expect.objectContaining({
        httpMetadata: { contentType: 'application/json' },
      })
    )
  })

  it('includes all collections and documents in snapshot', async () => {
    const testData = {
      users: [
        { id: 'user-1', data: JSON.stringify({ name: 'Alice' }) },
      ],
      orders: [
        { id: 'order-1', data: JSON.stringify({ product: 'Widget', qty: 5 }) },
        { id: 'order-2', data: JSON.stringify({ product: 'Gadget', qty: 3 }) },
      ],
    }
    mockSql = createMockSqlStorage(testData)

    const result = await createSnapshot(mockSql as unknown as SqlStorage, 'do-123', {
      bucket: mockBucket as unknown as R2Bucket,
    })

    // Verify all collections are included
    expect(result.collections).toContain('users')
    expect(result.collections).toContain('orders')
    expect(result.collections).toHaveLength(2)

    // Verify total doc count
    expect(result.totalDocs).toBe(3)

    // Verify stored JSON contains all data
    const storedData = mockBucket._storage.get(result.key)
    expect(storedData).toBeDefined()

    const parsed = JSON.parse(storedData!.body)
    expect(parsed.data.users).toHaveLength(1)
    expect(parsed.data.orders).toHaveLength(2)
    expect(parsed.data.users[0]).toMatchObject({ _id: 'user-1', name: 'Alice' })
    expect(parsed.data.orders[0]).toMatchObject({ _id: 'order-1', product: 'Widget', qty: 5 })
  })

  it('returns correct metadata (key, collections, totalDocs, timestamp)', async () => {
    const testData = {
      messages: [
        { id: 'msg-1', data: JSON.stringify({ text: 'Hello' }) },
        { id: 'msg-2', data: JSON.stringify({ text: 'World' }) },
      ],
    }
    mockSql = createMockSqlStorage(testData)

    const result = await createSnapshot(mockSql as unknown as SqlStorage, 'do-456', {
      bucket: mockBucket as unknown as R2Bucket,
      prefix: 'backups',
    })

    expect(result).toEqual({
      key: 'backups/do-456/2024-06-15T10-30-45-123Z.json',
      collections: ['messages'],
      totalDocs: 2,
      timestamp: '2024-06-15T10:30:45.123Z',
    })
  })

  it('sets correct R2 customMetadata', async () => {
    const testData = {
      products: [
        { id: 'prod-1', data: JSON.stringify({ name: 'Item A' }) },
      ],
      inventory: [
        { id: 'inv-1', data: JSON.stringify({ count: 100 }) },
        { id: 'inv-2', data: JSON.stringify({ count: 50 }) },
      ],
    }
    mockSql = createMockSqlStorage(testData)

    await createSnapshot(mockSql as unknown as SqlStorage, 'do-789', {
      bucket: mockBucket as unknown as R2Bucket,
    })

    expect(mockBucket.put).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      expect.objectContaining({
        customMetadata: {
          doId: 'do-789',
          timestamp: '2024-06-15T10:30:45.123Z',
          collections: 'products,inventory',
          docCount: '3',
        },
      })
    )
  })

  it('handles empty collections', async () => {
    mockSql = createMockSqlStorage({})

    const result = await createSnapshot(mockSql as unknown as SqlStorage, 'do-empty', {
      bucket: mockBucket as unknown as R2Bucket,
    })

    expect(result.collections).toHaveLength(0)
    expect(result.totalDocs).toBe(0)
  })
})

describe('restoreSnapshot', () => {
  let mockSql: MockSqlStorage
  let mockBucket: MockR2Bucket

  beforeEach(() => {
    mockSql = createMockSqlStorage()
    mockBucket = createMockR2Bucket()
  })

  it('loads snapshot from R2', async () => {
    const snapshotData = {
      doId: 'do-123',
      timestamp: '2024-06-15T10:30:45.123Z',
      collections: ['users'],
      totalDocs: 2,
      data: {
        users: [
          { _id: 'user-1', name: 'Alice' },
          { _id: 'user-2', name: 'Bob' },
        ],
      },
    }

    // Pre-populate bucket with snapshot
    await mockBucket.put(
      'snapshots/do-123/2024-06-15T10-30-45-123Z.json',
      JSON.stringify(snapshotData)
    )

    const result = await restoreSnapshot(
      mockSql as unknown as SqlStorage,
      mockBucket as unknown as R2Bucket,
      'snapshots/do-123/2024-06-15T10-30-45-123Z.json'
    )

    // Verify bucket.get was called with correct key
    expect(mockBucket.get).toHaveBeenCalledWith('snapshots/do-123/2024-06-15T10-30-45-123Z.json')
    expect(result.collections).toContain('users')
    expect(result.totalDocs).toBe(2)
  })

  it('clears existing collection data before restoring', async () => {
    const snapshotData = {
      data: {
        messages: [
          { _id: 'msg-1', text: 'Hello' },
        ],
      },
    }

    await mockBucket.put('snapshot.json', JSON.stringify(snapshotData))

    await restoreSnapshot(
      mockSql as unknown as SqlStorage,
      mockBucket as unknown as R2Bucket,
      'snapshot.json'
    )

    // Verify DELETE was called for the collection
    expect(mockSql.exec).toHaveBeenCalledWith(
      'DELETE FROM _collections WHERE collection = ?',
      'messages'
    )
  })

  it('inserts all documents from snapshot', async () => {
    const snapshotData = {
      data: {
        users: [
          { _id: 'user-1', name: 'Alice', role: 'admin' },
          { _id: 'user-2', name: 'Bob', role: 'user' },
        ],
        settings: [
          { _id: 'config', theme: 'dark' },
        ],
      },
    }

    await mockBucket.put('snapshot.json', JSON.stringify(snapshotData))

    const result = await restoreSnapshot(
      mockSql as unknown as SqlStorage,
      mockBucket as unknown as R2Bucket,
      'snapshot.json'
    )

    // Verify INSERT was called for each document
    expect(mockSql.exec).toHaveBeenCalledWith(
      'INSERT INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)',
      'users',
      'user-1',
      JSON.stringify({ name: 'Alice', role: 'admin' }),
      expect.any(String)
    )

    expect(mockSql.exec).toHaveBeenCalledWith(
      'INSERT INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)',
      'users',
      'user-2',
      JSON.stringify({ name: 'Bob', role: 'user' }),
      expect.any(String)
    )

    expect(mockSql.exec).toHaveBeenCalledWith(
      'INSERT INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)',
      'settings',
      'config',
      JSON.stringify({ theme: 'dark' }),
      expect.any(String)
    )

    expect(result.totalDocs).toBe(3)
  })

  it('returns correct counts after restore', async () => {
    const snapshotData = {
      data: {
        products: [
          { _id: 'p1', name: 'Product 1' },
          { _id: 'p2', name: 'Product 2' },
          { _id: 'p3', name: 'Product 3' },
        ],
        categories: [
          { _id: 'c1', name: 'Category 1' },
        ],
      },
    }

    await mockBucket.put('snapshot.json', JSON.stringify(snapshotData))

    const result = await restoreSnapshot(
      mockSql as unknown as SqlStorage,
      mockBucket as unknown as R2Bucket,
      'snapshot.json'
    )

    expect(result).toEqual({
      collections: ['products', 'categories'],
      totalDocs: 4,
    })
  })

  it('throws error when snapshot not found', async () => {
    await expect(
      restoreSnapshot(
        mockSql as unknown as SqlStorage,
        mockBucket as unknown as R2Bucket,
        'nonexistent-snapshot.json'
      )
    ).rejects.toThrow('Snapshot not found: nonexistent-snapshot.json')
  })
})

describe('listSnapshots', () => {
  let mockBucket: MockR2Bucket

  beforeEach(() => {
    mockBucket = createMockR2Bucket()
  })

  it('lists snapshots with correct prefix', async () => {
    // Add some snapshots
    await mockBucket.put('snapshots/do-123/snap1.json', '{}')
    await mockBucket.put('snapshots/do-123/snap2.json', '{}')
    await mockBucket.put('snapshots/do-456/snap1.json', '{}')

    const result = await listSnapshots(mockBucket as unknown as R2Bucket, 'do-123')

    expect(mockBucket.list).toHaveBeenCalledWith({
      prefix: 'snapshots/do-123/',
      limit: 100,
    })

    expect(result.objects).toHaveLength(2)
    expect(result.objects.map(o => o.key)).toContain('snapshots/do-123/snap1.json')
    expect(result.objects.map(o => o.key)).toContain('snapshots/do-123/snap2.json')
  })

  it('respects limit parameter', async () => {
    // Add multiple snapshots
    for (let i = 0; i < 10; i++) {
      await mockBucket.put(`snapshots/do-123/snap${i}.json`, '{}')
    }

    const result = await listSnapshots(mockBucket as unknown as R2Bucket, 'do-123', { limit: 5 })

    expect(mockBucket.list).toHaveBeenCalledWith({
      prefix: 'snapshots/do-123/',
      limit: 5,
    })

    expect(result.objects).toHaveLength(5)
    expect(result.truncated).toBe(true)
  })

  it('uses custom prefix when provided', async () => {
    await mockBucket.put('backups/do-123/snap1.json', '{}')

    const result = await listSnapshots(mockBucket as unknown as R2Bucket, 'do-123', { prefix: 'backups' })

    expect(mockBucket.list).toHaveBeenCalledWith({
      prefix: 'backups/do-123/',
      limit: 100,
    })

    expect(result.objects).toHaveLength(1)
  })

  it('returns empty list when no snapshots exist', async () => {
    const result = await listSnapshots(mockBucket as unknown as R2Bucket, 'do-999')

    expect(result.objects).toHaveLength(0)
    expect(result.truncated).toBe(false)
  })
})

describe('deleteSnapshot', () => {
  let mockBucket: MockR2Bucket

  beforeEach(() => {
    mockBucket = createMockR2Bucket()
  })

  it('deletes snapshot by key', async () => {
    // Pre-populate bucket with a snapshot
    await mockBucket.put('snapshots/do-123/2024-06-15T10-30-45-123Z.json', '{}')
    expect(mockBucket._storage.has('snapshots/do-123/2024-06-15T10-30-45-123Z.json')).toBe(true)

    await deleteSnapshot(
      mockBucket as unknown as R2Bucket,
      'snapshots/do-123/2024-06-15T10-30-45-123Z.json'
    )

    // Verify bucket.delete was called with correct key
    expect(mockBucket.delete).toHaveBeenCalledWith('snapshots/do-123/2024-06-15T10-30-45-123Z.json')
    // Verify the snapshot was removed from storage
    expect(mockBucket._storage.has('snapshots/do-123/2024-06-15T10-30-45-123Z.json')).toBe(false)
  })

  it('handles non-existent key gracefully', async () => {
    // R2 bucket.delete does not throw for non-existent keys
    await expect(
      deleteSnapshot(
        mockBucket as unknown as R2Bucket,
        'snapshots/non-existent/snapshot.json'
      )
    ).resolves.toBeUndefined()

    expect(mockBucket.delete).toHaveBeenCalledWith('snapshots/non-existent/snapshot.json')
  })

  it('returns void on successful deletion', async () => {
    await mockBucket.put('snapshots/do-456/test-snapshot.json', '{}')

    const result = await deleteSnapshot(
      mockBucket as unknown as R2Bucket,
      'snapshots/do-456/test-snapshot.json'
    )

    expect(result).toBeUndefined()
  })
})

// =============================================================================
// QUERY TESTS
// =============================================================================

describe('buildQuery', () => {
  it('generates correct path pattern for same year and month date range', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      dateRange: {
        start: new Date('2024-01-15'),
        end: new Date('2024-01-20'),
      },
    }

    const query = buildQuery(options)

    // Should include year and month in path, but wildcard for days
    expect(query).toContain("r2://my-events/events/2024/01/**/*.jsonl")
  })

  it('generates correct path pattern for same day date range', () => {
    // Use explicit local date creation to avoid timezone issues
    const sameDay = new Date(2024, 5, 15) // June 15, 2024 (month is 0-indexed)
    const options: QueryOptions = {
      bucket: 'my-events',
      dateRange: {
        start: sameDay,
        end: sameDay,
      },
    }

    const query = buildQuery(options)

    // Should include year, month, and day in path
    expect(query).toContain("r2://my-events/events/2024/06/15/*.jsonl")
  })

  it('generates correct path pattern for cross-year date range', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      dateRange: {
        start: new Date('2023-12-01'),
        end: new Date('2024-02-28'),
      },
    }

    const query = buildQuery(options)

    // Should use wildcard for everything when crossing years
    expect(query).toContain("r2://my-events/events/**/*.jsonl")
  })

  it('generates correct path pattern with no date range', () => {
    const options: QueryOptions = {
      bucket: 'analytics-bucket',
    }

    const query = buildQuery(options)

    expect(query).toContain("r2://analytics-bucket/events/**/*.jsonl")
  })

  it('includes doId filter condition', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      doId: 'abc-123-xyz',
    }

    const query = buildQuery(options)

    expect(query).toContain(`"do".id = 'abc-123-xyz'`)
  })

  it('includes doClass filter condition', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      doClass: 'ChatRoom',
    }

    const query = buildQuery(options)

    expect(query).toContain(`"do".class = 'ChatRoom'`)
  })

  it('includes colo filter condition', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      colo: 'SFO',
    }

    const query = buildQuery(options)

    expect(query).toContain(`"do".colo = 'SFO'`)
  })

  it('includes eventTypes filter condition', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      eventTypes: ['rpc.call', 'collection.insert'],
    }

    const query = buildQuery(options)

    expect(query).toContain("type IN ('rpc.call', 'collection.insert')")
  })

  it('includes collection filter condition', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      collection: 'users',
    }

    const query = buildQuery(options)

    expect(query).toContain("collection = 'users'")
  })

  it('handles dateRange timestamp conditions', () => {
    const start = new Date('2024-03-01T00:00:00.000Z')
    const end = new Date('2024-03-31T23:59:59.999Z')

    const options: QueryOptions = {
      bucket: 'my-events',
      dateRange: { start, end },
    }

    const query = buildQuery(options)

    expect(query).toContain(`ts >= '${start.toISOString()}'`)
    expect(query).toContain(`ts <= '${end.toISOString()}'`)
  })

  it('adds ORDER BY clause', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
    }

    const query = buildQuery(options)

    expect(query).toContain('ORDER BY ts DESC')
  })

  it('uses custom orderBy when provided', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      orderBy: 'ts ASC',
    }

    const query = buildQuery(options)

    expect(query).toContain('ORDER BY ts ASC')
  })

  it('rejects invalid orderBy column', () => {
    expect(() => buildQuery({
      bucket: 'my-events',
      // @ts-expect-error Testing runtime validation of invalid column
      orderBy: 'durationMs ASC',
    })).toThrow('Invalid orderBy column: "durationMs"')
  })

  it('rejects invalid orderBy direction', () => {
    expect(() => buildQuery({
      bucket: 'my-events',
      // @ts-expect-error Testing runtime validation of invalid direction
      orderBy: 'ts ASCENDING',
    })).toThrow('Invalid orderBy direction: "ASCENDING"')
  })

  it('accepts orderBy with column only (no direction)', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      orderBy: 'type',
    }

    const query = buildQuery(options)

    expect(query).toContain('ORDER BY type')
  })

  it('adds LIMIT clause when specified', () => {
    const options: QueryOptions = {
      bucket: 'my-events',
      limit: 100,
    }

    const query = buildQuery(options)

    expect(query).toContain('LIMIT 100')
  })

  it('combines all filter conditions correctly', () => {
    // Use local dates to avoid timezone issues with path generation
    const start = new Date(2024, 5, 1, 0, 0, 0, 0) // June 1, 2024
    const end = new Date(2024, 5, 30, 23, 59, 59, 999) // June 30, 2024

    const options: QueryOptions = {
      bucket: 'production-events',
      dateRange: { start, end },
      doId: 'do-abc',
      doClass: 'ChatRoom',
      colo: 'LAX',
      eventTypes: ['rpc.call'],
      collection: 'messages',
      limit: 500,
      orderBy: 'ts ASC',
    }

    const query = buildQuery(options)

    // All conditions should be present - path uses year/month/** since dates span multiple days
    expect(query).toContain("r2://production-events/events/2024/06/**/*.jsonl")
    expect(query).toContain(`"do".id = 'do-abc'`)
    expect(query).toContain(`"do".class = 'ChatRoom'`)
    expect(query).toContain(`"do".colo = 'LAX'`)
    expect(query).toContain("type IN ('rpc.call')")
    expect(query).toContain("collection = 'messages'")
    expect(query).toContain(`ts >= '${start.toISOString()}'`)
    expect(query).toContain(`ts <= '${end.toISOString()}'`)
    expect(query).toContain('ORDER BY ts ASC')
    expect(query).toContain('LIMIT 500')

    // Conditions should be joined with AND
    expect(query).toContain('AND')
  })
})

describe('buildHistoryQuery', () => {
  it('filters to CDC event types', () => {
    const query = buildHistoryQuery({
      bucket: 'my-events',
      collection: 'users',
      docId: 'user-123',
    })

    expect(query).toContain("type IN ('collection.created', 'collection.updated', 'collection.deleted')")
  })

  it('includes docId filter', () => {
    const query = buildHistoryQuery({
      bucket: 'my-events',
      collection: 'orders',
      docId: 'order-456',
    })

    expect(query).toContain("docId = 'order-456'")
  })

  it('includes collection filter', () => {
    const query = buildHistoryQuery({
      bucket: 'my-events',
      collection: 'products',
      docId: 'prod-789',
    })

    expect(query).toContain("collection = 'products'")
  })

  it('orders by ts ASC for history', () => {
    const query = buildHistoryQuery({
      bucket: 'my-events',
      collection: 'users',
      docId: 'user-123',
    })

    expect(query).toContain('ORDER BY ts ASC')
  })

  it('applies date range filter when provided', () => {
    const start = new Date('2024-01-01T00:00:00.000Z')
    const end = new Date('2024-12-31T23:59:59.999Z')

    const query = buildHistoryQuery({
      bucket: 'my-events',
      collection: 'users',
      docId: 'user-123',
      dateRange: { start, end },
    })

    expect(query).toContain(`ts >= '${start.toISOString()}'`)
    expect(query).toContain(`ts <= '${end.toISOString()}'`)
  })

  it('builds complete history query with all conditions', () => {
    const query = buildHistoryQuery({
      bucket: 'analytics',
      collection: 'accounts',
      docId: 'acc-xyz',
      dateRange: {
        start: new Date('2024-06-01'),
        end: new Date('2024-06-30'),
      },
    })

    // Should have SELECT statement
    expect(query).toMatch(/SELECT \* FROM read_json_auto/)

    // Should have all necessary filters
    expect(query).toContain("docId = 'acc-xyz'")
    expect(query).toContain("collection = 'accounts'")
    expect(query).toContain("type IN ('collection.created', 'collection.updated', 'collection.deleted')")

    // Should order by timestamp ascending
    expect(query).toContain('ORDER BY ts ASC')
  })
})

describe('buildLatencyQuery', () => {
  it('filters to rpc.call events', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain("type = 'rpc.call'")
  })

  it('aggregates by class and method', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('GROUP BY "do".class, method')
  })

  it('includes percentiles (p50, p95, p99)', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('PERCENTILE_CONT(0.5)')
    expect(query).toContain('PERCENTILE_CONT(0.95)')
    expect(query).toContain('PERCENTILE_CONT(0.99)')

    // Should alias them appropriately
    expect(query).toContain('p50_ms')
    expect(query).toContain('p95_ms')
    expect(query).toContain('p99_ms')
  })

  it('includes average duration', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('AVG(durationMs)')
    expect(query).toContain('avg_duration_ms')
  })

  it('includes call count', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('COUNT(*)')
    expect(query).toContain('call_count')
  })

  it('includes max duration', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('MAX(durationMs)')
    expect(query).toContain('max_duration_ms')
  })

  it('includes error count', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('error_count')
  })

  it('filters by doClass when provided', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
      doClass: 'ChatRoom',
    })

    expect(query).toContain(`"do".class = 'ChatRoom'`)
  })

  it('filters by method when provided', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
      method: 'sendMessage',
    })

    expect(query).toContain("method = 'sendMessage'")
  })

  it('uses date range for path pattern', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
      dateRange: {
        start: new Date('2024-06-01'),
        end: new Date('2024-06-30'),
      },
    })

    expect(query).toContain('r2://my-events/events/2024/')
  })

  it('orders by call_count DESC', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
    })

    expect(query).toContain('ORDER BY call_count DESC')
  })

  it('builds complete latency query with all options', () => {
    const query = buildLatencyQuery({
      bucket: 'production',
      dateRange: {
        start: new Date('2024-03-01'),
        end: new Date('2024-03-31'),
      },
      doClass: 'GameRoom',
      method: 'move',
    })

    // Should have all columns
    expect(query).toContain('"do".class')
    expect(query).toContain('method')
    expect(query).toContain('COUNT(*) as call_count')
    expect(query).toContain('AVG(durationMs) as avg_duration_ms')
    expect(query).toContain('PERCENTILE_CONT(0.5)')
    expect(query).toContain('PERCENTILE_CONT(0.95)')
    expect(query).toContain('PERCENTILE_CONT(0.99)')
    expect(query).toContain('MAX(durationMs) as max_duration_ms')

    // Should have filters
    expect(query).toContain("type = 'rpc.call'")
    expect(query).toContain(`"do".class = 'GameRoom'`)
    expect(query).toContain("method = 'move'")

    // Should have GROUP BY and ORDER BY
    expect(query).toContain('GROUP BY "do".class, method')
    expect(query).toContain('ORDER BY call_count DESC')
  })
})

describe('buildPITRRangeQuery', () => {
  it('includes startBookmark and endBookmark conditions', () => {
    const query = buildPITRRangeQuery({
      bucket: 'my-events',
      startBookmark: 'bookmark-001',
      endBookmark: 'bookmark-100',
    })

    expect(query).toContain("bookmark > 'bookmark-001'")
    expect(query).toContain("bookmark <= 'bookmark-100'")
  })

  it('orders by ts ASC for replay', () => {
    const query = buildPITRRangeQuery({
      bucket: 'my-events',
      startBookmark: 'start',
      endBookmark: 'end',
    })

    expect(query).toContain('ORDER BY ts ASC')
  })

  it('filters by collection when provided', () => {
    const query = buildPITRRangeQuery({
      bucket: 'my-events',
      startBookmark: 'start',
      endBookmark: 'end',
      collection: 'users',
    })

    expect(query).toContain("collection = 'users'")
  })

  it('omits collection filter when not provided', () => {
    const query = buildPITRRangeQuery({
      bucket: 'my-events',
      startBookmark: 'start',
      endBookmark: 'end',
    })

    expect(query).not.toContain('collection =')
  })

  it('uses correct R2 path pattern', () => {
    const query = buildPITRRangeQuery({
      bucket: 'analytics',
      startBookmark: 'bm-1',
      endBookmark: 'bm-2',
    })

    expect(query).toContain("r2://analytics/events/**/*.jsonl")
  })

  it('builds complete PITR range query with all options', () => {
    const query = buildPITRRangeQuery({
      bucket: 'production',
      startBookmark: '2024-01-15T10:00:00.000Z-001',
      endBookmark: '2024-01-15T12:00:00.000Z-050',
      collection: 'orders',
    })

    // Should have SELECT statement (with potential newline after SELECT *)
    expect(query).toMatch(/SELECT \*[\s\S]*FROM read_json_auto/)

    // Should have all conditions
    expect(query).toContain("bookmark > '2024-01-15T10:00:00.000Z-001'")
    expect(query).toContain("bookmark <= '2024-01-15T12:00:00.000Z-050'")
    expect(query).toContain("collection = 'orders'")

    // Should order by ts ASC
    expect(query).toContain('ORDER BY ts ASC')
  })
})

describe('SQL injection prevention', () => {
  it('escapes SQL injection attempts in doId filter', () => {
    const query = buildQuery({
      bucket: 'my-events',
      doId: "'; DROP TABLE events; --",
    })

    // The single quote in the input should be escaped to double single quotes
    // Input: '; DROP TABLE events; --
    // Output string value: ''; DROP TABLE events; --'
    // The attacker's quote is escaped, so the entire malicious input becomes a string literal
    expect(query).toContain("''")  // Escaped quote exists
    // Verify the escaped pattern - the single quote becomes two single quotes
    expect(query).toContain(`"do".id = '''; DROP TABLE events; --'`)
  })

  it('escapes SQL injection attempts in doClass filter', () => {
    const query = buildQuery({
      bucket: 'my-events',
      doClass: "ChatRoom'; DELETE FROM users; --",
    })

    // The quote is escaped, making DELETE part of the string literal
    expect(query).toContain("''")  // Escaped quote
    expect(query).toContain(`"do".class = 'ChatRoom''; DELETE FROM users; --'`)
  })

  it('escapes SQL injection attempts in collection filter', () => {
    const query = buildQuery({
      bucket: 'my-events',
      collection: "users'; UPDATE accounts SET balance=1000000; --",
    })

    expect(query).toContain("''")  // Escaped quote
    expect(query).toContain(`collection = 'users''; UPDATE accounts SET balance=1000000; --'`)
  })

  it('escapes SQL injection attempts in eventTypes filter', () => {
    const query = buildQuery({
      bucket: 'my-events',
      eventTypes: ["rpc.call'; TRUNCATE events; --"],
    })

    expect(query).toContain("''")  // Escaped quote
    expect(query).toContain(`'rpc.call''; TRUNCATE events; --'`)
  })

  it('escapes SQL injection attempts in colo filter', () => {
    const query = buildQuery({
      bucket: 'my-events',
      colo: "SFO'; DROP DATABASE; --",
    })

    expect(query).toContain("''")  // Escaped quote
    expect(query).toContain(`"do".colo = 'SFO''; DROP DATABASE; --'`)
  })

  it('escapes SQL injection attempts in buildHistoryQuery docId', () => {
    const query = buildHistoryQuery({
      bucket: 'my-events',
      collection: 'users',
      docId: "user-123'; DROP TABLE users; --",
    })

    expect(query).toContain("''")  // Escaped quote
    expect(query).toContain(`docId = 'user-123''; DROP TABLE users; --'`)
  })

  it('escapes SQL injection attempts in buildLatencyQuery method', () => {
    const query = buildLatencyQuery({
      bucket: 'my-events',
      method: "sendMessage'; DELETE FROM events; --",
    })

    expect(query).toContain("''")  // Escaped quote
    expect(query).toContain(`method = 'sendMessage''; DELETE FROM events; --'`)
  })
})

// Import afterEach for fake timers cleanup
import { afterEach } from 'vitest'
