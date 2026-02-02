/**
 * Snapshot Tests
 *
 * Tests for R2-based snapshot/backup utilities for Collections
 *
 * Functions tested:
 * - createSnapshot() - creates point-in-time snapshots to R2
 * - restoreSnapshot() - restores collections from R2 snapshots
 * - listSnapshots() - lists available snapshots for a DO
 * - deleteSnapshot() - deletes a snapshot from R2
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  createSnapshot,
  restoreSnapshot,
  listSnapshots,
  deleteSnapshot,
  type SnapshotOptions,
  type SnapshotResult,
} from '../snapshot.js'
import { InvalidR2PathError } from '../r2-path.js'

// ============================================================================
// Mock Factories
// ============================================================================

interface MockR2ObjectBody {
  key: string
  size: number
  text: () => Promise<string>
  json: <T>() => Promise<T>
}

interface MockR2PutResult {
  key: string
  size: number
}

interface MockR2ListResult {
  objects: Array<{ key: string; size: number; uploaded: Date }>
  truncated: boolean
}

/**
 * Creates a mock SqlStorage with in-memory _collections table
 */
function createMockSqlStorage(initialData: Record<string, Array<{ id: string; data: Record<string, unknown> }>> = {}) {
  // Store documents keyed by collection:id
  const collectionsTable = new Map<string, { collection: string; id: string; data: string; updated_at: string }>()

  // Populate initial data
  for (const [collection, docs] of Object.entries(initialData)) {
    for (const doc of docs) {
      const key = `${collection}:${doc.id}`
      collectionsTable.set(key, {
        collection,
        id: doc.id,
        data: JSON.stringify(doc.data),
        updated_at: new Date().toISOString(),
      })
    }
  }

  const execFn = vi.fn((query: string, ...params: unknown[]) => {
    const trimmed = query.trim().toUpperCase()

    // SELECT DISTINCT collection FROM _collections
    if (trimmed.includes('SELECT DISTINCT COLLECTION FROM _COLLECTIONS')) {
      const collections = new Set<string>()
      for (const row of collectionsTable.values()) {
        collections.add(row.collection)
      }
      return {
        toArray: () => Array.from(collections).map(c => ({ collection: c })),
      }
    }

    // SELECT id, data FROM _collections WHERE collection = ?
    if (trimmed.includes('SELECT ID, DATA FROM _COLLECTIONS WHERE COLLECTION')) {
      const collection = params[0] as string
      const rows: Array<{ id: string; data: string }> = []
      for (const row of collectionsTable.values()) {
        if (row.collection === collection) {
          rows.push({ id: row.id, data: row.data })
        }
      }
      return {
        toArray: () => rows,
      }
    }

    // DELETE FROM _collections WHERE collection = ?
    if (trimmed.includes('DELETE FROM _COLLECTIONS WHERE COLLECTION')) {
      const collection = params[0] as string
      for (const [key, row] of collectionsTable) {
        if (row.collection === collection) {
          collectionsTable.delete(key)
        }
      }
      return { toArray: () => [] }
    }

    // INSERT INTO _collections
    if (trimmed.includes('INSERT INTO _COLLECTIONS')) {
      const [collection, id, data, updated_at] = params as [string, string, string, string]
      const key = `${collection}:${id}`
      collectionsTable.set(key, { collection, id, data, updated_at })
      return { toArray: () => [] }
    }

    return { toArray: () => [] }
  })

  return {
    exec: execFn,
    _data: collectionsTable,
  } as unknown as SqlStorage & {
    exec: typeof execFn
    _data: Map<string, { collection: string; id: string; data: string; updated_at: string }>
  }
}

/**
 * Creates a mock R2Bucket with in-memory storage
 */
function createMockR2Bucket(initialFiles: Record<string, string> = {}) {
  const files = new Map<string, string>(Object.entries(initialFiles))
  const deletedKeys: string[] = []

  return {
    files,
    deletedKeys,

    put: vi.fn(async (key: string, body: string, options?: R2PutOptions): Promise<MockR2PutResult> => {
      files.set(key, body)
      return {
        key,
        size: body.length,
      }
    }),

    get: vi.fn(async (key: string): Promise<MockR2ObjectBody | null> => {
      const body = files.get(key)
      if (!body) return null

      return {
        key,
        size: body.length,
        text: async () => body,
        json: async <T>() => JSON.parse(body) as T,
      }
    }),

    delete: vi.fn(async (key: string): Promise<void> => {
      files.delete(key)
      deletedKeys.push(key)
    }),

    list: vi.fn(async (options?: { prefix?: string; limit?: number }): Promise<MockR2ListResult> => {
      const objects: Array<{ key: string; size: number; uploaded: Date }> = []
      const limit = options?.limit ?? 100

      for (const [key, body] of files) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          objects.push({
            key,
            size: body.length,
            uploaded: new Date(),
          })
        }
      }

      // Sort by key and apply limit
      objects.sort((a, b) => a.key.localeCompare(b.key))
      const truncated = objects.length > limit
      const limitedObjects = objects.slice(0, limit)

      return {
        objects: limitedObjects,
        truncated,
      }
    }),

    head: vi.fn(async (key: string) => {
      const body = files.get(key)
      if (!body) return null
      return { key, size: body.length }
    }),
  } as unknown as R2Bucket & {
    files: Map<string, string>
    deletedKeys: string[]
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('Snapshot Utilities', () => {
  let mockBucket: ReturnType<typeof createMockR2Bucket>
  let mockSql: ReturnType<typeof createMockSqlStorage>

  beforeEach(() => {
    mockBucket = createMockR2Bucket()
    mockSql = createMockSqlStorage()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:30:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  // ==========================================================================
  // createSnapshot() Tests
  // ==========================================================================

  describe('createSnapshot()', () => {
    describe('basic functionality', () => {
      it('creates a snapshot with data from all collections', async () => {
        mockSql = createMockSqlStorage({
          users: [
            { id: 'user-1', data: { name: 'Alice', email: 'alice@test.com' } },
            { id: 'user-2', data: { name: 'Bob', email: 'bob@test.com' } },
          ],
          posts: [
            { id: 'post-1', data: { title: 'Hello', author: 'user-1' } },
          ],
        })

        const result = await createSnapshot(mockSql, 'test-do-123', {
          bucket: mockBucket,
        })

        expect(result.collections).toContain('users')
        expect(result.collections).toContain('posts')
        expect(result.totalDocs).toBe(3)
        expect(result.timestamp).toBe('2024-01-15T12:30:00.000Z')
        expect(result.key).toMatch(/^snapshots\/test-do-123\//)
      })

      it('stores snapshot in R2 with correct JSON structure', async () => {
        mockSql = createMockSqlStorage({
          users: [
            { id: 'user-1', data: { name: 'Alice' } },
          ],
        })

        await createSnapshot(mockSql, 'test-do', { bucket: mockBucket })

        // Verify the snapshot was stored
        expect(mockBucket.put).toHaveBeenCalled()

        // Get the stored content
        const putCall = mockBucket.put.mock.calls[0]
        const storedBody = JSON.parse(putCall[1] as string)

        expect(storedBody.doId).toBe('test-do')
        expect(storedBody.timestamp).toBe('2024-01-15T12:30:00.000Z')
        expect(storedBody.collections).toEqual(['users'])
        expect(storedBody.totalDocs).toBe(1)
        expect(storedBody.data.users).toHaveLength(1)
        expect(storedBody.data.users[0]).toEqual({ _id: 'user-1', name: 'Alice' })
      })

      it('includes _id field in snapshot documents', async () => {
        mockSql = createMockSqlStorage({
          items: [
            { id: 'item-abc', data: { value: 42 } },
          ],
        })

        await createSnapshot(mockSql, 'do-id', { bucket: mockBucket })

        const putCall = mockBucket.put.mock.calls[0]
        const storedBody = JSON.parse(putCall[1] as string)

        expect(storedBody.data.items[0]._id).toBe('item-abc')
        expect(storedBody.data.items[0].value).toBe(42)
      })

      it('stores snapshot with correct R2 metadata', async () => {
        mockSql = createMockSqlStorage({
          users: [{ id: 'u1', data: { name: 'Test' } }],
          orders: [{ id: 'o1', data: { total: 100 } }],
        })

        await createSnapshot(mockSql, 'my-do', { bucket: mockBucket })

        const putCall = mockBucket.put.mock.calls[0]
        const options = putCall[2] as R2PutOptions

        expect(options?.httpMetadata?.contentType).toBe('application/json')
        expect(options?.customMetadata?.doId).toBe('my-do')
        expect(options?.customMetadata?.timestamp).toBe('2024-01-15T12:30:00.000Z')
        expect(options?.customMetadata?.collections).toBe('users,orders')
        expect(options?.customMetadata?.docCount).toBe('2')
      })
    })

    describe('custom prefix', () => {
      it('uses default prefix when not specified', async () => {
        mockSql = createMockSqlStorage({
          items: [{ id: 'i1', data: {} }],
        })

        const result = await createSnapshot(mockSql, 'do-123', { bucket: mockBucket })

        expect(result.key).toMatch(/^snapshots\/do-123\//)
      })

      it('uses custom prefix when specified', async () => {
        mockSql = createMockSqlStorage({
          items: [{ id: 'i1', data: {} }],
        })

        const result = await createSnapshot(mockSql, 'do-123', {
          bucket: mockBucket,
          prefix: 'backups/daily',
        })

        expect(result.key).toMatch(/^backups\/daily\/do-123\//)
      })
    })

    describe('timestamp formatting', () => {
      it('uses ISO timestamp in filename with colons and dots replaced', async () => {
        mockSql = createMockSqlStorage({
          data: [{ id: 'd1', data: {} }],
        })

        vi.setSystemTime(new Date('2024-03-20T14:45:30.123Z'))

        const result = await createSnapshot(mockSql, 'do', { bucket: mockBucket })

        // Timestamp should have colons and dots replaced with hyphens
        expect(result.key).toContain('2024-03-20T14-45-30-123Z')
      })
    })

    describe('multiple collections', () => {
      it('handles multiple collections correctly', async () => {
        mockSql = createMockSqlStorage({
          users: [
            { id: 'u1', data: { name: 'User 1' } },
            { id: 'u2', data: { name: 'User 2' } },
          ],
          products: [
            { id: 'p1', data: { name: 'Product 1' } },
          ],
          orders: [
            { id: 'o1', data: { total: 100 } },
            { id: 'o2', data: { total: 200 } },
            { id: 'o3', data: { total: 300 } },
          ],
        })

        const result = await createSnapshot(mockSql, 'shop-do', { bucket: mockBucket })

        expect(result.collections).toHaveLength(3)
        expect(result.collections).toContain('users')
        expect(result.collections).toContain('products')
        expect(result.collections).toContain('orders')
        expect(result.totalDocs).toBe(6)
      })
    })
  })

  // ==========================================================================
  // restoreSnapshot() Tests
  // ==========================================================================

  describe('restoreSnapshot()', () => {
    describe('basic functionality', () => {
      it('restores documents from snapshot to collections', async () => {
        const snapshotData = {
          doId: 'restored-do',
          timestamp: '2024-01-10T00:00:00.000Z',
          collections: ['users'],
          totalDocs: 2,
          data: {
            users: [
              { _id: 'user-1', name: 'Alice', email: 'alice@test.com' },
              { _id: 'user-2', name: 'Bob', email: 'bob@test.com' },
            ],
          },
        }
        mockBucket = createMockR2Bucket({
          'snapshots/do-123/snapshot.json': JSON.stringify(snapshotData),
        })

        const result = await restoreSnapshot(
          mockSql,
          mockBucket,
          'snapshots/do-123/snapshot.json'
        )

        expect(result.collections).toEqual(['users'])
        expect(result.totalDocs).toBe(2)
      })

      it('clears existing collection data before restoring', async () => {
        // Pre-populate SQL with existing data
        mockSql = createMockSqlStorage({
          users: [
            { id: 'old-user', data: { name: 'Old User' } },
          ],
        })

        const snapshotData = {
          data: {
            users: [
              { _id: 'new-user', name: 'New User' },
            ],
          },
        }
        mockBucket = createMockR2Bucket({
          'snapshots/do/snap.json': JSON.stringify(snapshotData),
        })

        await restoreSnapshot(mockSql, mockBucket, 'snapshots/do/snap.json')

        // Verify DELETE was called
        const deleteCalls = mockSql.exec.mock.calls.filter(
          call => (call[0] as string).toUpperCase().includes('DELETE')
        )
        expect(deleteCalls.length).toBeGreaterThan(0)
      })

      it('inserts documents with correct structure', async () => {
        const snapshotData = {
          data: {
            items: [
              { _id: 'item-1', value: 42, nested: { key: 'val' } },
            ],
          },
        }
        mockBucket = createMockR2Bucket({
          'snap.json': JSON.stringify(snapshotData),
        })

        await restoreSnapshot(mockSql, mockBucket, 'snap.json')

        // Find the INSERT call
        const insertCalls = mockSql.exec.mock.calls.filter(
          call => (call[0] as string).toUpperCase().includes('INSERT')
        )
        expect(insertCalls.length).toBe(1)

        const insertCall = insertCalls[0]
        expect(insertCall[1]).toBe('items') // collection
        expect(insertCall[2]).toBe('item-1') // id
        expect(JSON.parse(insertCall[3] as string)).toEqual({ value: 42, nested: { key: 'val' } })
      })

      it('restores multiple collections', async () => {
        const snapshotData = {
          data: {
            users: [
              { _id: 'u1', name: 'Alice' },
              { _id: 'u2', name: 'Bob' },
            ],
            posts: [
              { _id: 'p1', title: 'Hello' },
            ],
            comments: [
              { _id: 'c1', text: 'Nice!' },
              { _id: 'c2', text: 'Great!' },
            ],
          },
        }
        mockBucket = createMockR2Bucket({
          'snapshot.json': JSON.stringify(snapshotData),
        })

        const result = await restoreSnapshot(mockSql, mockBucket, 'snapshot.json')

        expect(result.collections).toHaveLength(3)
        expect(result.collections).toContain('users')
        expect(result.collections).toContain('posts')
        expect(result.collections).toContain('comments')
        expect(result.totalDocs).toBe(5)
      })
    })

    describe('error handling', () => {
      it('throws error when snapshot not found', async () => {
        await expect(
          restoreSnapshot(mockSql, mockBucket, 'nonexistent/snapshot.json')
        ).rejects.toThrow('Snapshot not found: nonexistent/snapshot.json')
      })

      it('throws InvalidR2PathError for path traversal attempts', async () => {
        await expect(
          restoreSnapshot(mockSql, mockBucket, '../../../etc/passwd')
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('throws InvalidR2PathError for paths with double dots', async () => {
        await expect(
          restoreSnapshot(mockSql, mockBucket, 'snapshots/../secrets/data.json')
        ).rejects.toThrow(InvalidR2PathError)
      })
    })
  })

  // ==========================================================================
  // listSnapshots() Tests
  // ==========================================================================

  describe('listSnapshots()', () => {
    describe('basic functionality', () => {
      it('lists snapshots for a specific DO', async () => {
        mockBucket = createMockR2Bucket({
          'snapshots/do-123/2024-01-10.json': '{}',
          'snapshots/do-123/2024-01-11.json': '{}',
          'snapshots/do-456/2024-01-10.json': '{}', // Different DO
        })

        const result = await listSnapshots(mockBucket, 'do-123')

        expect(result.objects).toHaveLength(2)
        expect(result.objects[0].key).toContain('do-123')
        expect(result.objects[1].key).toContain('do-123')
      })

      it('uses default prefix of "snapshots"', async () => {
        mockBucket = createMockR2Bucket({
          'snapshots/my-do/snap1.json': '{}',
        })

        await listSnapshots(mockBucket, 'my-do')

        expect(mockBucket.list).toHaveBeenCalledWith({
          prefix: 'snapshots/my-do/',
          limit: 100,
        })
      })

      it('uses custom prefix when specified', async () => {
        mockBucket = createMockR2Bucket({
          'backups/daily/my-do/snap1.json': '{}',
        })

        await listSnapshots(mockBucket, 'my-do', { prefix: 'backups/daily' })

        expect(mockBucket.list).toHaveBeenCalledWith({
          prefix: 'backups/daily/my-do/',
          limit: 100,
        })
      })

      it('respects limit option', async () => {
        mockBucket = createMockR2Bucket({
          'snapshots/do/snap1.json': '{}',
          'snapshots/do/snap2.json': '{}',
          'snapshots/do/snap3.json': '{}',
        })

        await listSnapshots(mockBucket, 'do', { limit: 2 })

        expect(mockBucket.list).toHaveBeenCalledWith({
          prefix: 'snapshots/do/',
          limit: 2,
        })
      })

      it('returns empty list when no snapshots exist', async () => {
        const result = await listSnapshots(mockBucket, 'nonexistent-do')

        expect(result.objects).toHaveLength(0)
      })
    })

    describe('path safety', () => {
      it('throws InvalidR2PathError for path traversal in doId', async () => {
        await expect(
          listSnapshots(mockBucket, '../../../etc')
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('throws InvalidR2PathError for doId with slashes', async () => {
        await expect(
          listSnapshots(mockBucket, 'do/../../secrets')
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('throws InvalidR2PathError for path traversal in prefix', async () => {
        await expect(
          listSnapshots(mockBucket, 'valid-do', { prefix: '../secrets' })
        ).rejects.toThrow(InvalidR2PathError)
      })
    })
  })

  // ==========================================================================
  // deleteSnapshot() Tests
  // ==========================================================================

  describe('deleteSnapshot()', () => {
    describe('basic functionality', () => {
      it('deletes a snapshot from R2', async () => {
        mockBucket = createMockR2Bucket({
          'snapshots/do/2024-01-15.json': '{"data": {}}',
        })

        await deleteSnapshot(mockBucket, 'snapshots/do/2024-01-15.json')

        expect(mockBucket.delete).toHaveBeenCalledWith('snapshots/do/2024-01-15.json')
        expect(mockBucket.files.has('snapshots/do/2024-01-15.json')).toBe(false)
      })

      it('does not throw when deleting nonexistent snapshot', async () => {
        await expect(
          deleteSnapshot(mockBucket, 'nonexistent.json')
        ).resolves.toBeUndefined()
      })
    })

    describe('path safety', () => {
      it('throws InvalidR2PathError for path traversal attempts', async () => {
        await expect(
          deleteSnapshot(mockBucket, '../../../important/file.json')
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('throws InvalidR2PathError for paths with backslashes', async () => {
        await expect(
          deleteSnapshot(mockBucket, 'snapshots\\..\\secrets.json')
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('throws InvalidR2PathError for null bytes in path', async () => {
        await expect(
          deleteSnapshot(mockBucket, 'snapshots/do/file\0.json')
        ).rejects.toThrow(InvalidR2PathError)
      })
    })
  })

  // ==========================================================================
  // Error Handling Tests
  // ==========================================================================

  describe('error handling', () => {
    describe('corrupted data', () => {
      it('restoreSnapshot throws on invalid JSON in snapshot', async () => {
        mockBucket = createMockR2Bucket({
          'invalid.json': 'not valid json {{{',
        })

        await expect(
          restoreSnapshot(mockSql, mockBucket, 'invalid.json')
        ).rejects.toThrow()
      })

      it('restoreSnapshot throws on snapshot with missing data field', async () => {
        mockBucket = createMockR2Bucket({
          'missing-data.json': JSON.stringify({ doId: 'test', timestamp: '2024-01-15' }),
        })

        // Should throw since data field is required
        await expect(
          restoreSnapshot(mockSql, mockBucket, 'missing-data.json')
        ).rejects.toThrow()
      })

      it('restoreSnapshot handles empty data object', async () => {
        mockBucket = createMockR2Bucket({
          'empty-data.json': JSON.stringify({ data: {} }),
        })

        const result = await restoreSnapshot(mockSql, mockBucket, 'empty-data.json')
        expect(result.totalDocs).toBe(0)
        expect(result.collections).toHaveLength(0)
      })
    })

    describe('path validation', () => {
      it('createSnapshot throws for invalid doId with path traversal', async () => {
        mockSql = createMockSqlStorage({
          items: [{ id: 'i1', data: {} }],
        })

        await expect(
          createSnapshot(mockSql, '../../../etc', { bucket: mockBucket })
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('createSnapshot throws for doId with slashes', async () => {
        mockSql = createMockSqlStorage({
          items: [{ id: 'i1', data: {} }],
        })

        await expect(
          createSnapshot(mockSql, 'valid/../../secrets', { bucket: mockBucket })
        ).rejects.toThrow(InvalidR2PathError)
      })

      it('createSnapshot throws for prefix with path traversal', async () => {
        mockSql = createMockSqlStorage({
          items: [{ id: 'i1', data: {} }],
        })

        await expect(
          createSnapshot(mockSql, 'valid-do', { bucket: mockBucket, prefix: '../secrets' })
        ).rejects.toThrow(InvalidR2PathError)
      })
    })
  })

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('edge cases', () => {
    describe('empty state', () => {
      it('createSnapshot handles DO with no collections', async () => {
        mockSql = createMockSqlStorage({}) // Empty

        const result = await createSnapshot(mockSql, 'empty-do', { bucket: mockBucket })

        expect(result.collections).toHaveLength(0)
        expect(result.totalDocs).toBe(0)
      })

      it('createSnapshot handles collections with no documents', async () => {
        // Edge case: collection exists but has no docs
        mockSql = createMockSqlStorage({})

        const result = await createSnapshot(mockSql, 'no-docs-do', { bucket: mockBucket })

        expect(result.totalDocs).toBe(0)
      })

      it('restoreSnapshot handles snapshot with empty collections', async () => {
        mockBucket = createMockR2Bucket({
          'empty-collections.json': JSON.stringify({
            data: {
              users: [],
              posts: [],
            },
          }),
        })

        const result = await restoreSnapshot(mockSql, mockBucket, 'empty-collections.json')

        expect(result.collections).toHaveLength(2)
        expect(result.totalDocs).toBe(0)
      })
    })

    describe('large state', () => {
      it('createSnapshot handles many collections', async () => {
        const collections: Record<string, Array<{ id: string; data: Record<string, unknown> }>> = {}
        for (let i = 0; i < 50; i++) {
          collections[`collection_${i}`] = [
            { id: `doc_${i}`, data: { index: i } },
          ]
        }
        mockSql = createMockSqlStorage(collections)

        const result = await createSnapshot(mockSql, 'many-collections', { bucket: mockBucket })

        expect(result.collections).toHaveLength(50)
        expect(result.totalDocs).toBe(50)
      })

      it('createSnapshot handles collection with many documents', async () => {
        const docs: Array<{ id: string; data: Record<string, unknown> }> = []
        for (let i = 0; i < 1000; i++) {
          docs.push({ id: `doc-${i}`, data: { value: i, name: `Document ${i}` } })
        }
        mockSql = createMockSqlStorage({ large_collection: docs })

        const result = await createSnapshot(mockSql, 'large-do', { bucket: mockBucket })

        expect(result.totalDocs).toBe(1000)
        expect(result.collections).toEqual(['large_collection'])
      })

      it('restoreSnapshot handles many documents', async () => {
        const docs = []
        for (let i = 0; i < 500; i++) {
          docs.push({ _id: `doc-${i}`, value: i })
        }
        mockBucket = createMockR2Bucket({
          'large.json': JSON.stringify({
            data: { items: docs },
          }),
        })

        const result = await restoreSnapshot(mockSql, mockBucket, 'large.json')

        expect(result.totalDocs).toBe(500)
      })
    })

    describe('special characters', () => {
      it('createSnapshot handles doId with allowed special characters', async () => {
        mockSql = createMockSqlStorage({
          items: [{ id: 'i1', data: {} }],
        })

        // Hyphens and underscores should be allowed
        const result = await createSnapshot(mockSql, 'do-123_test', { bucket: mockBucket })

        expect(result.key).toContain('do-123_test')
      })

      it('handles documents with special characters in data', async () => {
        mockSql = createMockSqlStorage({
          items: [
            { id: 'i1', data: { text: 'Hello "World"', emoji: '🎉', unicode: 'こんにちは' } },
          ],
        })

        const result = await createSnapshot(mockSql, 'special-chars', { bucket: mockBucket })

        const putCall = mockBucket.put.mock.calls[0]
        const storedBody = JSON.parse(putCall[1] as string)

        expect(storedBody.data.items[0].text).toBe('Hello "World"')
        expect(storedBody.data.items[0].emoji).toBe('🎉')
        expect(storedBody.data.items[0].unicode).toBe('こんにちは')
      })

      it('restoreSnapshot handles documents with special characters', async () => {
        mockBucket = createMockR2Bucket({
          'special.json': JSON.stringify({
            data: {
              items: [
                { _id: 'i1', text: 'Line1\nLine2\tTabbed', quote: '"quoted"' },
              ],
            },
          }),
        })

        const result = await restoreSnapshot(mockSql, mockBucket, 'special.json')

        expect(result.totalDocs).toBe(1)

        // Verify the inserted data
        const insertCalls = mockSql.exec.mock.calls.filter(
          call => (call[0] as string).toUpperCase().includes('INSERT')
        )
        const insertedData = JSON.parse(insertCalls[0][3] as string)
        expect(insertedData.text).toBe('Line1\nLine2\tTabbed')
        expect(insertedData.quote).toBe('"quoted"')
      })
    })

    describe('nested data', () => {
      it('handles deeply nested objects', async () => {
        mockSql = createMockSqlStorage({
          items: [
            {
              id: 'nested',
              data: {
                level1: {
                  level2: {
                    level3: {
                      level4: {
                        value: 'deep',
                      },
                    },
                  },
                },
              },
            },
          ],
        })

        await createSnapshot(mockSql, 'nested-do', { bucket: mockBucket })

        const putCall = mockBucket.put.mock.calls[0]
        const storedBody = JSON.parse(putCall[1] as string)

        expect(storedBody.data.items[0].level1.level2.level3.level4.value).toBe('deep')
      })

      it('handles arrays in documents', async () => {
        mockSql = createMockSqlStorage({
          items: [
            {
              id: 'arrays',
              data: {
                tags: ['a', 'b', 'c'],
                numbers: [1, 2, 3],
                mixed: [{ key: 'val' }, 'string', 42],
              },
            },
          ],
        })

        await createSnapshot(mockSql, 'array-do', { bucket: mockBucket })

        const putCall = mockBucket.put.mock.calls[0]
        const storedBody = JSON.parse(putCall[1] as string)

        expect(storedBody.data.items[0].tags).toEqual(['a', 'b', 'c'])
        expect(storedBody.data.items[0].numbers).toEqual([1, 2, 3])
        expect(storedBody.data.items[0].mixed).toEqual([{ key: 'val' }, 'string', 42])
      })
    })

    describe('round-trip integrity', () => {
      it('preserves data through create and restore cycle', async () => {
        const originalData = {
          users: [
            { id: 'u1', data: { name: 'Alice', age: 30, active: true } },
            { id: 'u2', data: { name: 'Bob', age: 25, active: false } },
          ],
          settings: [
            { id: 'config', data: { theme: 'dark', notifications: { email: true, push: false } } },
          ],
        }
        mockSql = createMockSqlStorage(originalData)

        // Create snapshot
        const createResult = await createSnapshot(mockSql, 'round-trip-do', { bucket: mockBucket })

        // Clear SQL and restore
        mockSql = createMockSqlStorage({})
        const restoreResult = await restoreSnapshot(mockSql, mockBucket, createResult.key)

        expect(restoreResult.collections).toHaveLength(2)
        expect(restoreResult.totalDocs).toBe(3)

        // Verify all data was restored correctly
        const insertCalls = mockSql.exec.mock.calls.filter(
          call => (call[0] as string).toUpperCase().includes('INSERT')
        )
        expect(insertCalls).toHaveLength(3)
      })
    })
  })
})
