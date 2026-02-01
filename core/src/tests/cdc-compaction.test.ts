/**
 * CDC Compaction Tests
 *
 * TDD Red Phase: Tests for CDC (Change Data Capture) compaction functionality
 *
 * CDC compaction merges delta files into the main data.parquet file:
 * 1. Read current data.parquet (if exists)
 * 2. Read all pending delta files
 * 3. Apply deltas to current state (insert/update/delete)
 * 4. Write new data.parquet
 * 5. Archive or delete processed deltas
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  compactCollection,
  applyDeltasToState,
  writeDataParquet,
  type CompactionDeltaRecord,
  type CompactionResult,
  type CollectionManifest,
} from '../cdc-compaction.js'
import { readParquetRecords } from '../compaction.js'

// ============================================================================
// Mock Factories
// ============================================================================

interface MockR2ObjectBody {
  key: string
  body: ArrayBuffer | string
  customMetadata?: Record<string, string>
}

/**
 * Creates a mock R2Bucket with configurable objects
 */
function createMockR2Bucket(initialObjects: Map<string, MockR2ObjectBody> = new Map()) {
  const objects = new Map(initialObjects)

  return {
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null

      const body = obj.body
      return {
        key,
        body: new ReadableStream(),
        arrayBuffer: async () => (typeof body === 'string' ? new TextEncoder().encode(body).buffer : body),
        text: async () => (typeof body === 'string' ? body : new TextDecoder().decode(body as ArrayBuffer)),
        json: async () => JSON.parse(typeof body === 'string' ? body : new TextDecoder().decode(body as ArrayBuffer)),
        customMetadata: obj.customMetadata ?? {},
      }
    }),
    put: vi.fn(async (key: string, body: ArrayBuffer | string, options?: { customMetadata?: Record<string, string> }) => {
      objects.set(key, { key, body, customMetadata: options?.customMetadata })
      return { key }
    }),
    delete: vi.fn(async (keys: string | string[]) => {
      const keyList = Array.isArray(keys) ? keys : [keys]
      for (const key of keyList) {
        objects.delete(key)
      }
    }),
    list: vi.fn(async (options?: { prefix?: string }) => {
      const prefix = options?.prefix ?? ''
      const matching = Array.from(objects.entries())
        .filter(([key]) => key.startsWith(prefix))
        .map(([key]) => ({
          key,
          size: 100,
          etag: 'test-etag',
          uploaded: new Date(),
        }))
        .sort((a, b) => a.key.localeCompare(b.key))

      return { objects: matching, truncated: false }
    }),
    // Expose for test assertions
    _objects: objects,
  } as unknown as R2Bucket & { _objects: Map<string, MockR2ObjectBody> }
}

/**
 * Creates a delta record for testing
 */
function createDelta(
  op: 'insert' | 'update' | 'delete',
  id: string,
  doc?: Record<string, unknown>,
  seq?: number
): CompactionDeltaRecord {
  return {
    op,
    id,
    doc,
    ts: Date.now(),
    seq: seq ?? 0,
  }
}

/**
 * Creates a JSONL string from delta records
 */
function deltasToJsonl(deltas: CompactionDeltaRecord[]): string {
  return deltas.map((d) => JSON.stringify(d)).join('\n')
}

// ============================================================================
// Tests: Initial Compaction
// ============================================================================

describe('CDC Compaction', () => {
  describe('initial compaction', () => {
    it('creates data.parquet from deltas when none exists', async () => {
      const bucket = createMockR2Bucket()

      // Add delta files with inserts
      const delta1 = deltasToJsonl([
        createDelta('insert', 'user-1', { name: 'Alice', age: 30 }, 1),
        createDelta('insert', 'user-2', { name: 'Bob', age: 25 }, 2),
      ])
      await bucket.put('cdc/ns/users/deltas/000001.jsonl', delta1)

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(2)
      expect(result.dataFile).toBe('cdc/ns/users/data.parquet')

      // Verify data.parquet was written
      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      expect(dataFile).not.toBeNull()

      // Read back and verify contents
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)
      expect(records).toHaveLength(2)
    })

    it('handles empty collection with no deltas', async () => {
      const bucket = createMockR2Bucket()

      const result = await compactCollection(bucket, 'ns', 'empty_collection')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(0)
      expect(result.deltasProcessed).toBe(0)
    })

    it('sorts output by primary key', async () => {
      const bucket = createMockR2Bucket()

      // Add deltas in non-sorted order
      const deltas = deltasToJsonl([
        createDelta('insert', 'user-c', { name: 'Charlie' }, 1),
        createDelta('insert', 'user-a', { name: 'Alice' }, 2),
        createDelta('insert', 'user-b', { name: 'Bob' }, 3),
      ])
      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltas)

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Read back and verify sorted order
      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records[0].id).toBe('user-a')
      expect(records[1].id).toBe('user-b')
      expect(records[2].id).toBe('user-c')
    })
  })

  // ============================================================================
  // Tests: Merge Strategy
  // ============================================================================

  describe('merge strategy', () => {
    it('applies insert to empty state', () => {
      const state = new Map<string, Record<string, unknown>>()
      const deltas: CompactionDeltaRecord[] = [createDelta('insert', 'user-1', { name: 'Alice' })]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.size).toBe(1)
      expect(newState.get('user-1')).toEqual({ name: 'Alice' })
    })

    it('applies update to existing record', () => {
      const state = new Map<string, Record<string, unknown>>([['user-1', { name: 'Alice', age: 30 }]])
      const deltas: CompactionDeltaRecord[] = [createDelta('update', 'user-1', { name: 'Alice', age: 31 })]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.get('user-1')).toEqual({ name: 'Alice', age: 31 })
    })

    it('applies delete to remove record', () => {
      const state = new Map<string, Record<string, unknown>>([
        ['user-1', { name: 'Alice' }],
        ['user-2', { name: 'Bob' }],
      ])
      const deltas: CompactionDeltaRecord[] = [createDelta('delete', 'user-1')]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.size).toBe(1)
      expect(newState.has('user-1')).toBe(false)
      expect(newState.has('user-2')).toBe(true)
    })

    it('handles multiple updates to same record (last wins)', () => {
      const state = new Map<string, Record<string, unknown>>()
      const deltas: CompactionDeltaRecord[] = [
        createDelta('insert', 'user-1', { name: 'Alice', version: 1 }, 1),
        createDelta('update', 'user-1', { name: 'Alice', version: 2 }, 2),
        createDelta('update', 'user-1', { name: 'Alice', version: 3 }, 3),
      ]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.get('user-1')).toEqual({ name: 'Alice', version: 3 })
    })

    it('preserves records not in deltas', () => {
      const state = new Map<string, Record<string, unknown>>([
        ['user-1', { name: 'Alice' }],
        ['user-2', { name: 'Bob' }],
        ['user-3', { name: 'Charlie' }],
      ])
      const deltas: CompactionDeltaRecord[] = [createDelta('update', 'user-2', { name: 'Bobby' })]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.size).toBe(3)
      expect(newState.get('user-1')).toEqual({ name: 'Alice' })
      expect(newState.get('user-2')).toEqual({ name: 'Bobby' })
      expect(newState.get('user-3')).toEqual({ name: 'Charlie' })
    })

    it('handles insert then delete of same record', () => {
      const state = new Map<string, Record<string, unknown>>()
      const deltas: CompactionDeltaRecord[] = [
        createDelta('insert', 'user-1', { name: 'Alice' }, 1),
        createDelta('delete', 'user-1', undefined, 2),
      ]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.size).toBe(0)
      expect(newState.has('user-1')).toBe(false)
    })

    it('handles delete then reinsert of same record', () => {
      const state = new Map<string, Record<string, unknown>>([['user-1', { name: 'Alice', version: 1 }]])
      const deltas: CompactionDeltaRecord[] = [
        createDelta('delete', 'user-1', undefined, 1),
        createDelta('insert', 'user-1', { name: 'Alice', version: 2 }, 2),
      ]

      const newState = applyDeltasToState(state, deltas)

      expect(newState.size).toBe(1)
      expect(newState.get('user-1')).toEqual({ name: 'Alice', version: 2 })
    })
  })

  // ============================================================================
  // Tests: Compaction Ordering
  // ============================================================================

  describe('compaction ordering', () => {
    it('processes delta files in sequence order', async () => {
      const bucket = createMockR2Bucket()

      // Delta files should be processed in numeric order
      await bucket.put('cdc/ns/users/deltas/000003.jsonl', deltasToJsonl([createDelta('update', 'user-1', { v: 3 }, 3)]))
      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { v: 1 }, 1)]))
      await bucket.put('cdc/ns/users/deltas/000002.jsonl', deltasToJsonl([createDelta('update', 'user-1', { v: 2 }, 2)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Final state should reflect order: insert v1 -> update v2 -> update v3
      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records[0].v).toBe(3)
    })

    it('final state reflects chronological order', async () => {
      const bucket = createMockR2Bucket()

      // Simulate time-ordered operations
      await bucket.put(
        'cdc/ns/users/deltas/000001.jsonl',
        deltasToJsonl([
          createDelta('insert', 'user-1', { name: 'Alice', status: 'active' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob', status: 'active' }, 2),
        ])
      )
      await bucket.put(
        'cdc/ns/users/deltas/000002.jsonl',
        deltasToJsonl([
          createDelta('update', 'user-1', { name: 'Alice', status: 'inactive' }, 3),
          createDelta('delete', 'user-2', undefined, 4),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1) // Only user-1 remains

      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(1)
      expect(records[0].id).toBe('user-1')
      expect(records[0].status).toBe('inactive')
    })

    it('handles out-of-order delta numbers gracefully', async () => {
      const bucket = createMockR2Bucket()

      // Files named out of order but should still be sorted by name
      await bucket.put('cdc/ns/users/deltas/000010.jsonl', deltasToJsonl([createDelta('update', 'user-1', { v: 10 }, 10)]))
      await bucket.put('cdc/ns/users/deltas/000002.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { v: 2 }, 2)]))
      await bucket.put('cdc/ns/users/deltas/000005.jsonl', deltasToJsonl([createDelta('update', 'user-1', { v: 5 }, 5)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Order should be: 000002 -> 000005 -> 000010
      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records[0].v).toBe(10) // Last update wins
    })
  })

  // ============================================================================
  // Tests: Output File
  // ============================================================================

  describe('output file', () => {
    it('writes sorted data.parquet', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put(
        'cdc/ns/users/deltas/000001.jsonl',
        deltasToJsonl([
          createDelta('insert', 'z-user', { name: 'Zara' }, 1),
          createDelta('insert', 'a-user', { name: 'Adam' }, 2),
          createDelta('insert', 'm-user', { name: 'Mary' }, 3),
        ])
      )

      await compactCollection(bucket, 'ns', 'users')

      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      // Should be sorted by id
      expect(records[0].id).toBe('a-user')
      expect(records[1].id).toBe('m-user')
      expect(records[2].id).toBe('z-user')
    })

    it('excludes deleted records from output', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put(
        'cdc/ns/users/deltas/000001.jsonl',
        deltasToJsonl([
          createDelta('insert', 'user-1', { name: 'Alice' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob' }, 2),
          createDelta('insert', 'user-3', { name: 'Charlie' }, 3),
        ])
      )
      await bucket.put('cdc/ns/users/deltas/000002.jsonl', deltasToJsonl([createDelta('delete', 'user-2', undefined, 4)]))

      await compactCollection(bucket, 'ns', 'users')

      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(2)
      expect(records.find((r) => r.id === 'user-2')).toBeUndefined()
    })

    it('preserves all columns from original records', async () => {
      const bucket = createMockR2Bucket()

      const complexDoc = {
        name: 'Alice',
        age: 30,
        email: 'alice@example.com',
        active: true,
        score: 95.5,
        metadata: JSON.stringify({ role: 'admin', permissions: ['read', 'write'] }),
      }

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', complexDoc, 1)]))

      await compactCollection(bucket, 'ns', 'users')

      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(1)
      expect(records[0].name).toBe('Alice')
      expect(records[0].age).toBe(30)
      expect(records[0].email).toBe('alice@example.com')
      expect(records[0].active).toBe(true)
      expect(Number(records[0].score)).toBeCloseTo(95.5)
    })
  })

  // ============================================================================
  // Tests: writeDataParquet
  // ============================================================================

  describe('writeDataParquet', () => {
    it('writes state map to parquet buffer', async () => {
      const state = new Map<string, Record<string, unknown>>([
        ['user-1', { name: 'Alice', age: 30 }],
        ['user-2', { name: 'Bob', age: 25 }],
      ])

      const buffer = writeDataParquet(state)

      expect(buffer.byteLength).toBeGreaterThan(0)

      const records = await readParquetRecords(buffer)
      expect(records).toHaveLength(2)
    })

    it('includes id column from map keys', async () => {
      const state = new Map<string, Record<string, unknown>>([['my-doc-id', { name: 'Test' }]])

      const buffer = writeDataParquet(state)
      const records = await readParquetRecords(buffer)

      expect(records[0].id).toBe('my-doc-id')
      expect(records[0].name).toBe('Test')
    })

    it('handles empty state', async () => {
      const state = new Map<string, Record<string, unknown>>()

      const buffer = writeDataParquet(state)

      expect(buffer.byteLength).toBeGreaterThan(0) // Has parquet header

      const records = await readParquetRecords(buffer)
      expect(records).toHaveLength(0)
    })

    it('sorts records by id in output', async () => {
      const state = new Map<string, Record<string, unknown>>([
        ['c', { name: 'C' }],
        ['a', { name: 'A' }],
        ['b', { name: 'B' }],
      ])

      const buffer = writeDataParquet(state)
      const records = await readParquetRecords(buffer)

      expect(records[0].id).toBe('a')
      expect(records[1].id).toBe('b')
      expect(records[2].id).toBe('c')
    })
  })

  // ============================================================================
  // Tests: Delta Cleanup
  // ============================================================================

  describe('delta cleanup', () => {
    it('lists processed delta files in result', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('cdc/ns/users/deltas/000002.jsonl', deltasToJsonl([createDelta('insert', 'user-2', { name: 'Bob' }, 2)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.deltasProcessed).toBe(2)
      expect(result.processedFiles).toContain('cdc/ns/users/deltas/000001.jsonl')
      expect(result.processedFiles).toContain('cdc/ns/users/deltas/000002.jsonl')
    })

    it('archives deltas to processed folder when archive option is set', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))

      const result = await compactCollection(bucket, 'ns', 'users', { archiveDeltas: true })

      expect(result.success).toBe(true)

      // Original should be moved to processed folder
      const archived = await bucket.get('cdc/ns/users/processed/000001.jsonl')
      expect(archived).not.toBeNull()

      // Original should no longer exist in deltas folder
      const original = await bucket.get('cdc/ns/users/deltas/000001.jsonl')
      expect(original).toBeNull()
    })

    it('deletes old delta files after compaction when delete option is set', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('cdc/ns/users/deltas/000002.jsonl', deltasToJsonl([createDelta('insert', 'user-2', { name: 'Bob' }, 2)]))

      const result = await compactCollection(bucket, 'ns', 'users', { deleteDeltas: true })

      expect(result.success).toBe(true)

      // Both deltas should be deleted
      const delta1 = await bucket.get('cdc/ns/users/deltas/000001.jsonl')
      const delta2 = await bucket.get('cdc/ns/users/deltas/000002.jsonl')
      expect(delta1).toBeNull()
      expect(delta2).toBeNull()
    })

    it('keeps delta files by default', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))

      await compactCollection(bucket, 'ns', 'users')

      // Delta should still exist
      const delta = await bucket.get('cdc/ns/users/deltas/000001.jsonl')
      expect(delta).not.toBeNull()
    })
  })

  // ============================================================================
  // Tests: Manifest Update
  // ============================================================================

  describe('manifest update', () => {
    it('updates manifest with new data.parquet stats', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put(
        'cdc/ns/users/deltas/000001.jsonl',
        deltasToJsonl([
          createDelta('insert', 'user-1', { name: 'Alice' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob' }, 2),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Check manifest was written
      const manifestFile = await bucket.get('cdc/ns/users/manifest.json')
      expect(manifestFile).not.toBeNull()

      const manifest: CollectionManifest = await manifestFile!.json()
      expect(manifest.dataFile).toBe('cdc/ns/users/data.parquet')
      expect(manifest.recordCount).toBe(2)
      expect(manifest.fileSizeBytes).toBeGreaterThan(0)
    })

    it('records last compaction time', async () => {
      const bucket = createMockR2Bucket()

      const beforeCompaction = Date.now()
      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))

      await compactCollection(bucket, 'ns', 'users')

      const manifestFile = await bucket.get('cdc/ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.lastCompactionAt).toBeDefined()
      const compactionTime = new Date(manifest.lastCompactionAt!).getTime()
      expect(compactionTime).toBeGreaterThanOrEqual(beforeCompaction)
      expect(compactionTime).toBeLessThanOrEqual(Date.now())
    })

    it('tracks row count and file size', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put(
        'cdc/ns/users/deltas/000001.jsonl',
        deltasToJsonl([
          createDelta('insert', 'user-1', { name: 'Alice', data: 'some data' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob', data: 'more data' }, 2),
          createDelta('insert', 'user-3', { name: 'Charlie', data: 'extra data' }, 3),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.recordCount).toBe(3)
      expect(result.fileSizeBytes).toBeGreaterThan(0)

      const manifestFile = await bucket.get('cdc/ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.recordCount).toBe(3)
      expect(manifest.fileSizeBytes).toBe(result.fileSizeBytes)
    })

    it('updates existing manifest on subsequent compactions', async () => {
      const bucket = createMockR2Bucket()

      // First compaction
      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await compactCollection(bucket, 'ns', 'users', { deleteDeltas: true })

      // Second compaction with more data
      await bucket.put('cdc/ns/users/deltas/000002.jsonl', deltasToJsonl([createDelta('insert', 'user-2', { name: 'Bob' }, 2)]))
      await compactCollection(bucket, 'ns', 'users', { deleteDeltas: true })

      const manifestFile = await bucket.get('cdc/ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.recordCount).toBe(2) // Both users
      expect(manifest.compactionCount).toBe(2)
    })

    it('tracks delta sequence number in manifest', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('cdc/ns/users/deltas/000005.jsonl', deltasToJsonl([createDelta('insert', 'user-2', { name: 'Bob' }, 5)]))

      await compactCollection(bucket, 'ns', 'users')

      const manifestFile = await bucket.get('cdc/ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.lastDeltaSequence).toBe(5)
    })
  })

  // ============================================================================
  // Tests: Merging with Existing Data
  // ============================================================================

  describe('merging with existing data', () => {
    it('merges deltas with existing data.parquet', async () => {
      const bucket = createMockR2Bucket()

      // Set up existing data.parquet with some records
      const existingState = new Map<string, Record<string, unknown>>([
        ['user-1', { name: 'Alice', age: 30 }],
        ['user-2', { name: 'Bob', age: 25 }],
      ])
      const existingParquet = writeDataParquet(existingState)
      await bucket.put('cdc/ns/users/data.parquet', existingParquet)

      // Add delta that modifies existing and adds new
      await bucket.put(
        'cdc/ns/users/deltas/000001.jsonl',
        deltasToJsonl([
          createDelta('update', 'user-1', { name: 'Alice', age: 31 }, 1),
          createDelta('insert', 'user-3', { name: 'Charlie', age: 35 }, 2),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(3)

      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(3)

      const alice = records.find((r) => r.id === 'user-1')
      expect(alice?.age).toBe(31) // Updated

      const charlie = records.find((r) => r.id === 'user-3')
      expect(charlie).toBeDefined() // New record
    })

    it('handles delete of record from existing data', async () => {
      const bucket = createMockR2Bucket()

      // Existing data
      const existingState = new Map<string, Record<string, unknown>>([
        ['user-1', { name: 'Alice' }],
        ['user-2', { name: 'Bob' }],
      ])
      const existingParquet = writeDataParquet(existingState)
      await bucket.put('cdc/ns/users/data.parquet', existingParquet)

      // Delta deletes user-1
      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl([createDelta('delete', 'user-1', undefined, 1)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.recordCount).toBe(1)

      const dataFile = await bucket.get('cdc/ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(1)
      expect(records[0].id).toBe('user-2')
    })
  })

  // ============================================================================
  // Tests: Edge Cases
  // ============================================================================

  describe('edge cases', () => {
    it('handles delta with missing doc for insert (should skip)', () => {
      const state = new Map<string, Record<string, unknown>>()
      const deltas: CompactionDeltaRecord[] = [
        { op: 'insert', id: 'user-1', ts: Date.now(), seq: 1 }, // Missing doc
      ]

      const newState = applyDeltasToState(state, deltas)

      // Should skip invalid delta
      expect(newState.size).toBe(0)
    })

    it('handles update to non-existent record (upsert behavior)', () => {
      const state = new Map<string, Record<string, unknown>>()
      const deltas: CompactionDeltaRecord[] = [createDelta('update', 'user-1', { name: 'Alice' })]

      const newState = applyDeltasToState(state, deltas)

      // Update should act as upsert
      expect(newState.size).toBe(1)
      expect(newState.get('user-1')).toEqual({ name: 'Alice' })
    })

    it('handles delete of non-existent record (no-op)', () => {
      const state = new Map<string, Record<string, unknown>>()
      const deltas: CompactionDeltaRecord[] = [createDelta('delete', 'user-1')]

      const newState = applyDeltasToState(state, deltas)

      // Delete of non-existent should be no-op
      expect(newState.size).toBe(0)
    })

    it('handles empty delta file', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', '')

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(0)
    })

    it('handles malformed JSONL lines gracefully', async () => {
      const bucket = createMockR2Bucket()

      const badJsonl = `{"op":"insert","id":"user-1","doc":{"name":"Alice"},"ts":123,"seq":1}
not valid json
{"op":"insert","id":"user-2","doc":{"name":"Bob"},"ts":124,"seq":2}`

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', badJsonl)

      const result = await compactCollection(bucket, 'ns', 'users')

      // Should process valid lines and skip invalid
      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(2)
      expect(result.errors).toHaveLength(1)
    })

    it('handles very large delta files', async () => {
      const bucket = createMockR2Bucket()

      // Generate many deltas
      const deltas: CompactionDeltaRecord[] = []
      for (let i = 0; i < 1000; i++) {
        deltas.push(createDelta('insert', `user-${i.toString().padStart(4, '0')}`, { name: `User ${i}`, index: i }, i))
      }

      await bucket.put('cdc/ns/users/deltas/000001.jsonl', deltasToJsonl(deltas))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1000)
    })
  })
})
