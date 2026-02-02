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
import { writeDeltaFile, type DeltaRecord } from '../cdc-delta.js'

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
 * Converts CompactionDeltaRecords to DeltaRecords for Parquet writing
 */
function toDeltaRecords(deltas: CompactionDeltaRecord[]): DeltaRecord[] {
  return deltas.map((d) => ({
    pk: d.id,
    op: d.op,
    data: d.doc ?? null,
    prev: null,
    ts: new Date(d.ts).toISOString(),
    bookmark: null,
  }))
}

/**
 * Creates a Parquet buffer from delta records
 */
function deltasToParquet(deltas: CompactionDeltaRecord[]): ArrayBuffer {
  return writeDeltaFile(toDeltaRecords(deltas))
}

// ============================================================================
// Tests: Initial Compaction
// ============================================================================

describe('CDC Compaction', () => {
  describe('initial compaction', () => {
    it('creates data.parquet from deltas when none exists', async () => {
      const bucket = createMockR2Bucket()

      // Add delta files with inserts
      const delta1 = deltasToParquet([
        createDelta('insert', 'user-1', { name: 'Alice', age: 30 }, 1),
        createDelta('insert', 'user-2', { name: 'Bob', age: 25 }, 2),
      ])
      await bucket.put('ns/users/deltas/000001.parquet', delta1)

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(2)
      expect(result.dataFile).toBe('ns/users/data.parquet')

      // Verify data.parquet was written
      const dataFile = await bucket.get('ns/users/data.parquet')
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
      const deltas = deltasToParquet([
        createDelta('insert', 'user-c', { name: 'Charlie' }, 1),
        createDelta('insert', 'user-a', { name: 'Alice' }, 2),
        createDelta('insert', 'user-b', { name: 'Bob' }, 3),
      ])
      await bucket.put('ns/users/deltas/000001.parquet', deltas)

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Read back and verify sorted order
      const dataFile = await bucket.get('ns/users/data.parquet')
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
      await bucket.put('ns/users/deltas/000003.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 3 }, 3)]))
      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { v: 1 }, 1)]))
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 2 }, 2)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Final state should reflect order: insert v1 -> update v2 -> update v3
      const dataFile = await bucket.get('ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records[0].v).toBe(3)
    })

    it('final state reflects chronological order', async () => {
      const bucket = createMockR2Bucket()

      // Simulate time-ordered operations
      await bucket.put(
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('insert', 'user-1', { name: 'Alice', status: 'active' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob', status: 'active' }, 2),
        ])
      )
      await bucket.put(
        'ns/users/deltas/000002.parquet',
        deltasToParquet([
          createDelta('update', 'user-1', { name: 'Alice', status: 'inactive' }, 3),
          createDelta('delete', 'user-2', undefined, 4),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1) // Only user-1 remains

      const dataFile = await bucket.get('ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(1)
      expect(records[0].id).toBe('user-1')
      expect(records[0].status).toBe('inactive')
    })

    it('handles out-of-order delta numbers gracefully', async () => {
      const bucket = createMockR2Bucket()

      // Files named out of order but should still be sorted by name
      await bucket.put('ns/users/deltas/000010.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 10 }, 10)]))
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('insert', 'user-1', { v: 2 }, 2)]))
      await bucket.put('ns/users/deltas/000005.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 5 }, 5)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Order should be: 000002 -> 000005 -> 000010
      const dataFile = await bucket.get('ns/users/data.parquet')
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
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('insert', 'z-user', { name: 'Zara' }, 1),
          createDelta('insert', 'a-user', { name: 'Adam' }, 2),
          createDelta('insert', 'm-user', { name: 'Mary' }, 3),
        ])
      )

      await compactCollection(bucket, 'ns', 'users')

      const dataFile = await bucket.get('ns/users/data.parquet')
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
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('insert', 'user-1', { name: 'Alice' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob' }, 2),
          createDelta('insert', 'user-3', { name: 'Charlie' }, 3),
        ])
      )
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('delete', 'user-2', undefined, 4)]))

      await compactCollection(bucket, 'ns', 'users')

      const dataFile = await bucket.get('ns/users/data.parquet')
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

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', complexDoc, 1)]))

      await compactCollection(bucket, 'ns', 'users')

      const dataFile = await bucket.get('ns/users/data.parquet')
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

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('insert', 'user-2', { name: 'Bob' }, 2)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.deltasProcessed).toBe(2)
      expect(result.processedFiles).toContain('ns/users/deltas/000001.parquet')
      expect(result.processedFiles).toContain('ns/users/deltas/000002.parquet')
    })

    it('archives deltas to processed folder when archive option is set', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))

      const result = await compactCollection(bucket, 'ns', 'users', { archiveDeltas: true })

      expect(result.success).toBe(true)

      // Original should be moved to processed folder
      const archived = await bucket.get('ns/users/processed/000001.parquet')
      expect(archived).not.toBeNull()

      // Original should no longer exist in deltas folder
      const original = await bucket.get('ns/users/deltas/000001.parquet')
      expect(original).toBeNull()
    })

    it('deletes old delta files after compaction when delete option is set', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('insert', 'user-2', { name: 'Bob' }, 2)]))

      const result = await compactCollection(bucket, 'ns', 'users', { deleteDeltas: true })

      expect(result.success).toBe(true)

      // Both deltas should be deleted
      const delta1 = await bucket.get('ns/users/deltas/000001.parquet')
      const delta2 = await bucket.get('ns/users/deltas/000002.parquet')
      expect(delta1).toBeNull()
      expect(delta2).toBeNull()
    })

    it('keeps delta files by default', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))

      await compactCollection(bucket, 'ns', 'users')

      // Delta should still exist
      const delta = await bucket.get('ns/users/deltas/000001.parquet')
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
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('insert', 'user-1', { name: 'Alice' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob' }, 2),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)

      // Check manifest was written
      const manifestFile = await bucket.get('ns/users/manifest.json')
      expect(manifestFile).not.toBeNull()

      const manifest: CollectionManifest = await manifestFile!.json()
      expect(manifest.dataFile).toBe('ns/users/data.parquet')
      expect(manifest.recordCount).toBe(2)
      expect(manifest.fileSizeBytes).toBeGreaterThan(0)
    })

    it('records last compaction time', async () => {
      const bucket = createMockR2Bucket()

      const beforeCompaction = Date.now()
      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))

      await compactCollection(bucket, 'ns', 'users')

      const manifestFile = await bucket.get('ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.lastCompactionAt).toBeDefined()
      const compactionTime = new Date(manifest.lastCompactionAt!).getTime()
      expect(compactionTime).toBeGreaterThanOrEqual(beforeCompaction)
      expect(compactionTime).toBeLessThanOrEqual(Date.now())
    })

    it('tracks row count and file size', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put(
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('insert', 'user-1', { name: 'Alice', data: 'some data' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob', data: 'more data' }, 2),
          createDelta('insert', 'user-3', { name: 'Charlie', data: 'extra data' }, 3),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.recordCount).toBe(3)
      expect(result.fileSizeBytes).toBeGreaterThan(0)

      const manifestFile = await bucket.get('ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.recordCount).toBe(3)
      expect(manifest.fileSizeBytes).toBe(result.fileSizeBytes)
    })

    it('updates existing manifest on subsequent compactions', async () => {
      const bucket = createMockR2Bucket()

      // First compaction
      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await compactCollection(bucket, 'ns', 'users', { deleteDeltas: true })

      // Second compaction with more data
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('insert', 'user-2', { name: 'Bob' }, 2)]))
      await compactCollection(bucket, 'ns', 'users', { deleteDeltas: true })

      const manifestFile = await bucket.get('ns/users/manifest.json')
      const manifest: CollectionManifest = await manifestFile!.json()

      expect(manifest.recordCount).toBe(2) // Both users
      expect(manifest.compactionCount).toBe(2)
    })

    it('tracks delta sequence number in manifest', async () => {
      const bucket = createMockR2Bucket()

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('ns/users/deltas/000005.parquet', deltasToParquet([createDelta('insert', 'user-2', { name: 'Bob' }, 5)]))

      await compactCollection(bucket, 'ns', 'users')

      const manifestFile = await bucket.get('ns/users/manifest.json')
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
      await bucket.put('ns/users/data.parquet', existingParquet)

      // Add delta that modifies existing and adds new
      await bucket.put(
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('update', 'user-1', { name: 'Alice', age: 31 }, 1),
          createDelta('insert', 'user-3', { name: 'Charlie', age: 35 }, 2),
        ])
      )

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(3)

      const dataFile = await bucket.get('ns/users/data.parquet')
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
      await bucket.put('ns/users/data.parquet', existingParquet)

      // Delta deletes user-1
      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('delete', 'user-1', undefined, 1)]))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.recordCount).toBe(1)

      const dataFile = await bucket.get('ns/users/data.parquet')
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

      // Write a valid Parquet file with no records
      const emptyParquet = deltasToParquet([])
      await bucket.put('ns/users/deltas/000001.parquet', emptyParquet)

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(0)
    })

    it('handles malformed Parquet files gracefully', async () => {
      const bucket = createMockR2Bucket()

      // Add one valid delta file and one corrupted file
      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { name: 'Alice' }, 1)]))
      await bucket.put('ns/users/deltas/000002.parquet', new TextEncoder().encode('not valid parquet data').buffer)

      const result = await compactCollection(bucket, 'ns', 'users')

      // Should process valid file and report error for invalid
      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1)
      expect(result.errors).toBeDefined()
      expect(result.errors!.length).toBeGreaterThan(0)
    })

    it('handles very large delta files', async () => {
      const bucket = createMockR2Bucket()

      // Generate many deltas
      const deltas: CompactionDeltaRecord[] = []
      for (let i = 0; i < 1000; i++) {
        deltas.push(createDelta('insert', `user-${i.toString().padStart(4, '0')}`, { name: `User ${i}`, index: i }, i))
      }

      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet(deltas))

      const result = await compactCollection(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1000)
    })
  })
})

// ============================================================================
// Tests: Parallel Compaction
// ============================================================================

import {
  compactCollectionParallel,
  mergeChunkStates,
  type ParallelCompactionProgress,
} from '../cdc-compaction.js'

describe('Parallel CDC Compaction', () => {
  describe('mergeChunkStates', () => {
    it('merges multiple chunk states correctly', () => {
      const baseState = new Map<string, Record<string, unknown>>([['user-1', { name: 'Original' }]])

      const chunk1 = {
        state: new Map<string, Record<string, unknown>>([
          ['user-2', { name: 'From Chunk 1' }],
          ['user-3', { name: 'Also Chunk 1' }],
        ]),
        processedFiles: [],
        lastSequence: 1,
        errors: [],
      }

      const chunk2 = {
        state: new Map<string, Record<string, unknown>>([
          ['user-1', { name: 'Updated in Chunk 2' }],
          ['user-4', { name: 'From Chunk 2' }],
        ]),
        processedFiles: [],
        lastSequence: 2,
        errors: [],
      }

      const result = mergeChunkStates(baseState, [chunk1, chunk2])

      expect(result.size).toBe(4)
      expect(result.get('user-1')).toEqual({ name: 'Updated in Chunk 2' }) // Updated
      expect(result.get('user-2')).toEqual({ name: 'From Chunk 1' })
      expect(result.get('user-3')).toEqual({ name: 'Also Chunk 1' })
      expect(result.get('user-4')).toEqual({ name: 'From Chunk 2' })
    })

    it('handles deletions marked with __deleted__', () => {
      const baseState = new Map<string, Record<string, unknown>>([
        ['user-1', { name: 'Alice' }],
        ['user-2', { name: 'Bob' }],
      ])

      const chunk = {
        state: new Map<string, Record<string, unknown>>([['user-1', { __deleted__: true }]]),
        processedFiles: [],
        lastSequence: 1,
        errors: [],
      }

      const result = mergeChunkStates(baseState, [chunk])

      expect(result.size).toBe(1)
      expect(result.has('user-1')).toBe(false)
      expect(result.get('user-2')).toEqual({ name: 'Bob' })
    })

    it('handles empty chunk results', () => {
      const baseState = new Map<string, Record<string, unknown>>([['user-1', { name: 'Alice' }]])

      const result = mergeChunkStates(baseState, [])

      expect(result.size).toBe(1)
      expect(result.get('user-1')).toEqual({ name: 'Alice' })
    })

    it('preserves insertion order of chunks', () => {
      const baseState = new Map<string, Record<string, unknown>>()

      const chunks = [
        {
          state: new Map<string, Record<string, unknown>>([['user-1', { v: 1 }]]),
          processedFiles: [],
          lastSequence: 1,
          errors: [],
        },
        {
          state: new Map<string, Record<string, unknown>>([['user-1', { v: 2 }]]),
          processedFiles: [],
          lastSequence: 2,
          errors: [],
        },
        {
          state: new Map<string, Record<string, unknown>>([['user-1', { v: 3 }]]),
          processedFiles: [],
          lastSequence: 3,
          errors: [],
        },
      ]

      const result = mergeChunkStates(baseState, chunks)

      // Last chunk should win
      expect(result.get('user-1')).toEqual({ v: 3 })
    })
  })

  describe('compactCollectionParallel', () => {
    it('processes delta files in parallel', async () => {
      const bucket = createMockR2Bucket()

      // Create multiple delta files to trigger parallel processing (need >= 5)
      for (let i = 1; i <= 10; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([
            createDelta('insert', `user-${i}`, { name: `User ${i}`, batch: i }, i),
          ])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 4,
        chunkSize: 3,
      })

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(10)
      expect(result.deltasProcessed).toBe(10)
    })

    it('falls back to sequential for small delta counts', async () => {
      const bucket = createMockR2Bucket()

      // Only 3 delta files (below MIN_DELTAS_FOR_PARALLEL threshold of 5)
      for (let i = 1; i <= 3; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users')

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(3)
    })

    it('maintains correct ordering when processing in parallel', async () => {
      const bucket = createMockR2Bucket()

      // Create files where later deltas update earlier records
      await bucket.put('ns/users/deltas/000001.parquet', deltasToParquet([createDelta('insert', 'user-1', { v: 1 }, 1)]))
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 2 }, 2)]))
      await bucket.put('ns/users/deltas/000003.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 3 }, 3)]))
      await bucket.put('ns/users/deltas/000004.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 4 }, 4)]))
      await bucket.put('ns/users/deltas/000005.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 5 }, 5)]))
      await bucket.put('ns/users/deltas/000006.parquet', deltasToParquet([createDelta('update', 'user-1', { v: 6 }, 6)]))

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 3,
        chunkSize: 2,
      })

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1)

      const dataFile = await bucket.get('ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      // Should have the final value from the last delta
      expect(records[0].v).toBe(6)
    })

    it('handles deletions correctly in parallel processing', async () => {
      const bucket = createMockR2Bucket()

      // Create records and then delete some
      await bucket.put(
        'ns/users/deltas/000001.parquet',
        deltasToParquet([
          createDelta('insert', 'user-1', { name: 'Alice' }, 1),
          createDelta('insert', 'user-2', { name: 'Bob' }, 2),
          createDelta('insert', 'user-3', { name: 'Charlie' }, 3),
        ])
      )
      await bucket.put('ns/users/deltas/000002.parquet', deltasToParquet([createDelta('insert', 'user-4', { name: 'Dave' }, 4)]))
      await bucket.put('ns/users/deltas/000003.parquet', deltasToParquet([createDelta('delete', 'user-2', undefined, 5)]))
      await bucket.put('ns/users/deltas/000004.parquet', deltasToParquet([createDelta('insert', 'user-5', { name: 'Eve' }, 6)]))
      await bucket.put('ns/users/deltas/000005.parquet', deltasToParquet([createDelta('delete', 'user-4', undefined, 7)]))

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 2,
        chunkSize: 2,
      })

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(3) // user-1, user-3, user-5 remain

      const dataFile = await bucket.get('ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      const ids = records.map((r) => r.id)
      expect(ids).toContain('user-1')
      expect(ids).toContain('user-3')
      expect(ids).toContain('user-5')
      expect(ids).not.toContain('user-2')
      expect(ids).not.toContain('user-4')
    })

    it('reports progress during processing', async () => {
      const bucket = createMockR2Bucket()

      // Create enough delta files to trigger parallel processing
      for (let i = 1; i <= 8; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const progressUpdates: ParallelCompactionProgress[] = []

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 2,
        chunkSize: 2,
        onProgress: (progress) => {
          progressUpdates.push({ ...progress })
        },
      })

      expect(result.success).toBe(true)
      expect(progressUpdates.length).toBeGreaterThan(0)

      // Check that progress increases monotonically
      let lastPercent = 0
      for (const update of progressUpdates) {
        expect(update.percentComplete).toBeGreaterThanOrEqual(lastPercent)
        lastPercent = update.percentComplete
      }

      // Final update should be 100%
      const finalProgress = progressUpdates[progressUpdates.length - 1]
      expect(finalProgress.percentComplete).toBe(100)
      expect(finalProgress.processedDeltas).toBe(8)
    })

    it('clamps parallelism to valid range', async () => {
      const bucket = createMockR2Bucket()

      for (let i = 1; i <= 6; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      // Test with parallelism > MAX (16)
      const result1 = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 100, // Should be clamped to 16
      })
      expect(result1.success).toBe(true)

      // Test with parallelism < 1
      const result2 = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 0, // Should be clamped to 1
      })
      expect(result2.success).toBe(true)
    })

    it('handles errors gracefully in parallel chunks', async () => {
      const bucket = createMockR2Bucket()

      // Create some valid delta files
      for (let i = 1; i <= 4; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      // Add an invalid file in the middle
      await bucket.put('ns/users/deltas/000005.parquet', new TextEncoder().encode('not valid parquet').buffer)

      // Add more valid files
      for (let i = 6; i <= 8; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 2,
        chunkSize: 3,
      })

      // Should succeed with partial results
      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(7) // 8 files - 1 invalid
      expect(result.errors).toBeDefined()
      expect(result.errors!.some((e) => e.includes('000005'))).toBe(true)
    })

    it('merges with existing data.parquet', async () => {
      const bucket = createMockR2Bucket()

      // Set up existing data
      const existingState = new Map<string, Record<string, unknown>>([
        ['existing-1', { name: 'Existing 1' }],
        ['existing-2', { name: 'Existing 2' }],
      ])
      await bucket.put('ns/users/data.parquet', writeDataParquet(existingState))

      // Add new delta files
      for (let i = 1; i <= 6; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 2,
        chunkSize: 2,
      })

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(8) // 2 existing + 6 new

      const dataFile = await bucket.get('ns/users/data.parquet')
      const buffer = await dataFile!.arrayBuffer()
      const records = await readParquetRecords(buffer)

      const ids = records.map((r) => r.id)
      expect(ids).toContain('existing-1')
      expect(ids).toContain('existing-2')
    })

    it('deletes deltas after parallel compaction when requested', async () => {
      const bucket = createMockR2Bucket()

      for (let i = 1; i <= 6; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        deleteDeltas: true,
        parallelism: 2,
        chunkSize: 2,
      })

      expect(result.success).toBe(true)

      // All delta files should be deleted
      for (let i = 1; i <= 6; i++) {
        const delta = await bucket.get(`ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`)
        expect(delta).toBeNull()
      }
    })

    it('archives deltas after parallel compaction when requested', async () => {
      const bucket = createMockR2Bucket()

      for (let i = 1; i <= 6; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        archiveDeltas: true,
        parallelism: 2,
        chunkSize: 2,
      })

      expect(result.success).toBe(true)

      // Delta files should be moved to processed folder
      for (let i = 1; i <= 6; i++) {
        const original = await bucket.get(`ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`)
        expect(original).toBeNull()

        const archived = await bucket.get(`ns/users/processed/${i.toString().padStart(6, '0')}.parquet`)
        expect(archived).not.toBeNull()
      }
    })

    it('updates manifest correctly', async () => {
      const bucket = createMockR2Bucket()

      for (let i = 1; i <= 6; i++) {
        await bucket.put(
          `ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`,
          deltasToParquet([createDelta('insert', `user-${i}`, { name: `User ${i}` }, i)])
        )
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 2,
        chunkSize: 2,
      })

      expect(result.success).toBe(true)

      const manifestFile = await bucket.get('ns/users/manifest.json')
      expect(manifestFile).not.toBeNull()

      const manifest = await manifestFile!.json()
      expect(manifest.recordCount).toBe(6)
      expect(manifest.lastDeltaSequence).toBe(6)
      expect(manifest.compactionCount).toBe(1)
      expect(manifest.lastCompactionAt).toBeDefined()
    })

    it('handles large datasets efficiently', async () => {
      const bucket = createMockR2Bucket()

      // Create 50 delta files with 20 records each = 1000 total records
      for (let i = 1; i <= 50; i++) {
        const deltas: CompactionDeltaRecord[] = []
        for (let j = 0; j < 20; j++) {
          const userId = (i - 1) * 20 + j
          deltas.push(
            createDelta('insert', `user-${userId.toString().padStart(5, '0')}`, { name: `User ${userId}`, batch: i }, userId)
          )
        }
        await bucket.put(`ns/users/deltas/${i.toString().padStart(6, '0')}.parquet`, deltasToParquet(deltas))
      }

      const result = await compactCollectionParallel(bucket, 'ns', 'users', {
        parallelism: 8,
        chunkSize: 10,
      })

      expect(result.success).toBe(true)
      expect(result.recordCount).toBe(1000)
      expect(result.deltasProcessed).toBe(50)
    })
  })
})
