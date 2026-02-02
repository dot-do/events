/**
 * CDC Snapshot Tests
 *
 * TDD Red Phase: Tests for CDC snapshot functionality (PITR - Point-in-Time Recovery)
 *
 * Storage layout:
 * {ns}/{collection}/
 * ├── snapshots/
 * │   ├── 2024-01-31.parquet    # Daily snapshot
 * │   ├── 2024-01.parquet       # Monthly snapshot
 * │   └── manifest.json         # Snapshot metadata
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  createSnapshot,
  listSnapshots,
  reconstructState,
  cleanupSnapshots,
  type SnapshotInfo,
  type RetentionPolicy,
  type SnapshotManifest,
} from '../cdc-snapshot.js'
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'

// ============================================================================
// Mock Factories
// ============================================================================

/**
 * Creates a test Parquet buffer with the given records
 */
function createTestParquet(
  records: Array<{ id: string; name: string; updated_at: number; [key: string]: unknown }>
) {
  if (records.length === 0) {
    return parquetWriteBuffer({
      columnData: [{ name: '_empty', data: [], type: 'STRING' }],
      codec: 'UNCOMPRESSED',
      statistics: true,
    })
  }

  return parquetWriteBuffer({
    columnData: [
      {
        name: 'id',
        data: records.map((r) => r.id),
        type: 'STRING',
      },
      {
        name: 'name',
        data: records.map((r) => r.name),
        type: 'STRING',
      },
      {
        name: 'updated_at',
        data: BigInt64Array.from(records.map((r) => BigInt(r.updated_at))),
        type: 'INT64',
      },
    ],
    codec: 'UNCOMPRESSED',
    statistics: true,
  })
}

/**
 * Creates a delta Parquet buffer representing CDC changes
 */
function createDeltaParquet(
  records: Array<{
    id: string
    op: 'insert' | 'update' | 'delete'
    ts: number
    doc?: Record<string, unknown>
  }>
) {
  if (records.length === 0) {
    return parquetWriteBuffer({
      columnData: [{ name: '_empty', data: [], type: 'STRING' }],
      codec: 'UNCOMPRESSED',
      statistics: true,
    })
  }

  return parquetWriteBuffer({
    columnData: [
      {
        name: 'id',
        data: records.map((r) => r.id),
        type: 'STRING',
      },
      {
        name: 'op',
        data: records.map((r) => r.op),
        type: 'STRING',
      },
      {
        name: 'ts',
        data: BigInt64Array.from(records.map((r) => BigInt(r.ts))),
        type: 'INT64',
      },
      {
        name: 'doc',
        data: records.map((r) => (r.doc ? JSON.stringify(r.doc) : '')),
        type: 'STRING',
      },
    ],
    codec: 'UNCOMPRESSED',
    statistics: true,
  })
}

interface MockR2ObjectBody {
  key: string
  size: number
  arrayBuffer: () => Promise<ArrayBuffer>
}

interface MockR2PutResult {
  key: string
  size: number
}

/**
 * Creates a mock R2Bucket with file storage
 */
function createMockR2Bucket(initialFiles: Record<string, ArrayBuffer> = {}) {
  const files = new Map<string, ArrayBuffer>(Object.entries(initialFiles))
  const deletedFiles: string[] = []

  return {
    files,
    deletedFiles,

    list: vi.fn(async (options?: { prefix?: string }) => {
      const objects: Array<{ key: string; size: number; uploaded: Date }> = []

      for (const [key, buffer] of files) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          objects.push({
            key,
            size: buffer.byteLength,
            uploaded: new Date(),
          })
        }
      }

      objects.sort((a, b) => a.key.localeCompare(b.key))

      return {
        objects,
        truncated: false,
        cursor: undefined,
      }
    }),

    get: vi.fn(async (key: string): Promise<MockR2ObjectBody | null> => {
      const buffer = files.get(key)
      if (!buffer) return null

      return {
        key,
        size: buffer.byteLength,
        arrayBuffer: async () => buffer,
      }
    }),

    put: vi.fn(async (key: string, data: ArrayBuffer | string): Promise<MockR2PutResult> => {
      // Convert string to ArrayBuffer if needed
      const buffer = typeof data === 'string'
        ? new TextEncoder().encode(data).buffer
        : data
      files.set(key, buffer)
      return {
        key,
        size: buffer.byteLength,
      }
    }),

    delete: vi.fn(async (key: string): Promise<void> => {
      files.delete(key)
      deletedFiles.push(key)
    }),
  } as unknown as R2Bucket & {
    files: Map<string, ArrayBuffer>
    deletedFiles: string[]
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('CDC Snapshots', () => {
  let mockBucket: ReturnType<typeof createMockR2Bucket>

  beforeEach(() => {
    mockBucket = createMockR2Bucket()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  // ==========================================================================
  // Snapshot Creation Tests
  // ==========================================================================

  describe('createSnapshot()', () => {
    describe('daily snapshots', () => {
      it('creates daily snapshot from current data.parquet', async () => {
        // Setup: data.parquet exists with records
        const dataBuffer = createTestParquet([
          { id: 'user-1', name: 'Alice', updated_at: 1000 },
          { id: 'user-2', name: 'Bob', updated_at: 2000 },
        ])
        mockBucket.files.set('myapp/users/data.parquet', dataBuffer)

        // Set current date
        vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

        const result = await createSnapshot(mockBucket, 'myapp', 'users', 'daily')

        expect(result.type).toBe('daily')
        expect(result.path).toBe('myapp/users/snapshots/2024-01-31.parquet')
        expect(result.rowCount).toBe(2)
        expect(mockBucket.files.has('myapp/users/snapshots/2024-01-31.parquet')).toBe(true)
      })

      it('uses correct naming format: YYYY-MM-DD.parquet', async () => {
        const dataBuffer = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
        mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

        vi.setSystemTime(new Date('2024-12-25T12:30:00Z'))

        const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

        expect(result.path).toBe('ns/coll/snapshots/2024-12-25.parquet')
      })

      it('overwrites existing daily snapshot for same date', async () => {
        const oldBuffer = createTestParquet([{ id: 'old', name: 'Old', updated_at: 1000 }])
        const newBuffer = createTestParquet([
          { id: 'new-1', name: 'New1', updated_at: 2000 },
          { id: 'new-2', name: 'New2', updated_at: 3000 },
        ])

        mockBucket.files.set('ns/coll/snapshots/2024-01-31.parquet', oldBuffer)
        mockBucket.files.set('ns/coll/data.parquet', newBuffer)

        vi.setSystemTime(new Date('2024-01-31T23:59:00Z'))

        const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

        expect(result.rowCount).toBe(2) // New data
      })
    })

    describe('monthly snapshots', () => {
      it('creates monthly snapshot from daily snapshots', async () => {
        // Setup: Multiple daily snapshots for January
        const day1 = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
        const day15 = createTestParquet([
          { id: 'user-1', name: 'Alice Updated', updated_at: 2000 },
          { id: 'user-2', name: 'Bob', updated_at: 2500 },
        ])
        const day31 = createTestParquet([
          { id: 'user-1', name: 'Alice Final', updated_at: 3000 },
          { id: 'user-2', name: 'Bob', updated_at: 2500 },
          { id: 'user-3', name: 'Charlie', updated_at: 3500 },
        ])

        mockBucket.files.set('ns/coll/snapshots/2024-01-01.parquet', day1)
        mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', day15)
        mockBucket.files.set('ns/coll/snapshots/2024-01-31.parquet', day31)

        vi.setSystemTime(new Date('2024-02-01T00:00:00Z'))

        const result = await createSnapshot(mockBucket, 'ns', 'coll', 'monthly')

        expect(result.type).toBe('monthly')
        expect(result.path).toBe('ns/coll/snapshots/2024-01.parquet')
        // Monthly snapshot should use the last daily snapshot's data
        expect(result.rowCount).toBe(3)
      })

      it('uses correct naming format: YYYY-MM.parquet', async () => {
        const daySnapshot = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
        mockBucket.files.set('ns/coll/snapshots/2024-06-30.parquet', daySnapshot)

        vi.setSystemTime(new Date('2024-07-01T00:00:00Z'))

        const result = await createSnapshot(mockBucket, 'ns', 'coll', 'monthly')

        expect(result.path).toBe('ns/coll/snapshots/2024-06.parquet')
      })

      it('uses last daily snapshot of the month for monthly snapshot', async () => {
        // Multiple daily snapshots - monthly should use the last one
        const earlyMonth = createTestParquet([{ id: 'user-1', name: 'Early', updated_at: 1000 }])
        const lateMonth = createTestParquet([
          { id: 'user-1', name: 'Late', updated_at: 2000 },
          { id: 'user-2', name: 'New', updated_at: 2500 },
        ])

        mockBucket.files.set('ns/coll/snapshots/2024-01-10.parquet', earlyMonth)
        mockBucket.files.set('ns/coll/snapshots/2024-01-28.parquet', lateMonth)

        vi.setSystemTime(new Date('2024-02-01T00:00:00Z'))

        const result = await createSnapshot(mockBucket, 'ns', 'coll', 'monthly')

        // Should have 2 rows from the last daily snapshot
        expect(result.rowCount).toBe(2)
      })
    })
  })

  // ==========================================================================
  // Snapshot Timing Tests
  // ==========================================================================

  describe('snapshot timing', () => {
    it('daily snapshot created at default midnight UTC', async () => {
      const dataBuffer = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      // Midnight UTC
      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      expect(result.timestamp.getUTCHours()).toBe(0)
      expect(result.timestamp.getUTCMinutes()).toBe(0)
    })

    it('monthly snapshot created on first day of month', async () => {
      const daySnapshot = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
      mockBucket.files.set('ns/coll/snapshots/2024-01-31.parquet', daySnapshot)

      // First day of February
      vi.setSystemTime(new Date('2024-02-01T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'monthly')

      expect(result.path).toBe('ns/coll/snapshots/2024-01.parquet')
    })

    it('skips daily snapshot if no changes since last snapshot', async () => {
      const existingSnapshot = createTestParquet([
        { id: 'user-1', name: 'Alice', updated_at: 1000 },
      ])
      const currentData = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])

      // Yesterday's snapshot
      mockBucket.files.set('ns/coll/snapshots/2024-01-30.parquet', existingSnapshot)
      mockBucket.files.set('ns/coll/data.parquet', currentData)

      // Manifest with last snapshot info
      const manifest: SnapshotManifest = {
        snapshots: [
          {
            type: 'daily',
            path: 'ns/coll/snapshots/2024-01-30.parquet',
            timestamp: new Date('2024-01-30T00:00:00Z'),
            rowCount: 1,
            sizeBytes: existingSnapshot.byteLength,
            checksum: 'abc123',
          },
        ],
        lastModified: new Date('2024-01-30T00:00:00Z'),
      }
      mockBucket.files.set(
        'ns/coll/snapshots/manifest.json',
        new TextEncoder().encode(JSON.stringify(manifest)).buffer
      )

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      // Should return null or indicate skipped
      expect(result.skipped).toBe(true)
      expect(result.reason).toBe('no_changes')
    })

    it('creates snapshot if data has changed since last snapshot', async () => {
      const existingSnapshot = createTestParquet([
        { id: 'user-1', name: 'Alice', updated_at: 1000 },
      ])
      const currentData = createTestParquet([
        { id: 'user-1', name: 'Alice Updated', updated_at: 2000 },
      ])

      mockBucket.files.set('ns/coll/snapshots/2024-01-30.parquet', existingSnapshot)
      mockBucket.files.set('ns/coll/data.parquet', currentData)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      expect(result.skipped).toBeFalsy()
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01-31.parquet')).toBe(true)
    })
  })

  // ==========================================================================
  // Snapshot Content Tests
  // ==========================================================================

  describe('snapshot content', () => {
    it('snapshot contains complete state at point in time', async () => {
      const dataBuffer = createTestParquet([
        { id: 'user-1', name: 'Alice', updated_at: 1000 },
        { id: 'user-2', name: 'Bob', updated_at: 2000 },
        { id: 'user-3', name: 'Charlie', updated_at: 3000 },
      ])
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      expect(result.rowCount).toBe(3)
      expect(result.sizeBytes).toBeGreaterThan(0)
    })

    it('includes all columns from data.parquet', async () => {
      // Create parquet with additional columns
      const dataBuffer = parquetWriteBuffer({
        columnData: [
          { name: 'id', data: ['user-1', 'user-2'], type: 'STRING' },
          { name: 'name', data: ['Alice', 'Bob'], type: 'STRING' },
          { name: 'email', data: ['alice@test.com', 'bob@test.com'], type: 'STRING' },
          { name: 'age', data: Int32Array.from([30, 25]), type: 'INT32' },
          { name: 'active', data: [true, false], type: 'BOOLEAN' },
          { name: 'updated_at', data: BigInt64Array.from([1000n, 2000n]), type: 'INT64' },
        ],
        codec: 'UNCOMPRESSED',
        statistics: true,
      })
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      expect(result.columns).toContain('id')
      expect(result.columns).toContain('name')
      expect(result.columns).toContain('email')
      expect(result.columns).toContain('age')
      expect(result.columns).toContain('active')
      expect(result.columns).toContain('updated_at')
    })

    it('records are sorted by primary key', async () => {
      // Records in unsorted order
      const dataBuffer = createTestParquet([
        { id: 'user-3', name: 'Charlie', updated_at: 3000 },
        { id: 'user-1', name: 'Alice', updated_at: 1000 },
        { id: 'user-2', name: 'Bob', updated_at: 2000 },
      ])
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      // Verify sorted order in snapshot
      expect(result.sortedBy).toBe('id')
    })
  })

  // ==========================================================================
  // PITR Reconstruction Tests
  // ==========================================================================

  describe('reconstructState()', () => {
    it('reconstructs state at specific timestamp', async () => {
      // Snapshot at Jan 15
      const snapshotBuffer = createTestParquet([
        { id: 'user-1', name: 'Alice', updated_at: 1000 },
        { id: 'user-2', name: 'Bob', updated_at: 1500 },
      ])
      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', snapshotBuffer)

      // Deltas after snapshot
      const deltaBuffer = createDeltaParquet([
        { id: 'user-1', op: 'update', ts: 2000, doc: { id: 'user-1', name: 'Alice Updated' } },
        { id: 'user-3', op: 'insert', ts: 2500, doc: { id: 'user-3', name: 'Charlie' } },
        { id: 'user-2', op: 'delete', ts: 3000 },
      ])
      mockBucket.files.set('ns/coll/deltas/2024-01-16.parquet', deltaBuffer)

      // Reconstruct at ts=2500 (after Alice update, after Charlie insert, before Bob delete)
      const state = await reconstructState(
        mockBucket,
        'ns',
        'coll',
        new Date('2024-01-16T00:00:02.500Z')
      )

      expect(state.size).toBe(3)
      expect(state.get('user-1')).toEqual({ id: 'user-1', name: 'Alice Updated' })
      expect(state.get('user-2')).toEqual({ id: 'user-2', name: 'Bob', updated_at: 1500n })
      expect(state.get('user-3')).toEqual({ id: 'user-3', name: 'Charlie' })
    })

    it('uses nearest snapshot + deltas', async () => {
      // Multiple snapshots available
      const jan15Snapshot = createTestParquet([
        { id: 'user-1', name: 'Alice Jan15', updated_at: 1000 },
      ])
      const jan20Snapshot = createTestParquet([
        { id: 'user-1', name: 'Alice Jan20', updated_at: 2000 },
        { id: 'user-2', name: 'Bob', updated_at: 1500 },
      ])

      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', jan15Snapshot)
      mockBucket.files.set('ns/coll/snapshots/2024-01-20.parquet', jan20Snapshot)

      // Delta after Jan 20
      const deltaBuffer = createDeltaParquet([
        { id: 'user-3', op: 'insert', ts: 3000, doc: { id: 'user-3', name: 'Charlie' } },
      ])
      mockBucket.files.set('ns/coll/deltas/2024-01-21.parquet', deltaBuffer)

      // Reconstruct at Jan 21 - should use Jan 20 snapshot (nearest) + Jan 21 delta
      const state = await reconstructState(mockBucket, 'ns', 'coll', new Date('2024-01-21T12:00:00Z'))

      expect(state.size).toBe(3)
      expect(state.get('user-1')).toMatchObject({ name: 'Alice Jan20' })
    })

    it('handles missing snapshots by using data.parquet + all deltas', async () => {
      // No snapshots, only data.parquet and deltas
      const dataBuffer = createTestParquet([
        { id: 'user-1', name: 'Alice', updated_at: 1000 },
      ])
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      // Multiple delta files
      const delta1 = createDeltaParquet([
        { id: 'user-2', op: 'insert', ts: 2000, doc: { id: 'user-2', name: 'Bob' } },
      ])
      const delta2 = createDeltaParquet([
        { id: 'user-1', op: 'update', ts: 3000, doc: { id: 'user-1', name: 'Alice Updated' } },
      ])

      mockBucket.files.set('ns/coll/deltas/2024-01-15.parquet', delta1)
      mockBucket.files.set('ns/coll/deltas/2024-01-16.parquet', delta2)

      // Reconstruct at Jan 16 - should apply all deltas to data.parquet
      const state = await reconstructState(mockBucket, 'ns', 'coll', new Date('2024-01-16T23:59:59Z'))

      expect(state.size).toBe(2)
      expect(state.get('user-1')).toMatchObject({ name: 'Alice Updated' })
      expect(state.get('user-2')).toMatchObject({ name: 'Bob' })
    })

    it('returns empty map when no data exists', async () => {
      const state = await reconstructState(mockBucket, 'ns', 'empty', new Date('2024-01-31T00:00:00Z'))

      expect(state.size).toBe(0)
    })

    it('applies deltas in correct timestamp order', async () => {
      const snapshotBuffer = createTestParquet([
        { id: 'user-1', name: 'Alice Original', updated_at: 1000 },
      ])
      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', snapshotBuffer)

      // Multiple updates to same record
      const deltaBuffer = createDeltaParquet([
        { id: 'user-1', op: 'update', ts: 2000, doc: { id: 'user-1', name: 'Alice V2' } },
        { id: 'user-1', op: 'update', ts: 3000, doc: { id: 'user-1', name: 'Alice V3' } },
        { id: 'user-1', op: 'update', ts: 4000, doc: { id: 'user-1', name: 'Alice V4' } },
      ])
      mockBucket.files.set('ns/coll/deltas/2024-01-16.parquet', deltaBuffer)

      // Reconstruct at ts=3000 - should have V3
      const state = await reconstructState(
        mockBucket,
        'ns',
        'coll',
        new Date('2024-01-16T00:00:03.000Z')
      )

      expect(state.get('user-1')).toEqual({ id: 'user-1', name: 'Alice V3' })
    })
  })

  // ==========================================================================
  // Snapshot Manifest Tests
  // ==========================================================================

  describe('listSnapshots()', () => {
    it('tracks all snapshots with timestamps', async () => {
      const snap1 = createTestParquet([{ id: 'user-1', name: 'A', updated_at: 1000 }])
      const snap2 = createTestParquet([
        { id: 'user-1', name: 'A', updated_at: 1000 },
        { id: 'user-2', name: 'B', updated_at: 2000 },
      ])

      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', snap1)
      mockBucket.files.set('ns/coll/snapshots/2024-01-20.parquet', snap2)
      mockBucket.files.set('ns/coll/snapshots/2024-01.parquet', snap2)

      const snapshots = await listSnapshots(mockBucket, 'ns', 'coll')

      expect(snapshots).toHaveLength(3)
      expect(snapshots.map((s) => s.path)).toContain('ns/coll/snapshots/2024-01-15.parquet')
      expect(snapshots.map((s) => s.path)).toContain('ns/coll/snapshots/2024-01-20.parquet')
      expect(snapshots.map((s) => s.path)).toContain('ns/coll/snapshots/2024-01.parquet')
    })

    it('records row counts and file sizes', async () => {
      const snap = createTestParquet([
        { id: 'user-1', name: 'A', updated_at: 1000 },
        { id: 'user-2', name: 'B', updated_at: 2000 },
      ])
      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', snap)

      const snapshots = await listSnapshots(mockBucket, 'ns', 'coll')

      expect(snapshots[0].rowCount).toBe(2)
      expect(snapshots[0].sizeBytes).toBeGreaterThan(0)
    })

    it('lists available PITR points', async () => {
      const snap1 = createTestParquet([{ id: 'user-1', name: 'A', updated_at: 1000 }])
      const snap2 = createTestParquet([{ id: 'user-1', name: 'B', updated_at: 2000 }])

      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', snap1)
      mockBucket.files.set('ns/coll/snapshots/2024-01-20.parquet', snap2)

      const snapshots = await listSnapshots(mockBucket, 'ns', 'coll')

      // Each snapshot represents a PITR point
      const pitrPoints = snapshots.map((s) => s.timestamp)
      expect(pitrPoints).toHaveLength(2)
      expect(pitrPoints[0]).toBeInstanceOf(Date)
      expect(pitrPoints[1]).toBeInstanceOf(Date)
    })

    it('returns empty array when no snapshots exist', async () => {
      const snapshots = await listSnapshots(mockBucket, 'ns', 'empty')

      expect(snapshots).toHaveLength(0)
    })

    it('distinguishes between daily and monthly snapshots', async () => {
      const dailySnap = createTestParquet([{ id: 'user-1', name: 'A', updated_at: 1000 }])
      const monthlySnap = createTestParquet([{ id: 'user-1', name: 'B', updated_at: 2000 }])

      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', dailySnap)
      mockBucket.files.set('ns/coll/snapshots/2024-01.parquet', monthlySnap)

      const snapshots = await listSnapshots(mockBucket, 'ns', 'coll')

      const daily = snapshots.find((s) => s.type === 'daily')
      const monthly = snapshots.find((s) => s.type === 'monthly')

      expect(daily).toBeDefined()
      expect(daily?.path).toBe('ns/coll/snapshots/2024-01-15.parquet')
      expect(monthly).toBeDefined()
      expect(monthly?.path).toBe('ns/coll/snapshots/2024-01.parquet')
    })
  })

  // ==========================================================================
  // Snapshot Cleanup Tests
  // ==========================================================================

  describe('cleanupSnapshots()', () => {
    it('retains configurable number of daily snapshots', async () => {
      // Create 5 daily snapshots
      for (let day = 1; day <= 5; day++) {
        const snap = createTestParquet([{ id: 'user-1', name: `Day${day}`, updated_at: day * 1000 }])
        const dateStr = `2024-01-${day.toString().padStart(2, '0')}`
        mockBucket.files.set(`ns/coll/snapshots/${dateStr}.parquet`, snap)
      }

      const retention: RetentionPolicy = {
        dailySnapshots: 3,
        monthlySnapshots: 12,
      }

      await cleanupSnapshots(mockBucket, 'ns', 'coll', retention)

      // Should keep only the 3 most recent daily snapshots
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01-05.parquet')).toBe(true)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01-04.parquet')).toBe(true)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01-03.parquet')).toBe(true)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01-02.parquet')).toBe(false)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01-01.parquet')).toBe(false)
    })

    it('retains configurable number of monthly snapshots', async () => {
      // Create 5 monthly snapshots
      for (let month = 1; month <= 5; month++) {
        const snap = createTestParquet([
          { id: 'user-1', name: `Month${month}`, updated_at: month * 1000 },
        ])
        const monthStr = `2024-${month.toString().padStart(2, '0')}`
        mockBucket.files.set(`ns/coll/snapshots/${monthStr}.parquet`, snap)
      }

      const retention: RetentionPolicy = {
        dailySnapshots: 30,
        monthlySnapshots: 3,
      }

      await cleanupSnapshots(mockBucket, 'ns', 'coll', retention)

      // Should keep only the 3 most recent monthly snapshots
      expect(mockBucket.files.has('ns/coll/snapshots/2024-05.parquet')).toBe(true)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-04.parquet')).toBe(true)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-03.parquet')).toBe(true)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-02.parquet')).toBe(false)
      expect(mockBucket.files.has('ns/coll/snapshots/2024-01.parquet')).toBe(false)
    })

    it('deletes old snapshots based on retention policy', async () => {
      // Mix of daily and monthly snapshots
      const snap = createTestParquet([{ id: 'user-1', name: 'A', updated_at: 1000 }])

      mockBucket.files.set('ns/coll/snapshots/2024-01-01.parquet', snap) // Daily - old
      mockBucket.files.set('ns/coll/snapshots/2024-01-02.parquet', snap) // Daily - old
      mockBucket.files.set('ns/coll/snapshots/2024-01-03.parquet', snap) // Daily - keep
      mockBucket.files.set('ns/coll/snapshots/2024-01.parquet', snap) // Monthly - keep
      mockBucket.files.set('ns/coll/snapshots/2023-12.parquet', snap) // Monthly - old

      const retention: RetentionPolicy = {
        dailySnapshots: 1,
        monthlySnapshots: 1,
      }

      await cleanupSnapshots(mockBucket, 'ns', 'coll', retention)

      expect(mockBucket.deletedFiles).toContain('ns/coll/snapshots/2024-01-01.parquet')
      expect(mockBucket.deletedFiles).toContain('ns/coll/snapshots/2024-01-02.parquet')
      expect(mockBucket.deletedFiles).toContain('ns/coll/snapshots/2023-12.parquet')
      expect(mockBucket.deletedFiles).not.toContain('ns/coll/snapshots/2024-01-03.parquet')
      expect(mockBucket.deletedFiles).not.toContain('ns/coll/snapshots/2024-01.parquet')
    })

    it('does nothing when under retention limits', async () => {
      const snap = createTestParquet([{ id: 'user-1', name: 'A', updated_at: 1000 }])

      mockBucket.files.set('ns/coll/snapshots/2024-01-01.parquet', snap)
      mockBucket.files.set('ns/coll/snapshots/2024-01.parquet', snap)

      const retention: RetentionPolicy = {
        dailySnapshots: 30,
        monthlySnapshots: 12,
      }

      await cleanupSnapshots(mockBucket, 'ns', 'coll', retention)

      expect(mockBucket.deletedFiles).toHaveLength(0)
    })

    it('updates manifest after cleanup', async () => {
      const snap = createTestParquet([{ id: 'user-1', name: 'A', updated_at: 1000 }])

      mockBucket.files.set('ns/coll/snapshots/2024-01-01.parquet', snap)
      mockBucket.files.set('ns/coll/snapshots/2024-01-02.parquet', snap)
      mockBucket.files.set('ns/coll/snapshots/2024-01-03.parquet', snap)

      const retention: RetentionPolicy = {
        dailySnapshots: 1,
        monthlySnapshots: 12,
      }

      await cleanupSnapshots(mockBucket, 'ns', 'coll', retention)

      // Manifest should be updated
      const manifestBuffer = mockBucket.files.get('ns/coll/snapshots/manifest.json')
      expect(manifestBuffer).toBeDefined()

      const manifest: SnapshotManifest = JSON.parse(
        new TextDecoder().decode(manifestBuffer!)
      )
      expect(manifest.snapshots).toHaveLength(1)
      expect(manifest.snapshots[0].path).toBe('ns/coll/snapshots/2024-01-03.parquet')
    })
  })

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('edge cases', () => {
    it('handles empty data.parquet', async () => {
      const emptyData = createTestParquet([])
      mockBucket.files.set('ns/coll/data.parquet', emptyData)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      expect(result.rowCount).toBe(0)
    })

    it('handles missing data.parquet for daily snapshot', async () => {
      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      await expect(createSnapshot(mockBucket, 'ns', 'missing', 'daily')).rejects.toThrow(
        'data.parquet not found'
      )
    })

    it('handles missing daily snapshots for monthly snapshot', async () => {
      vi.setSystemTime(new Date('2024-02-01T00:00:00Z'))

      await expect(createSnapshot(mockBucket, 'ns', 'empty', 'monthly')).rejects.toThrow(
        'no daily snapshots found'
      )
    })

    it('handles concurrent snapshot creation', async () => {
      const dataBuffer = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      // Concurrent calls should not fail
      const [result1, result2] = await Promise.all([
        createSnapshot(mockBucket, 'ns', 'coll', 'daily'),
        createSnapshot(mockBucket, 'ns', 'coll', 'daily'),
      ])

      // Both should succeed (one will overwrite the other)
      expect(result1.path).toBe(result2.path)
    })

    it('handles special characters in namespace and collection names', async () => {
      const dataBuffer = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
      mockBucket.files.set('my-app_v2/user-profiles/data.parquet', dataBuffer)

      vi.setSystemTime(new Date('2024-01-31T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'my-app_v2', 'user-profiles', 'daily')

      expect(result.path).toBe('my-app_v2/user-profiles/snapshots/2024-01-31.parquet')
    })

    it('reconstructs state at exact snapshot timestamp', async () => {
      const snapshotBuffer = createTestParquet([
        { id: 'user-1', name: 'Alice Snapshot', updated_at: 1000 },
      ])
      mockBucket.files.set('ns/coll/snapshots/2024-01-15.parquet', snapshotBuffer)

      // Reconstruct exactly at snapshot time
      const state = await reconstructState(mockBucket, 'ns', 'coll', new Date('2024-01-15T00:00:00Z'))

      expect(state.size).toBe(1)
      expect(state.get('user-1')).toMatchObject({ name: 'Alice Snapshot' })
    })

    it('handles leap year dates correctly', async () => {
      const dataBuffer = createTestParquet([{ id: 'user-1', name: 'Alice', updated_at: 1000 }])
      mockBucket.files.set('ns/coll/data.parquet', dataBuffer)

      // Leap year Feb 29
      vi.setSystemTime(new Date('2024-02-29T00:00:00Z'))

      const result = await createSnapshot(mockBucket, 'ns', 'coll', 'daily')

      expect(result.path).toBe('ns/coll/snapshots/2024-02-29.parquet')
    })
  })
})
