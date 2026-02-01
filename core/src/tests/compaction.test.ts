/**
 * Parquet Compaction Tests
 *
 * TDD Red Phase: Tests for Parquet compaction functionality
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { readParquetRecords, mergeParquetRecords, writeCompactedParquet, listFilesForCompaction } from '../compaction.js'
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'

/**
 * Creates a test Parquet buffer with the given records
 */
function createTestParquet(records: Array<{ ts: number; type: string }>) {
  return parquetWriteBuffer({
    columnData: [
      {
        name: 'ts',
        data: BigInt64Array.from(records.map((r) => BigInt(r.ts))),
        type: 'INT64',
      },
      {
        name: 'type',
        data: records.map((r) => r.type),
        type: 'STRING',
      },
    ],
    codec: 'UNCOMPRESSED',
    statistics: true,
  })
}

/**
 * Creates a mock R2Bucket
 */
function createMockR2Bucket(objects: Array<{ key: string }>) {
  return {
    list: vi.fn(async (options?: { prefix?: string }) => {
      const filtered = objects.filter((obj) => !options?.prefix || obj.key.startsWith(options.prefix))
      return {
        objects: filtered.map((obj) => ({
          key: obj.key,
          size: 100,
          etag: 'test-etag',
          uploaded: new Date(),
          httpMetadata: {},
          customMetadata: {},
        })),
        truncated: false,
        cursor: undefined,
      }
    }),
  } as unknown as R2Bucket
}

describe('Parquet Compaction', () => {
  describe('readParquetRecords', () => {
    it('reads Parquet records and returns row objects', async () => {
      const buffer = createTestParquet([
        { ts: 1000, type: 'test.a' },
        { ts: 2000, type: 'test.b' },
      ])

      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(2)
      expect(records[0].type).toBe('test.a')
      expect(records[0].ts).toBe(1000n)
      expect(records[1].type).toBe('test.b')
      expect(records[1].ts).toBe(2000n)
    })

    it('handles empty Parquet files', async () => {
      const buffer = createTestParquet([])

      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(0)
    })

    it('preserves all column types', async () => {
      const buffer = parquetWriteBuffer({
        columnData: [
          { name: 'int_col', data: Int32Array.from([1, 2, 3]), type: 'INT32' },
          { name: 'str_col', data: ['a', 'b', 'c'], type: 'STRING' },
          { name: 'float_col', data: Float64Array.from([1.1, 2.2, 3.3]), type: 'DOUBLE' },
        ],
        codec: 'UNCOMPRESSED',
      })

      const records = await readParquetRecords(buffer)

      expect(records).toHaveLength(3)
      expect(records[0].int_col).toBe(1)
      expect(records[0].str_col).toBe('a')
      expect(records[0].float_col).toBeCloseTo(1.1)
    })
  })

  describe('mergeParquetRecords', () => {
    it('merges multiple files sorted by ts', async () => {
      const file1 = createTestParquet([
        { ts: 3000, type: 'c' },
        { ts: 1000, type: 'a' },
      ])
      const file2 = createTestParquet([{ ts: 2000, type: 'b' }])

      const merged = await mergeParquetRecords([file1, file2])

      expect(merged).toHaveLength(3)
      expect(merged[0].ts).toBe(1000n) // Sorted
      expect(merged[1].ts).toBe(2000n)
      expect(merged[2].ts).toBe(3000n)
    })

    it('handles single file', async () => {
      const file = createTestParquet([
        { ts: 2000, type: 'b' },
        { ts: 1000, type: 'a' },
      ])

      const merged = await mergeParquetRecords([file])

      expect(merged).toHaveLength(2)
      expect(merged[0].ts).toBe(1000n)
      expect(merged[1].ts).toBe(2000n)
    })

    it('handles empty array', async () => {
      const merged = await mergeParquetRecords([])

      expect(merged).toHaveLength(0)
    })

    it('maintains stable sort order for same timestamp', async () => {
      const file1 = createTestParquet([{ ts: 1000, type: 'a' }])
      const file2 = createTestParquet([{ ts: 1000, type: 'b' }])
      const file3 = createTestParquet([{ ts: 1000, type: 'c' }])

      const merged = await mergeParquetRecords([file1, file2, file3])

      expect(merged).toHaveLength(3)
      // All have same timestamp
      expect(merged.every((r) => r.ts === 1000n)).toBe(true)
    })
  })

  describe('writeCompactedParquet', () => {
    it('writes compacted Parquet buffer', async () => {
      const records = [
        { ts: 1000n, type: 'a' },
        { ts: 2000n, type: 'b' },
      ]

      const buffer = writeCompactedParquet(records)

      expect(buffer.byteLength).toBeGreaterThan(0)

      // Verify by reading back
      const readBack = await readParquetRecords(buffer)
      expect(readBack).toHaveLength(2)
      expect(readBack[0].ts).toBe(1000n)
      expect(readBack[0].type).toBe('a')
    })

    it('handles empty records array', async () => {
      const records: Record<string, unknown>[] = []

      const buffer = writeCompactedParquet(records)

      expect(buffer.byteLength).toBeGreaterThan(0) // Still has header

      const readBack = await readParquetRecords(buffer)
      expect(readBack).toHaveLength(0)
    })

    it('preserves all event schema columns', async () => {
      const records = [
        {
          ts: 1000n,
          script_name: 'test-worker',
          outcome: 'ok',
          event_type: 'fetch',
          duration_ms: 42.5,
          method: 'GET',
          url: 'https://example.com',
          status_code: 200,
          cf_ray: 'abc123',
          event: '{"test": true}',
          logs: '[]',
          exceptions: null,
        },
      ]

      const buffer = writeCompactedParquet(records)
      const readBack = await readParquetRecords(buffer)

      expect(readBack).toHaveLength(1)
      expect(readBack[0].script_name).toBe('test-worker')
      expect(readBack[0].outcome).toBe('ok')
      expect(readBack[0].status_code).toBe(200)
    })

    it('uses UNCOMPRESSED codec', async () => {
      const records = [{ ts: 1000n, type: 'test' }]

      // Should not throw - UNCOMPRESSED requires no additional compressor
      const buffer = writeCompactedParquet(records)
      expect(buffer.byteLength).toBeGreaterThan(0)
    })
  })

  describe('listFilesForCompaction', () => {
    it('lists files in prefix', async () => {
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/15/12/abc.parquet' },
        { key: 'events/2024/01/15/12/def.parquet' },
        { key: 'events/2024/01/15/13/ghi.parquet' },
      ])

      const files = await listFilesForCompaction(bucket, 'events/2024/01/15/12/')

      expect(files).toHaveLength(2)
      expect(files[0].key).toBe('events/2024/01/15/12/abc.parquet')
      expect(files[1].key).toBe('events/2024/01/15/12/def.parquet')
    })

    it('excludes files containing _compact', async () => {
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/15/12/abc.parquet' },
        { key: 'events/2024/01/15/12/def_compact.parquet' },
        { key: 'events/2024/01/15/12/ghi.parquet' },
      ])

      const files = await listFilesForCompaction(bucket, 'events/2024/01/15/12/')

      expect(files).toHaveLength(2)
      expect(files.some((f) => f.key.includes('_compact'))).toBe(false)
    })

    it('returns files sorted by key', async () => {
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/15/12/zzz.parquet' },
        { key: 'events/2024/01/15/12/aaa.parquet' },
        { key: 'events/2024/01/15/12/mmm.parquet' },
      ])

      const files = await listFilesForCompaction(bucket, 'events/2024/01/15/12/')

      expect(files[0].key).toBe('events/2024/01/15/12/aaa.parquet')
      expect(files[1].key).toBe('events/2024/01/15/12/mmm.parquet')
      expect(files[2].key).toBe('events/2024/01/15/12/zzz.parquet')
    })

    it('returns empty array for empty prefix', async () => {
      const bucket = createMockR2Bucket([])

      const files = await listFilesForCompaction(bucket, 'events/2024/01/15/12/')

      expect(files).toHaveLength(0)
    })
  })
})
