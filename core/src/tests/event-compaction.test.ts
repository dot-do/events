/**
 * Event Stream Compaction Tests
 *
 * Tests for compacting hourly Parquet event files into daily files.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { compactEventStream } from '../event-compaction.js'
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
 * Creates a mock R2Bucket with configurable files and content
 */
function createMockR2Bucket(
  objects: Array<{ key: string; content?: ArrayBuffer }>,
  options: {
    onPut?: (key: string, content: ArrayBuffer | ArrayBufferView | string | null | Blob | ReadableStream) => void
    onDelete?: (key: string) => void
  } = {}
) {
  const store = new Map<string, ArrayBuffer>()

  // Initialize store with provided objects
  for (const obj of objects) {
    if (obj.content) {
      store.set(obj.key, obj.content)
    }
  }

  return {
    list: vi.fn(async (opts?: { prefix?: string; cursor?: string; limit?: number; delimiter?: string }) => {
      const filtered = objects.filter((obj) => !opts?.prefix || obj.key.startsWith(opts.prefix))
      return {
        objects: filtered.map((obj) => ({
          key: obj.key,
          size: obj.content?.byteLength ?? 100,
          etag: 'test-etag',
          uploaded: new Date(),
          httpMetadata: {},
          customMetadata: {},
        })),
        truncated: false,
        cursor: undefined,
      }
    }),
    get: vi.fn(async (key: string) => {
      const content = store.get(key)
      if (!content) return null
      return {
        key,
        size: content.byteLength,
        arrayBuffer: async () => content,
      }
    }),
    put: vi.fn(async (key: string, content: ArrayBuffer | ArrayBufferView | string | null | Blob | ReadableStream) => {
      if (content instanceof ArrayBuffer) {
        store.set(key, content)
      }
      options.onPut?.(key, content)
    }),
    delete: vi.fn(async (key: string | string[]) => {
      const keys = Array.isArray(key) ? key : [key]
      for (const k of keys) {
        store.delete(k)
        options.onDelete?.(k)
      }
    }),
  } as unknown as R2Bucket
}

describe('Event Stream Compaction', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    // Set to 2024-01-20 at noon UTC (so we can look back at 2024-01-19, etc.)
    vi.setSystemTime(new Date('2024-01-20T12:00:00Z'))
  })

  describe('compactEventStream', () => {
    it('skips days with no files', async () => {
      const bucket = createMockR2Bucket([])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3,
      })

      expect(result.days).toHaveLength(0)
      expect(result.totalSourceFiles).toBe(0)
      expect(result.totalOutputFiles).toBe(0)
    })

    it('skips days with only one file', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/19/00/abc.parquet', content },
      ])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3,
      })

      // Day with one file shouldn't be in results
      expect(result.days).toHaveLength(0)
      expect(bucket.delete).not.toHaveBeenCalled()
    })

    it('compacts multiple files for a day into one', async () => {
      const content1 = createTestParquet([{ ts: 1000, type: 'a' }])
      const content2 = createTestParquet([{ ts: 2000, type: 'b' }])
      const putKeys: string[] = []
      const deletedKeys: string[] = []

      const bucket = createMockR2Bucket(
        [
          { key: 'events/2024/01/19/10/abc.parquet', content: content1 },
          { key: 'events/2024/01/19/11/def.parquet', content: content2 },
        ],
        {
          onPut: (key) => putKeys.push(key),
          onDelete: (key) => deletedKeys.push(key),
        }
      )

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3,
      })

      expect(result.days).toHaveLength(1)
      expect(result.days[0].success).toBe(true)
      expect(result.days[0].sourceFiles).toBe(2)
      expect(result.days[0].outputFiles).toBe(1)
      expect(result.days[0].totalRecords).toBe(2)

      // Verify source files were deleted
      expect(deletedKeys).toContain('events/2024/01/19/10/abc.parquet')
      expect(deletedKeys).toContain('events/2024/01/19/11/def.parquet')

      // Verify output file was created
      expect(putKeys.length).toBe(1)
      expect(putKeys[0]).toMatch(/events\/2024\/01\/19\/_compact_.*\.parquet/)
    })

    it('handles dry run mode without writing', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/19/10/abc.parquet', content },
        { key: 'events/2024/01/19/11/def.parquet', content },
      ])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3,
        dryRun: true,
      })

      expect(result.days).toHaveLength(1)
      expect(result.days[0].dryRun).toBe(true)
      expect(bucket.put).not.toHaveBeenCalled()
      expect(bucket.delete).not.toHaveBeenCalled()
    })

    it('skips already compacted files', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/19/10/abc.parquet', content },
        { key: 'events/2024/01/19/_compact_xyz.parquet', content },
      ])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3,
      })

      // Only one non-compact file, so no compaction needed
      expect(result.days).toHaveLength(0)
    })

    it('processes multiple prefixes', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/19/10/a.parquet', content },
        { key: 'events/2024/01/19/11/b.parquet', content },
        { key: 'webhooks/2024/01/19/10/c.parquet', content },
        { key: 'webhooks/2024/01/19/11/d.parquet', content },
      ])

      const result = await compactEventStream(bucket, {
        prefixes: ['events', 'webhooks'],
        daysBack: 3,
      })

      expect(result.days).toHaveLength(2)
    })

    it('does not compact today (files still being written)', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])

      // Today is 2024-01-20 (set in beforeEach)
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/20/10/a.parquet', content },
        { key: 'events/2024/01/20/11/b.parquet', content },
      ])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3,
      })

      // Today should be excluded
      expect(result.days).toHaveLength(0)
      expect(bucket.put).not.toHaveBeenCalled()
    })

    it('respects daysBack parameter', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])

      // Files from 8 days ago
      const bucket = createMockR2Bucket([
        { key: 'events/2024/01/12/10/a.parquet', content },
        { key: 'events/2024/01/12/11/b.parquet', content },
      ])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 3, // Only look back 3 days
      })

      // 8 days ago is outside the 3-day window
      expect(result.days).toHaveLength(0)
    })

    it('returns duration and timestamps', async () => {
      const bucket = createMockR2Bucket([])

      const result = await compactEventStream(bucket, {
        prefixes: ['events'],
        daysBack: 1,
      })

      expect(result.startedAt).toBeDefined()
      expect(result.completedAt).toBeDefined()
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
    })

    it('handles read errors gracefully', async () => {
      const content = createTestParquet([{ ts: 1000, type: 'test' }])
      const mockBucket = createMockR2Bucket([
        { key: 'events/2024/01/19/10/a.parquet', content },
        { key: 'events/2024/01/19/11/b.parquet', content },
        { key: 'events/2024/01/19/12/c.parquet', content },
      ])

      // Make get return null for the second file
      mockBucket.get = vi.fn(async (key: string) => {
        if (key.includes('b.parquet')) {
          return null
        }
        return {
          key,
          size: content.byteLength,
          arrayBuffer: async () => content,
        }
      })

      const result = await compactEventStream(mockBucket, {
        prefixes: ['events'],
        daysBack: 3,
      })

      // Should still succeed with the files that were readable
      expect(result.days).toHaveLength(1)
      // Should have merged the 2 readable files (a.parquet and c.parquet)
      expect(result.days[0].sourceFiles).toBeGreaterThanOrEqual(2)
    })
  })
})
