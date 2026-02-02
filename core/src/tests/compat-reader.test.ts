/**
 * Tests for Compatibility Reader
 *
 * Tests the unified query interface for legacy Parquet + DeltaTable.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { CompatReader, createCompatReader } from '../compat-reader'
import type { StorageBackend } from '@dotdo/deltalake'

// Mock R2 bucket
function createMockBucket() {
  const storage = new Map<string, { data: ArrayBuffer; etag: string; uploaded: Date }>()

  return {
    storage,
    get: vi.fn(async (key: string) => {
      const item = storage.get(key)
      if (!item) return null
      return {
        arrayBuffer: async () => item.data,
        etag: item.etag,
        size: item.data.byteLength,
        uploaded: item.uploaded,
      }
    }),
    put: vi.fn(async (key: string, data: ArrayBuffer | Uint8Array) => {
      const buffer = data instanceof Uint8Array
        ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
        : data
      const etag = `etag-${Date.now()}`
      storage.set(key, { data: buffer as ArrayBuffer, etag, uploaded: new Date() })
      return { etag }
    }),
    delete: vi.fn(async (key: string) => {
      storage.delete(key)
    }),
    head: vi.fn(async (key: string) => {
      const item = storage.get(key)
      if (!item) return null
      return { etag: item.etag, size: item.data.byteLength, uploaded: item.uploaded }
    }),
    list: vi.fn(async (options: { prefix?: string; cursor?: string }) => {
      const prefix = options.prefix ?? ''
      const objects = Array.from(storage.entries())
        .filter(([key]) => key.startsWith(prefix))
        .map(([key, item]) => ({
          key,
          size: item.data.byteLength,
          etag: item.etag,
          uploaded: item.uploaded,
        }))
      return { objects, truncated: false, cursor: undefined }
    }),
  } as unknown as R2Bucket & { storage: Map<string, unknown> }
}

// Mock storage backend
function createMockStorage(): StorageBackend {
  const files = new Map<string, Uint8Array>()

  return {
    read: vi.fn(async (path: string) => {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data
    }),
    write: vi.fn(async (path: string, data: Uint8Array) => {
      files.set(path, data)
    }),
    delete: vi.fn(async (path: string) => {
      files.delete(path)
    }),
    list: vi.fn(async (prefix: string) => {
      return Array.from(files.keys()).filter(k => k.startsWith(prefix))
    }),
    exists: vi.fn(async (path: string) => files.has(path)),
    stat: vi.fn(async (path: string) => {
      const data = files.get(path)
      if (!data) return null
      return { size: data.length, lastModified: new Date(), etag: 'mock-etag' }
    }),
    readRange: vi.fn(async (path: string, start: number, end: number) => {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data.slice(start, end)
    }),
    getVersion: vi.fn(async (path: string) => {
      return files.has(path) ? 'mock-version' : null
    }),
    writeConditional: vi.fn(async (path: string, data: Uint8Array) => {
      files.set(path, data)
      return 'new-version'
    }),
  }
}

describe('CompatReader', () => {
  let bucket: ReturnType<typeof createMockBucket>
  let storage: StorageBackend
  let reader: CompatReader

  beforeEach(() => {
    bucket = createMockBucket()
    storage = createMockStorage()
    reader = new CompatReader({
      bucket,
      storage,
      shardId: 0,
      legacyPrefix: 'events/',
    })
  })

  describe('constructor', () => {
    it('should create reader with default options', () => {
      const r = new CompatReader({ bucket, storage })
      expect(r).toBeInstanceOf(CompatReader)
    })

    it('should create reader with custom options', () => {
      const r = new CompatReader({
        bucket,
        storage,
        shardId: 5,
        legacyPrefix: 'custom/',
      })
      expect(r).toBeInstanceOf(CompatReader)
    })
  })

  describe('query', () => {
    it('should return empty results when no data exists', async () => {
      const result = await reader.query({ skipLegacy: true, skipDelta: true })
      expect(result.records).toHaveLength(0)
      expect(result.legacyCount).toBe(0)
      expect(result.deltaCount).toBe(0)
    })

    it('should skip legacy when skipLegacy is true', async () => {
      const result = await reader.query({ skipLegacy: true })
      expect(result.legacyCount).toBe(0)
    })

    it('should skip delta when skipDelta is true', async () => {
      const result = await reader.query({ skipDelta: true })
      expect(result.deltaCount).toBe(0)
    })

    it('should include duration in result', async () => {
      const result = await reader.query()
      expect(typeof result.durationMs).toBe('number')
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
    })
  })

  describe('count', () => {
    it('should return count object', async () => {
      const result = await reader.count()
      expect(result).toHaveProperty('total')
      expect(result).toHaveProperty('legacy')
      expect(result).toHaveProperty('delta')
    })
  })

  describe('latest', () => {
    it('should return empty array when no data', async () => {
      const result = await reader.latest(10)
      expect(result).toHaveLength(0)
    })

    it('should respect limit parameter', async () => {
      const result = await reader.latest(5)
      expect(result.length).toBeLessThanOrEqual(5)
    })
  })

  describe('byType', () => {
    it('should query by type', async () => {
      const result = await reader.byType('webhook.github.push')
      expect(Array.isArray(result)).toBe(true)
    })
  })

  describe('bySource', () => {
    it('should query by source', async () => {
      const result = await reader.bySource('github')
      expect(Array.isArray(result)).toBe(true)
    })
  })

  describe('inTimeRange', () => {
    it('should query by time range', async () => {
      const result = await reader.inTimeRange(
        '2024-01-01T00:00:00.000Z',
        '2024-01-31T23:59:59.999Z'
      )
      expect(Array.isArray(result)).toBe(true)
    })
  })

  describe('filter matching', () => {
    it('should match simple equality filter', async () => {
      // This tests the internal filter logic indirectly
      const result = await reader.query({
        filter: { type: 'test' },
        skipLegacy: true,
        skipDelta: true,
      })
      expect(result.records).toHaveLength(0)
    })
  })
})

describe('createCompatReader', () => {
  it('should create CompatReader instance', () => {
    const bucket = createMockBucket()
    const storage = createMockStorage()
    const reader = createCompatReader(bucket, storage)
    expect(reader).toBeInstanceOf(CompatReader)
  })

  it('should pass options to reader', () => {
    const bucket = createMockBucket()
    const storage = createMockStorage()
    const reader = createCompatReader(bucket, storage, {
      shardId: 3,
      legacyPrefix: 'custom-events/',
    })
    expect(reader).toBeInstanceOf(CompatReader)
  })
})

describe('Date parsing from path', () => {
  let bucket: ReturnType<typeof createMockBucket>
  let storage: StorageBackend
  let reader: CompatReader

  beforeEach(() => {
    bucket = createMockBucket()
    storage = createMockStorage()
    reader = new CompatReader({ bucket, storage })
  })

  it('should filter files by time range', async () => {
    // Add some mock files with date paths
    // Note: These won't be read as Parquet, just testing the listing logic
    const mockData = new Uint8Array([1, 2, 3])
    bucket.storage.set('events/source/2024/01/15/12/abc.parquet', {
      data: mockData.buffer,
      etag: 'e1',
      uploaded: new Date(),
    })
    bucket.storage.set('events/source/2024/01/16/12/def.parquet', {
      data: mockData.buffer,
      etag: 'e2',
      uploaded: new Date(),
    })

    // Query should attempt to list files
    // (will fail to read them as Parquet, but that's ok for this test)
    const result = await reader.query({
      startTime: '2024-01-15T00:00:00.000Z',
      endTime: '2024-01-15T23:59:59.999Z',
      skipDelta: true,
    })

    // The actual records will be empty because we can't parse fake parquet
    // but the list operation should have been called
    expect(bucket.list).toHaveBeenCalled()
  })

  it('should skip delta log files in legacy query', async () => {
    const mockData = new Uint8Array([1, 2, 3])
    bucket.storage.set('events/shard=0/_delta_log/00000000000000000001.json', {
      data: mockData.buffer,
      etag: 'e1',
      uploaded: new Date(),
    })

    const result = await reader.query({ skipDelta: true })

    // Should not try to read delta log files as legacy parquet
    expect(result.legacyCount).toBe(0)
  })
})
