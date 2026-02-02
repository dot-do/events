/**
 * Tests for R2StorageBackend
 *
 * Tests the StorageBackend implementation for R2 buckets.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  R2StorageBackend,
  createR2Storage,
  FileNotFoundError,
  VersionMismatchError,
} from '../deltalake-storage'

// Mock R2 bucket
function createMockBucket() {
  const storage = new Map<string, { data: ArrayBuffer; etag: string; uploaded: Date }>()

  return {
    storage,
    get: vi.fn(async (key: string, options?: { range?: { offset: number; length: number } }) => {
      const item = storage.get(key)
      if (!item) return null

      let data = item.data
      if (options?.range) {
        data = data.slice(options.range.offset, options.range.offset + options.range.length)
      }

      return {
        arrayBuffer: async () => data,
        etag: item.etag,
        size: item.data.byteLength,
        uploaded: item.uploaded,
      }
    }),
    put: vi.fn(async (key: string, data: ArrayBuffer | Uint8Array) => {
      const buffer = data instanceof Uint8Array ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : data
      const etag = `etag-${Date.now()}-${Math.random().toString(36).slice(2)}`
      storage.set(key, {
        data: buffer as ArrayBuffer,
        etag,
        uploaded: new Date(),
      })
      return { etag }
    }),
    delete: vi.fn(async (key: string) => {
      storage.delete(key)
    }),
    head: vi.fn(async (key: string) => {
      const item = storage.get(key)
      if (!item) return null
      return {
        etag: item.etag,
        size: item.data.byteLength,
        uploaded: item.uploaded,
      }
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
      return {
        objects,
        truncated: false,
        cursor: undefined,
      }
    }),
  } as unknown as R2Bucket & { storage: Map<string, unknown> }
}

describe('R2StorageBackend', () => {
  let bucket: ReturnType<typeof createMockBucket>
  let storage: R2StorageBackend

  beforeEach(() => {
    bucket = createMockBucket()
    storage = new R2StorageBackend(bucket)
  })

  describe('constructor', () => {
    it('should throw if bucket is invalid', () => {
      expect(() => new R2StorageBackend(null as unknown as R2Bucket)).toThrow(
        'R2StorageBackend requires a valid R2Bucket instance'
      )
      expect(() => new R2StorageBackend({} as unknown as R2Bucket)).toThrow(
        'R2StorageBackend requires a valid R2Bucket instance'
      )
    })
  })

  describe('read', () => {
    it('should read file contents', async () => {
      const data = new TextEncoder().encode('hello world')
      await bucket.put('test.txt', data)

      const result = await storage.read('test.txt')
      expect(new TextDecoder().decode(result)).toBe('hello world')
    })

    it('should throw FileNotFoundError for missing file', async () => {
      await expect(storage.read('missing.txt')).rejects.toThrow(FileNotFoundError)
      await expect(storage.read('missing.txt')).rejects.toMatchObject({
        path: 'missing.txt',
        operation: 'read',
      })
    })
  })

  describe('write', () => {
    it('should write file contents', async () => {
      const data = new TextEncoder().encode('test data')
      await storage.write('output.txt', data)

      const result = await storage.read('output.txt')
      expect(new TextDecoder().decode(result)).toBe('test data')
    })
  })

  describe('list', () => {
    it('should list files with prefix', async () => {
      await bucket.put('events/2024/01/a.parquet', new Uint8Array([1]))
      await bucket.put('events/2024/01/b.parquet', new Uint8Array([2]))
      await bucket.put('events/2024/02/c.parquet', new Uint8Array([3]))
      await bucket.put('other/file.txt', new Uint8Array([4]))

      const result = await storage.list('events/2024/01/')
      expect(result).toHaveLength(2)
      expect(result).toContain('events/2024/01/a.parquet')
      expect(result).toContain('events/2024/01/b.parquet')
    })

    it('should return empty array for no matches', async () => {
      const result = await storage.list('nonexistent/')
      expect(result).toHaveLength(0)
    })
  })

  describe('delete', () => {
    it('should delete file', async () => {
      await bucket.put('to-delete.txt', new Uint8Array([1, 2, 3]))
      expect(await storage.exists('to-delete.txt')).toBe(true)

      await storage.delete('to-delete.txt')
      expect(await storage.exists('to-delete.txt')).toBe(false)
    })

    it('should not throw for missing file (idempotent)', async () => {
      await expect(storage.delete('nonexistent.txt')).resolves.not.toThrow()
    })
  })

  describe('exists', () => {
    it('should return true for existing file', async () => {
      await bucket.put('exists.txt', new Uint8Array([1]))
      expect(await storage.exists('exists.txt')).toBe(true)
    })

    it('should return false for missing file', async () => {
      expect(await storage.exists('missing.txt')).toBe(false)
    })
  })

  describe('stat', () => {
    it('should return file metadata', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await bucket.put('file.bin', data)

      const stat = await storage.stat('file.bin')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(5)
      expect(stat!.lastModified).toBeInstanceOf(Date)
      expect(stat!.etag).toBeDefined()
    })

    it('should return null for missing file', async () => {
      const stat = await storage.stat('missing.bin')
      expect(stat).toBeNull()
    })
  })

  describe('readRange', () => {
    it('should read byte range from file', async () => {
      const data = new TextEncoder().encode('0123456789')
      await bucket.put('numbers.txt', data)

      const result = await storage.readRange('numbers.txt', 3, 7)
      expect(new TextDecoder().decode(result)).toBe('3456')
    })

    it('should throw FileNotFoundError for missing file', async () => {
      await expect(storage.readRange('missing.txt', 0, 10)).rejects.toThrow(FileNotFoundError)
    })
  })

  describe('getVersion', () => {
    it('should return etag for existing file', async () => {
      await bucket.put('versioned.txt', new Uint8Array([1]))

      const version = await storage.getVersion('versioned.txt')
      expect(version).toBeDefined()
      expect(typeof version).toBe('string')
    })

    it('should return null for missing file', async () => {
      const version = await storage.getVersion('missing.txt')
      expect(version).toBeNull()
    })
  })

  describe('writeConditional', () => {
    it('should create file when expectedVersion is null and file does not exist', async () => {
      const data = new TextEncoder().encode('new file')
      const newVersion = await storage.writeConditional('new.txt', data, null)

      expect(newVersion).toBeDefined()
      const content = await storage.read('new.txt')
      expect(new TextDecoder().decode(content)).toBe('new file')
    })

    it('should throw VersionMismatchError when creating but file exists', async () => {
      await bucket.put('existing.txt', new Uint8Array([1]))

      const data = new TextEncoder().encode('overwrite attempt')
      await expect(storage.writeConditional('existing.txt', data, null)).rejects.toThrow(
        VersionMismatchError
      )
    })

    it('should update file when version matches', async () => {
      await bucket.put('update.txt', new TextEncoder().encode('v1'))
      const currentVersion = await storage.getVersion('update.txt')

      const data = new TextEncoder().encode('v2')
      const newVersion = await storage.writeConditional('update.txt', data, currentVersion)

      expect(newVersion).toBeDefined()
      expect(newVersion).not.toBe(currentVersion)
      const content = await storage.read('update.txt')
      expect(new TextDecoder().decode(content)).toBe('v2')
    })

    it('should throw VersionMismatchError when version does not match', async () => {
      await bucket.put('update.txt', new TextEncoder().encode('v1'))

      const data = new TextEncoder().encode('v2')
      await expect(storage.writeConditional('update.txt', data, 'wrong-etag')).rejects.toThrow(
        VersionMismatchError
      )
    })

    it('should throw VersionMismatchError when updating non-existent file', async () => {
      const data = new TextEncoder().encode('data')
      await expect(storage.writeConditional('missing.txt', data, 'some-etag')).rejects.toThrow(
        VersionMismatchError
      )
    })

    it('should serialize concurrent writes to same path', async () => {
      // Create initial file
      await bucket.put('concurrent.txt', new TextEncoder().encode('initial'))
      const version = await storage.getVersion('concurrent.txt')

      // Start two concurrent writes
      const write1 = storage.writeConditional(
        'concurrent.txt',
        new TextEncoder().encode('write1'),
        version
      )
      const write2 = storage.writeConditional(
        'concurrent.txt',
        new TextEncoder().encode('write2'),
        version
      )

      // One should succeed, one should fail
      const results = await Promise.allSettled([write1, write2])
      const successes = results.filter((r) => r.status === 'fulfilled')
      const failures = results.filter((r) => r.status === 'rejected')

      expect(successes).toHaveLength(1)
      expect(failures).toHaveLength(1)
      expect((failures[0] as PromiseRejectedResult).reason).toBeInstanceOf(VersionMismatchError)
    })
  })
})

describe('createR2Storage', () => {
  it('should create R2StorageBackend instance', () => {
    const bucket = createMockBucket()
    const storage = createR2Storage(bucket)
    expect(storage).toBeInstanceOf(R2StorageBackend)
  })
})
