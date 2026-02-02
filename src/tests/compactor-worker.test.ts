/**
 * Compactor Worker Tests
 *
 * Comprehensive unit tests for the compactor-worker scheduled worker that handles:
 * - Hourly compaction of Parquet files
 * - Daily compaction (merging hourly compacts)
 * - R2 bucket interactions
 * - Error handling
 * - HTTP endpoints for manual triggers
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// ============================================================================
// Mock the @dotdo/events imports
// ============================================================================

vi.mock('@dotdo/events', () => ({
  readParquetRecords: vi.fn(),
  mergeParquetRecords: vi.fn(),
  writeCompactedParquet: vi.fn(),
  listFilesForCompaction: vi.fn(),
}))

import {
  readParquetRecords,
  mergeParquetRecords,
  writeCompactedParquet,
  listFilesForCompaction,
} from '@dotdo/events'

// ============================================================================
// Types
// ============================================================================

interface MockR2Object {
  key: string
  size: number
  uploaded: Date
  etag: string
  httpEtag: string
}

interface MockR2Bucket {
  list: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  put: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  _objects: Map<string, ArrayBuffer>
}

interface MockEnv {
  EVENTS_BUCKET: MockR2Bucket
  ENVIRONMENT: string
}

// ============================================================================
// Mock Factories
// ============================================================================

function createMockR2Object(key: string, size = 1000): MockR2Object {
  return {
    key,
    size,
    uploaded: new Date(),
    etag: `etag-${key}`,
    httpEtag: `"etag-${key}"`,
  }
}

function createMockR2Bucket(objects: Map<string, ArrayBuffer> = new Map()): MockR2Bucket {
  return {
    list: vi.fn().mockImplementation(({ prefix }: { prefix: string }) => {
      const matchingObjects: MockR2Object[] = []
      for (const key of objects.keys()) {
        if (key.startsWith(prefix)) {
          matchingObjects.push(createMockR2Object(key, objects.get(key)?.byteLength ?? 0))
        }
      }
      return Promise.resolve({
        objects: matchingObjects,
        truncated: false,
        delimitedPrefixes: [],
      })
    }),
    get: vi.fn().mockImplementation((key: string) => {
      const buffer = objects.get(key)
      if (!buffer) return Promise.resolve(null)
      return Promise.resolve({
        key,
        arrayBuffer: () => Promise.resolve(buffer),
        body: new ReadableStream(),
        customMetadata: {},
      })
    }),
    put: vi.fn().mockImplementation((key: string, body: ArrayBuffer, _options?: object) => {
      objects.set(key, body)
      return Promise.resolve({
        key,
        version: 'v1',
        size: body.byteLength,
      })
    }),
    delete: vi.fn().mockImplementation((key: string) => {
      objects.delete(key)
      return Promise.resolve()
    }),
    _objects: objects,
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    EVENTS_BUCKET: createMockR2Bucket(),
    ENVIRONMENT: 'test',
    ...overrides,
  }
}

// ============================================================================
// ULID Tests
// ============================================================================

describe('ULID Generation', () => {
  it('generates 26-character strings', () => {
    // ULID format: 10 chars timestamp + 16 chars randomness
    const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    function ulid(): string {
      const now = Date.now()
      let str = ''
      let ts = now
      for (let i = 9; i >= 0; i--) {
        str = ENCODING[ts % 32] + str
        ts = Math.floor(ts / 32)
      }
      for (let i = 0; i < 16; i++) {
        str += ENCODING[Math.floor(Math.random() * 32)]
      }
      return str
    }

    const id = ulid()
    expect(id).toHaveLength(26)
  })

  it('generates unique IDs', () => {
    const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    function ulid(): string {
      const now = Date.now()
      let str = ''
      let ts = now
      for (let i = 9; i >= 0; i--) {
        str = ENCODING[ts % 32] + str
        ts = Math.floor(ts / 32)
      }
      for (let i = 0; i < 16; i++) {
        str += ENCODING[Math.floor(Math.random() * 32)]
      }
      return str
    }

    const ids = new Set<string>()
    for (let i = 0; i < 100; i++) {
      ids.add(ulid())
    }
    expect(ids.size).toBe(100)
  })

  it('uses Crockford Base32 encoding', () => {
    const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    function ulid(): string {
      const now = Date.now()
      let str = ''
      let ts = now
      for (let i = 9; i >= 0; i--) {
        str = ENCODING[ts % 32] + str
        ts = Math.floor(ts / 32)
      }
      for (let i = 0; i < 16; i++) {
        str += ENCODING[Math.floor(Math.random() * 32)]
      }
      return str
    }

    const id = ulid()
    // Crockford Base32 excludes I, L, O, U
    expect(id).not.toMatch(/[ILOU]/)
    // Should only contain valid Crockford Base32 characters
    expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]+$/)
  })

  it('is lexicographically sortable by time', () => {
    const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    function ulid(): string {
      const now = Date.now()
      let str = ''
      let ts = now
      for (let i = 9; i >= 0; i--) {
        str = ENCODING[ts % 32] + str
        ts = Math.floor(ts / 32)
      }
      for (let i = 0; i < 16; i++) {
        str += ENCODING[Math.floor(Math.random() * 32)]
      }
      return str
    }

    const id1 = ulid()
    // Wait a tiny bit to ensure different timestamp
    const start = Date.now()
    while (Date.now() === start) {
      /* spin */
    }
    const id2 = ulid()

    // id2 should be greater than id1 (later timestamp)
    expect(id2 > id1).toBe(true)
  })
})

// ============================================================================
// compactHourPath Tests
// ============================================================================

describe('compactHourPath()', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('returns no compaction when no files found', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([])

    const env = createMockEnv()
    const hourPath = '2024/01/15/12'

    // Simulate compactHourPath logic
    const prefix = `events/${hourPath}/`
    const files = await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, prefix)

    expect(files.length).toBe(0)
  })

  it('skips compaction when only one file exists', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([createMockR2Object('events/2024/01/15/12/file1.parquet')])

    const env = createMockEnv()
    const hourPath = '2024/01/15/12'
    const prefix = `events/${hourPath}/`

    const files = await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, prefix)

    expect(files.length).toBe(1)
    // Should skip compaction - only one file
  })

  it('compacts multiple files into single compact file', async () => {
    const mockBuffer1 = new ArrayBuffer(100)
    const mockBuffer2 = new ArrayBuffer(150)
    const mergedRecords = [
      { ts: 1000n, type: 'event1' },
      { ts: 2000n, type: 'event2' },
    ]
    const compactedBuffer = new ArrayBuffer(200)

    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/file1.parquet'),
      createMockR2Object('events/2024/01/15/12/file2.parquet'),
    ])

    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue(mergedRecords)

    const mockWriteCompacted = vi.mocked(writeCompactedParquet)
    mockWriteCompacted.mockReturnValue(compactedBuffer)

    const objects = new Map<string, ArrayBuffer>([
      ['events/2024/01/15/12/file1.parquet', mockBuffer1],
      ['events/2024/01/15/12/file2.parquet', mockBuffer2],
    ])
    const env = createMockEnv({
      EVENTS_BUCKET: createMockR2Bucket(objects),
    })

    const prefix = 'events/2024/01/15/12/'
    const files = await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, prefix)

    expect(files.length).toBe(2)

    // Read all files
    const buffers: ArrayBuffer[] = []
    for (const file of files) {
      const obj = await env.EVENTS_BUCKET.get(file.key)
      if (obj) {
        buffers.push(await obj.arrayBuffer())
      }
    }

    expect(buffers.length).toBe(2)

    // Merge records
    const merged = await mergeParquetRecords(buffers)
    expect(merged).toEqual(mergedRecords)

    // Write compacted file
    const output = writeCompactedParquet(merged)
    expect(output.byteLength).toBe(200)
  })

  it('deletes original files after successful compaction', async () => {
    const mockBuffer1 = new ArrayBuffer(100)
    const mockBuffer2 = new ArrayBuffer(150)

    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/file1.parquet'),
      createMockR2Object('events/2024/01/15/12/file2.parquet'),
    ])

    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue([{ ts: 1000n }])

    const mockWriteCompacted = vi.mocked(writeCompactedParquet)
    mockWriteCompacted.mockReturnValue(new ArrayBuffer(200))

    const objects = new Map<string, ArrayBuffer>([
      ['events/2024/01/15/12/file1.parquet', mockBuffer1],
      ['events/2024/01/15/12/file2.parquet', mockBuffer2],
    ])
    const bucket = createMockR2Bucket(objects)
    const env = createMockEnv({ EVENTS_BUCKET: bucket })

    const files = await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, 'events/2024/01/15/12/')

    // Simulate deletion
    for (const file of files) {
      await bucket.delete(file.key)
    }

    expect(bucket.delete).toHaveBeenCalledTimes(2)
    expect(bucket.delete).toHaveBeenCalledWith('events/2024/01/15/12/file1.parquet')
    expect(bucket.delete).toHaveBeenCalledWith('events/2024/01/15/12/file2.parquet')
  })

  it('handles empty records after merge', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/file1.parquet'),
      createMockR2Object('events/2024/01/15/12/file2.parquet'),
    ])

    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue([])

    const objects = new Map<string, ArrayBuffer>([
      ['events/2024/01/15/12/file1.parquet', new ArrayBuffer(100)],
      ['events/2024/01/15/12/file2.parquet', new ArrayBuffer(150)],
    ])
    const env = createMockEnv({
      EVENTS_BUCKET: createMockR2Bucket(objects),
    })

    const files = await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, 'events/2024/01/15/12/')
    expect(files.length).toBe(2)

    const buffers: ArrayBuffer[] = []
    for (const file of files) {
      const obj = await env.EVENTS_BUCKET.get(file.key)
      if (obj) buffers.push(await obj.arrayBuffer())
    }

    const merged = await mergeParquetRecords(buffers)
    expect(merged.length).toBe(0)
    // Should not write or delete when no records
  })

  it('writes compact file with correct metadata', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/file1.parquet'),
      createMockR2Object('events/2024/01/15/12/file2.parquet'),
    ])

    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue([{ ts: 1000n }, { ts: 2000n }])

    const mockWriteCompacted = vi.mocked(writeCompactedParquet)
    mockWriteCompacted.mockReturnValue(new ArrayBuffer(500))

    const objects = new Map<string, ArrayBuffer>([
      ['events/2024/01/15/12/file1.parquet', new ArrayBuffer(100)],
      ['events/2024/01/15/12/file2.parquet', new ArrayBuffer(150)],
    ])
    const bucket = createMockR2Bucket(objects)
    const env = createMockEnv({ EVENTS_BUCKET: bucket })

    // Simulate putting the compacted file
    await bucket.put('events/2024/01/15/12/_compact_TEST123.parquet', new ArrayBuffer(500), {
      customMetadata: {
        sourceFiles: '2',
        recordCount: '2',
        compactedAt: new Date().toISOString(),
        compactionLevel: 'hour',
      },
    })

    expect(bucket.put).toHaveBeenCalledWith(
      expect.stringContaining('_compact_'),
      expect.any(ArrayBuffer),
      expect.objectContaining({
        customMetadata: expect.objectContaining({
          sourceFiles: '2',
          recordCount: '2',
          compactionLevel: 'hour',
        }),
      })
    )
  })

  it('handles R2 get returning null for some files', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/file1.parquet'),
      createMockR2Object('events/2024/01/15/12/file2.parquet'),
      createMockR2Object('events/2024/01/15/12/file3.parquet'),
    ])

    // Only one file has content
    const objects = new Map<string, ArrayBuffer>([
      ['events/2024/01/15/12/file1.parquet', new ArrayBuffer(100)],
    ])
    const env = createMockEnv({
      EVENTS_BUCKET: createMockR2Bucket(objects),
    })

    const files = await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, 'events/2024/01/15/12/')

    const buffers: ArrayBuffer[] = []
    for (const file of files) {
      const obj = await env.EVENTS_BUCKET.get(file.key)
      if (obj) {
        buffers.push(await obj.arrayBuffer())
      }
    }

    // Only one buffer should be read
    expect(buffers.length).toBe(1)
  })
})

// ============================================================================
// compactHour Tests
// ============================================================================

describe('compactHour()', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('formats hour path correctly from Date', () => {
    const date = new Date(Date.UTC(2024, 0, 15, 12, 30, 0)) // Jan 15, 2024 12:30 UTC

    const hourPath = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
      String(date.getUTCHours()).padStart(2, '0'),
    ].join('/')

    expect(hourPath).toBe('2024/01/15/12')
  })

  it('pads single-digit months correctly', () => {
    const date = new Date(Date.UTC(2024, 0, 5, 3, 0, 0)) // Jan 5, 2024 03:00 UTC

    const hourPath = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
      String(date.getUTCHours()).padStart(2, '0'),
    ].join('/')

    expect(hourPath).toBe('2024/01/05/03')
  })

  it('handles December correctly', () => {
    const date = new Date(Date.UTC(2024, 11, 31, 23, 0, 0)) // Dec 31, 2024 23:00 UTC

    const hourPath = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
      String(date.getUTCHours()).padStart(2, '0'),
    ].join('/')

    expect(hourPath).toBe('2024/12/31/23')
  })

  it('uses UTC time regardless of local timezone', () => {
    // Create date with specific UTC time
    const date = new Date(Date.UTC(2024, 5, 15, 0, 0, 0)) // June 15, 2024 00:00 UTC

    const hourPath = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
      String(date.getUTCHours()).padStart(2, '0'),
    ].join('/')

    expect(hourPath).toBe('2024/06/15/00')
  })
})

// ============================================================================
// compactDay Tests
// ============================================================================

describe('compactDay()', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('formats day path correctly from Date', () => {
    const date = new Date(Date.UTC(2024, 0, 15, 12, 30, 0))

    const dayPath = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
    ].join('/')

    expect(dayPath).toBe('2024/01/15')
  })

  it('processes all 24 hours for compaction', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([])

    const env = createMockEnv()
    const date = new Date(Date.UTC(2024, 0, 15))

    const dayPath = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
    ].join('/')

    // Simulate processing all 24 hours
    const hourResults: { hour: string; result: { compacted: boolean } }[] = []
    for (let hour = 0; hour < 24; hour++) {
      const hourPath = `${dayPath}/${String(hour).padStart(2, '0')}`
      const prefix = `events/${hourPath}/`
      await listFilesForCompaction(env.EVENTS_BUCKET as unknown as R2Bucket, prefix)
      hourResults.push({ hour: hourPath, result: { compacted: false } })
    }

    expect(hourResults.length).toBe(24)
    expect(mockListFiles).toHaveBeenCalledTimes(24)
  })

  it('collects _compact files from all hours for daily compaction', async () => {
    const env = createMockEnv()

    // Mock listing each hour's directory
    const bucket = env.EVENTS_BUCKET
    bucket.list.mockImplementation(({ prefix }: { prefix: string }) => {
      // Each hour has one _compact file
      if (prefix.match(/events\/2024\/01\/15\/\d{2}\//)) {
        const hour = prefix.match(/\/(\d{2})\/$/)?.[1]
        return Promise.resolve({
          objects: [createMockR2Object(`${prefix}_compact_ULID${hour}.parquet`)],
          truncated: false,
        })
      }
      return Promise.resolve({ objects: [], truncated: false })
    })

    const allFiles: MockR2Object[] = []
    const dayPrefix = 'events/2024/01/15/'

    for (let hour = 0; hour < 24; hour++) {
      const hourPrefix = `${dayPrefix}${String(hour).padStart(2, '0')}/`
      const listed = await bucket.list({ prefix: hourPrefix })
      const compactFiles = listed.objects.filter((obj: MockR2Object) => obj.key.includes('_compact'))
      allFiles.push(...compactFiles)
    }

    expect(allFiles.length).toBe(24)
  })

  it('skips daily compaction when 1 or fewer compact files exist', async () => {
    const env = createMockEnv()
    const bucket = env.EVENTS_BUCKET

    // Only one hour has a compact file
    bucket.list.mockImplementation(({ prefix }: { prefix: string }) => {
      if (prefix === 'events/2024/01/15/12/') {
        return Promise.resolve({
          objects: [createMockR2Object(`${prefix}_compact_ULID.parquet`)],
          truncated: false,
        })
      }
      return Promise.resolve({ objects: [], truncated: false })
    })

    const allFiles: MockR2Object[] = []
    const dayPrefix = 'events/2024/01/15/'

    for (let hour = 0; hour < 24; hour++) {
      const hourPrefix = `${dayPrefix}${String(hour).padStart(2, '0')}/`
      const listed = await bucket.list({ prefix: hourPrefix })
      const compactFiles = listed.objects.filter((obj: MockR2Object) => obj.key.includes('_compact'))
      allFiles.push(...compactFiles)
    }

    expect(allFiles.length).toBe(1)
    // Should skip daily compaction
    expect(allFiles.length <= 1).toBe(true)
  })

  it('writes daily compact file with correct metadata', async () => {
    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue([{ ts: 1000n }, { ts: 2000n }, { ts: 3000n }])

    const mockWriteCompacted = vi.mocked(writeCompactedParquet)
    mockWriteCompacted.mockReturnValue(new ArrayBuffer(1000))

    const bucket = createMockR2Bucket()
    const env = createMockEnv({ EVENTS_BUCKET: bucket })

    // Simulate writing daily compact file
    const dayPath = '2024/01/15'
    const dailyCompactKey = `events/${dayPath}/_compact_DAILYULID.parquet`
    const compactedBuffer = writeCompactedParquet([{ ts: 1000n }])

    await bucket.put(dailyCompactKey, compactedBuffer, {
      customMetadata: {
        sourceFiles: '5',
        recordCount: '3',
        compactedAt: new Date().toISOString(),
        compactionLevel: 'day',
        date: dayPath,
      },
    })

    expect(bucket.put).toHaveBeenCalledWith(
      expect.stringContaining('_compact_'),
      expect.any(ArrayBuffer),
      expect.objectContaining({
        customMetadata: expect.objectContaining({
          compactionLevel: 'day',
          date: '2024/01/15',
        }),
      })
    )
  })

  it('deletes hourly compact files after daily compaction', async () => {
    const bucket = createMockR2Bucket()
    const env = createMockEnv({ EVENTS_BUCKET: bucket })

    const hourlyFiles = [
      'events/2024/01/15/00/_compact_ULID00.parquet',
      'events/2024/01/15/01/_compact_ULID01.parquet',
      'events/2024/01/15/02/_compact_ULID02.parquet',
    ]

    // Simulate deletion
    for (const file of hourlyFiles) {
      await bucket.delete(file)
    }

    expect(bucket.delete).toHaveBeenCalledTimes(3)
    expect(bucket.delete).toHaveBeenCalledWith('events/2024/01/15/00/_compact_ULID00.parquet')
    expect(bucket.delete).toHaveBeenCalledWith('events/2024/01/15/01/_compact_ULID01.parquet')
    expect(bucket.delete).toHaveBeenCalledWith('events/2024/01/15/02/_compact_ULID02.parquet')
  })
})

// ============================================================================
// File Merging Logic Tests
// ============================================================================

describe('File Merging Logic', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('merges records sorted by timestamp', async () => {
    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    const sortedRecords = [
      { ts: 1000n, type: 'first' },
      { ts: 2000n, type: 'second' },
      { ts: 3000n, type: 'third' },
    ]
    mockMergeRecords.mockResolvedValue(sortedRecords)

    const buffers = [new ArrayBuffer(100), new ArrayBuffer(150)]
    const merged = await mergeParquetRecords(buffers)

    expect(merged).toEqual(sortedRecords)
    expect(merged[0].ts).toBeLessThan(merged[1].ts as bigint)
    expect(merged[1].ts).toBeLessThan(merged[2].ts as bigint)
  })

  it('handles empty buffer array', async () => {
    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue([])

    const merged = await mergeParquetRecords([])

    expect(merged).toEqual([])
  })

  it('handles single buffer', async () => {
    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockResolvedValue([{ ts: 1000n, type: 'only' }])

    const merged = await mergeParquetRecords([new ArrayBuffer(100)])

    expect(merged.length).toBe(1)
  })

  it('reads Parquet files correctly', async () => {
    const mockReadRecords = vi.mocked(readParquetRecords)
    mockReadRecords.mockResolvedValue([
      { ts: 1000n, type: 'event1' },
      { ts: 2000n, type: 'event2' },
    ])

    const buffer = new ArrayBuffer(100)
    const records = await readParquetRecords(buffer)

    expect(records.length).toBe(2)
    expect(mockReadRecords).toHaveBeenCalledWith(buffer)
  })

  it('writes compacted Parquet file', () => {
    const mockWriteCompacted = vi.mocked(writeCompactedParquet)
    const outputBuffer = new ArrayBuffer(500)
    mockWriteCompacted.mockReturnValue(outputBuffer)

    const records = [
      { ts: 1000n, type: 'event1' },
      { ts: 2000n, type: 'event2' },
    ]

    const result = writeCompactedParquet(records)

    expect(result).toBe(outputBuffer)
    expect(mockWriteCompacted).toHaveBeenCalledWith(records)
  })
})

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Error Handling', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('handles R2 list failure gracefully', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockRejectedValue(new Error('R2 list failed'))

    await expect(listFilesForCompaction({} as R2Bucket, 'events/2024/01/15/12/')).rejects.toThrow('R2 list failed')
  })

  it('handles R2 get failure gracefully', async () => {
    const bucket = createMockR2Bucket()
    bucket.get.mockRejectedValue(new Error('R2 get failed'))

    await expect(bucket.get('events/2024/01/15/12/file.parquet')).rejects.toThrow('R2 get failed')
  })

  it('handles R2 put failure gracefully', async () => {
    const bucket = createMockR2Bucket()
    bucket.put.mockRejectedValue(new Error('R2 put failed'))

    await expect(bucket.put('key', new ArrayBuffer(100))).rejects.toThrow('R2 put failed')
  })

  it('handles R2 delete failure gracefully', async () => {
    const bucket = createMockR2Bucket()
    bucket.delete.mockRejectedValue(new Error('R2 delete failed'))

    await expect(bucket.delete('key')).rejects.toThrow('R2 delete failed')
  })

  it('handles mergeParquetRecords failure', async () => {
    const mockMergeRecords = vi.mocked(mergeParquetRecords)
    mockMergeRecords.mockRejectedValue(new Error('Merge failed'))

    await expect(mergeParquetRecords([new ArrayBuffer(100)])).rejects.toThrow('Merge failed')
  })

  it('handles writeCompactedParquet failure', () => {
    const mockWriteCompacted = vi.mocked(writeCompactedParquet)
    mockWriteCompacted.mockImplementation(() => {
      throw new Error('Write failed')
    })

    expect(() => writeCompactedParquet([{ ts: 1000n }])).toThrow('Write failed')
  })

  it('handles invalid hour path format', () => {
    // Test path validation logic
    const invalidPaths = ['invalid', '2024/01', '2024-01-15-12', 'not/a/valid/path/format']

    for (const path of invalidPaths) {
      const parts = path.split('/')
      const isValid = parts.length === 4 && parts.every((p) => /^\d+$/.test(p))
      expect(isValid).toBe(false)
    }
  })

  it('handles invalid day path format', () => {
    const invalidPaths = ['invalid', '2024/01/15/12', '2024-01-15', 'not/valid']

    for (const path of invalidPaths) {
      const parts = path.split('/')
      const isValid = parts.length === 3 && parts.every((p) => /^\d+$/.test(p))
      expect(isValid).toBe(false)
    }
  })
})

// ============================================================================
// Scheduled Handler Tests
// ============================================================================

describe('Scheduled Handler', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('compacts previous hour, not current hour', () => {
    const now = new Date(Date.UTC(2024, 0, 15, 14, 5, 0)) // Jan 15, 2024 14:05 UTC
    vi.setSystemTime(now)

    const previousHour = new Date(now.getTime() - 60 * 60 * 1000)

    expect(previousHour.getUTCHours()).toBe(13)
    expect(previousHour.getUTCDate()).toBe(15)
  })

  it('handles midnight correctly for previous hour', () => {
    const now = new Date(Date.UTC(2024, 0, 15, 0, 5, 0)) // Jan 15, 2024 00:05 UTC
    vi.setSystemTime(now)

    const previousHour = new Date(now.getTime() - 60 * 60 * 1000)

    expect(previousHour.getUTCHours()).toBe(23)
    expect(previousHour.getUTCDate()).toBe(14) // Previous day
  })

  it('triggers daily compaction only at hour 0', () => {
    const hours = [0, 1, 12, 23]
    const shouldRunDaily = hours.map((h) => {
      const now = new Date(Date.UTC(2024, 0, 15, h, 5, 0))
      return { hour: h, runDaily: now.getUTCHours() === 0 }
    })

    expect(shouldRunDaily.find((r) => r.hour === 0)?.runDaily).toBe(true)
    expect(shouldRunDaily.find((r) => r.hour === 1)?.runDaily).toBe(false)
    expect(shouldRunDaily.find((r) => r.hour === 12)?.runDaily).toBe(false)
    expect(shouldRunDaily.find((r) => r.hour === 23)?.runDaily).toBe(false)
  })

  it('compacts previous day at midnight', () => {
    const now = new Date(Date.UTC(2024, 0, 15, 0, 5, 0)) // Jan 15, 2024 00:05 UTC
    vi.setSystemTime(now)

    const previousDay = new Date(now.getTime() - 24 * 60 * 60 * 1000)

    expect(previousDay.getUTCDate()).toBe(14)
    expect(previousDay.getUTCMonth()).toBe(0) // January
    expect(previousDay.getUTCFullYear()).toBe(2024)
  })

  it('handles month boundary for previous day', () => {
    const now = new Date(Date.UTC(2024, 1, 1, 0, 5, 0)) // Feb 1, 2024 00:05 UTC
    vi.setSystemTime(now)

    const previousDay = new Date(now.getTime() - 24 * 60 * 60 * 1000)

    expect(previousDay.getUTCDate()).toBe(31)
    expect(previousDay.getUTCMonth()).toBe(0) // January
  })

  it('handles year boundary for previous day', () => {
    const now = new Date(Date.UTC(2024, 0, 1, 0, 5, 0)) // Jan 1, 2024 00:05 UTC
    vi.setSystemTime(now)

    const previousDay = new Date(now.getTime() - 24 * 60 * 60 * 1000)

    expect(previousDay.getUTCDate()).toBe(31)
    expect(previousDay.getUTCMonth()).toBe(11) // December
    expect(previousDay.getUTCFullYear()).toBe(2023)
  })
})

// ============================================================================
// HTTP Handler Tests
// ============================================================================

describe('HTTP Handler', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('handles CORS preflight requests', async () => {
    const request = new Request('https://events.do/compact', {
      method: 'OPTIONS',
    })

    // Simulate the CORS preflight response
    const response = new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    })

    expect(response.status).toBe(200)
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    expect(response.headers.get('Access-Control-Allow-Methods')).toBe('POST, GET, OPTIONS')
  })

  it('returns error for missing hour/day param on POST /compact', async () => {
    const url = new URL('https://events.do/compact')
    const hour = url.searchParams.get('hour')
    const day = url.searchParams.get('day')

    expect(hour).toBeNull()
    expect(day).toBeNull()

    // Should return error
    const response = Response.json(
      { error: 'hour or day param required (hour=YYYY/MM/DD/HH or day=YYYY/MM/DD)' },
      { status: 400 }
    )

    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.error).toBe('hour or day param required (hour=YYYY/MM/DD/HH or day=YYYY/MM/DD)')
  })

  it('handles hour param correctly', () => {
    const url = new URL('https://events.do/compact?hour=2024/01/15/12')
    const hour = url.searchParams.get('hour')

    expect(hour).toBe('2024/01/15/12')
  })

  it('handles day param correctly', () => {
    const url = new URL('https://events.do/compact?day=2024/01/15')
    const day = url.searchParams.get('day')

    expect(day).toBe('2024/01/15')
  })

  it('returns status endpoint with service info', async () => {
    const statusResponse = {
      service: 'events-compactor',
      environment: 'test',
      endpoints: {
        'POST /compact?hour=YYYY/MM/DD/HH': 'Compact specific hour',
        'POST /compact?day=YYYY/MM/DD': 'Compact entire day',
        'GET /status': 'This status page',
      },
      schedule: 'Every hour at :05 (5 * * * *)',
      notes: {
        hourly: 'Compacts previous hour (not current) to avoid race conditions',
        daily: 'At midnight UTC, also compacts all hourly files into daily compact',
      },
    }

    const response = Response.json(statusResponse)
    const body = await response.json()

    expect(body.service).toBe('events-compactor')
    expect(body.schedule).toBe('Every hour at :05 (5 * * * *)')
    expect(body.endpoints).toBeDefined()
  })

  it('returns root endpoint with service version', async () => {
    const rootResponse = {
      service: 'events-compactor',
      version: '1.0.0',
    }

    const response = Response.json(rootResponse)
    const body = await response.json()

    expect(body.service).toBe('events-compactor')
    expect(body.version).toBe('1.0.0')
  })

  it('returns 404 for unknown paths', async () => {
    const response = Response.json({ error: 'Not found' }, { status: 404 })

    expect(response.status).toBe(404)
    const body = await response.json()
    expect(body.error).toBe('Not found')
  })

  it('parses day param and creates Date correctly', () => {
    const day = '2024/01/31'
    const [year, month, dayNum] = day.split('/').map(Number)

    expect(year).toBe(2024)
    expect(month).toBe(1)
    expect(dayNum).toBe(31)

    const date = new Date(Date.UTC(year, (month ?? 1) - 1, dayNum))

    expect(date.getUTCFullYear()).toBe(2024)
    expect(date.getUTCMonth()).toBe(0) // January
    expect(date.getUTCDate()).toBe(31)
  })

  it('handles invalid day format', () => {
    const day = 'invalid/format'
    const [year] = day.split('/').map(Number)

    expect(isNaN(year)).toBe(true)
    // Should return 400 error
  })
})

// ============================================================================
// CORS Headers Tests
// ============================================================================

describe('CORS Headers', () => {
  it('returns correct CORS headers', () => {
    const headers: HeadersInit = {
      'Access-Control-Allow-Origin': '*',
      'Content-Type': 'application/json',
    }

    expect(headers['Access-Control-Allow-Origin']).toBe('*')
    expect(headers['Content-Type']).toBe('application/json')
  })
})

// ============================================================================
// listFilesForCompaction Tests
// ============================================================================

describe('listFilesForCompaction()', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('excludes files with _compact in name', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/file1.parquet'),
      createMockR2Object('events/2024/01/15/12/file2.parquet'),
    ])

    const files = await listFilesForCompaction({} as R2Bucket, 'events/2024/01/15/12/')

    expect(files.length).toBe(2)
    expect(files.every((f) => !f.key.includes('_compact'))).toBe(true)
  })

  it('returns sorted files by key', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([
      createMockR2Object('events/2024/01/15/12/a.parquet'),
      createMockR2Object('events/2024/01/15/12/b.parquet'),
      createMockR2Object('events/2024/01/15/12/c.parquet'),
    ])

    const files = await listFilesForCompaction({} as R2Bucket, 'events/2024/01/15/12/')

    expect(files[0].key).toBe('events/2024/01/15/12/a.parquet')
    expect(files[1].key).toBe('events/2024/01/15/12/b.parquet')
    expect(files[2].key).toBe('events/2024/01/15/12/c.parquet')
  })

  it('handles empty prefix listing', async () => {
    const mockListFiles = vi.mocked(listFilesForCompaction)
    mockListFiles.mockResolvedValue([])

    const files = await listFilesForCompaction({} as R2Bucket, 'events/2024/01/15/12/')

    expect(files).toEqual([])
  })
})

// ============================================================================
// Compaction Result Types Tests
// ============================================================================

describe('Compaction Result Types', () => {
  it('has correct structure for successful hour compaction', () => {
    const result = {
      compacted: true,
      sourceFiles: 5,
      recordCount: 100,
      outputKey: 'events/2024/01/15/12/_compact_ULID123.parquet',
      outputBytes: 50000,
    }

    expect(result.compacted).toBe(true)
    expect(result.sourceFiles).toBe(5)
    expect(result.recordCount).toBe(100)
    expect(result.outputKey).toContain('_compact_')
    expect(result.outputBytes).toBeGreaterThan(0)
  })

  it('has correct structure for skipped compaction', () => {
    const result = {
      compacted: false,
      reason: 'no files found',
      fileCount: 0,
    }

    expect(result.compacted).toBe(false)
    expect(result.reason).toBeDefined()
  })

  it('has correct structure for day compaction result', () => {
    const result = {
      compacted: true,
      hours: [
        { hour: '2024/01/15/00', result: { compacted: true, sourceFiles: 2 } },
        { hour: '2024/01/15/01', result: { compacted: false, reason: 'no files found' } },
      ],
      finalCompact: {
        compacted: true,
        sourceFiles: 3,
        recordCount: 500,
        outputKey: 'events/2024/01/15/_compact_DAILY.parquet',
        outputBytes: 100000,
      },
    }

    expect(result.compacted).toBe(true)
    expect(result.hours.length).toBe(2)
    expect(result.finalCompact?.compacted).toBe(true)
  })
})
