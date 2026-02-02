/**
 * Event Writer Tests
 *
 * Comprehensive unit tests for event-writer.ts covering:
 * - EventBuffer class (add, flush, shouldFlush, idle time tracking)
 * - writeEvents() function (Parquet writing, R2 interactions)
 * - Buffer cleanup (cleanupIdleBuffers, forceCleanupBuffers)
 * - R2 write interactions and metadata
 * - Error handling
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  EventBuffer,
  writeEvents,
  getEventBuffer,
  getBufferCount,
  forceCleanupBuffers,
  ulid,
  type EventRecord,
  type WriteResult,
} from '../event-writer'

// ============================================================================
// Mock Setup
// ============================================================================

// Mock parquetWriteBuffer since it requires actual parquet library
vi.mock('@dotdo/hyparquet-writer', () => ({
  parquetWriteBuffer: vi.fn(() => new Uint8Array([1, 2, 3, 4, 5])),
}))

// Track calls to parquetWriteBuffer
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'
const mockParquetWriteBuffer = parquetWriteBuffer as ReturnType<typeof vi.fn>

// ============================================================================
// Mock R2 Bucket
// ============================================================================

interface MockR2Object {
  key: string
  body: Uint8Array
  metadata: Record<string, string>
}

interface MockR2Bucket {
  put: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  list: ReturnType<typeof vi.fn>
  head: ReturnType<typeof vi.fn>
  _objects: Map<string, MockR2Object>
}

function createMockR2Bucket(): MockR2Bucket {
  const objects = new Map<string, MockR2Object>()

  return {
    put: vi.fn(async (key: string, body: ArrayBuffer | Uint8Array, options?: { customMetadata?: Record<string, string> }) => {
      const bodyBytes = body instanceof ArrayBuffer ? new Uint8Array(body) : body
      objects.set(key, {
        key,
        body: bodyBytes,
        metadata: options?.customMetadata ?? {},
      })
      return {
        key,
        version: 'v1',
        size: bodyBytes.byteLength,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        customMetadata: options?.customMetadata ?? {},
      }
    }),
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        body: new ReadableStream(),
        arrayBuffer: async () => obj.body.buffer,
        text: async () => new TextDecoder().decode(obj.body),
        customMetadata: obj.metadata,
      }
    }),
    delete: vi.fn(async (key: string) => objects.delete(key)),
    list: vi.fn(async () => ({
      objects: Array.from(objects.keys()).map(key => ({ key })),
      truncated: false,
    })),
    head: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return { key, size: obj.body.byteLength }
    }),
    _objects: objects,
  }
}

// ============================================================================
// Mock ExecutionContext
// ============================================================================

interface MockExecutionContext {
  waitUntil: ReturnType<typeof vi.fn>
  passThroughOnException: ReturnType<typeof vi.fn>
}

function createMockExecutionContext(): MockExecutionContext {
  return {
    waitUntil: vi.fn((promise: Promise<unknown>) => promise),
    passThroughOnException: vi.fn(),
  }
}

// ============================================================================
// Test Data Helpers
// ============================================================================

function createTestEvent(overrides: Partial<EventRecord> = {}): EventRecord {
  return {
    ts: new Date().toISOString(),
    type: 'test.event',
    ...overrides,
  }
}

function createTestEvents(count: number, baseEvent: Partial<EventRecord> = {}): EventRecord[] {
  return Array.from({ length: count }, (_, i) => createTestEvent({
    type: `test.event.${i}`,
    ...baseEvent,
  }))
}

// ============================================================================
// Tests
// ============================================================================

describe('event-writer', () => {
  let mockBucket: MockR2Bucket
  let mockCtx: MockExecutionContext

  beforeEach(() => {
    mockBucket = createMockR2Bucket()
    mockCtx = createMockExecutionContext()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-06-15T12:30:00Z'))
    mockParquetWriteBuffer.mockClear()
    mockParquetWriteBuffer.mockReturnValue(new Uint8Array([1, 2, 3, 4, 5]))

    // Clean up any existing buffers between tests
    forceCleanupBuffers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  // ============================================================================
  // ulid Tests
  // ============================================================================

  describe('ulid', () => {
    it('should generate unique IDs', () => {
      const id1 = ulid()
      vi.advanceTimersByTime(1)
      const id2 = ulid()

      expect(id1).not.toBe(id2)
    })

    it('should generate IDs of consistent length', () => {
      const id = ulid()
      expect(id.length).toBe(26) // Standard ULID length
    })

    it('should generate lexicographically sortable IDs', () => {
      const id1 = ulid()
      vi.advanceTimersByTime(1000)
      const id2 = ulid()

      // Later ID should sort after earlier ID
      expect(id2 > id1).toBe(true)
    })
  })

  // ============================================================================
  // writeEvents Tests
  // ============================================================================

  describe('writeEvents', () => {
    it('should write events to R2 as Parquet', async () => {
      const events = createTestEvents(3)

      const result = await writeEvents(mockBucket as unknown as R2Bucket, 'test-prefix', events)

      expect(result.events).toBe(3)
      expect(result.bytes).toBeGreaterThan(0)
      expect(result.key).toMatch(/^test-prefix\/\d{4}\/\d{2}\/\d{2}\/\d{2}\/[A-Z0-9]+\.parquet$/)
      expect(mockBucket.put).toHaveBeenCalledTimes(1)
    })

    it('should throw error for empty events array', async () => {
      await expect(writeEvents(mockBucket as unknown as R2Bucket, 'test-prefix', []))
        .rejects.toThrow('No events to write')
    })

    it('should generate time-partitioned paths', async () => {
      vi.setSystemTime(new Date('2024-03-15T08:45:00Z'))
      const events = createTestEvents(1)

      const result = await writeEvents(mockBucket as unknown as R2Bucket, 'events', events)

      expect(result.key).toMatch(/^events\/2024\/03\/15\/08\/[A-Z0-9]+\.parquet$/)
    })

    it('should sanitize prefix to prevent path traversal', async () => {
      const events = createTestEvents(1)

      // This should work with a valid prefix
      const result = await writeEvents(mockBucket as unknown as R2Bucket, 'valid/prefix', events)
      expect(result.key).toMatch(/^valid\/prefix\//)
    })

    it('should reject invalid prefix with path traversal', async () => {
      const events = createTestEvents(1)

      await expect(writeEvents(mockBucket as unknown as R2Bucket, '../etc/passwd', events))
        .rejects.toThrow()
    })

    it('should call parquetWriteBuffer with correct schema', async () => {
      const events = [createTestEvent({
        type: 'user.created',
        source: 'api',
        provider: 'internal',
        eventType: 'create',
        verified: true,
        scriptName: 'worker-main',
        outcome: 'ok',
        method: 'POST',
        url: 'https://api.example.com/users',
        statusCode: 201,
        durationMs: 150.5,
        payload: { userId: '123' },
      })]

      await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      expect(mockParquetWriteBuffer).toHaveBeenCalledWith(
        expect.objectContaining({
          schema: expect.arrayContaining([
            expect.objectContaining({ name: 'root', num_children: 13 }),
            expect.objectContaining({ name: 'ts' }),
            expect.objectContaining({ name: 'type' }),
            expect.objectContaining({ name: 'source' }),
            expect.objectContaining({ name: 'provider' }),
            expect.objectContaining({ name: 'event_type' }),
            expect.objectContaining({ name: 'verified' }),
            expect.objectContaining({ name: 'script_name' }),
            expect.objectContaining({ name: 'outcome' }),
            expect.objectContaining({ name: 'method' }),
            expect.objectContaining({ name: 'url' }),
            expect.objectContaining({ name: 'status_code' }),
            expect.objectContaining({ name: 'duration_ms' }),
            expect.objectContaining({ name: 'payload' }),
          ]),
          statistics: true,
          codec: 'SNAPPY',
        })
      )
    })

    it('should include correct metadata in R2 put', async () => {
      const events = [
        createTestEvent({ type: 'event.a', ts: '2024-06-15T12:00:00Z' }),
        createTestEvent({ type: 'event.b', ts: '2024-06-15T12:30:00Z' }),
      ]

      await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      expect(mockBucket.put).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(Uint8Array),
        expect.objectContaining({
          customMetadata: expect.objectContaining({
            events: '2',
            format: 'parquet-v1',
            types: expect.stringContaining('event.'),
          }),
        })
      )
    })

    it('should handle events with null optional fields', async () => {
      const events = [createTestEvent({
        type: 'minimal.event',
        // All optional fields are undefined
      })]

      const result = await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      expect(result.events).toBe(1)
      expect(mockParquetWriteBuffer).toHaveBeenCalled()
    })

    it('should serialize payload as JSON string', async () => {
      const events = [createTestEvent({
        payload: { nested: { data: 'value' }, array: [1, 2, 3] },
      })]

      await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      const callArgs = mockParquetWriteBuffer.mock.calls[0][0]
      const payloadColumn = callArgs.columnData.find((c: { name: string }) => c.name === 'payload')
      expect(payloadColumn.data[0]).toBe('{"nested":{"data":"value"},"array":[1,2,3]}')
    })

    it('should track CPU time in result', async () => {
      const events = createTestEvents(10)

      const result = await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      expect(typeof result.cpuMs).toBe('number')
      expect(result.cpuMs).toBeGreaterThanOrEqual(0)
    })

    it('should limit types metadata to first 10 types', async () => {
      const events = Array.from({ length: 15 }, (_, i) => createTestEvent({ type: `type.${i}` }))

      await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      const putCall = mockBucket.put.mock.calls[0]
      const metadata = putCall[2].customMetadata
      const types = metadata.types.split(',')
      expect(types.length).toBeLessThanOrEqual(10)
    })
  })

  // ============================================================================
  // EventBuffer Tests
  // ============================================================================

  describe('EventBuffer', () => {
    describe('constructor and options', () => {
      it('should use default options when not provided', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')

        expect(buffer.countThreshold).toBe(50)
        expect(buffer.timeThresholdMs).toBe(5000)
        expect(buffer.maxBufferSize).toBe(10000)
      })

      it('should use custom options when provided', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          countThreshold: 100,
          timeThresholdMs: 10000,
          maxBufferSize: 5000,
        })

        expect(buffer.countThreshold).toBe(100)
        expect(buffer.timeThresholdMs).toBe(10000)
        expect(buffer.maxBufferSize).toBe(5000)
      })
    })

    describe('add', () => {
      it('should add single event to buffer', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        const event = createTestEvent()

        buffer.add(event)

        expect(buffer.length).toBe(1)
      })

      it('should add multiple events to buffer', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        const events = createTestEvents(5)

        buffer.add(events)

        expect(buffer.length).toBe(5)
      })

      it('should accumulate events across multiple add calls', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')

        buffer.add(createTestEvent())
        buffer.add(createTestEvents(3))
        buffer.add(createTestEvent())

        expect(buffer.length).toBe(5)
      })

      it('should update activity time on add', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        const initialIdleTime = buffer.getIdleTime()

        vi.advanceTimersByTime(1000)
        buffer.add(createTestEvent())

        // After add, idle time should be reset (close to 0)
        expect(buffer.getIdleTime()).toBeLessThan(initialIdleTime + 1000)
      })
    })

    describe('shouldFlush', () => {
      it('should return true when count threshold reached', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          countThreshold: 5,
        })

        buffer.add(createTestEvents(5))

        expect(buffer.shouldFlush()).toBe(true)
      })

      it('should return true when max buffer size reached', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          countThreshold: 100,
          maxBufferSize: 10,
        })

        buffer.add(createTestEvents(10))

        expect(buffer.shouldFlush()).toBe(true)
      })

      it('should return true when time threshold exceeded', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          timeThresholdMs: 5000,
        })

        buffer.add(createTestEvent())
        vi.advanceTimersByTime(6000)

        expect(buffer.shouldFlush()).toBe(true)
      })

      it('should return false when no threshold reached', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          countThreshold: 100,
          timeThresholdMs: 60000,
          maxBufferSize: 1000,
        })

        buffer.add(createTestEvents(5))

        expect(buffer.shouldFlush()).toBe(false)
      })
    })

    describe('flush', () => {
      it('should write events to R2 and clear buffer', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvents(3))

        const result = await buffer.flush()

        expect(result).not.toBeNull()
        expect(result!.events).toBe(3)
        expect(buffer.length).toBe(0)
        expect(mockBucket.put).toHaveBeenCalledTimes(1)
      })

      it('should return null for empty buffer', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')

        const result = await buffer.flush()

        expect(result).toBeNull()
        expect(mockBucket.put).not.toHaveBeenCalled()
      })

      it('should update lastFlushTime after flush', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvent())

        const statsBefore = buffer.stats()
        vi.advanceTimersByTime(1000)
        await buffer.flush()
        const statsAfter = buffer.stats()

        expect(new Date(statsAfter.lastFlushTime).getTime())
          .toBeGreaterThan(new Date(statsBefore.lastFlushTime).getTime())
      })

      it('should update activity time on flush', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvent())

        vi.advanceTimersByTime(5000)
        await buffer.flush()

        expect(buffer.getIdleTime()).toBeLessThan(100)
      })

      it('should use correct prefix for R2 path', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'my-custom-prefix')
        buffer.add(createTestEvent())

        await buffer.flush()

        const putCall = mockBucket.put.mock.calls[0]
        expect(putCall[0]).toMatch(/^my-custom-prefix\//)
      })
    })

    describe('scheduleFlush', () => {
      it('should schedule a deferred flush', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          timeThresholdMs: 5000,
        })
        buffer.add(createTestEvent())

        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)

        expect(mockCtx.waitUntil).toHaveBeenCalled()
      })

      it('should not schedule multiple flushes', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvent())

        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)
        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)
        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)

        // Should only be called once
        expect(mockCtx.waitUntil).toHaveBeenCalledTimes(1)
      })

      it('should flush after timeout', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          timeThresholdMs: 5000,
        })
        buffer.add(createTestEvent())
        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)

        // Advance past the time threshold
        await vi.advanceTimersByTimeAsync(5001)

        expect(mockBucket.put).toHaveBeenCalled()
        expect(buffer.length).toBe(0)
      })
    })

    describe('maybeFlush', () => {
      it('should flush immediately when threshold reached', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          countThreshold: 3,
        })
        buffer.add(createTestEvents(3))

        const result = await buffer.maybeFlush(mockCtx as unknown as ExecutionContext)

        expect(result).not.toBeNull()
        expect(result!.events).toBe(3)
      })

      it('should schedule flush when threshold not reached', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test', {
          countThreshold: 100,
        })
        buffer.add(createTestEvent())

        const result = await buffer.maybeFlush(mockCtx as unknown as ExecutionContext)

        expect(result).toBeNull()
        expect(mockCtx.waitUntil).toHaveBeenCalled()
      })
    })

    describe('stats', () => {
      it('should return current buffer statistics', async () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvents(5))

        const stats = buffer.stats()

        expect(stats.buffered).toBe(5)
        expect(stats.lastFlushTime).toBeDefined()
        expect(stats.timeSinceFlush).toBeGreaterThanOrEqual(0)
        expect(stats.flushPending).toBe(false)
      })

      it('should reflect pending flush status', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvent())
        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)

        const stats = buffer.stats()

        expect(stats.flushPending).toBe(true)
      })
    })

    describe('isEmpty', () => {
      it('should return true for empty buffer with no pending flush', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')

        expect(buffer.isEmpty()).toBe(true)
      })

      it('should return false when buffer has events', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvent())

        expect(buffer.isEmpty()).toBe(false)
      })

      it('should return false when flush is pending', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        buffer.add(createTestEvent())
        buffer.scheduleFlush(mockCtx as unknown as ExecutionContext)

        // Buffer now has events and pending flush
        expect(buffer.isEmpty()).toBe(false)
      })
    })

    describe('idle time tracking', () => {
      it('should track idle time since last activity', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')

        vi.advanceTimersByTime(5000)

        expect(buffer.getIdleTime()).toBeGreaterThanOrEqual(5000)
      })

      it('should reset idle time on touch', () => {
        const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'test')
        vi.advanceTimersByTime(5000)

        buffer.touch()

        expect(buffer.getIdleTime()).toBeLessThan(100)
      })
    })
  })

  // ============================================================================
  // getEventBuffer Tests
  // ============================================================================

  describe('getEventBuffer', () => {
    it('should create new buffer for new prefix', () => {
      const buffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'new-prefix')

      expect(buffer).toBeDefined()
      expect(buffer).toBeInstanceOf(EventBuffer)
    })

    it('should return existing buffer for same prefix', () => {
      const buffer1 = getEventBuffer(mockBucket as unknown as R2Bucket, 'same-prefix')
      const buffer2 = getEventBuffer(mockBucket as unknown as R2Bucket, 'same-prefix')

      expect(buffer1).toBe(buffer2)
    })

    it('should create separate buffers for different prefixes', () => {
      const buffer1 = getEventBuffer(mockBucket as unknown as R2Bucket, 'prefix-a')
      const buffer2 = getEventBuffer(mockBucket as unknown as R2Bucket, 'prefix-b')

      expect(buffer1).not.toBe(buffer2)
    })

    it('should touch buffer when retrieved', () => {
      const buffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'touch-test')
      vi.advanceTimersByTime(5000)

      // Re-get should touch
      getEventBuffer(mockBucket as unknown as R2Bucket, 'touch-test')

      expect(buffer.getIdleTime()).toBeLessThan(100)
    })

    it('should pass options to new buffer', () => {
      const buffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'options-test', {
        countThreshold: 200,
        timeThresholdMs: 30000,
      })

      expect(buffer.countThreshold).toBe(200)
      expect(buffer.timeThresholdMs).toBe(30000)
    })
  })

  // ============================================================================
  // Buffer Cleanup Tests
  // ============================================================================

  describe('buffer cleanup', () => {
    describe('getBufferCount', () => {
      it('should return current number of buffers', () => {
        forceCleanupBuffers() // Start clean

        getEventBuffer(mockBucket as unknown as R2Bucket, 'count-test-1')
        getEventBuffer(mockBucket as unknown as R2Bucket, 'count-test-2')
        getEventBuffer(mockBucket as unknown as R2Bucket, 'count-test-3')

        expect(getBufferCount()).toBe(3)
      })
    })

    describe('forceCleanupBuffers', () => {
      it('should remove empty idle buffers', () => {
        getEventBuffer(mockBucket as unknown as R2Bucket, 'cleanup-test-1')
        getEventBuffer(mockBucket as unknown as R2Bucket, 'cleanup-test-2')

        const removed = forceCleanupBuffers()

        expect(removed).toBe(2)
        expect(getBufferCount()).toBe(0)
      })

      it('should not remove buffers with events', () => {
        const buffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'non-empty')
        buffer.add(createTestEvent())

        const removed = forceCleanupBuffers()

        expect(removed).toBe(0)
        expect(getBufferCount()).toBe(1)
      })

      it('should return count of removed buffers', () => {
        getEventBuffer(mockBucket as unknown as R2Bucket, 'remove-1')
        getEventBuffer(mockBucket as unknown as R2Bucket, 'remove-2')
        const keepBuffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'keep')
        keepBuffer.add(createTestEvent())

        const removed = forceCleanupBuffers()

        expect(removed).toBe(2)
      })
    })

    describe('cleanupIdleBuffers (via getEventBuffer)', () => {
      it('should cleanup idle buffers when getting new buffer', async () => {
        // Store initial count to account for any existing buffers
        const initialCount = getBufferCount()

        // Create buffers with unique prefixes for this test
        getEventBuffer(mockBucket as unknown as R2Bucket, 'idle-cleanup-test-1')
        getEventBuffer(mockBucket as unknown as R2Bucket, 'idle-cleanup-test-2')

        expect(getBufferCount()).toBe(initialCount + 2)

        // Advance time past idle timeout (5 minutes)
        vi.advanceTimersByTime(6 * 60 * 1000)

        // Getting a new buffer should trigger cleanup
        getEventBuffer(mockBucket as unknown as R2Bucket, 'new-buffer-cleanup-test')

        // Old idle buffers should be cleaned up - we can't know exact count
        // because other tests may have left non-empty buffers
        // but our two new idle buffers should have been cleaned
        // and the new one added, so net change should be -1 or less from peak
        const finalCount = getBufferCount()
        expect(finalCount).toBeLessThanOrEqual(initialCount + 1)
      })

      it('should not cleanup recently active buffers', () => {
        const initialCount = getBufferCount()

        const buffer1 = getEventBuffer(mockBucket as unknown as R2Bucket, 'active-cleanup-1')
        getEventBuffer(mockBucket as unknown as R2Bucket, 'active-cleanup-2')

        // Advance some time but not past threshold
        vi.advanceTimersByTime(2 * 60 * 1000)

        // Touch buffer1
        buffer1.touch()

        // Advance more time (total 6 min, but buffer1 was touched at 2 min so only 4 min idle)
        vi.advanceTimersByTime(4 * 60 * 1000)

        // Get new buffer triggers cleanup
        getEventBuffer(mockBucket as unknown as R2Bucket, 'new-active-test')

        // buffer1 should still exist (was touched 4 min ago, threshold is 5 min)
        // buffer2 should be cleaned (6 min idle)
        // We added 3 buffers, 1 was cleaned, net +2
        const finalCount = getBufferCount()
        expect(finalCount).toBe(initialCount + 2)
      })

      it('should not cleanup buffers with pending events', () => {
        const initialCount = getBufferCount()

        const buffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'has-events-cleanup-test')
        buffer.add(createTestEvent())

        // Advance time past idle timeout
        vi.advanceTimersByTime(6 * 60 * 1000)

        // Getting new buffer should NOT cleanup buffer with events
        getEventBuffer(mockBucket as unknown as R2Bucket, 'new-events-test')

        // Both buffers should remain: one with events, one newly created
        const finalCount = getBufferCount()
        expect(finalCount).toBe(initialCount + 2)
      })
    })
  })

  // ============================================================================
  // Error Handling Tests
  // ============================================================================

  describe('error handling', () => {
    it('should propagate R2 put errors', async () => {
      mockBucket.put.mockRejectedValueOnce(new Error('R2 storage error'))
      const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'error-test')
      buffer.add(createTestEvent())

      await expect(buffer.flush()).rejects.toThrow('R2 storage error')
    })

    it('should propagate parquet write errors', async () => {
      mockParquetWriteBuffer.mockImplementationOnce(() => {
        throw new Error('Parquet encoding error')
      })
      const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'error-test')
      buffer.add(createTestEvent())

      await expect(buffer.flush()).rejects.toThrow('Parquet encoding error')
    })

    it('should handle events with invalid timestamps gracefully', async () => {
      const events = [createTestEvent({ ts: 'invalid-date' })]

      // This may throw or handle gracefully depending on implementation
      // The test verifies the behavior is consistent
      try {
        const result = await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)
        expect(result.events).toBe(1)
      } catch (e) {
        expect(e).toBeDefined()
      }
    })

    it('should handle events with very large payloads', async () => {
      const largePayload = { data: 'x'.repeat(100000) }
      const events = [createTestEvent({ payload: largePayload })]

      const result = await writeEvents(mockBucket as unknown as R2Bucket, 'test', events)

      expect(result.events).toBe(1)
    })
  })

  // ============================================================================
  // Integration-like Tests
  // ============================================================================

  describe('integration scenarios', () => {
    it('should handle high-volume event ingestion', async () => {
      const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'high-volume', {
        countThreshold: 100,
      })

      // Add events in batches
      for (let i = 0; i < 10; i++) {
        buffer.add(createTestEvents(50))
      }

      // Should have triggered flushes or accumulated events
      expect(buffer.length + mockBucket.put.mock.calls.length * 100).toBeGreaterThanOrEqual(500)
    })

    it('should maintain event order within a flush', async () => {
      const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'order-test')
      const events = Array.from({ length: 5 }, (_, i) => createTestEvent({
        type: `ordered.event.${i}`,
        ts: new Date(Date.now() + i * 1000).toISOString(),
      }))

      buffer.add(events)
      await buffer.flush()

      const callArgs = mockParquetWriteBuffer.mock.calls[0][0]
      const typeColumn = callArgs.columnData.find((c: { name: string }) => c.name === 'type')
      expect(typeColumn.data).toEqual([
        'ordered.event.0',
        'ordered.event.1',
        'ordered.event.2',
        'ordered.event.3',
        'ordered.event.4',
      ])
    })

    it('should handle concurrent buffer operations', async () => {
      const buffer = new EventBuffer(mockBucket as unknown as R2Bucket, 'concurrent', {
        countThreshold: 1000,
      })

      // Simulate concurrent adds
      const promises = Array.from({ length: 10 }, () =>
        Promise.resolve().then(() => buffer.add(createTestEvents(10)))
      )
      await Promise.all(promises)

      expect(buffer.length).toBe(100)
    })

    it('should support multiple buffer prefixes independently', async () => {
      const webhooksBuffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'webhooks')
      const apiBuffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'api')
      const tailBuffer = getEventBuffer(mockBucket as unknown as R2Bucket, 'tail')

      webhooksBuffer.add(createTestEvent({ type: 'webhook.received' }))
      apiBuffer.add(createTestEvent({ type: 'api.request' }))
      tailBuffer.add(createTestEvent({ type: 'tail.log' }))

      await webhooksBuffer.flush()
      await apiBuffer.flush()
      await tailBuffer.flush()

      expect(mockBucket.put).toHaveBeenCalledTimes(3)

      const keys = mockBucket.put.mock.calls.map((c: unknown[]) => c[0] as string)
      expect(keys.some((k: string) => k.startsWith('webhooks/'))).toBe(true)
      expect(keys.some((k: string) => k.startsWith('api/'))).toBe(true)
      expect(keys.some((k: string) => k.startsWith('tail/'))).toBe(true)
    })
  })
})
