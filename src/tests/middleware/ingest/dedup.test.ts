/**
 * Ingest Deduplication Middleware Tests
 *
 * Unit tests for src/middleware/ingest/dedup.ts
 * Tests atomic deduplication via R2 conditional put and namespace isolation
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  dedupMiddleware,
  writeDedupMarker,
} from '../../../middleware/ingest/dedup'
import type { IngestContext } from '../../../middleware/ingest/types'

// ============================================================================
// Mock Types
// ============================================================================

interface MockR2Bucket {
  head: ReturnType<typeof vi.fn>
  put: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockBucket(overrides: Partial<MockR2Bucket> = {}): MockR2Bucket {
  return {
    head: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue({}),
    get: vi.fn().mockResolvedValue(null),
    ...overrides,
  }
}

function createMockContext(overrides: Partial<IngestContext> = {}): IngestContext {
  const mockBucket = createMockBucket()
  return {
    request: new Request('https://events.do/ingest', { method: 'POST' }),
    env: {
      EVENTS_BUCKET: mockBucket,
    } as any,
    ctx: {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as any,
    tenant: {
      namespace: 'test-namespace',
      isAdmin: false,
      keyId: 'test-key',
    },
    batch: {
      events: [
        { type: 'event1', ts: '2024-01-01T00:00:00Z' },
        { type: 'event2', ts: '2024-01-01T00:00:01Z' },
      ],
    },
    batchId: undefined,
    startTime: performance.now(),
    ...overrides,
  }
}

// ============================================================================
// dedupMiddleware Tests
// ============================================================================

describe('dedupMiddleware', () => {
  describe('without batchId', () => {
    it('continues without dedup check when batchId is undefined', async () => {
      const context = createMockContext({ batchId: undefined })

      const result = await dedupMiddleware(context)

      expect(result.continue).toBe(true)
      expect(context.env.EVENTS_BUCKET.put).not.toHaveBeenCalled()
    })

    it('continues when batchId is empty string', async () => {
      const context = createMockContext({ batchId: '' })

      const result = await dedupMiddleware(context)

      expect(result.continue).toBe(true)
    })
  })

  describe('new batch (not duplicated)', () => {
    it('continues when batch has not been seen before', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue({ key: 'created' }), // Successfully created (marker didn't exist)
      })
      const context = createMockContext({
        batchId: 'new-batch-123',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      expect(result.continue).toBe(true)
      expect(context.deduplicated).toBeFalsy()
      expect(context.dedupMarkerWritten).toBe(true)
    })

    it('writes to correct namespace-isolated dedup key', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue({ key: 'created' }),
      })
      const context = createMockContext({
        batchId: 'batch-abc',
        tenant: { namespace: 'acme', isAdmin: false, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await dedupMiddleware(context)

      expect(mockBucket.put).toHaveBeenCalledWith(
        'ns/acme/dedup/batch-abc',
        expect.any(String),
        expect.objectContaining({
          onlyIf: { uploadedBefore: expect.any(Date) },
        })
      )
    })

    it('uses legacy path for admin with default namespace', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue({ key: 'created' }),
      })
      const context = createMockContext({
        batchId: 'batch-xyz',
        tenant: { namespace: 'default', isAdmin: true, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await dedupMiddleware(context)

      expect(mockBucket.put).toHaveBeenCalledWith(
        'dedup/batch-xyz',
        expect.any(String),
        expect.any(Object)
      )
    })
  })

  describe('duplicate batch', () => {
    it('returns success response for duplicate batch (put returns null)', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue(null), // null means marker already exists
      })
      const context = createMockContext({
        batchId: 'duplicate-batch',
        batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
        tenant: { namespace: 'acme', isAdmin: false, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(200)
    })

    it('returns success response for duplicate batch (412 error)', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockRejectedValue(new Error('412 Precondition Failed')),
      })
      const context = createMockContext({
        batchId: 'duplicate-batch',
        batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
        tenant: { namespace: 'acme', isAdmin: false, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(200)
    })

    it('includes deduplicated flag in response', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue(null), // null means marker already exists
      })
      const context = createMockContext({
        batchId: 'dup-batch',
        batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }, { type: 'e2', ts: '2024-01-01T00:00:01Z' }] },
        tenant: { namespace: 'team', isAdmin: false, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)
      const json = await result.response?.json()

      expect(json.deduplicated).toBe(true)
      expect(json.ok).toBe(true)
      expect(json.received).toBe(2)
      expect(json.namespace).toBe('team')
    })

    it('sets deduplicated flag on context', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue(null), // null means marker already exists
      })
      const context = createMockContext({
        batchId: 'dup-batch',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await dedupMiddleware(context)

      expect(context.deduplicated).toBe(true)
    })

    it('includes CORS headers in response', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue(null), // null means marker already exists
      })
      const context = createMockContext({
        batchId: 'dup-batch',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      expect(result.response?.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })
  })

  describe('error handling', () => {
    it('allows batchId with path-like characters since R2 handles path safety', async () => {
      // R2 bucket keys can contain path-like characters safely
      // The middleware doesn't validate path traversal since R2 doesn't use filesystem paths
      const mockBucket = createMockBucket({
        put: vi.fn().mockResolvedValue({ key: 'created' }),
      })
      const context = createMockContext({
        batchId: '../../../etc/passwd',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      // Should continue since R2 treats this as a regular key string
      expect(result.continue).toBe(true)
    })

    it('rethrows non-412 errors', async () => {
      const mockBucket = createMockBucket({
        put: vi.fn().mockRejectedValue(new Error('Network error')),
      })
      const context = createMockContext({
        batchId: 'batch-123',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await expect(dedupMiddleware(context)).rejects.toThrow('Network error')
    })
  })
})

// ============================================================================
// writeDedupMarker Tests
// ============================================================================

describe('writeDedupMarker', () => {
  describe('without batchId', () => {
    it('does nothing when batchId is undefined', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: undefined,
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      expect(mockBucket.put).not.toHaveBeenCalled()
    })

    it('does nothing when batchId is empty', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: '',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      expect(mockBucket.put).not.toHaveBeenCalled()
    })
  })

  describe('with batchId', () => {
    it('does nothing when dedupMarkerWritten is true (already written atomically)', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-123',
        dedupMarkerWritten: true,
        tenant: { namespace: 'acme', isAdmin: false, keyId: 'key' },
        batch: {
          events: [
            { type: 'e1', ts: '2024-01-01T00:00:00Z' },
            { type: 'e2', ts: '2024-01-01T00:00:01Z' },
          ],
        },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      // Should not write since marker was already written atomically
      expect(mockBucket.put).not.toHaveBeenCalled()
    })

    it('does nothing even without dedupMarkerWritten flag (safety net only)', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-123',
        tenant: { namespace: 'acme', isAdmin: false, keyId: 'key' },
        batch: {
          events: [
            { type: 'e1', ts: '2024-01-01T00:00:00Z' },
            { type: 'e2', ts: '2024-01-01T00:00:01Z' },
          ],
        },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      // With atomic dedup, writeDedupMarker is now a no-op safety net
      expect(mockBucket.put).not.toHaveBeenCalled()
    })
  })
})

// ============================================================================
// Integration Tests
// ============================================================================

describe('dedup integration', () => {
  it('atomic dedup writes marker on first call', async () => {
    const mockBucket = createMockBucket({
      put: vi.fn().mockResolvedValue({ key: 'created' }),
    })
    const context = createMockContext({
      batchId: 'consistent-batch',
      tenant: { namespace: 'myteam', isAdmin: false, keyId: 'key' },
      batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
      env: { EVENTS_BUCKET: mockBucket } as any,
    })

    // dedupMiddleware atomically writes the marker
    await dedupMiddleware(context)
    const putKey = mockBucket.put.mock.calls[0][0]

    expect(putKey).toBe('ns/myteam/dedup/consistent-batch')
    expect(context.dedupMarkerWritten).toBe(true)

    // writeDedupMarker is now a no-op since marker was already written
    await writeDedupMarker(context)
    expect(mockBucket.put).toHaveBeenCalledTimes(1) // Still only 1 call
  })

  it('different namespaces have different dedup keys', async () => {
    const mockBucket1 = createMockBucket({
      put: vi.fn().mockResolvedValue({ key: 'created' }),
    })
    const mockBucket2 = createMockBucket({
      put: vi.fn().mockResolvedValue({ key: 'created' }),
    })

    const context1 = createMockContext({
      batchId: 'same-batch',
      tenant: { namespace: 'team-a', isAdmin: false, keyId: 'key' },
      env: { EVENTS_BUCKET: mockBucket1 } as any,
    })

    const context2 = createMockContext({
      batchId: 'same-batch',
      tenant: { namespace: 'team-b', isAdmin: false, keyId: 'key' },
      env: { EVENTS_BUCKET: mockBucket2 } as any,
    })

    await dedupMiddleware(context1)
    await dedupMiddleware(context2)

    const key1 = mockBucket1.put.mock.calls[0][0]
    const key2 = mockBucket2.put.mock.calls[0][0]

    expect(key1).toBe('ns/team-a/dedup/same-batch')
    expect(key2).toBe('ns/team-b/dedup/same-batch')
    expect(key1).not.toBe(key2)
  })
})
