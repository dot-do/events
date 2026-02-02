/**
 * Ingest Deduplication Middleware Tests
 *
 * Unit tests for src/middleware/ingest/dedup.ts
 * Tests deduplication via R2 markers and namespace isolation
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
      expect(context.env.EVENTS_BUCKET.head).not.toHaveBeenCalled()
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
        head: vi.fn().mockResolvedValue(null), // Not found
      })
      const context = createMockContext({
        batchId: 'new-batch-123',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      expect(result.continue).toBe(true)
      expect(context.deduplicated).toBeFalsy()
    })

    it('checks correct namespace-isolated dedup key', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-abc',
        tenant: { namespace: 'acme', isAdmin: false, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await dedupMiddleware(context)

      expect(mockBucket.head).toHaveBeenCalledWith('ns/acme/dedup/batch-abc')
    })

    it('uses legacy path for admin with default namespace', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-xyz',
        tenant: { namespace: 'default', isAdmin: true, keyId: 'key' },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await dedupMiddleware(context)

      expect(mockBucket.head).toHaveBeenCalledWith('dedup/batch-xyz')
    })
  })

  describe('duplicate batch', () => {
    it('returns success response for duplicate batch', async () => {
      const mockBucket = createMockBucket({
        head: vi.fn().mockResolvedValue({ key: 'exists' }), // Found
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
        head: vi.fn().mockResolvedValue({ key: 'exists' }),
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
        head: vi.fn().mockResolvedValue({ key: 'exists' }),
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
        head: vi.fn().mockResolvedValue({ key: 'exists' }),
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
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: '../../../etc/passwd',
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      const result = await dedupMiddleware(context)

      // Should continue since R2 treats this as a regular key string
      expect(result.continue).toBe(true)
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
    it('writes dedup marker to R2', async () => {
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

      expect(mockBucket.put).toHaveBeenCalledTimes(1)
      expect(mockBucket.put).toHaveBeenCalledWith(
        'ns/acme/dedup/batch-123',
        expect.any(String),
        expect.objectContaining({
          httpMetadata: { contentType: 'application/json' },
        })
      )
    })

    it('includes correct metadata in marker', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-xyz',
        tenant: { namespace: 'team', isAdmin: false, keyId: 'key' },
        batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      const putCall = mockBucket.put.mock.calls[0]
      const body = JSON.parse(putCall[1])

      expect(body.batchId).toBe('batch-xyz')
      expect(body.namespace).toBe('team')
      expect(body.eventCount).toBe(1)
      expect(body.ingestedAt).toBeDefined()
    })

    it('includes custom metadata on R2 object', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-abc',
        tenant: { namespace: 'ns', isAdmin: false, keyId: 'key' },
        batch: {
          events: [
            { type: 'e1', ts: '2024-01-01T00:00:00Z' },
            { type: 'e2', ts: '2024-01-01T00:00:01Z' },
            { type: 'e3', ts: '2024-01-01T00:00:02Z' },
          ],
        },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      const putCall = mockBucket.put.mock.calls[0]
      const options = putCall[2]

      expect(options.customMetadata.batchId).toBe('batch-abc')
      expect(options.customMetadata.namespace).toBe('ns')
      expect(options.customMetadata.eventCount).toBe('3')
    })

    it('uses legacy path for admin with default namespace', async () => {
      const mockBucket = createMockBucket()
      const context = createMockContext({
        batchId: 'batch-admin',
        tenant: { namespace: 'default', isAdmin: true, keyId: 'key' },
        batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
        env: { EVENTS_BUCKET: mockBucket } as any,
      })

      await writeDedupMarker(context)

      expect(mockBucket.put).toHaveBeenCalledWith(
        'dedup/batch-admin',
        expect.any(String),
        expect.any(Object)
      )
    })
  })
})

// ============================================================================
// Integration Tests
// ============================================================================

describe('dedup integration', () => {
  it('dedup check and marker write use same key', async () => {
    const mockBucket = createMockBucket()
    const context = createMockContext({
      batchId: 'consistent-batch',
      tenant: { namespace: 'myteam', isAdmin: false, keyId: 'key' },
      batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
      env: { EVENTS_BUCKET: mockBucket } as any,
    })

    // First, check dedup (batch not found)
    await dedupMiddleware(context)
    const checkKey = mockBucket.head.mock.calls[0][0]

    // Then, write marker
    await writeDedupMarker(context)
    const writeKey = mockBucket.put.mock.calls[0][0]

    expect(checkKey).toBe(writeKey)
    expect(checkKey).toBe('ns/myteam/dedup/consistent-batch')
  })

  it('different namespaces have different dedup keys', async () => {
    const mockBucket1 = createMockBucket()
    const mockBucket2 = createMockBucket()

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

    const key1 = mockBucket1.head.mock.calls[0][0]
    const key2 = mockBucket2.head.mock.calls[0][0]

    expect(key1).toBe('ns/team-a/dedup/same-batch')
    expect(key2).toBe('ns/team-b/dedup/same-batch')
    expect(key1).not.toBe(key2)
  })
})
