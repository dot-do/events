/**
 * Ingest Validation Middleware Tests
 *
 * Unit tests for src/middleware/ingest/validate.ts
 * Tests event and batch validation, JSON parsing, and size limits
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  validateEvent,
  validateBatch,
  validateEventSize,
  parseJsonMiddleware,
  validateBatchMiddleware,
  validateEventsMiddleware,
} from '../../../middleware/ingest/validate'
import {
  MAX_EVENT_SIZE,
  MAX_BATCH_SIZE,
  MAX_TYPE_LENGTH,
} from '../../../middleware/ingest/types'
import type { IngestContext } from '../../../middleware/ingest/types'

// ============================================================================
// Helper Functions
// ============================================================================

function createMockContext(overrides: Partial<IngestContext> = {}): IngestContext {
  return {
    request: new Request('https://events.do/ingest', { method: 'POST' }),
    env: {
      ANALYTICS: undefined,
    } as any,
    ctx: {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as any,
    tenant: {
      namespace: 'test',
      isAdmin: false,
      keyId: 'test-key',
    },
    startTime: performance.now(),
    ...overrides,
  }
}

function createRequestWithBody(body: unknown): Request {
  return new Request('https://events.do/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

function createRequestWithRawBody(body: string): Request {
  return new Request('https://events.do/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  })
}

// ============================================================================
// validateEvent Tests
// ============================================================================

describe('validateEvent', () => {
  describe('valid events', () => {
    it('accepts event with type and ts', () => {
      const event = { type: 'user.created', ts: '2024-01-15T10:00:00Z' }

      expect(validateEvent(event)).toBe(true)
    })

    it('accepts event with additional fields', () => {
      const event = {
        type: 'user.created',
        ts: '2024-01-15T10:00:00Z',
        userId: '123',
        email: 'test@example.com',
      }

      expect(validateEvent(event)).toBe(true)
    })

    it('accepts event with complex type name', () => {
      const event = {
        type: 'collection.user.profile.updated',
        ts: '2024-01-15T10:00:00Z',
      }

      expect(validateEvent(event)).toBe(true)
    })

    it('accepts event with millisecond timestamp', () => {
      const event = {
        type: 'event',
        ts: '2024-01-15T10:00:00.123Z',
      }

      expect(validateEvent(event)).toBe(true)
    })
  })

  describe('invalid events', () => {
    it('rejects null', () => {
      expect(validateEvent(null)).toBe(false)
    })

    it('rejects undefined', () => {
      expect(validateEvent(undefined)).toBe(false)
    })

    it('rejects non-object', () => {
      expect(validateEvent('string')).toBe(false)
      expect(validateEvent(123)).toBe(false)
      expect(validateEvent(true)).toBe(false)
    })

    it('rejects event without type', () => {
      const event = { ts: '2024-01-15T10:00:00Z' }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with non-string type', () => {
      const event = { type: 123, ts: '2024-01-15T10:00:00Z' }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with empty type', () => {
      const event = { type: '', ts: '2024-01-15T10:00:00Z' }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with type exceeding max length', () => {
      const event = {
        type: 'a'.repeat(MAX_TYPE_LENGTH + 1),
        ts: '2024-01-15T10:00:00Z',
      }

      expect(validateEvent(event)).toBe(false)
    })

    it('accepts event with type at max length', () => {
      const event = {
        type: 'a'.repeat(MAX_TYPE_LENGTH),
        ts: '2024-01-15T10:00:00Z',
      }

      expect(validateEvent(event)).toBe(true)
    })

    it('rejects event without ts', () => {
      const event = { type: 'event' }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with non-string ts', () => {
      const event = { type: 'event', ts: 1234567890 }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with invalid ts format', () => {
      const event = { type: 'event', ts: 'not-a-date' }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with invalid date', () => {
      const event = { type: 'event', ts: '2024-13-45T99:99:99Z' }

      expect(validateEvent(event)).toBe(false)
    })
  })
})

// ============================================================================
// validateBatch Tests
// ============================================================================

describe('validateBatch', () => {
  describe('valid batches', () => {
    it('accepts batch with events array', () => {
      const batch = { events: [] }

      expect(validateBatch(batch)).toBe(true)
    })

    it('accepts batch with multiple events', () => {
      const batch = {
        events: [
          { type: 'a', ts: '2024-01-01T00:00:00Z' },
          { type: 'b', ts: '2024-01-01T00:00:00Z' },
        ],
      }

      expect(validateBatch(batch)).toBe(true)
    })

    it('accepts batch with additional fields', () => {
      const batch = {
        events: [{ type: 'a', ts: '2024-01-01T00:00:00Z' }],
        batchId: 'batch-123',
        source: 'test',
      }

      expect(validateBatch(batch)).toBe(true)
    })

    it('accepts batch with max events', () => {
      const batch = {
        events: Array(MAX_BATCH_SIZE).fill({ type: 'event', ts: '2024-01-01T00:00:00Z' }),
      }

      expect(validateBatch(batch)).toBe(true)
    })
  })

  describe('invalid batches', () => {
    it('rejects null', () => {
      expect(validateBatch(null)).toBe(false)
    })

    it('rejects undefined', () => {
      expect(validateBatch(undefined)).toBe(false)
    })

    it('rejects non-object', () => {
      expect(validateBatch('string')).toBe(false)
      expect(validateBatch(123)).toBe(false)
    })

    it('rejects batch without events', () => {
      const batch = { batchId: '123' }

      expect(validateBatch(batch)).toBe(false)
    })

    it('rejects batch with non-array events', () => {
      const batch = { events: 'not-an-array' }

      expect(validateBatch(batch)).toBe(false)
    })

    it('rejects batch with events as object', () => {
      const batch = { events: { '0': { type: 'a', ts: '2024-01-01T00:00:00Z' } } }

      expect(validateBatch(batch)).toBe(false)
    })

    it('rejects batch exceeding max size', () => {
      const batch = {
        events: Array(MAX_BATCH_SIZE + 1).fill({ type: 'event', ts: '2024-01-01T00:00:00Z' }),
      }

      expect(validateBatch(batch)).toBe(false)
    })
  })
})

// ============================================================================
// validateEventSize Tests
// ============================================================================

describe('validateEventSize', () => {
  it('accepts small event', () => {
    const event = { type: 'event', ts: '2024-01-01T00:00:00Z', data: 'small' }

    expect(validateEventSize(event)).toBe(true)
  })

  it('accepts event at max size', () => {
    const data = 'x'.repeat(MAX_EVENT_SIZE - 100) // Leave room for other fields
    const event = { type: 'event', ts: '2024-01-01T00:00:00Z', data }

    expect(validateEventSize(event)).toBe(true)
  })

  it('rejects event exceeding max size', () => {
    const data = 'x'.repeat(MAX_EVENT_SIZE + 1000)
    const event = { type: 'event', ts: '2024-01-01T00:00:00Z', data }

    expect(validateEventSize(event)).toBe(false)
  })

  it('handles deeply nested objects', () => {
    const smallNested = {
      type: 'event',
      ts: '2024-01-01T00:00:00Z',
      a: { b: { c: { d: 'value' } } },
    }

    expect(validateEventSize(smallNested)).toBe(true)
  })
})

// ============================================================================
// parseJsonMiddleware Tests
// ============================================================================

describe('parseJsonMiddleware', () => {
  it('parses valid JSON', async () => {
    const body = { events: [{ type: 'test', ts: '2024-01-01T00:00:00Z' }] }
    const context = createMockContext({
      request: createRequestWithBody(body),
    })

    const result = await parseJsonMiddleware(context)

    expect(result.continue).toBe(true)
    expect(context.rawBody).toEqual(body)
  })

  it('returns error for invalid JSON', async () => {
    const context = createMockContext({
      request: createRequestWithRawBody('{ invalid json }'),
    })

    const result = await parseJsonMiddleware(context)

    expect(result.continue).toBe(false)
    expect(result.response?.status).toBe(400)
  })

  it('includes CORS headers in error response', async () => {
    const context = createMockContext({
      request: createRequestWithRawBody('not-json'),
    })

    const result = await parseJsonMiddleware(context)

    expect(result.response?.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })

  it('handles empty body', async () => {
    const context = createMockContext({
      request: new Request('https://events.do/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '',
      }),
    })

    const result = await parseJsonMiddleware(context)

    expect(result.continue).toBe(false)
  })
})

// ============================================================================
// validateBatchMiddleware Tests
// ============================================================================

describe('validateBatchMiddleware', () => {
  it('validates correct batch structure', async () => {
    const context = createMockContext({
      rawBody: {
        events: [{ type: 'test', ts: '2024-01-01T00:00:00Z' }],
      },
    })

    const result = await validateBatchMiddleware(context)

    expect(result.continue).toBe(true)
    expect(context.batch).toBeDefined()
    expect(context.batch?.events).toHaveLength(1)
  })

  it('extracts batchId from body', async () => {
    const context = createMockContext({
      rawBody: {
        events: [{ type: 'test', ts: '2024-01-01T00:00:00Z' }],
        batchId: 'batch-abc-123',
      },
    })

    const result = await validateBatchMiddleware(context)

    expect(result.continue).toBe(true)
    expect(context.batchId).toBe('batch-abc-123')
  })

  it('returns error for invalid batch', async () => {
    const context = createMockContext({
      rawBody: { notEvents: [] },
    })

    const result = await validateBatchMiddleware(context)

    expect(result.continue).toBe(false)
    expect(result.response?.status).toBe(400)
  })

  it('returns error for batch exceeding max size', async () => {
    const context = createMockContext({
      rawBody: {
        events: Array(MAX_BATCH_SIZE + 1).fill({ type: 'test', ts: '2024-01-01T00:00:00Z' }),
      },
    })

    const result = await validateBatchMiddleware(context)

    expect(result.continue).toBe(false)
  })
})

// ============================================================================
// validateEventsMiddleware Tests
// ============================================================================

describe('validateEventsMiddleware', () => {
  it('validates all events in batch', async () => {
    const context = createMockContext({
      batch: {
        events: [
          { type: 'event1', ts: '2024-01-01T00:00:00Z' },
          { type: 'event2', ts: '2024-01-01T00:00:01Z' },
        ],
      },
    })

    const result = await validateEventsMiddleware(context)

    expect(result.continue).toBe(true)
  })

  it('returns error for invalid events', async () => {
    const context = createMockContext({
      batch: {
        events: [
          { type: 'valid', ts: '2024-01-01T00:00:00Z' },
          { type: '', ts: '2024-01-01T00:00:00Z' }, // Invalid: empty type
          { ts: '2024-01-01T00:00:00Z' }, // Invalid: no type
        ],
      },
    })

    const result = await validateEventsMiddleware(context)

    expect(result.continue).toBe(false)
    expect(result.response?.status).toBe(400)
  })

  it('reports indices of invalid events', async () => {
    const context = createMockContext({
      batch: {
        events: [
          { type: 'valid', ts: '2024-01-01T00:00:00Z' },
          { type: '', ts: '2024-01-01T00:00:00Z' }, // Invalid at index 1
          { type: 'valid2', ts: '2024-01-01T00:00:00Z' },
          { type: 123, ts: '2024-01-01T00:00:00Z' }, // Invalid at index 3
        ],
      },
    })

    const result = await validateEventsMiddleware(context)
    const json = await result.response?.json()

    expect(result.continue).toBe(false)
    expect(json.details?.indices).toContain(1)
    expect(json.details?.indices).toContain(3)
  })

  it('returns error for oversized events', async () => {
    const largeData = 'x'.repeat(MAX_EVENT_SIZE + 1000)
    const context = createMockContext({
      batch: {
        events: [{ type: 'valid', ts: '2024-01-01T00:00:00Z', data: largeData }],
      },
    })

    const result = await validateEventsMiddleware(context)

    expect(result.continue).toBe(false)
    expect(result.response?.status).toBe(400)
  })

  it('limits reported indices to 10', async () => {
    const invalidEvents = Array(15).fill({ type: '', ts: '2024-01-01T00:00:00Z' })
    const context = createMockContext({
      batch: { events: invalidEvents },
    })

    const result = await validateEventsMiddleware(context)
    const json = await result.response?.json()

    expect(json.details?.indices).toHaveLength(10)
  })

  it('handles empty batch', async () => {
    const context = createMockContext({
      batch: { events: [] },
    })

    const result = await validateEventsMiddleware(context)

    expect(result.continue).toBe(true)
  })
})
