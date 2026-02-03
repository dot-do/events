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
  isValidTimestamp,
  parseJsonMiddleware,
  validateBatchMiddleware,
  validateEventsMiddleware,
} from '../../../middleware/ingest/validate'
import {
  MAX_EVENT_SIZE,
  MAX_BATCH_SIZE,
  MAX_TYPE_LENGTH,
  MAX_TIMESTAMP_AGE_MS,
  MAX_TIMESTAMP_FUTURE_MS,
} from '../../../middleware/ingest/types'

// Helper to create a valid timestamp within bounds (current time)
function validTimestamp(): string {
  return new Date().toISOString()
}

// Helper to create a timestamp N hours ago
function hoursAgo(hours: number): string {
  return new Date(Date.now() - hours * 60 * 60 * 1000).toISOString()
}

// Helper to create a timestamp N hours from now
function hoursFromNow(hours: number): string {
  return new Date(Date.now() + hours * 60 * 60 * 1000).toISOString()
}

// Helper to create a timestamp N days ago
function daysAgo(days: number): string {
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString()
}
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
// isValidTimestamp Tests
// ============================================================================

describe('isValidTimestamp', () => {
  describe('valid timestamps', () => {
    it('accepts current timestamp', () => {
      expect(isValidTimestamp(new Date().toISOString())).toBe(true)
    })

    it('accepts timestamp from 1 hour ago', () => {
      expect(isValidTimestamp(hoursAgo(1))).toBe(true)
    })

    it('accepts timestamp from 6 days ago', () => {
      expect(isValidTimestamp(daysAgo(6))).toBe(true)
    })

    it('accepts timestamp 30 minutes in the future', () => {
      const ts = new Date(Date.now() + 30 * 60 * 1000).toISOString()
      expect(isValidTimestamp(ts)).toBe(true)
    })

    it('accepts timestamp with milliseconds', () => {
      expect(isValidTimestamp(new Date().toISOString())).toBe(true)
    })
  })

  describe('invalid timestamps - format', () => {
    it('rejects non-string', () => {
      expect(isValidTimestamp(123456789)).toBe(false)
      expect(isValidTimestamp(null)).toBe(false)
      expect(isValidTimestamp(undefined)).toBe(false)
    })

    it('rejects unparseable string', () => {
      expect(isValidTimestamp('not-a-date')).toBe(false)
      expect(isValidTimestamp('2024-13-45T99:99:99Z')).toBe(false)
    })
  })

  describe('invalid timestamps - too old', () => {
    it('rejects timestamp 8 days ago', () => {
      expect(isValidTimestamp(daysAgo(8))).toBe(false)
    })

    it('rejects timestamp 30 days ago', () => {
      expect(isValidTimestamp(daysAgo(30))).toBe(false)
    })

    it('accepts timestamp exactly at 7 day boundary', () => {
      // Just under 7 days ago should be valid
      const justUnder7Days = new Date(Date.now() - MAX_TIMESTAMP_AGE_MS + 1000).toISOString()
      expect(isValidTimestamp(justUnder7Days)).toBe(true)
    })

    it('rejects timestamp just past 7 day boundary', () => {
      // Just over 7 days ago should be invalid
      const justOver7Days = new Date(Date.now() - MAX_TIMESTAMP_AGE_MS - 1000).toISOString()
      expect(isValidTimestamp(justOver7Days)).toBe(false)
    })
  })

  describe('invalid timestamps - too far in future', () => {
    it('rejects timestamp 2 hours in the future', () => {
      expect(isValidTimestamp(hoursFromNow(2))).toBe(false)
    })

    it('rejects timestamp 24 hours in the future', () => {
      expect(isValidTimestamp(hoursFromNow(24))).toBe(false)
    })

    it('accepts timestamp just under 1 hour in the future', () => {
      const justUnder1Hour = new Date(Date.now() + MAX_TIMESTAMP_FUTURE_MS - 1000).toISOString()
      expect(isValidTimestamp(justUnder1Hour)).toBe(true)
    })

    it('rejects timestamp just over 1 hour in the future', () => {
      const justOver1Hour = new Date(Date.now() + MAX_TIMESTAMP_FUTURE_MS + 1000).toISOString()
      expect(isValidTimestamp(justOver1Hour)).toBe(false)
    })
  })
})

// ============================================================================
// validateEvent Tests
// ============================================================================

describe('validateEvent', () => {
  describe('valid events', () => {
    it('accepts event with type and ts', () => {
      const event = { type: 'user.created', ts: validTimestamp() }

      expect(validateEvent(event)).toBe(true)
    })

    it('accepts event with additional fields', () => {
      const event = {
        type: 'user.created',
        ts: validTimestamp(),
        userId: '123',
        email: 'test@example.com',
      }

      expect(validateEvent(event)).toBe(true)
    })

    it('accepts event with complex type name', () => {
      const event = {
        type: 'collection.user.profile.updated',
        ts: validTimestamp(),
      }

      expect(validateEvent(event)).toBe(true)
    })

    it('accepts event with millisecond timestamp', () => {
      const event = {
        type: 'event',
        ts: validTimestamp(),
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
      const event = { ts: validTimestamp() }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with non-string type', () => {
      const event = { type: 123, ts: validTimestamp() }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with empty type', () => {
      const event = { type: '', ts: validTimestamp() }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with type exceeding max length', () => {
      const event = {
        type: 'a'.repeat(MAX_TYPE_LENGTH + 1),
        ts: validTimestamp(),
      }

      expect(validateEvent(event)).toBe(false)
    })

    it('accepts event with type at max length', () => {
      const event = {
        type: 'a'.repeat(MAX_TYPE_LENGTH),
        ts: validTimestamp(),
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

    it('rejects event with timestamp too old (8 days ago)', () => {
      const event = { type: 'event', ts: daysAgo(8) }

      expect(validateEvent(event)).toBe(false)
    })

    it('rejects event with timestamp too far in future (2 hours)', () => {
      const event = { type: 'event', ts: hoursFromNow(2) }

      expect(validateEvent(event)).toBe(false)
    })

    it('accepts event with timestamp within bounds', () => {
      const event = { type: 'event', ts: hoursAgo(1) }

      expect(validateEvent(event)).toBe(true)
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
          { type: 'a', ts: validTimestamp() },
          { type: 'b', ts: validTimestamp() },
        ],
      }

      expect(validateBatch(batch)).toBe(true)
    })

    it('accepts batch with additional fields', () => {
      const batch = {
        events: [{ type: 'a', ts: validTimestamp() }],
        batchId: 'batch-123',
        source: 'test',
      }

      expect(validateBatch(batch)).toBe(true)
    })

    it('accepts batch with max events', () => {
      const batch = {
        events: Array(MAX_BATCH_SIZE).fill({ type: 'event', ts: validTimestamp() }),
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
      const batch = { events: { '0': { type: 'a', ts: validTimestamp() } } }

      expect(validateBatch(batch)).toBe(false)
    })

    it('rejects batch exceeding max size', () => {
      const batch = {
        events: Array(MAX_BATCH_SIZE + 1).fill({ type: 'event', ts: validTimestamp() }),
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
    const event = { type: 'event', ts: validTimestamp(), data: 'small' }

    expect(validateEventSize(event)).toBe(true)
  })

  it('accepts event at max size', () => {
    const data = 'x'.repeat(MAX_EVENT_SIZE - 100) // Leave room for other fields
    const event = { type: 'event', ts: validTimestamp(), data }

    expect(validateEventSize(event)).toBe(true)
  })

  it('rejects event exceeding max size', () => {
    const data = 'x'.repeat(MAX_EVENT_SIZE + 1000)
    const event = { type: 'event', ts: validTimestamp(), data }

    expect(validateEventSize(event)).toBe(false)
  })

  it('handles deeply nested objects', () => {
    const smallNested = {
      type: 'event',
      ts: validTimestamp(),
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
    const body = { events: [{ type: 'test', ts: validTimestamp() }] }
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
        events: [{ type: 'test', ts: validTimestamp() }],
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
        events: [{ type: 'test', ts: validTimestamp() }],
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
        events: Array(MAX_BATCH_SIZE + 1).fill({ type: 'test', ts: validTimestamp() }),
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
          { type: 'event1', ts: validTimestamp() },
          { type: 'event2', ts: validTimestamp() },
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
          { type: 'valid', ts: validTimestamp() },
          { type: '', ts: validTimestamp() }, // Invalid: empty type
          { ts: validTimestamp() }, // Invalid: no type
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
          { type: 'valid', ts: validTimestamp() },
          { type: '', ts: validTimestamp() }, // Invalid at index 1
          { type: 'valid2', ts: validTimestamp() },
          { type: 123, ts: validTimestamp() }, // Invalid at index 3
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
        events: [{ type: 'valid', ts: validTimestamp(), data: largeData }],
      },
    })

    const result = await validateEventsMiddleware(context)

    expect(result.continue).toBe(false)
    expect(result.response?.status).toBe(400)
  })

  it('limits reported indices to 10', async () => {
    const invalidEvents = Array(15).fill({ type: '', ts: validTimestamp() })
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
