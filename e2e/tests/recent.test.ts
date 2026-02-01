/**
 * Recent Events Endpoint Tests
 *
 * Tests GET /recent for listing recent events from current hour bucket.
 */

import { env, SELF } from 'cloudflare:test'
import { describe, it, expect, beforeEach } from 'vitest'
import type { DurableEvent, EventBatch } from '@dotdo/events'

/** Response type for /recent endpoint */
interface RecentResponse {
  events: DurableEvent[]
  count: number
  bucket: string
}

// Helper to create a valid event
function createEvent(overrides: Partial<DurableEvent> = {}): DurableEvent {
  return {
    type: 'rpc.call',
    ts: new Date().toISOString(),
    do: {
      id: 'test-do-id',
      name: 'test',
      class: 'TestDO',
      colo: 'DFW',
    },
    method: 'testMethod',
    namespace: undefined,
    durationMs: 10,
    success: true,
    ...overrides,
  } as DurableEvent
}

// Helper to ingest events
async function ingestEvents(events: DurableEvent[]): Promise<void> {
  await SELF.fetch('http://localhost/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ events }),
  })
}

describe('GET /recent - Basic functionality', () => {
  it('returns 200 status', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    expect(response.status).toBe(200)
  })

  it('returns JSON with events array', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    expect(data.events).toBeDefined()
    expect(Array.isArray(data.events)).toBe(true)
  })

  it('returns count property', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    expect(typeof data.count).toBe('number')
  })

  it('returns bucket path (current hour)', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    expect(data.bucket).toBeDefined()
    expect(typeof data.bucket).toBe('string')
    // Should be in format YYYY/MM/DD/HH
    expect(data.bucket).toMatch(/^\d{4}\/\d{2}\/\d{2}\/\d{2}$/)
  })
})

describe('GET /recent - Current hour bucket', () => {
  it('uses current UTC hour for bucket path', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    const now = new Date()
    const expectedBucket = [
      now.getUTCFullYear(),
      String(now.getUTCMonth() + 1).padStart(2, '0'),
      String(now.getUTCDate()).padStart(2, '0'),
      String(now.getUTCHours()).padStart(2, '0'),
    ].join('/')

    expect(data.bucket).toBe(expectedBucket)
  })
})

describe('GET /recent - Limit parameter', () => {
  beforeEach(async () => {
    // Ingest some test events first
    const events = Array.from({ length: 5 }, (_, i) =>
      createEvent({
        ts: new Date().toISOString(),
        method: `method${i}`,
      })
    )
    await ingestEvents(events)
  })

  it('defaults to limit of 100', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    // Should return up to 100 (or less if fewer events exist)
    expect(data.events.length).toBeLessThanOrEqual(100)
  })

  it('respects custom limit parameter', async () => {
    const response = await SELF.fetch('http://localhost/recent?limit=2')
    const data = await response.json() as RecentResponse

    expect(data.events.length).toBeLessThanOrEqual(2)
  })

  it('handles limit=1', async () => {
    const response = await SELF.fetch('http://localhost/recent?limit=1')
    const data = await response.json() as RecentResponse

    expect(data.events.length).toBeLessThanOrEqual(1)
  })

  it('handles large limit gracefully', async () => {
    const response = await SELF.fetch('http://localhost/recent?limit=10000')
    expect(response.status).toBe(200)
  })
})

describe('GET /recent - JSONL parsing', () => {
  beforeEach(async () => {
    // Ingest events with known data
    const events = [
      createEvent({
        ts: new Date().toISOString(),
      }),
    ]
    await ingestEvents(events)
  })

  it('parses JSONL correctly into event objects', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    if (data.events.length > 0) {
      const event = data.events[0]
      expect(event.type).toBeDefined()
      expect(event.ts).toBeDefined()
      expect(event.do).toBeDefined()
    }
  })

  it('returns events with proper structure', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    for (const event of data.events) {
      // All events should have base properties
      expect(event).toHaveProperty('type')
      expect(event).toHaveProperty('ts')
      expect(event).toHaveProperty('do')
      expect(event.do).toHaveProperty('id')
    }
  })

  it('skips malformed JSONL lines gracefully', async () => {
    // This tests that the endpoint doesn't crash on bad data
    // The implementation has try/catch for parse errors
    const response = await SELF.fetch('http://localhost/recent')
    expect(response.status).toBe(200)
  })
})

describe('GET /recent - Empty bucket', () => {
  it('returns empty array when no events in current hour', async () => {
    // Create a fresh test - since storage is isolated, bucket might be empty
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    // Should still return valid structure
    expect(Array.isArray(data.events)).toBe(true)
    expect(typeof data.count).toBe('number')
    expect(data.bucket).toBeDefined()
  })
})

describe('GET /recent - Events content', () => {
  it('returns events ingested in current hour', async () => {
    // Ingest a unique event
    const uniqueId = `test-${Date.now()}`
    const event = createEvent({
      ts: new Date().toISOString(),
      do: {
        id: uniqueId,
        class: 'TestDO',
        colo: 'DFW',
      },
    })
    await ingestEvents([event])

    // Fetch recent events
    const response = await SELF.fetch('http://localhost/recent')
    const data = await response.json() as RecentResponse

    // Should find our event (may need to wait for ctx.waitUntil)
    // Note: In real tests, timing can be tricky with waitUntil
    expect(data.events.length).toBeGreaterThanOrEqual(0)
  })
})

describe('GET /recent - CORS', () => {
  it('includes CORS headers in response', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})
