/**
 * Real Infrastructure E2E Tests
 *
 * These tests run against actual Cloudflare infrastructure, not mocks.
 * Run with: npx vitest run e2e/real-infra.test.ts
 *
 * Prerequisites:
 * - events_http stream created with HTTP ingest enabled
 * - events_http_pipeline connected to events_sink (Iceberg)
 * - CLOUDFLARE_API_TOKEN set for R2 SQL queries
 */

import { describe, it, expect, beforeAll } from 'vitest'

// Pipeline endpoints
const INGEST_ENDPOINT = 'https://46678f20f29748488165c19d7e43f331.ingest.cloudflare.com'
const WORKER_ENDPOINT = 'https://events.workers.do'

// R2 Data Catalog
const WAREHOUSE = 'b6641681fe423910342b9ffa1364c76d_platform-events'
const CATALOG_URI = 'https://catalog.cloudflarestorage.com/b6641681fe423910342b9ffa1364c76d/platform-events'

interface Event {
  type: string
  timestamp?: number
  source?: string
  data?: Record<string, unknown>
  [key: string]: unknown
}

describe('Real Infrastructure Tests', () => {
  const testId = `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

  describe('HTTP Ingest Endpoint (Pipeline Stream)', () => {
    it('accepts single event', async () => {
      const event: Event = {
        type: 'e2e.single',
        timestamp: Date.now(),
        source: 'e2e-test',
        data: { testId, message: 'Single event test' }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      expect(result.result.committed).toBe(1)
    })

    it('accepts batch of events', async () => {
      const events: Event[] = Array.from({ length: 100 }, (_, i) => ({
        type: 'e2e.batch',
        timestamp: Date.now(),
        source: 'e2e-test',
        data: { testId, index: i }
      }))

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(events)
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      expect(result.result.committed).toBe(100)
    })

    it('accepts events without required type (validation at processing)', async () => {
      // Note: Pipeline accepts at ingest but drops invalid events during processing
      // Events without required fields are committed but silently dropped
      const event = {
        timestamp: Date.now(),
        source: 'e2e-test',
        data: { message: 'Missing type field' }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      // Pipeline accepts events at ingest (validates during processing)
      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      // Events are committed to stream, but will be dropped during pipeline processing
      expect(result.result.committed).toBe(1)
    })

    it('handles CORS preflight', async () => {
      const response = await fetch(INGEST_ENDPOINT, {
        method: 'OPTIONS',
        headers: {
          'Origin': 'https://example.com',
          'Access-Control-Request-Method': 'POST'
        }
      })

      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })
  })

  describe('CDC Events', () => {
    it('accepts collection.insert event', async () => {
      const event: Event = {
        type: 'collection.insert',
        timestamp: Date.now(),
        source: 'e2e-test',
        do_id: 'test-do-123',
        do_class: 'TestDO',
        collection: 'users',
        doc_id: 'user-456',
        doc: { name: 'Test User', email: 'test@example.com' },
        bookmark: 'sqlite-bookmark-123',
        data: { testId }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      expect(result.result.committed).toBe(1)
    })

    it('accepts collection.update with prev document', async () => {
      const event: Event = {
        type: 'collection.update',
        timestamp: Date.now(),
        source: 'e2e-test',
        do_id: 'test-do-123',
        do_class: 'TestDO',
        collection: 'users',
        doc_id: 'user-456',
        prev: { name: 'Test User', email: 'test@example.com' },
        doc: { name: 'Updated User', email: 'updated@example.com' },
        bookmark: 'sqlite-bookmark-124',
        data: { testId }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      expect(result.result.committed).toBe(1)
    })

    it('accepts collection.delete event', async () => {
      const event: Event = {
        type: 'collection.delete',
        timestamp: Date.now(),
        source: 'e2e-test',
        do_id: 'test-do-123',
        do_class: 'TestDO',
        collection: 'users',
        doc_id: 'user-456',
        prev: { name: 'Updated User', email: 'updated@example.com' },
        bookmark: 'sqlite-bookmark-125',
        data: { testId }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      expect(result.result.committed).toBe(1)
    })
  })

  describe('Analytics Events', () => {
    it('accepts pageview event', async () => {
      const event: Event = {
        type: 'pageview',
        timestamp: Date.now(),
        source: 'e2e-test',
        data: {
          testId,
          page: '/test-page',
          referrer: 'https://google.com',
          userAgent: 'Mozilla/5.0 Test',
          sessionId: 'session-123'
        }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
    })

    it('accepts track event', async () => {
      const event: Event = {
        type: 'track',
        event: 'button_click',
        timestamp: Date.now(),
        source: 'e2e-test',
        data: {
          testId,
          element: 'signup-button',
          value: 1
        }
      }

      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([event])
      })

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
    })
  })

  describe('Performance', () => {
    it('handles high-volume batch (1000 events)', async () => {
      const events: Event[] = Array.from({ length: 1000 }, (_, i) => ({
        type: 'e2e.highvolume',
        timestamp: Date.now(),
        source: 'e2e-test',
        data: { testId, index: i }
      }))

      const start = Date.now()
      const response = await fetch(INGEST_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(events)
      })
      const latency = Date.now() - start

      expect(response.status).toBe(200)
      const result = await response.json() as { success: boolean; result: { committed: number } }
      expect(result.success).toBe(true)
      expect(result.result.committed).toBe(1000)

      console.log(`High-volume batch: ${latency}ms for 1000 events (${(1000 / latency * 1000).toFixed(0)} events/sec)`)
    })

    it('handles concurrent requests', async () => {
      const concurrency = 10
      const eventsPerRequest = 100

      const requests = Array.from({ length: concurrency }, (_, batch) => {
        const events: Event[] = Array.from({ length: eventsPerRequest }, (_, i) => ({
          type: 'e2e.concurrent',
          timestamp: Date.now(),
          source: 'e2e-test',
          data: { testId, batch, index: i }
        }))

        return fetch(INGEST_ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(events)
        })
      })

      const start = Date.now()
      const responses = await Promise.all(requests)
      const latency = Date.now() - start

      for (const response of responses) {
        expect(response.status).toBe(200)
      }

      const totalEvents = concurrency * eventsPerRequest
      console.log(`Concurrent: ${latency}ms for ${totalEvents} events across ${concurrency} requests (${(totalEvents / latency * 1000).toFixed(0)} events/sec)`)
    })
  })

  describe.skip('R2 SQL Queries (requires CLOUDFLARE_API_TOKEN)', () => {
    // These tests require the R2 SQL auth token
    // Skip by default, enable when token is available

    it('queries events from Iceberg table', async () => {
      const token = process.env.CLOUDFLARE_API_TOKEN
      if (!token) {
        console.log('Skipping: CLOUDFLARE_API_TOKEN not set')
        return
      }

      // Wait for pipeline to flush (5 minute default, but we test with shorter timeout)
      console.log('Waiting for pipeline flush...')
      await new Promise(r => setTimeout(r, 5000))

      // Query via R2 SQL API
      const response = await fetch('https://api.cloudflare.com/client/v4/r2/sql/query', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          warehouse: WAREHOUSE,
          query: 'SELECT COUNT(*) as total FROM events.events'
        })
      })

      expect(response.status).toBe(200)
      const result = await response.json()
      console.log('Query result:', result)
    })
  })
})

describe('Events.do Worker (if deployed)', () => {
  it.skip('health check', async () => {
    const response = await fetch(`${WORKER_ENDPOINT}/health`)

    expect(response.status).toBe(200)
    const result = await response.json() as { status: string }
    expect(result.status).toBe('ok')
  })
})

// Export test utilities for use in other tests
export { INGEST_ENDPOINT, WAREHOUSE, CATALOG_URI }
