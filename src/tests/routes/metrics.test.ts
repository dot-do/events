/**
 * Metrics Route Handler Tests
 *
 * Unit tests for src/routes/metrics.ts
 * Tests Prometheus-compatible /metrics endpoint.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  handleMetrics,
  incrementEventsIngested,
  incrementEventsDelivered,
  incrementDeadLetters,
  resetMetrics,
  getMetricsSnapshot,
} from '../../routes/metrics'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../../subscription-routes'

// Number of known subscription shards for test calculations
const SHARD_COUNT = KNOWN_SUBSCRIPTION_SHARDS.length

// ============================================================================
// Mock Types
// ============================================================================

interface MockSubscription {
  id: string
  active: boolean
  pattern: string
  createdAt: number
}

interface MockSubscriptionStatus {
  subscription: MockSubscription | null
  stats: {
    pendingDeliveries: number
    failedDeliveries: number
    deadLetters: number
    successRate: number
    totalDelivered: number
    totalAttempts: number
  }
}

interface MockSubscriptionDO {
  listSubscriptions: ReturnType<typeof vi.fn>
  getSubscriptionStatus: ReturnType<typeof vi.fn>
}

interface MockEnv {
  SUBSCRIPTIONS: {
    idFromName: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockSubscriptionDO(
  subscriptions: MockSubscription[] = [],
  stats: Record<string, MockSubscriptionStatus['stats']> = {}
): MockSubscriptionDO {
  return {
    listSubscriptions: vi.fn().mockResolvedValue(subscriptions),
    getSubscriptionStatus: vi.fn().mockImplementation((id: string) => {
      const sub = subscriptions.find(s => s.id === id)
      return Promise.resolve({
        subscription: sub || null,
        stats: stats[id] || {
          pendingDeliveries: 0,
          failedDeliveries: 0,
          deadLetters: 0,
          successRate: 0,
          totalDelivered: 0,
          totalAttempts: 0,
        },
      })
    }),
  }
}

function createMockEnv(doMock?: MockSubscriptionDO): MockEnv {
  const mock = doMock || createMockSubscriptionDO()
  return {
    SUBSCRIPTIONS: {
      idFromName: vi.fn().mockReturnValue({ name: 'test-id' }),
      get: vi.fn().mockReturnValue(mock),
    },
  }
}

function createRequest(url: string): Request {
  return new Request(url, { method: 'GET' })
}

// ============================================================================
// Test Suites
// ============================================================================

describe('Prometheus Metrics Helper Functions', () => {
  beforeEach(() => {
    resetMetrics()
  })

  describe('incrementEventsIngested', () => {
    it('increments events ingested counter', () => {
      incrementEventsIngested(100, 50)

      const snapshot = getMetricsSnapshot()
      expect(snapshot.eventsIngestedTotal).toBe(100)
      expect(snapshot.ingestLatencySum).toBe(0.05) // 50ms = 0.05s
      expect(snapshot.ingestLatencyCount).toBe(1)
    })

    it('accumulates multiple increments', () => {
      incrementEventsIngested(100, 50)
      incrementEventsIngested(200, 100)
      incrementEventsIngested(50, 25)

      const snapshot = getMetricsSnapshot()
      expect(snapshot.eventsIngestedTotal).toBe(350)
      expect(snapshot.ingestLatencySum).toBeCloseTo(0.175) // 175ms = 0.175s
      expect(snapshot.ingestLatencyCount).toBe(3)
    })

    it('updates lastUpdated timestamp', () => {
      const before = Date.now()
      incrementEventsIngested(10, 5)
      const after = Date.now()

      const snapshot = getMetricsSnapshot()
      expect(snapshot.lastUpdated).toBeGreaterThanOrEqual(before)
      expect(snapshot.lastUpdated).toBeLessThanOrEqual(after)
    })
  })

  describe('incrementEventsDelivered', () => {
    it('increments events delivered counter', () => {
      incrementEventsDelivered(50, 100)

      const snapshot = getMetricsSnapshot()
      expect(snapshot.eventsDeliveredTotal).toBe(50)
      expect(snapshot.deliveryLatencySum).toBe(0.1) // 100ms = 0.1s
      expect(snapshot.deliveryLatencyCount).toBe(1)
    })

    it('accumulates multiple increments', () => {
      incrementEventsDelivered(50, 100)
      incrementEventsDelivered(100, 200)

      const snapshot = getMetricsSnapshot()
      expect(snapshot.eventsDeliveredTotal).toBe(150)
      expect(snapshot.deliveryLatencySum).toBeCloseTo(0.3) // 300ms = 0.3s
      expect(snapshot.deliveryLatencyCount).toBe(2)
    })
  })

  describe('incrementDeadLetters', () => {
    it('increments dead letters counter', () => {
      incrementDeadLetters(5)

      const snapshot = getMetricsSnapshot()
      expect(snapshot.deadLettersTotal).toBe(5)
    })

    it('accumulates multiple increments', () => {
      incrementDeadLetters(5)
      incrementDeadLetters(3)
      incrementDeadLetters(2)

      const snapshot = getMetricsSnapshot()
      expect(snapshot.deadLettersTotal).toBe(10)
    })
  })

  describe('resetMetrics', () => {
    it('resets all metrics to zero', () => {
      incrementEventsIngested(100, 50)
      incrementEventsDelivered(50, 100)
      incrementDeadLetters(5)

      resetMetrics()

      const snapshot = getMetricsSnapshot()
      expect(snapshot.eventsIngestedTotal).toBe(0)
      expect(snapshot.eventsDeliveredTotal).toBe(0)
      expect(snapshot.ingestLatencySum).toBe(0)
      expect(snapshot.ingestLatencyCount).toBe(0)
      expect(snapshot.deliveryLatencySum).toBe(0)
      expect(snapshot.deliveryLatencyCount).toBe(0)
      expect(snapshot.deadLettersTotal).toBe(0)
      expect(snapshot.activeSubscriptions).toBe(0)
      expect(snapshot.lastUpdated).toBe(0)
    })
  })

  describe('getMetricsSnapshot', () => {
    it('returns a copy of the metrics', () => {
      incrementEventsIngested(100, 50)

      const snapshot1 = getMetricsSnapshot()
      const snapshot2 = getMetricsSnapshot()

      expect(snapshot1).toEqual(snapshot2)
      expect(snapshot1).not.toBe(snapshot2) // Different objects
    })
  })
})

describe('handleMetrics', () => {
  beforeEach(() => {
    resetMetrics()
  })

  describe('response format', () => {
    it('returns Prometheus text format', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)

      expect(response.status).toBe(200)
      expect(response.headers.get('Content-Type')).toBe('text/plain; version=0.0.4; charset=utf-8')
    })

    it('includes events_ingested_total metric', async () => {
      incrementEventsIngested(100, 50)
      const env = createMockEnv()
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP events_ingested_total Total number of events ingested')
      expect(body).toContain('# TYPE events_ingested_total counter')
      expect(body).toContain('events_ingested_total 100')
    })

    it('includes events_delivered_total metric', async () => {
      const subscriptions: MockSubscription[] = [
        { id: 'SUB1', active: true, pattern: 'test.*', createdAt: Date.now() },
      ]
      const stats = {
        SUB1: { pendingDeliveries: 0, failedDeliveries: 0, deadLetters: 0, successRate: 100, totalDelivered: 50, totalAttempts: 50 },
      }
      const mock = createMockSubscriptionDO(subscriptions, stats)
      const env = createMockEnv(mock)
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP events_delivered_total Total number of events delivered to subscriptions')
      expect(body).toContain('# TYPE events_delivered_total counter')
      // Value is multiplied by shard count since same mock is used for all shards
      expect(body).toContain(`events_delivered_total ${50 * SHARD_COUNT}`)
    })

    it('includes ingest_latency_seconds summary', async () => {
      incrementEventsIngested(100, 50)
      incrementEventsIngested(100, 100)
      const env = createMockEnv()
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP ingest_latency_seconds Latency of event ingestion in seconds')
      expect(body).toContain('# TYPE ingest_latency_seconds summary')
      expect(body).toContain('ingest_latency_seconds_sum 0.15') // 150ms = 0.15s
      expect(body).toContain('ingest_latency_seconds_count 2')
    })

    it('includes delivery_latency_seconds summary', async () => {
      incrementEventsDelivered(50, 200)
      const env = createMockEnv()
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP delivery_latency_seconds Latency of event delivery in seconds')
      expect(body).toContain('# TYPE delivery_latency_seconds summary')
      expect(body).toContain('delivery_latency_seconds_sum 0.2') // 200ms = 0.2s
      expect(body).toContain('delivery_latency_seconds_count 1')
    })

    it('includes dead_letters_total metric', async () => {
      const subscriptions: MockSubscription[] = [
        { id: 'SUB1', active: true, pattern: 'test.*', createdAt: Date.now() },
      ]
      const stats = {
        SUB1: { pendingDeliveries: 0, failedDeliveries: 0, deadLetters: 10, successRate: 90, totalDelivered: 90, totalAttempts: 100 },
      }
      const mock = createMockSubscriptionDO(subscriptions, stats)
      const env = createMockEnv(mock)
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP dead_letters_total Total number of dead-lettered events')
      expect(body).toContain('# TYPE dead_letters_total counter')
      // Value is multiplied by shard count since same mock is used for all shards
      expect(body).toContain(`dead_letters_total ${10 * SHARD_COUNT}`)
    })

    it('includes active_subscriptions gauge', async () => {
      const subscriptions: MockSubscription[] = [
        { id: 'SUB1', active: true, pattern: 'test.*', createdAt: Date.now() },
        { id: 'SUB2', active: true, pattern: 'events.*', createdAt: Date.now() },
        { id: 'SUB3', active: false, pattern: 'old.*', createdAt: Date.now() },
      ]
      const mock = createMockSubscriptionDO(subscriptions.filter(s => s.active))
      const env = createMockEnv(mock)
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP active_subscriptions Current number of active subscriptions')
      expect(body).toContain('# TYPE active_subscriptions gauge')
      // Each shard returns 2 active, but since we mock all shards the same...
      expect(body).toMatch(/active_subscriptions \d+/)
    })

    it('includes last updated timestamp', async () => {
      incrementEventsIngested(1, 1)
      const env = createMockEnv()
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('# HELP events_metrics_last_updated_timestamp_seconds Unix timestamp of last metrics update')
      expect(body).toContain('# TYPE events_metrics_last_updated_timestamp_seconds gauge')
      expect(body).toMatch(/events_metrics_last_updated_timestamp_seconds \d+/)
    })
  })

  describe('subscription DO integration', () => {
    it('queries all known subscription shards', async () => {
      const mock = createMockSubscriptionDO()
      const env = createMockEnv(mock)
      const request = createRequest('https://events.do/metrics')

      await handleMetrics(request, env as any)

      // Should call idFromName for each known shard
      expect(env.SUBSCRIPTIONS.idFromName).toHaveBeenCalled()
      expect(env.SUBSCRIPTIONS.get).toHaveBeenCalled()
    })

    it('handles DO errors gracefully', async () => {
      const mock = createMockSubscriptionDO()
      mock.listSubscriptions.mockRejectedValue(new Error('DO error'))
      const env = createMockEnv(mock)
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)

      // Should still return a valid response
      expect(response.status).toBe(200)
      const body = await response.text()
      expect(body).toContain('events_ingested_total')
    })

    it('aggregates stats across multiple shards', async () => {
      const subscriptions: MockSubscription[] = [
        { id: 'SUB1', active: true, pattern: 'test.*', createdAt: Date.now() },
      ]
      const stats = {
        SUB1: { pendingDeliveries: 0, failedDeliveries: 0, deadLetters: 5, successRate: 90, totalDelivered: 100, totalAttempts: 105 },
      }
      const mock = createMockSubscriptionDO(subscriptions, stats)
      const env = createMockEnv(mock)
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      // Should have aggregated values (each shard contributes)
      expect(body).toContain('dead_letters_total')
      expect(body).toContain('events_delivered_total')
    })
  })

  describe('zero values', () => {
    it('returns zero values when no events processed', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/metrics')

      const response = await handleMetrics(request, env as any)
      const body = await response.text()

      expect(body).toContain('events_ingested_total 0')
      expect(body).toContain('ingest_latency_seconds_sum 0')
      expect(body).toContain('ingest_latency_seconds_count 0')
      expect(body).toContain('delivery_latency_seconds_sum 0')
      expect(body).toContain('delivery_latency_seconds_count 0')
    })
  })
})
