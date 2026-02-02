/**
 * Metrics Tests
 *
 * Unit tests for the Cloudflare Analytics Engine metrics integration.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  writeMetric,
  recordIngestMetric,
  recordQueueMetric,
  recordCDCMetric,
  recordSubscriptionMetric,
  recordR2WriteMetric,
  recordWriterDOMetric,
  MetricTimer,
  type AnalyticsEngineDataset,
  type MetricDimensions,
  type MetricValues,
} from '../metrics'

// ============================================================================
// Mock Analytics Engine
// ============================================================================

function createMockAnalytics(): AnalyticsEngineDataset & {
  writeDataPoint: ReturnType<typeof vi.fn>
} {
  return {
    writeDataPoint: vi.fn(),
  }
}

describe('Metrics', () => {
  // ============================================================================
  // writeMetric Tests
  // ============================================================================

  describe('writeMetric', () => {
    it('writes data point with all dimensions and values', () => {
      const analytics = createMockAnalytics()
      const dimensions: MetricDimensions = {
        namespace: 'ingest',
        operation: 'batch',
        status: 'success',
        errorType: 'timeout',
        context: 'shard-1',
      }
      const values: MetricValues = {
        count: 100,
        latencyMs: 50,
        bytes: 1024,
        errors: 2,
      }

      writeMetric(analytics, dimensions, values)

      expect(analytics.writeDataPoint).toHaveBeenCalledTimes(1)
      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'success', 'timeout', 'shard-1'],
        doubles: [100, 50, 1024, 2],
        indexes: ['ingest:batch'],
      })
    })

    it('uses empty strings for undefined optional dimensions', () => {
      const analytics = createMockAnalytics()
      const dimensions: MetricDimensions = {
        namespace: 'queue',
        operation: 'message',
        status: 'error',
        // errorType and context are undefined
      }
      const values: MetricValues = {
        count: 10,
      }

      writeMetric(analytics, dimensions, values)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'message', 'error', '', ''],
        doubles: [10, 0, 0, 0],
        indexes: ['queue:message'],
      })
    })

    it('uses 0 for undefined optional values', () => {
      const analytics = createMockAnalytics()
      const dimensions: MetricDimensions = {
        namespace: 'cdc',
        operation: 'process',
        status: 'success',
      }
      const values: MetricValues = {
        count: 5,
        // latencyMs, bytes, errors are undefined
      }

      writeMetric(analytics, dimensions, values)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['cdc', 'process', 'success', '', ''],
        doubles: [5, 0, 0, 0],
        indexes: ['cdc:process'],
      })
    })

    it('silently skips when analytics is undefined', () => {
      // Should not throw
      expect(() => {
        writeMetric(undefined, {
          namespace: 'ingest',
          operation: 'batch',
          status: 'success',
        }, {
          count: 10,
        })
      }).not.toThrow()
    })

    it('suppresses errors from writeDataPoint', () => {
      const analytics = createMockAnalytics()
      analytics.writeDataPoint.mockImplementation(() => {
        throw new Error('Analytics Engine unavailable')
      })

      // Should not throw
      expect(() => {
        writeMetric(analytics, {
          namespace: 'ingest',
          operation: 'batch',
          status: 'success',
        }, {
          count: 10,
        })
      }).not.toThrow()

      expect(analytics.writeDataPoint).toHaveBeenCalledTimes(1)
    })

    it('handles all namespace types', () => {
      const analytics = createMockAnalytics()
      const namespaces = ['ingest', 'queue', 'cdc', 'subscription', 'r2_write', 'writer_do'] as const

      for (const namespace of namespaces) {
        writeMetric(analytics, {
          namespace,
          operation: 'test',
          status: 'success',
        }, {
          count: 1,
        })
      }

      expect(analytics.writeDataPoint).toHaveBeenCalledTimes(6)
    })

    it('handles all status types', () => {
      const analytics = createMockAnalytics()
      const statuses = ['success', 'error', 'rate_limited', 'validation_error', 'backpressure'] as const

      for (const status of statuses) {
        writeMetric(analytics, {
          namespace: 'ingest',
          operation: 'test',
          status,
        }, {
          count: 1,
        })
      }

      expect(analytics.writeDataPoint).toHaveBeenCalledTimes(5)
    })

    it('handles zero values correctly', () => {
      const analytics = createMockAnalytics()
      writeMetric(analytics, {
        namespace: 'ingest',
        operation: 'batch',
        status: 'success',
      }, {
        count: 0,
        latencyMs: 0,
        bytes: 0,
        errors: 0,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'success', '', ''],
        doubles: [0, 0, 0, 0],
        indexes: ['ingest:batch'],
      })
    })

    it('handles large numeric values', () => {
      const analytics = createMockAnalytics()
      writeMetric(analytics, {
        namespace: 'r2_write',
        operation: 'flush',
        status: 'success',
      }, {
        count: 1000000,
        latencyMs: 30000,
        bytes: 10737418240, // 10 GB
        errors: 0,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['r2_write', 'flush', 'success', '', ''],
        doubles: [1000000, 30000, 10737418240, 0],
        indexes: ['r2_write:flush'],
      })
    })
  })

  // ============================================================================
  // recordIngestMetric Tests
  // ============================================================================

  describe('recordIngestMetric', () => {
    it('records successful ingest metric', () => {
      const analytics = createMockAnalytics()

      recordIngestMetric(analytics, 'success', 100, 25)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'success', '', ''],
        doubles: [100, 25, 0, 0],
        indexes: ['ingest:batch'],
      })
    })

    it('records error ingest metric with error type', () => {
      const analytics = createMockAnalytics()

      recordIngestMetric(analytics, 'error', 50, 100, 'validation_failed')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'error', 'validation_failed', ''],
        doubles: [50, 100, 0, 0],
        indexes: ['ingest:batch'],
      })
    })

    it('records rate limited ingest metric', () => {
      const analytics = createMockAnalytics()

      recordIngestMetric(analytics, 'rate_limited', 200, 5)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'rate_limited', '', ''],
        doubles: [200, 5, 0, 0],
        indexes: ['ingest:batch'],
      })
    })

    it('records validation error metric', () => {
      const analytics = createMockAnalytics()

      recordIngestMetric(analytics, 'validation_error', 10, 2, 'invalid_schema')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'validation_error', 'invalid_schema', ''],
        doubles: [10, 2, 0, 0],
        indexes: ['ingest:batch'],
      })
    })

    it('handles undefined analytics gracefully', () => {
      expect(() => {
        recordIngestMetric(undefined, 'success', 100, 25)
      }).not.toThrow()
    })
  })

  // ============================================================================
  // recordQueueMetric Tests
  // ============================================================================

  describe('recordQueueMetric', () => {
    it('records batch operation with events count', () => {
      const analytics = createMockAnalytics()

      recordQueueMetric(analytics, 'batch', 'success', { events: 500 }, 150)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'batch', 'success', '', ''],
        doubles: [500, 150, 0, 0],
        indexes: ['queue:batch'],
      })
    })

    it('records message operation with messages count', () => {
      const analytics = createMockAnalytics()

      recordQueueMetric(analytics, 'message', 'success', { messages: 10 }, 20)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'message', 'success', '', ''],
        doubles: [10, 20, 0, 0],
        indexes: ['queue:message'],
      })
    })

    it('records dead_letter operation with error count', () => {
      const analytics = createMockAnalytics()

      recordQueueMetric(analytics, 'dead_letter', 'error', { messages: 5, errors: 5 }, 50)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'dead_letter', 'error', '', ''],
        doubles: [5, 50, 0, 5],
        indexes: ['queue:dead_letter'],
      })
    })

    it('prefers events count over messages count', () => {
      const analytics = createMockAnalytics()

      recordQueueMetric(analytics, 'batch', 'success', { events: 100, messages: 50 }, 30)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'batch', 'success', '', ''],
        doubles: [100, 30, 0, 0],
        indexes: ['queue:batch'],
      })
    })

    it('handles empty counts object', () => {
      const analytics = createMockAnalytics()

      recordQueueMetric(analytics, 'batch', 'success', {}, 10)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'batch', 'success', '', ''],
        doubles: [0, 10, 0, 0],
        indexes: ['queue:batch'],
      })
    })

    it('handles undefined analytics gracefully', () => {
      expect(() => {
        recordQueueMetric(undefined, 'batch', 'success', { events: 100 }, 50)
      }).not.toThrow()
    })
  })

  // ============================================================================
  // recordCDCMetric Tests
  // ============================================================================

  describe('recordCDCMetric', () => {
    it('records successful CDC metric', () => {
      const analytics = createMockAnalytics()

      recordCDCMetric(analytics, 'success', 50)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['cdc', 'process', 'success', '', ''],
        doubles: [50, 0, 0, 0],
        indexes: ['cdc:process'],
      })
    })

    it('records CDC metric with collection context', () => {
      const analytics = createMockAnalytics()

      recordCDCMetric(analytics, 'success', 100, 'users')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['cdc', 'process', 'success', '', 'users'],
        doubles: [100, 0, 0, 0],
        indexes: ['cdc:process'],
      })
    })

    it('records error CDC metric', () => {
      const analytics = createMockAnalytics()

      recordCDCMetric(analytics, 'error', 10, 'orders')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['cdc', 'process', 'error', '', 'orders'],
        doubles: [10, 0, 0, 0],
        indexes: ['cdc:process'],
      })
    })

    it('handles undefined analytics gracefully', () => {
      expect(() => {
        recordCDCMetric(undefined, 'success', 50, 'users')
      }).not.toThrow()
    })
  })

  // ============================================================================
  // recordSubscriptionMetric Tests
  // ============================================================================

  describe('recordSubscriptionMetric', () => {
    it('records successful subscription metric', () => {
      const analytics = createMockAnalytics()

      recordSubscriptionMetric(analytics, 'success', 25)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['subscription', 'fanout', 'success', '', ''],
        doubles: [25, 0, 0, 0],
        indexes: ['subscription:fanout'],
      })
    })

    it('records subscription metric with shard key', () => {
      const analytics = createMockAnalytics()

      recordSubscriptionMetric(analytics, 'success', 100, 'shard-3')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['subscription', 'fanout', 'success', '', 'shard-3'],
        doubles: [100, 0, 0, 0],
        indexes: ['subscription:fanout'],
      })
    })

    it('records error subscription metric with error type', () => {
      const analytics = createMockAnalytics()

      recordSubscriptionMetric(analytics, 'error', 5, 'shard-1', 'delivery_failed')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['subscription', 'fanout', 'error', 'delivery_failed', 'shard-1'],
        doubles: [5, 0, 0, 0],
        indexes: ['subscription:fanout'],
      })
    })

    it('records backpressure metric', () => {
      const analytics = createMockAnalytics()

      recordSubscriptionMetric(analytics, 'backpressure', 200, 'shard-2')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['subscription', 'fanout', 'backpressure', '', 'shard-2'],
        doubles: [200, 0, 0, 0],
        indexes: ['subscription:fanout'],
      })
    })

    it('handles undefined analytics gracefully', () => {
      expect(() => {
        recordSubscriptionMetric(undefined, 'success', 25, 'shard-1')
      }).not.toThrow()
    })
  })

  // ============================================================================
  // recordR2WriteMetric Tests
  // ============================================================================

  describe('recordR2WriteMetric', () => {
    it('records successful R2 write metric', () => {
      const analytics = createMockAnalytics()

      recordR2WriteMetric(analytics, 'success', 1000, 102400, 250)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['r2_write', 'flush', 'success', '', ''],
        doubles: [1000, 250, 102400, 0],
        indexes: ['r2_write:flush'],
      })
    })

    it('records R2 write metric with source context', () => {
      const analytics = createMockAnalytics()

      recordR2WriteMetric(analytics, 'success', 500, 51200, 100, 'queue_batch')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['r2_write', 'flush', 'success', '', 'queue_batch'],
        doubles: [500, 100, 51200, 0],
        indexes: ['r2_write:flush'],
      })
    })

    it('records error R2 write metric', () => {
      const analytics = createMockAnalytics()

      recordR2WriteMetric(analytics, 'error', 100, 10240, 5000, 'writer_do')

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['r2_write', 'flush', 'error', '', 'writer_do'],
        doubles: [100, 5000, 10240, 0],
        indexes: ['r2_write:flush'],
      })
    })

    it('handles large byte values', () => {
      const analytics = createMockAnalytics()
      const oneGB = 1073741824

      recordR2WriteMetric(analytics, 'success', 10000, oneGB, 10000)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['r2_write', 'flush', 'success', '', ''],
        doubles: [10000, 10000, oneGB, 0],
        indexes: ['r2_write:flush'],
      })
    })

    it('handles undefined analytics gracefully', () => {
      expect(() => {
        recordR2WriteMetric(undefined, 'success', 100, 1024, 50)
      }).not.toThrow()
    })
  })

  // ============================================================================
  // recordWriterDOMetric Tests
  // ============================================================================

  describe('recordWriterDOMetric', () => {
    it('records ingest operation metric', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'ingest', 'success', { events: 100 }, 25)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'ingest', 'success', '', ''],
        doubles: [100, 25, 0, 0],
        indexes: ['writer_do:ingest'],
      })
    })

    it('records flush operation metric with bytes', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'flush', 'success', { events: 500, bytes: 51200 }, 150)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'flush', 'success', '', ''],
        doubles: [500, 150, 51200, 0],
        indexes: ['writer_do:flush'],
      })
    })

    it('records backpressure operation metric', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'backpressure', 'backpressure', { events: 1000 })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'backpressure', 'backpressure', '', ''],
        doubles: [1000, 0, 0, 0],
        indexes: ['writer_do:backpressure'],
      })
    })

    it('records restore operation metric', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'restore', 'success', { events: 250, bytes: 25600 }, 500)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'restore', 'success', '', ''],
        doubles: [250, 500, 25600, 0],
        indexes: ['writer_do:restore'],
      })
    })

    it('records metric with shard context', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'ingest', 'success', { events: 100, shard: 5 }, 20)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'ingest', 'success', '', 'shard-5'],
        doubles: [100, 20, 0, 0],
        indexes: ['writer_do:ingest'],
      })
    })

    it('handles shard 0 correctly', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'flush', 'success', { events: 50, shard: 0 }, 10)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'flush', 'success', '', 'shard-0'],
        doubles: [50, 10, 0, 0],
        indexes: ['writer_do:flush'],
      })
    })

    it('handles error operation metric', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'flush', 'error', { events: 100, bytes: 10240, shard: 2 }, 1000)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'flush', 'error', '', 'shard-2'],
        doubles: [100, 1000, 10240, 0],
        indexes: ['writer_do:flush'],
      })
    })

    it('handles empty counts object', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'ingest', 'success', {}, 10)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'ingest', 'success', '', ''],
        doubles: [0, 10, 0, 0],
        indexes: ['writer_do:ingest'],
      })
    })

    it('handles undefined latency', () => {
      const analytics = createMockAnalytics()

      recordWriterDOMetric(analytics, 'backpressure', 'backpressure', { events: 200 })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'backpressure', 'backpressure', '', ''],
        doubles: [200, 0, 0, 0],
        indexes: ['writer_do:backpressure'],
      })
    })

    it('handles undefined analytics gracefully', () => {
      expect(() => {
        recordWriterDOMetric(undefined, 'ingest', 'success', { events: 100 }, 25)
      }).not.toThrow()
    })
  })

  // ============================================================================
  // MetricTimer Tests
  // ============================================================================

  describe('MetricTimer', () => {
    beforeEach(() => {
      // Mock performance.now for predictable test results
      vi.spyOn(performance, 'now')
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('returns elapsed time', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValueOnce(1000) // Constructor call
      mockNow.mockReturnValueOnce(1050) // elapsed() call

      const timer = new MetricTimer()
      const elapsed = timer.elapsed()

      expect(elapsed).toBe(50)
    })

    it('returns cumulative elapsed time on multiple calls', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValueOnce(1000) // Constructor call
      mockNow.mockReturnValueOnce(1025) // First elapsed() call
      mockNow.mockReturnValueOnce(1075) // Second elapsed() call

      const timer = new MetricTimer()

      expect(timer.elapsed()).toBe(25)
      expect(timer.elapsed()).toBe(75)
    })

    it('resets the timer and returns elapsed time', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValueOnce(1000) // Constructor call
      mockNow.mockReturnValueOnce(1100) // reset() call - gets elapsed
      mockNow.mockReturnValueOnce(2000) // reset() call - sets new start
      mockNow.mockReturnValueOnce(2050) // elapsed() call after reset

      const timer = new MetricTimer()

      const resetTime = timer.reset()
      expect(resetTime).toBe(100)

      const elapsedAfterReset = timer.elapsed()
      expect(elapsedAfterReset).toBe(50)
    })

    it('handles zero elapsed time', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValue(1000) // Same time for all calls

      const timer = new MetricTimer()
      expect(timer.elapsed()).toBe(0)
    })

    it('handles very small elapsed times (microseconds)', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValueOnce(1000.000) // Constructor call
      mockNow.mockReturnValueOnce(1000.001) // elapsed() call

      const timer = new MetricTimer()
      expect(timer.elapsed()).toBeCloseTo(0.001, 3)
    })

    it('handles very large elapsed times', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValueOnce(0) // Constructor call
      mockNow.mockReturnValueOnce(3600000) // 1 hour in ms

      const timer = new MetricTimer()
      expect(timer.elapsed()).toBe(3600000)
    })

    it('works correctly in real-time scenario', async () => {
      // Restore real performance.now for this test
      vi.restoreAllMocks()

      const timer = new MetricTimer()

      // Wait a small amount of time
      await new Promise((resolve) => setTimeout(resolve, 10))

      const elapsed = timer.elapsed()

      // Should be at least 10ms (allowing some margin)
      expect(elapsed).toBeGreaterThanOrEqual(9)
      expect(elapsed).toBeLessThan(100) // Sanity check
    })

    it('reset returns correct value and starts fresh', () => {
      const mockNow = vi.mocked(performance.now)
      mockNow.mockReturnValueOnce(0) // Constructor
      mockNow.mockReturnValueOnce(100) // First elapsed check before reset
      mockNow.mockReturnValueOnce(100) // reset() - gets elapsed
      mockNow.mockReturnValueOnce(100) // reset() - sets new start
      mockNow.mockReturnValueOnce(150) // elapsed after reset

      const timer = new MetricTimer()

      // Check elapsed before reset
      expect(timer.elapsed()).toBe(100)

      // Reset should return elapsed and restart
      const resetValue = timer.reset()
      expect(resetValue).toBe(100)

      // After reset, timer starts fresh
      expect(timer.elapsed()).toBe(50)
    })
  })

  // ============================================================================
  // Error Suppression Tests
  // ============================================================================

  describe('Error Suppression', () => {
    it('suppresses synchronous errors from writeDataPoint', () => {
      const analytics = createMockAnalytics()
      analytics.writeDataPoint.mockImplementation(() => {
        throw new Error('Sync error')
      })

      expect(() => {
        writeMetric(analytics, {
          namespace: 'ingest',
          operation: 'batch',
          status: 'success',
        }, {
          count: 10,
        })
      }).not.toThrow()
    })

    it('suppresses TypeError from writeDataPoint', () => {
      const analytics = createMockAnalytics()
      analytics.writeDataPoint.mockImplementation(() => {
        throw new TypeError('Cannot read property')
      })

      expect(() => {
        writeMetric(analytics, {
          namespace: 'queue',
          operation: 'message',
          status: 'error',
        }, {
          count: 5,
        })
      }).not.toThrow()
    })

    it('suppresses RangeError from writeDataPoint', () => {
      const analytics = createMockAnalytics()
      analytics.writeDataPoint.mockImplementation(() => {
        throw new RangeError('Maximum call stack exceeded')
      })

      expect(() => {
        writeMetric(analytics, {
          namespace: 'cdc',
          operation: 'process',
          status: 'success',
        }, {
          count: 1,
        })
      }).not.toThrow()
    })

    it('convenience functions also suppress errors', () => {
      const analytics = createMockAnalytics()
      analytics.writeDataPoint.mockImplementation(() => {
        throw new Error('Service unavailable')
      })

      // All should not throw
      expect(() => recordIngestMetric(analytics, 'success', 10, 5)).not.toThrow()
      expect(() => recordQueueMetric(analytics, 'batch', 'success', { events: 10 }, 5)).not.toThrow()
      expect(() => recordCDCMetric(analytics, 'success', 10)).not.toThrow()
      expect(() => recordSubscriptionMetric(analytics, 'success', 10)).not.toThrow()
      expect(() => recordR2WriteMetric(analytics, 'success', 10, 1024, 5)).not.toThrow()
      expect(() => recordWriterDOMetric(analytics, 'ingest', 'success', { events: 10 }, 5)).not.toThrow()
    })
  })

  // ============================================================================
  // Edge Cases
  // ============================================================================

  describe('Edge Cases', () => {
    it('handles special characters in context', () => {
      const analytics = createMockAnalytics()

      writeMetric(analytics, {
        namespace: 'subscription',
        operation: 'fanout',
        status: 'success',
        context: 'shard-with-special-chars_123',
        errorType: 'error/with/slashes',
      }, {
        count: 1,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['subscription', 'fanout', 'success', 'error/with/slashes', 'shard-with-special-chars_123'],
        doubles: [1, 0, 0, 0],
        indexes: ['subscription:fanout'],
      })
    })

    it('handles very long context strings', () => {
      const analytics = createMockAnalytics()
      const longContext = 'a'.repeat(1000)

      writeMetric(analytics, {
        namespace: 'r2_write',
        operation: 'flush',
        status: 'success',
        context: longContext,
      }, {
        count: 1,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['r2_write', 'flush', 'success', '', longContext],
        doubles: [1, 0, 0, 0],
        indexes: ['r2_write:flush'],
      })
    })

    it('handles negative numbers (edge case)', () => {
      const analytics = createMockAnalytics()

      writeMetric(analytics, {
        namespace: 'ingest',
        operation: 'batch',
        status: 'success',
      }, {
        count: -1, // Unusual but possible
        latencyMs: -100, // Invalid but should not crash
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'success', '', ''],
        doubles: [-1, -100, 0, 0],
        indexes: ['ingest:batch'],
      })
    })

    it('handles floating point values', () => {
      const analytics = createMockAnalytics()

      writeMetric(analytics, {
        namespace: 'queue',
        operation: 'batch',
        status: 'success',
      }, {
        count: 10.5,
        latencyMs: 25.123456,
        bytes: 1024.999,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['queue', 'batch', 'success', '', ''],
        doubles: [10.5, 25.123456, 1024.999, 0],
        indexes: ['queue:batch'],
      })
    })

    it('handles Infinity values', () => {
      const analytics = createMockAnalytics()

      writeMetric(analytics, {
        namespace: 'writer_do',
        operation: 'flush',
        status: 'error',
      }, {
        count: Infinity,
        latencyMs: Infinity,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['writer_do', 'flush', 'error', '', ''],
        doubles: [Infinity, Infinity, 0, 0],
        indexes: ['writer_do:flush'],
      })
    })

    it('handles NaN values', () => {
      const analytics = createMockAnalytics()

      writeMetric(analytics, {
        namespace: 'cdc',
        operation: 'process',
        status: 'error',
      }, {
        count: NaN,
        latencyMs: NaN,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['cdc', 'process', 'error', '', ''],
        doubles: [NaN, NaN, 0, 0],
        indexes: ['cdc:process'],
      })
    })

    it('handles empty strings for optional dimensions', () => {
      const analytics = createMockAnalytics()

      writeMetric(analytics, {
        namespace: 'ingest',
        operation: 'batch',
        status: 'success',
        errorType: '',
        context: '',
      }, {
        count: 1,
      })

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['ingest', 'batch', 'success', '', ''],
        doubles: [1, 0, 0, 0],
        indexes: ['ingest:batch'],
      })
    })
  })
})
