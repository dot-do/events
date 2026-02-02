/**
 * Metrics - Cloudflare Analytics Engine integration for observability
 *
 * Provides metrics collection for:
 * - Event ingestion (count, latency, errors)
 * - Queue processing (batches, events, latency)
 * - CDC/subscription delivery (fanouts, errors)
 * - R2 writes (count, bytes, latency)
 *
 * All metrics are sent to Cloudflare Analytics Engine for querying via GraphQL.
 */

/**
 * Analytics Engine binding interface
 * See: https://developers.cloudflare.com/analytics/analytics-engine/
 */
export interface AnalyticsEngineDataset {
  writeDataPoint(data: {
    blobs?: string[]
    doubles?: number[]
    indexes?: string[]
  }): void
}

/**
 * Metrics namespace for indexing in Analytics Engine
 */
export type MetricNamespace =
  | 'ingest'      // Event ingestion metrics
  | 'queue'       // Queue processing metrics
  | 'cdc'         // CDC processor metrics
  | 'subscription' // Subscription fanout metrics
  | 'r2_write'    // R2 write metrics
  | 'writer_do'   // EventWriterDO metrics

/**
 * Standard metric dimensions (stored as blobs)
 */
export interface MetricDimensions {
  /** Metric namespace (ingest, queue, cdc, etc.) */
  namespace: MetricNamespace
  /** Operation name (e.g., "batch", "fanout", "flush") */
  operation: string
  /** Status (success, error, rate_limited, etc.) */
  status: 'success' | 'error' | 'rate_limited' | 'validation_error' | 'backpressure'
  /** Optional error type for failures */
  errorType?: string
  /** Optional additional context (e.g., shard ID, provider) */
  context?: string
}

/**
 * Standard metric values (stored as doubles)
 */
export interface MetricValues {
  /** Number of items processed (events, batches, etc.) */
  count: number
  /** Operation latency in milliseconds */
  latencyMs?: number
  /** Size in bytes (for R2 writes) */
  bytes?: number
  /** Error count (for partial failures) */
  errors?: number
}

/**
 * Write a metric data point to Analytics Engine
 */
export function writeMetric(
  analytics: AnalyticsEngineDataset | undefined,
  dimensions: MetricDimensions,
  values: MetricValues
): void {
  if (!analytics) {
    return // Silently skip if analytics not configured
  }

  try {
    analytics.writeDataPoint({
      // Blobs: categorical/string dimensions for filtering
      blobs: [
        dimensions.namespace,
        dimensions.operation,
        dimensions.status,
        dimensions.errorType || '',
        dimensions.context || '',
      ],
      // Doubles: numeric values for aggregation
      doubles: [
        values.count,
        values.latencyMs ?? 0,
        values.bytes ?? 0,
        values.errors ?? 0,
      ],
      // Index: primary index for fast filtering (namespace + operation)
      indexes: [`${dimensions.namespace}:${dimensions.operation}`],
    })
  } catch (err) {
    // Don't let metrics failures affect the main code path
    console.warn('[metrics] Failed to write data point:', err)
  }
}

// ============================================================================
// Convenience functions for specific metric types
// ============================================================================

/**
 * Record event ingestion metrics
 */
export function recordIngestMetric(
  analytics: AnalyticsEngineDataset | undefined,
  status: MetricDimensions['status'],
  eventCount: number,
  latencyMs: number,
  errorType?: string
): void {
  writeMetric(analytics, {
    namespace: 'ingest',
    operation: 'batch',
    status,
    errorType,
  }, {
    count: eventCount,
    latencyMs,
  })
}

/**
 * Record queue processing metrics
 */
export function recordQueueMetric(
  analytics: AnalyticsEngineDataset | undefined,
  operation: 'batch' | 'message' | 'dead_letter',
  status: MetricDimensions['status'],
  counts: { messages?: number; events?: number; errors?: number },
  latencyMs: number
): void {
  writeMetric(analytics, {
    namespace: 'queue',
    operation,
    status,
  }, {
    count: counts.events ?? counts.messages ?? 0,
    latencyMs,
    errors: counts.errors,
  })
}

/**
 * Record CDC processing metrics
 */
export function recordCDCMetric(
  analytics: AnalyticsEngineDataset | undefined,
  status: MetricDimensions['status'],
  eventCount: number,
  collection?: string
): void {
  writeMetric(analytics, {
    namespace: 'cdc',
    operation: 'process',
    status,
    context: collection,
  }, {
    count: eventCount,
  })
}

/**
 * Record subscription fanout metrics
 */
export function recordSubscriptionMetric(
  analytics: AnalyticsEngineDataset | undefined,
  status: MetricDimensions['status'],
  eventCount: number,
  shardKey?: string,
  errorType?: string
): void {
  writeMetric(analytics, {
    namespace: 'subscription',
    operation: 'fanout',
    status,
    context: shardKey,
    errorType,
  }, {
    count: eventCount,
  })
}

/**
 * Record R2 write metrics
 */
export function recordR2WriteMetric(
  analytics: AnalyticsEngineDataset | undefined,
  status: MetricDimensions['status'],
  eventCount: number,
  bytes: number,
  latencyMs: number,
  source?: string
): void {
  writeMetric(analytics, {
    namespace: 'r2_write',
    operation: 'flush',
    status,
    context: source,
  }, {
    count: eventCount,
    bytes,
    latencyMs,
  })
}

/**
 * Record EventWriterDO metrics
 */
export function recordWriterDOMetric(
  analytics: AnalyticsEngineDataset | undefined,
  operation: 'ingest' | 'flush' | 'backpressure' | 'restore',
  status: MetricDimensions['status'],
  counts: { events?: number; bytes?: number; shard?: number },
  latencyMs?: number
): void {
  writeMetric(analytics, {
    namespace: 'writer_do',
    operation,
    status,
    context: counts.shard !== undefined ? `shard-${counts.shard}` : undefined,
  }, {
    count: counts.events ?? 0,
    bytes: counts.bytes,
    latencyMs,
  })
}

// ============================================================================
// Timer utility for latency measurement
// ============================================================================

/**
 * Simple timer for measuring operation latency
 */
export class MetricTimer {
  private startTime: number

  constructor() {
    this.startTime = performance.now()
  }

  /**
   * Get elapsed time in milliseconds
   */
  elapsed(): number {
    return performance.now() - this.startTime
  }

  /**
   * Reset the timer and return elapsed time
   */
  reset(): number {
    const elapsed = this.elapsed()
    this.startTime = performance.now()
    return elapsed
  }
}
