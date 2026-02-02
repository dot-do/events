/**
 * Prometheus Metrics Endpoint
 *
 * Provides a Prometheus-compatible /metrics endpoint for observability.
 * Exposes key metrics in Prometheus text format:
 * - events_ingested_total - Total events ingested
 * - events_delivered_total - Total events delivered to subscriptions
 * - ingest_latency_seconds - Histogram of ingestion latency
 * - delivery_latency_seconds - Histogram of delivery latency
 * - dead_letters_total - Total dead-lettered events
 * - active_subscriptions - Gauge of active subscriptions
 * - http_requests_total - Total HTTP requests by method, path, and status
 * - http_request_duration_seconds - HTTP request latency histogram
 * - ingest_errors_total - Total ingest errors by type
 * - event_throughput_rate - Events ingested per second (gauge)
 *
 * Metrics are collected from SubscriptionDOs and aggregated.
 */

import type { Env } from '../env'
import type { SubscriptionDO } from '../../core/src/subscription'
import { KNOWN_SUBSCRIPTION_SHARDS } from '../subscription-routes'

/**
 * HTTP request metrics by method, path pattern, and status code
 */
interface HttpRequestMetric {
  method: string
  path: string
  status: number
  count: number
  latencySum: number
}

/**
 * Error metrics by type
 */
interface ErrorMetric {
  type: string
  count: number
}

/**
 * In-memory metrics store for request-scoped counters.
 * These are reset on each worker isolate restart.
 * For persistent metrics, use Analytics Engine queries.
 */
interface MetricsSnapshot {
  eventsIngestedTotal: number
  eventsDeliveredTotal: number
  ingestLatencySum: number
  ingestLatencyCount: number
  deliveryLatencySum: number
  deliveryLatencyCount: number
  deadLettersTotal: number
  activeSubscriptions: number
  lastUpdated: number
  // New metrics for request counts, error rates, and throughput
  httpRequests: Map<string, HttpRequestMetric>
  ingestErrors: Map<string, ErrorMetric>
  // Throughput tracking
  throughputWindowStart: number
  throughputEventCount: number
}

// Global metrics store (per-isolate)
let metricsSnapshot: MetricsSnapshot = {
  eventsIngestedTotal: 0,
  eventsDeliveredTotal: 0,
  ingestLatencySum: 0,
  ingestLatencyCount: 0,
  deliveryLatencySum: 0,
  deliveryLatencyCount: 0,
  deadLettersTotal: 0,
  activeSubscriptions: 0,
  lastUpdated: 0,
  httpRequests: new Map(),
  ingestErrors: new Map(),
  throughputWindowStart: Date.now(),
  throughputEventCount: 0,
}

/**
 * Increment events ingested counter
 */
export function incrementEventsIngested(count: number, latencyMs: number): void {
  metricsSnapshot.eventsIngestedTotal += count
  metricsSnapshot.ingestLatencySum += latencyMs / 1000 // Convert to seconds
  metricsSnapshot.ingestLatencyCount += 1
  metricsSnapshot.lastUpdated = Date.now()
}

/**
 * Increment events delivered counter
 */
export function incrementEventsDelivered(count: number, latencyMs: number): void {
  metricsSnapshot.eventsDeliveredTotal += count
  metricsSnapshot.deliveryLatencySum += latencyMs / 1000 // Convert to seconds
  metricsSnapshot.deliveryLatencyCount += 1
  metricsSnapshot.lastUpdated = Date.now()
}

/**
 * Increment dead letters counter
 */
export function incrementDeadLetters(count: number): void {
  metricsSnapshot.deadLettersTotal += count
  metricsSnapshot.lastUpdated = Date.now()
}

/**
 * Record HTTP request metrics
 */
export function recordHttpRequest(
  method: string,
  path: string,
  status: number,
  latencyMs: number
): void {
  // Normalize path to pattern (e.g., /subscriptions/123 -> /subscriptions/:id)
  const normalizedPath = normalizePath(path)
  const key = `${method}:${normalizedPath}:${status}`

  const existing = metricsSnapshot.httpRequests.get(key)
  if (existing) {
    existing.count += 1
    existing.latencySum += latencyMs / 1000
  } else {
    metricsSnapshot.httpRequests.set(key, {
      method,
      path: normalizedPath,
      status,
      count: 1,
      latencySum: latencyMs / 1000,
    })
  }
  metricsSnapshot.lastUpdated = Date.now()
}

/**
 * Record ingest error by type
 */
export function recordIngestError(errorType: string): void {
  const existing = metricsSnapshot.ingestErrors.get(errorType)
  if (existing) {
    existing.count += 1
  } else {
    metricsSnapshot.ingestErrors.set(errorType, { type: errorType, count: 1 })
  }
  metricsSnapshot.lastUpdated = Date.now()
}

/**
 * Normalize path to pattern for grouping
 * e.g., /subscriptions/abc123 -> /subscriptions/:id
 */
function normalizePath(path: string): string {
  return path
    // UUID pattern
    .replace(/\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '/:id')
    // Generic ID patterns (alphanumeric strings that look like IDs)
    .replace(/\/SUB[A-Z0-9]+/g, '/:id')
    .replace(/\/[A-Z0-9]{20,}/g, '/:id')
    // Numeric IDs
    .replace(/\/\d+/g, '/:id')
}

/**
 * Get active subscription count from all subscription DOs
 */
async function getActiveSubscriptionCount(env: Pick<Env, 'SUBSCRIPTIONS'>): Promise<number> {
  let totalActive = 0

  try {
    // Query all known subscription shards in parallel
    const results = await Promise.allSettled(
      KNOWN_SUBSCRIPTION_SHARDS.map(async (shard) => {
        const id = env.SUBSCRIPTIONS.idFromName(shard)
        const stub = env.SUBSCRIPTIONS.get(id) as DurableObjectStub<SubscriptionDO>
        const subs = await stub.listSubscriptions({ active: true })
        return subs.length
      })
    )

    for (const result of results) {
      if (result.status === 'fulfilled') {
        totalActive += result.value
      }
    }
  } catch {
    // Silently ignore errors - return what we have
  }

  return totalActive
}

/**
 * Get dead letter counts from all subscription DOs
 */
async function getDeadLetterStats(env: Pick<Env, 'SUBSCRIPTIONS'>): Promise<{ total: number; delivered: number }> {
  let totalDeadLetters = 0
  let totalDelivered = 0

  try {
    // Query all known subscription shards in parallel
    const results = await Promise.allSettled(
      KNOWN_SUBSCRIPTION_SHARDS.map(async (shard) => {
        const id = env.SUBSCRIPTIONS.idFromName(shard)
        const stub = env.SUBSCRIPTIONS.get(id) as DurableObjectStub<SubscriptionDO>
        const subs = await stub.listSubscriptions({})

        let shardDeadLetters = 0
        let shardDelivered = 0

        for (const sub of subs) {
          const status = await stub.getSubscriptionStatus(sub.id)
          if (status.stats) {
            shardDeadLetters += status.stats.deadLetters || 0
            shardDelivered += status.stats.totalDelivered || 0
          }
        }

        return { deadLetters: shardDeadLetters, delivered: shardDelivered }
      })
    )

    for (const result of results) {
      if (result.status === 'fulfilled') {
        totalDeadLetters += result.value.deadLetters
        totalDelivered += result.value.delivered
      }
    }
  } catch {
    // Silently ignore errors - return what we have
  }

  return { total: totalDeadLetters, delivered: totalDelivered }
}

/**
 * Format a Prometheus metric line
 */
function formatMetric(
  name: string,
  value: number,
  type: 'counter' | 'gauge' | 'histogram',
  help: string,
  labels?: Record<string, string>
): string {
  const lines: string[] = []

  // Add HELP and TYPE comments
  lines.push(`# HELP ${name} ${help}`)
  lines.push(`# TYPE ${name} ${type}`)

  // Format labels
  let labelStr = ''
  if (labels && Object.keys(labels).length > 0) {
    labelStr = '{' + Object.entries(labels).map(([k, v]) => `${k}="${v}"`).join(',') + '}'
  }

  // Add metric value
  lines.push(`${name}${labelStr} ${value}`)

  return lines.join('\n')
}

/**
 * Handle /metrics endpoint
 * Returns Prometheus-formatted metrics
 */
export async function handleMetrics(
  request: Request,
  env: Pick<Env, 'SUBSCRIPTIONS'>
): Promise<Response> {
  const lines: string[] = []

  // Get live stats from subscription DOs
  const [activeSubscriptions, deadLetterStats] = await Promise.all([
    getActiveSubscriptionCount(env),
    getDeadLetterStats(env),
  ])

  // Update snapshot with live data
  metricsSnapshot.activeSubscriptions = activeSubscriptions
  metricsSnapshot.eventsDeliveredTotal = deadLetterStats.delivered
  metricsSnapshot.deadLettersTotal = deadLetterStats.total

  // events_ingested_total (counter)
  lines.push(formatMetric(
    'events_ingested_total',
    metricsSnapshot.eventsIngestedTotal,
    'counter',
    'Total number of events ingested'
  ))
  lines.push('')

  // events_delivered_total (counter)
  lines.push(formatMetric(
    'events_delivered_total',
    metricsSnapshot.eventsDeliveredTotal,
    'counter',
    'Total number of events delivered to subscriptions'
  ))
  lines.push('')

  // ingest_latency_seconds (histogram summary)
  lines.push('# HELP ingest_latency_seconds Latency of event ingestion in seconds')
  lines.push('# TYPE ingest_latency_seconds summary')
  const avgIngestLatency = metricsSnapshot.ingestLatencyCount > 0
    ? metricsSnapshot.ingestLatencySum / metricsSnapshot.ingestLatencyCount
    : 0
  lines.push(`ingest_latency_seconds_sum ${metricsSnapshot.ingestLatencySum}`)
  lines.push(`ingest_latency_seconds_count ${metricsSnapshot.ingestLatencyCount}`)
  lines.push('')

  // delivery_latency_seconds (histogram summary)
  lines.push('# HELP delivery_latency_seconds Latency of event delivery in seconds')
  lines.push('# TYPE delivery_latency_seconds summary')
  const avgDeliveryLatency = metricsSnapshot.deliveryLatencyCount > 0
    ? metricsSnapshot.deliveryLatencySum / metricsSnapshot.deliveryLatencyCount
    : 0
  lines.push(`delivery_latency_seconds_sum ${metricsSnapshot.deliveryLatencySum}`)
  lines.push(`delivery_latency_seconds_count ${metricsSnapshot.deliveryLatencyCount}`)
  lines.push('')

  // dead_letters_total (counter)
  lines.push(formatMetric(
    'dead_letters_total',
    metricsSnapshot.deadLettersTotal,
    'counter',
    'Total number of dead-lettered events'
  ))
  lines.push('')

  // active_subscriptions (gauge)
  lines.push(formatMetric(
    'active_subscriptions',
    metricsSnapshot.activeSubscriptions,
    'gauge',
    'Current number of active subscriptions'
  ))
  lines.push('')

  // Add metadata
  lines.push('# HELP events_metrics_last_updated_timestamp_seconds Unix timestamp of last metrics update')
  lines.push('# TYPE events_metrics_last_updated_timestamp_seconds gauge')
  lines.push(`events_metrics_last_updated_timestamp_seconds ${Math.floor(metricsSnapshot.lastUpdated / 1000)}`)

  return new Response(lines.join('\n') + '\n', {
    headers: {
      'Content-Type': 'text/plain; version=0.0.4; charset=utf-8',
    },
  })
}

/**
 * Reset metrics (for testing)
 */
export function resetMetrics(): void {
  metricsSnapshot = {
    eventsIngestedTotal: 0,
    eventsDeliveredTotal: 0,
    ingestLatencySum: 0,
    ingestLatencyCount: 0,
    deliveryLatencySum: 0,
    deliveryLatencyCount: 0,
    deadLettersTotal: 0,
    activeSubscriptions: 0,
    lastUpdated: 0,
  }
}

/**
 * Get current metrics snapshot (for testing)
 */
export function getMetricsSnapshot(): MetricsSnapshot {
  return { ...metricsSnapshot }
}
