/**
 * @dotdo/events-node - Node.js SDK for events.do
 *
 * HTTP client with batching, retry logic, and backpressure handling
 * for sending analytics events to events.do
 *
 * @example
 * ```typescript
 * import { EventsClient } from '@dotdo/events-node'
 *
 * const client = new EventsClient({
 *   apiKey: 'your-api-key'
 * })
 *
 * // Track events
 * client.track('signup', { plan: 'pro' })
 * client.identify('user-123', { email: 'user@example.com' })
 *
 * // Graceful shutdown
 * await client.shutdown()
 * ```
 */

export { EventsClient } from './client.js'

// Re-export all types
export type {
  Event,
  TrackEvent,
  PageEvent,
  IdentifyEvent,
  CustomEvent,
  BaseEvent,
  EventBatch,
  EventsClientConfig,
  SendResult,
  RetryBatch,
  ClientStats,
} from './types.js'

export { EventBufferFullError } from './types.js'

// Default configuration values for reference
export const DEFAULTS = {
  endpoint: 'https://events.workers.do/e',
  batchSize: 100,
  flushIntervalMs: 1000,
  maxRetries: 3,
  retryDelayMs: 1000,
  maxRetryDelayMs: 60000,
  maxRetryQueueSize: 10000,
  timeoutMs: 30000,
} as const
