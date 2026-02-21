/**
 * Type definitions for @dotdo/events-node
 */

/** Event types supported by the SDK */
export type EventType = 'track' | 'page' | 'identify' | 'custom'

/** Base event structure */
export interface BaseEvent {
  /** Event type */
  type: EventType | string
  /** ISO timestamp (auto-generated if not provided) */
  ts?: string
  /** Event properties/data */
  properties?: Record<string, unknown>
}

/** Track event - for custom analytics events */
export interface TrackEvent extends BaseEvent {
  type: 'track'
  /** Event name */
  event: string
  /** Event properties */
  properties?: Record<string, unknown>
  /** User ID (optional) */
  userId?: string
  /** Anonymous ID (optional) */
  anonymousId?: string
}

/** Page view event */
export interface PageEvent extends BaseEvent {
  type: 'page'
  /** Page name */
  name?: string
  /** Page category */
  category?: string
  /** Page properties */
  properties?: Record<string, unknown>
  /** User ID (optional) */
  userId?: string
  /** Anonymous ID (optional) */
  anonymousId?: string
}

/** Identify event - for user identification */
export interface IdentifyEvent extends BaseEvent {
  type: 'identify'
  /** User ID */
  userId: string
  /** User traits */
  traits?: Record<string, unknown>
}

/** Custom event with user-defined type */
export interface CustomEvent extends BaseEvent {
  type: `custom.${string}`
  /** Custom event data */
  data?: Record<string, unknown>
}

/** Union of all event types */
export type Event = TrackEvent | PageEvent | IdentifyEvent | CustomEvent | BaseEvent

/** Batch of events for sending */
export interface EventBatch {
  events: Event[]
  /** Optional batch ID for deduplication */
  batchId?: string
}

/** Configuration options for EventsClient */
export interface EventsClientConfig {
  /**
   * API endpoint URL
   * @default 'https://events.workers.do/e'
   */
  endpoint?: string

  /**
   * API key for authentication
   * Required unless the server allows unauthenticated requests
   */
  apiKey?: string

  /**
   * Number of events to batch before auto-flush
   * @default 100
   */
  batchSize?: number

  /**
   * Interval in ms to auto-flush pending events
   * @default 1000
   */
  flushIntervalMs?: number

  /**
   * Maximum number of retry attempts for failed requests
   * @default 3
   */
  maxRetries?: number

  /**
   * Base delay in ms for exponential backoff
   * @default 1000
   */
  retryDelayMs?: number

  /**
   * Maximum delay in ms for retry backoff
   * @default 60000
   */
  maxRetryDelayMs?: number

  /**
   * Maximum events to hold in retry queue
   * @default 10000
   */
  maxRetryQueueSize?: number

  /**
   * Request timeout in ms
   * @default 30000
   */
  timeoutMs?: number

  /**
   * Callback when events are successfully sent
   */
  onSuccess?: (events: Event[], response: SendResult) => void

  /**
   * Callback when events fail to send after all retries
   */
  onError?: (error: Error, events: Event[]) => void

  /**
   * Callback when events are dropped due to backpressure
   */
  onDrop?: (events: Event[], reason: string) => void

  /**
   * Enable debug logging
   * @default false
   */
  debug?: boolean
}

/** Result of sending events */
export interface SendResult {
  /** Whether the request was successful */
  ok: boolean
  /** Number of events received by server */
  received?: number
  /** Namespace the events were ingested to */
  namespace?: string
  /** Error message if failed */
  error?: string
  /** HTTP status code */
  status?: number
}

/** Pending retry batch */
export interface RetryBatch {
  /** Events to retry */
  events: Event[]
  /** Number of retry attempts */
  attempts: number
  /** Timestamp for next retry */
  nextRetry: number
}

/** Client statistics */
export interface ClientStats {
  /** Events currently queued */
  queuedCount: number
  /** Events pending retry */
  retryCount: number
  /** Total events sent successfully */
  sentCount: number
  /** Total events dropped */
  droppedCount: number
  /** Total retry attempts */
  totalRetries: number
}

/** Error thrown when the retry buffer is full */
export class EventBufferFullError extends Error {
  constructor(
    message: string,
    public readonly droppedCount: number
  ) {
    super(message)
    this.name = 'EventBufferFullError'
  }
}
