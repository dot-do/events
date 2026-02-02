/**
 * Worker Configuration Constants
 *
 * Centralized configuration for the events.do worker.
 * All magic numbers and tunable values are defined here.
 */

// Re-export core config for convenience
export {
  coreConfig,
  // Re-export commonly used core constants
  MAX_PATTERN_LENGTH,
  MAX_PATTERN_SEGMENTS,
  ALLOWED_PATTERN_CHARS,
  DEFAULT_SUBSCRIPTION_MAX_RETRIES,
  DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
} from '../core/src/config'

// ============================================================================
// Ingest Configuration
// ============================================================================

/** Maximum size per event in bytes (10KB) */
export const MAX_EVENT_SIZE = 10 * 1024

/** Maximum number of events per batch */
export const MAX_BATCH_SIZE = 1000

/** Maximum length for event type string */
export const MAX_TYPE_LENGTH = 256

// ============================================================================
// Rate Limiting Configuration
// ============================================================================

/** Default requests per minute limit */
export const DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE = 1000

/** Default events per minute limit */
export const DEFAULT_RATE_LIMIT_EVENTS_PER_MINUTE = 100000

/** Rate limit window duration (ms) - 1 minute */
export const RATE_LIMIT_WINDOW_MS = 60000

/** Rate limit cleanup threshold - keep last N windows (5 minutes) */
export const RATE_LIMIT_CLEANUP_WINDOWS = 5

/** Default retry after when rate limited (seconds) */
export const DEFAULT_RATE_LIMIT_RETRY_AFTER = 60

// ============================================================================
// Timeout Configuration
// ============================================================================

/** Default HTTP fetch timeout (ms) - 30 seconds */
export const DEFAULT_HTTP_TIMEOUT_MS = 30000

/** RPC call timeout (ms) - 30 seconds */
export const DEFAULT_RPC_TIMEOUT_MS = 30000

/** Queue processing timeout (ms) - 60 seconds */
export const QUEUE_PROCESSING_TIMEOUT_MS = 60000

// ============================================================================
// Event Writer Configuration
// ============================================================================

/** Default flush interval for EventWriterDO (ms) - 10 seconds */
export const EVENT_WRITER_FLUSH_INTERVAL_MS = 10000

/** Maximum events to buffer before force flush */
export const EVENT_WRITER_MAX_BUFFER_SIZE = 10000

/** Minimum events before writing (to reduce small file overhead) */
export const EVENT_WRITER_MIN_BATCH_SIZE = 100

// ============================================================================
// Webhook Configuration
// ============================================================================

/** Maximum webhook payload size (bytes) - 1MB */
export const MAX_WEBHOOK_PAYLOAD_SIZE = 1024 * 1024

/** Webhook signature verification timeout (ms) */
export const WEBHOOK_VERIFY_TIMEOUT_MS = 5000

// ============================================================================
// CDC Configuration
// ============================================================================

/** CDC compaction interval (ms) - 1 hour */
export const CDC_COMPACTION_INTERVAL_MS = 60 * 60 * 1000

/** CDC snapshot retention (ms) - 7 days */
export const CDC_SNAPSHOT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

// ============================================================================
// Scheduler Configuration
// ============================================================================

/** Scheduler lock timeout (ms) - 5 minutes */
export const SCHEDULER_LOCK_TIMEOUT_MS = 5 * 60 * 1000

/** Scheduled task interval (ms) - 1 hour */
export const SCHEDULED_TASK_INTERVAL_MS = 60 * 60 * 1000

// ============================================================================
// Exported Config Object (for type safety)
// ============================================================================

export const workerConfig = {
  ingest: {
    maxEventSize: MAX_EVENT_SIZE,
    maxBatchSize: MAX_BATCH_SIZE,
    maxTypeLength: MAX_TYPE_LENGTH,
  },
  rateLimit: {
    defaultRequestsPerMinute: DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE,
    defaultEventsPerMinute: DEFAULT_RATE_LIMIT_EVENTS_PER_MINUTE,
    windowMs: RATE_LIMIT_WINDOW_MS,
    cleanupWindows: RATE_LIMIT_CLEANUP_WINDOWS,
    defaultRetryAfter: DEFAULT_RATE_LIMIT_RETRY_AFTER,
  },
  timeout: {
    http: DEFAULT_HTTP_TIMEOUT_MS,
    rpc: DEFAULT_RPC_TIMEOUT_MS,
    queueProcessing: QUEUE_PROCESSING_TIMEOUT_MS,
  },
  eventWriter: {
    flushIntervalMs: EVENT_WRITER_FLUSH_INTERVAL_MS,
    maxBufferSize: EVENT_WRITER_MAX_BUFFER_SIZE,
    minBatchSize: EVENT_WRITER_MIN_BATCH_SIZE,
  },
  webhook: {
    maxPayloadSize: MAX_WEBHOOK_PAYLOAD_SIZE,
    verifyTimeoutMs: WEBHOOK_VERIFY_TIMEOUT_MS,
  },
  cdc: {
    compactionIntervalMs: CDC_COMPACTION_INTERVAL_MS,
    snapshotRetentionMs: CDC_SNAPSHOT_RETENTION_MS,
  },
  scheduler: {
    lockTimeoutMs: SCHEDULER_LOCK_TIMEOUT_MS,
    taskIntervalMs: SCHEDULED_TASK_INTERVAL_MS,
  },
} as const
