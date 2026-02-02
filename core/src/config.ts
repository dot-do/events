/**
 * Core Configuration Constants
 *
 * Centralized configuration for the @dotdo/events core package.
 * All magic numbers and tunable values are defined here.
 */

// ============================================================================
// EventEmitter Configuration
// ============================================================================

/** Default endpoint for event ingestion */
export const DEFAULT_EMITTER_ENDPOINT = 'https://events.workers.do/ingest'

/** Default number of events to batch before auto-flushing */
export const DEFAULT_BATCH_SIZE = 100

/** Default interval (ms) to auto-flush pending events */
export const DEFAULT_FLUSH_INTERVAL_MS = 1000

/** Default maximum events allowed in retry queue */
export const DEFAULT_MAX_RETRY_QUEUE_SIZE = 10000

/** Default consecutive failures before circuit breaker opens */
export const DEFAULT_MAX_CONSECUTIVE_FAILURES = 10

/** Default circuit breaker reset time (ms) - 5 minutes */
export const DEFAULT_CIRCUIT_BREAKER_RESET_MS = 300000

// ============================================================================
// Retry Configuration
// ============================================================================

/** Base delay for exponential backoff (ms) */
export const RETRY_BASE_DELAY_MS = 1000

/** Maximum delay for retry backoff (ms) - 1 minute */
export const RETRY_MAX_DELAY_MS = 60000

/** Maximum jitter to add to retry delay (ms) */
export const RETRY_JITTER_MS = 1000

// ============================================================================
// Subscription Configuration
// ============================================================================

/** Default maximum retries for delivery attempts */
export const DEFAULT_SUBSCRIPTION_MAX_RETRIES = 5

/** Default timeout for delivery RPC/HTTP calls (ms) - 30 seconds */
export const DEFAULT_SUBSCRIPTION_TIMEOUT_MS = 30000

/** Maximum deliveries to process per alarm */
export const SUBSCRIPTION_BATCH_LIMIT = 100

/** Base delay for subscription retry exponential backoff (ms) */
export const SUBSCRIPTION_RETRY_BASE_DELAY_MS = 1000

/** Maximum delay for subscription retry backoff (ms) - 5 minutes */
export const SUBSCRIPTION_RETRY_MAX_DELAY_MS = 300000

// ============================================================================
// Pattern Validation
// ============================================================================

/** Maximum length for subscription patterns */
export const MAX_PATTERN_LENGTH = 256

/** Maximum number of segments in a pattern (split by '.') */
export const MAX_PATTERN_SEGMENTS = 10

/** Regex for allowed characters in patterns */
export const ALLOWED_PATTERN_CHARS = /^[a-zA-Z0-9._*-]+$/

// ============================================================================
// Safety Limits
// ============================================================================

/** Maximum iterations for pattern matching to prevent runaway computation */
export const MAX_PATTERN_MATCH_ITERATIONS = 10000

// ============================================================================
// Storage Keys
// ============================================================================

/** Storage key for pending retry events */
export const STORAGE_KEY_RETRY = '_events:retry'

/** Storage key for retry count */
export const STORAGE_KEY_RETRY_COUNT = '_events:retryCount'

/** Storage key for pending batch (hibernation) */
export const STORAGE_KEY_BATCH = '_events:batch'

/** Storage key for circuit breaker state */
export const STORAGE_KEY_CIRCUIT_BREAKER = '_events:circuitBreaker'

// ============================================================================
// Exported Config Object (for type safety)
// ============================================================================

export const coreConfig = {
  emitter: {
    defaultEndpoint: DEFAULT_EMITTER_ENDPOINT,
    defaultBatchSize: DEFAULT_BATCH_SIZE,
    defaultFlushIntervalMs: DEFAULT_FLUSH_INTERVAL_MS,
    defaultMaxRetryQueueSize: DEFAULT_MAX_RETRY_QUEUE_SIZE,
    defaultMaxConsecutiveFailures: DEFAULT_MAX_CONSECUTIVE_FAILURES,
    defaultCircuitBreakerResetMs: DEFAULT_CIRCUIT_BREAKER_RESET_MS,
  },
  retry: {
    baseDelayMs: RETRY_BASE_DELAY_MS,
    maxDelayMs: RETRY_MAX_DELAY_MS,
    jitterMs: RETRY_JITTER_MS,
  },
  subscription: {
    defaultMaxRetries: DEFAULT_SUBSCRIPTION_MAX_RETRIES,
    defaultTimeoutMs: DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
    batchLimit: SUBSCRIPTION_BATCH_LIMIT,
    retryBaseDelayMs: SUBSCRIPTION_RETRY_BASE_DELAY_MS,
    retryMaxDelayMs: SUBSCRIPTION_RETRY_MAX_DELAY_MS,
  },
  pattern: {
    maxLength: MAX_PATTERN_LENGTH,
    maxSegments: MAX_PATTERN_SEGMENTS,
    allowedChars: ALLOWED_PATTERN_CHARS,
    maxMatchIterations: MAX_PATTERN_MATCH_ITERATIONS,
  },
  storageKeys: {
    retry: STORAGE_KEY_RETRY,
    retryCount: STORAGE_KEY_RETRY_COUNT,
    batch: STORAGE_KEY_BATCH,
    circuitBreaker: STORAGE_KEY_CIRCUIT_BREAKER,
  },
} as const
