/**
 * Ingest middleware chain
 *
 * Provides a modular, composable middleware system for the /ingest endpoint.
 *
 * @example
 * ```typescript
 * import {
 *   composeMiddleware,
 *   parseJsonMiddleware,
 *   validateBatchMiddleware,
 *   validateEventsMiddleware,
 *   dedupMiddleware,
 *   schemaValidationMiddleware,
 * } from './middleware/ingest'
 *
 * const pipeline = composeMiddleware([
 *   parseJsonMiddleware,
 *   validateBatchMiddleware,
 *   validateEventsMiddleware,
 *   dedupMiddleware,
 *   schemaValidationMiddleware,
 * ])
 * ```
 */

// Types
export type {
  IngestContext,
  IngestMiddleware,
  MiddlewareResult,
  SchemaValidationError,
  ValidationError,
  BatchValidationResult,
} from './types'

export {
  MAX_EVENT_SIZE,
  MAX_BATCH_SIZE,
  MAX_TYPE_LENGTH,
  EVENT_TYPE_PATTERN,
  DEFAULT_MAX_BODY_SIZE,
  MAX_WEBHOOK_BODY_SIZE,
  MAX_QUERY_BODY_SIZE,
} from './types'

// Composition utilities
export {
  composeMiddleware,
  withErrorHandling,
  conditionalMiddleware,
  fromSimpleFunction,
} from './compose'

// Validation middleware
export {
  isValidEventType,
  validateEvent,
  validateBatch,
  validateEventSize,
  parseJsonMiddleware,
  validateBatchMiddleware,
  validateEventsMiddleware,
  // Body size limit utilities
  getMaxBodySize,
  checkContentLength,
  readBodyWithLimit,
} from './validate'

// Deduplication middleware
export {
  dedupMiddleware,
  writeDedupMarker,
} from './dedup'

// Schema validation middleware
export {
  validateEventsAgainstSchemas,
  schemaValidationMiddleware,
} from './schema'

// Fanout middleware
export {
  determineFanoutMode,
  sendToQueue,
  processCDCEvents,
  processSubscriptionFanout,
  executeDirectFanout,
  executeQueueFanout,
} from './fanout'

// Encryption middleware
export {
  encryptionMiddleware,
  loadKeyStore,
  loadEncryptionConfig,
  decryptEvent,
  decryptEventBatch,
  validateClientEncryption,
  type EncryptionEnv,
  type EncryptedIngestContext,
} from './encryption'
