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
  validateEvent,
  validateBatch,
  validateEventSize,
  parseJsonMiddleware,
  validateBatchMiddleware,
  validateEventsMiddleware,
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
