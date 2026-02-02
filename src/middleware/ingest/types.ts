/**
 * Shared types for ingest middleware chain
 */

import type { DurableEvent, EventBatch } from '@dotdo/events'
import type { Env } from '../../env'
import type { TenantContext } from '../tenant'
import type { EventRecord } from '../../event-writer'

// ============================================================================
// Middleware Context
// ============================================================================

/**
 * Context passed through the ingest middleware chain.
 * Each middleware can read and augment this context.
 */
export interface IngestContext {
  /** The original request */
  request: Request

  /** Worker environment bindings */
  env: Env

  /** Worker execution context */
  ctx: ExecutionContext

  /** Authenticated tenant context (set by auth middleware) */
  tenant: TenantContext

  /** Parsed request body */
  rawBody?: unknown

  /** Validated event batch */
  batch?: EventBatch

  /** Optional batch ID for deduplication */
  batchId?: string

  /** Whether the batch was deduplicated (already ingested) */
  deduplicated?: boolean

  /** Events converted to EventRecord format */
  records?: EventRecord[]

  /** Fanout mode determined at the start */
  useQueueFanout?: boolean

  /** Namespace-prefixed source path for events */
  namespacedSource?: string

  /** Timestamp when request processing started */
  startTime: number
}

/**
 * Result of middleware execution
 */
export type MiddlewareResult =
  | { continue: true }
  | { continue: false; response: Response }

/**
 * A single middleware in the ingest chain
 */
export type IngestMiddleware = (
  context: IngestContext
) => Promise<MiddlewareResult>

// ============================================================================
// Validation Constants
// ============================================================================

export const MAX_EVENT_SIZE = 10 * 1024 // 10KB per event
export const MAX_BATCH_SIZE = 1000 // Max events per batch
export const MAX_TYPE_LENGTH = 256

// ============================================================================
// Schema Validation Types
// ============================================================================

/**
 * Schema validation error details returned to callers
 */
export interface SchemaValidationError {
  index: number
  eventType: string
  errors: ValidationError[]
}

/**
 * Validation error from the schema registry
 */
export interface ValidationError {
  path?: string
  message: string
}

/**
 * Type for validation results returned by the schema registry
 */
export interface BatchValidationResult {
  valid: boolean
  results: {
    index: number
    eventType: string
    valid: boolean
    errors: ValidationError[]
  }[]
}
