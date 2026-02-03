/**
 * Validation middleware for ingest
 *
 * Validates:
 * - JSON parsing
 * - Batch structure (events array, max size)
 * - Individual event structure (type, ts fields)
 * - Event size limits
 */

import type { EventBatch } from '@dotdo/events'
import {
  InvalidJsonError,
  InvalidBatchError,
  InvalidEventError,
  EventTooLargeError,
  PayloadTooLargeError,
  toErrorResponse,
} from '../../../core/src/errors'
import { corsHeaders } from '../../utils'
import { recordIngestMetric, MetricTimer } from '../../metrics'
import type {
  IngestContext,
  IngestMiddleware,
  MiddlewareResult,
} from './types'
import {
  MAX_EVENT_SIZE,
  MAX_BATCH_SIZE,
  MAX_TYPE_LENGTH,
  EVENT_TYPE_PATTERN,
  DEFAULT_MAX_BODY_SIZE,
  MAX_TIMESTAMP_AGE_MS,
  MAX_TIMESTAMP_FUTURE_MS,
} from './types'

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validate event type follows strict character allowlist.
 * Only allows: alphanumeric, dots, underscores, hyphens.
 * Max length: 128 characters.
 *
 * This prevents injection attacks and parsing issues.
 */
export function isValidEventType(type: unknown): type is string {
  if (typeof type !== 'string') return false
  if (type.length === 0 || type.length > MAX_TYPE_LENGTH) return false
  return EVENT_TYPE_PATTERN.test(type)
}

/**
 * Validate event timestamp is parseable and within reasonable bounds.
 * Rejects timestamps that are:
 * - Unparseable (invalid ISO format)
 * - Too old (more than 7 days in the past)
 * - Too far in the future (more than 1 hour ahead)
 *
 * @param ts - The timestamp string to validate
 * @returns True if timestamp is valid and within bounds
 */
export function isValidTimestamp(ts: unknown): ts is string {
  if (typeof ts !== 'string') return false

  const timestamp = Date.parse(ts)
  if (isNaN(timestamp)) return false

  const now = Date.now()
  const minTimestamp = now - MAX_TIMESTAMP_AGE_MS
  const maxTimestamp = now + MAX_TIMESTAMP_FUTURE_MS

  return timestamp >= minTimestamp && timestamp <= maxTimestamp
}

/**
 * Validate a single event has required fields
 */
export function validateEvent(event: unknown): event is { type: string; ts: string } {
  if (typeof event !== 'object' || event === null) return false
  const e = event as Record<string, unknown>
  if (!isValidEventType(e.type)) return false
  if (!isValidTimestamp(e.ts)) return false
  return true
}

/**
 * Validate batch structure
 */
export function validateBatch(batch: unknown): batch is { events: unknown[] } {
  if (typeof batch !== 'object' || batch === null) return false
  const b = batch as Record<string, unknown>
  if (!Array.isArray(b.events)) return false
  if (b.events.length > MAX_BATCH_SIZE) return false
  return true
}

/**
 * Validate event size
 */
export function validateEventSize(event: unknown): boolean {
  const size = JSON.stringify(event).length
  return size <= MAX_EVENT_SIZE
}

// ============================================================================
// Body Size Utilities
// ============================================================================

/**
 * Get the max body size from environment or use default.
 * @param env - Environment bindings
 * @param defaultSize - Default size if not configured
 * @returns Max body size in bytes
 */
export function getMaxBodySize(env: { MAX_INGEST_BODY_SIZE?: string }, defaultSize: number = DEFAULT_MAX_BODY_SIZE): number {
  if (env.MAX_INGEST_BODY_SIZE) {
    const parsed = parseInt(env.MAX_INGEST_BODY_SIZE, 10)
    if (!isNaN(parsed) && parsed > 0) {
      return parsed
    }
  }
  return defaultSize
}

/**
 * Check Content-Length header against max size limit.
 * Returns the content length if valid, or throws PayloadTooLargeError.
 *
 * @param request - The incoming request
 * @param maxSize - Maximum allowed body size in bytes
 * @returns Content length if provided and valid
 * @throws PayloadTooLargeError if content length exceeds limit
 */
export function checkContentLength(request: Request, maxSize: number): number | undefined {
  const contentLength = request.headers.get('content-length')
  if (contentLength) {
    const length = parseInt(contentLength, 10)
    if (!isNaN(length) && length > maxSize) {
      throw new PayloadTooLargeError(
        `Request body too large: ${length} bytes exceeds ${maxSize} byte limit`,
        { maxSize, contentLength: length }
      )
    }
    return length
  }
  return undefined
}

/**
 * Read request body with streaming size limit.
 * Reads chunks until either the body is complete or the size limit is exceeded.
 *
 * @param request - The incoming request
 * @param maxSize - Maximum allowed body size in bytes
 * @returns The body as a string
 * @throws PayloadTooLargeError if body exceeds limit during streaming
 */
export async function readBodyWithLimit(request: Request, maxSize: number): Promise<string> {
  // First check Content-Length header if available
  checkContentLength(request, maxSize)

  const body = request.body
  if (!body) {
    return ''
  }

  const reader = body.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  let totalSize = 0

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      totalSize += value.length
      if (totalSize > maxSize) {
        // Cancel the stream to free resources
        await reader.cancel()
        throw new PayloadTooLargeError(
          `Request body too large: exceeds ${maxSize} byte limit`,
          { maxSize, contentLength: totalSize }
        )
      }

      chunks.push(decoder.decode(value, { stream: true }))
    }

    // Flush any remaining bytes in the decoder
    chunks.push(decoder.decode())
    return chunks.join('')
  } finally {
    reader.releaseLock()
  }
}

// ============================================================================
// Validation Middleware
// ============================================================================

/**
 * Parse JSON body middleware with size limit checking.
 * Checks Content-Length header first, then uses streaming read with limit.
 */
export const parseJsonMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const timer = new MetricTimer()
  const maxSize = getMaxBodySize(context.env)

  try {
    // Read body with size limit
    const bodyText = await readBodyWithLimit(context.request, maxSize)

    // Parse JSON
    context.rawBody = JSON.parse(bodyText)
    return { continue: true }
  } catch (err) {
    // Handle payload too large error
    if (err instanceof PayloadTooLargeError) {
      recordIngestMetric(context.env.ANALYTICS, 'validation_error', 0, timer.elapsed(), 'payload_too_large')
      return {
        continue: false,
        response: toErrorResponse(err, { headers: corsHeaders() }),
      }
    }

    // Handle JSON parse error
    recordIngestMetric(context.env.ANALYTICS, 'validation_error', 0, timer.elapsed(), 'invalid_json')
    return {
      continue: false,
      response: toErrorResponse(
        new InvalidJsonError('Invalid JSON', err instanceof Error ? err : undefined),
        { headers: corsHeaders() }
      ),
    }
  }
}

/**
 * Validate batch structure middleware
 */
export const validateBatchMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const elapsed = performance.now() - context.startTime

  if (!validateBatch(context.rawBody)) {
    recordIngestMetric(context.env.ANALYTICS, 'validation_error', 0, elapsed, 'invalid_batch')
    return {
      continue: false,
      response: toErrorResponse(new InvalidBatchError(), { headers: corsHeaders() }),
    }
  }

  // Store validated batch for downstream middleware
  context.batch = context.rawBody as EventBatch

  // Extract optional batchId for deduplication
  context.batchId = (context.rawBody as Record<string, unknown>).batchId as string | undefined

  return { continue: true }
}

/**
 * Validate individual events middleware
 */
export const validateEventsMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const elapsed = performance.now() - context.startTime
  const batch = context.batch!

  const invalidEvents: number[] = []
  const oversizedEvents: number[] = []

  for (let i = 0; i < batch.events.length; i++) {
    const event = batch.events[i]
    if (!validateEvent(event)) {
      invalidEvents.push(i)
    } else if (!validateEventSize(event)) {
      oversizedEvents.push(i)
    }
  }

  if (invalidEvents.length > 0) {
    recordIngestMetric(
      context.env.ANALYTICS,
      'validation_error',
      batch.events.length,
      elapsed,
      'invalid_events'
    )
    return {
      continue: false,
      response: toErrorResponse(
        new InvalidEventError(
          `Invalid events at indices: ${invalidEvents.slice(0, 10).join(', ')}${invalidEvents.length > 10 ? '...' : ''}. Each event must have type (string, 1-${MAX_TYPE_LENGTH} chars, alphanumeric/dots/underscores/hyphens only) and ts (valid ISO timestamp within last 7 days and not more than 1 hour in the future)`,
          { indices: invalidEvents.slice(0, 10) }
        ),
        { headers: corsHeaders() }
      ),
    }
  }

  if (oversizedEvents.length > 0) {
    recordIngestMetric(
      context.env.ANALYTICS,
      'validation_error',
      batch.events.length,
      elapsed,
      'oversized_events'
    )
    return {
      continue: false,
      response: toErrorResponse(
        new EventTooLargeError(
          `Events exceed max size (10KB) at indices: ${oversizedEvents.slice(0, 10).join(', ')}${oversizedEvents.length > 10 ? '...' : ''}`,
          { indices: oversizedEvents.slice(0, 10), maxSize: MAX_EVENT_SIZE }
        ),
        { headers: corsHeaders() }
      ),
    }
  }

  return { continue: true }
}
