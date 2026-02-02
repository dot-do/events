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
} from './types'

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validate a single event has required fields
 */
export function validateEvent(event: unknown): event is { type: string; ts: string } {
  if (typeof event !== 'object' || event === null) return false
  const e = event as Record<string, unknown>
  if (typeof e.type !== 'string' || e.type.length === 0 || e.type.length > MAX_TYPE_LENGTH) return false
  if (typeof e.ts !== 'string' || isNaN(Date.parse(e.ts))) return false
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
// Validation Middleware
// ============================================================================

/**
 * Parse JSON body middleware
 */
export const parseJsonMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const timer = new MetricTimer()

  try {
    context.rawBody = await context.request.json()
    return { continue: true }
  } catch (err) {
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
          `Invalid events at indices: ${invalidEvents.slice(0, 10).join(', ')}${invalidEvents.length > 10 ? '...' : ''}. Each event must have type (string, 1-256 chars) and ts (valid ISO timestamp)`,
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
