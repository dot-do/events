/**
 * Schema validation middleware for ingest
 *
 * Validates events against registered JSON schemas (if enabled)
 */

import type { SchemaRegistryDO } from '../../../core/src/schema-registry'
import { SchemaValidationError as SchemaValidationErrorClass, toErrorResponse } from '../../../core/src/errors'
import {
  corsHeaders,
  withTimeout,
  CircuitBreaker,
  CircuitBreakerOpenError,
  TimeoutError,
} from '../../utils'
import { SCHEMA_VALIDATION_TIMEOUT_MS } from '../../config'
import { recordIngestMetric } from '../../metrics'
import { logger, logError } from '../../logger'
import type { Env } from '../../env'
import type {
  IngestContext,
  IngestMiddleware,
  MiddlewareResult,
  SchemaValidationError,
  BatchValidationResult,
} from './types'

// ============================================================================
// Circuit Breaker for Schema Registry
// ============================================================================

/**
 * Circuit breaker for schema registry calls.
 * Shared across all requests to track failures globally.
 */
const schemaRegistryCircuitBreaker = new CircuitBreaker('schema-registry', {
  failureThreshold: 5,
  resetTimeoutMs: 30000,
  successThreshold: 2,
})

/**
 * Get the schema registry circuit breaker (for testing/monitoring)
 */
export function getSchemaRegistryCircuitBreaker(): CircuitBreaker {
  return schemaRegistryCircuitBreaker
}

// ============================================================================
// Schema Validation
// ============================================================================

/**
 * Validate events against registered schemas
 * Returns null if validation passes or is disabled, otherwise returns validation errors
 *
 * Uses timeout protection and circuit breaker to prevent slow schema registry
 * from causing cascading failures.
 */
export async function validateEventsAgainstSchemas(
  events: { type: string; [key: string]: unknown }[],
  namespace: string,
  env: Env,
  timeoutMs: number = SCHEMA_VALIDATION_TIMEOUT_MS
): Promise<SchemaValidationError[] | null> {
  // Check if schema validation is enabled
  if (env.ENABLE_SCHEMA_VALIDATION !== 'true' || !env.SCHEMA_REGISTRY) {
    return null
  }

  // Check circuit breaker state before making the call
  if (schemaRegistryCircuitBreaker.getState() === 'open') {
    logger.warn('Schema registry circuit breaker is open, skipping validation', {
      component: 'schema',
      namespace,
      eventCount: events.length,
    })
    return null
  }

  try {
    // Get schema registry instance for this namespace
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    // Validate all events in batch with timeout and circuit breaker protection
    const result = await schemaRegistryCircuitBreaker.execute(async () => {
      return withTimeout(
        registry.validateEvents(events, namespace) as Promise<BatchValidationResult>,
        timeoutMs,
        `Schema validation timed out after ${timeoutMs}ms`
      )
    })

    if (!result.valid && result.results.length > 0) {
      // Return validation errors that caused rejection
      return result.results
        .filter((r) => !r.valid)
        .map((r) => ({
          index: r.index,
          eventType: r.eventType,
          errors: r.errors,
        }))
    }

    return null
  } catch (err) {
    // Handle timeout and circuit breaker errors specifically
    if (err instanceof TimeoutError) {
      logError(logger, 'Schema validation timed out - allowing events through', err, {
        component: 'schema',
        namespace,
        eventCount: events.length,
        timeoutMs,
      })
    } else if (err instanceof CircuitBreakerOpenError) {
      logger.warn('Schema registry circuit breaker open - allowing events through', {
        component: 'schema',
        namespace,
        eventCount: events.length,
      })
    } else {
      // Log but don't fail on schema registry errors - this allows events to pass through
      // even if the schema registry is unavailable
      logError(logger, 'Schema validation error - allowing events through', err, {
        component: 'schema',
        namespace,
        eventCount: events.length,
      })
    }
    return null
  }
}

// ============================================================================
// Schema Validation Middleware
// ============================================================================

/**
 * Schema validation middleware
 * Validates events against registered JSON schemas for the tenant's namespace
 */
export const schemaValidationMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const elapsed = performance.now() - context.startTime
  const { batch, tenant, env } = context

  const schemaErrors = await validateEventsAgainstSchemas(
    batch!.events as unknown as { type: string; [key: string]: unknown }[],
    tenant.namespace,
    env
  )

  if (schemaErrors && schemaErrors.length > 0) {
    recordIngestMetric(
      env.ANALYTICS,
      'validation_error',
      batch!.events.length,
      elapsed,
      'schema_validation'
    )

    // Format schema validation errors for the response
    const errorSummary = schemaErrors.slice(0, 5).map((e) => {
      const errorMsgs = e.errors
        .slice(0, 3)
        .map((err) => `${err.path || '(root)'}: ${err.message}`)
      return `Event[${e.index}] (${e.eventType}): ${errorMsgs.join('; ')}`
    })

    return {
      continue: false,
      response: toErrorResponse(
        new SchemaValidationErrorClass('Schema validation failed', {
          validationErrors: schemaErrors.slice(0, 10),
          summary: errorSummary,
        }),
        { headers: corsHeaders() }
      ),
    }
  }

  return { continue: true }
}

// Re-export types for use by consumers
export type { SchemaValidationError, BatchValidationResult } from './types'
