/**
 * Schema validation middleware for ingest
 *
 * Validates events against registered JSON schemas (if enabled)
 */

import type { SchemaRegistryDO } from '../../../core/src/schema-registry'
import { SchemaValidationError as SchemaValidationErrorClass, toErrorResponse } from '../../../core/src/errors'
import { corsHeaders } from '../../utils'
import { recordIngestMetric } from '../../metrics'
import type { Env } from '../../env'
import type {
  IngestContext,
  IngestMiddleware,
  MiddlewareResult,
  SchemaValidationError,
  BatchValidationResult,
} from './types'

// ============================================================================
// Schema Validation
// ============================================================================

/**
 * Validate events against registered schemas
 * Returns null if validation passes or is disabled, otherwise returns validation errors
 */
export async function validateEventsAgainstSchemas(
  events: { type: string; [key: string]: unknown }[],
  namespace: string,
  env: Env
): Promise<SchemaValidationError[] | null> {
  // Check if schema validation is enabled
  if (env.ENABLE_SCHEMA_VALIDATION !== 'true' || !env.SCHEMA_REGISTRY) {
    return null
  }

  try {
    // Get schema registry instance for this namespace
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    // Validate all events in batch
    const result = (await registry.validateEvents(events, namespace)) as BatchValidationResult

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
    // Log but don't fail on schema registry errors
    console.error('[ingest] Schema validation error:', err)
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
