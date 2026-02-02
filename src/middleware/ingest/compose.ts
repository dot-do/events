/**
 * Middleware composition for ingest pipeline
 *
 * Provides a clean way to compose middleware functions into a pipeline.
 */

import type {
  IngestContext,
  IngestMiddleware,
  MiddlewareResult,
} from './types'

// ============================================================================
// Middleware Composition
// ============================================================================

/**
 * Compose multiple middleware functions into a single pipeline.
 * Each middleware can either continue to the next or return a response.
 *
 * @param middlewares - Array of middleware functions to compose
 * @returns A single middleware function that runs all middlewares in sequence
 *
 * @example
 * ```typescript
 * const pipeline = composeMiddleware([
 *   parseJsonMiddleware,
 *   validateBatchMiddleware,
 *   rateLimitMiddleware,
 *   dedupMiddleware,
 *   validateEventsMiddleware,
 *   schemaValidationMiddleware,
 * ])
 *
 * const result = await pipeline(context)
 * if (!result.continue) {
 *   return result.response
 * }
 * // Continue with main processing
 * ```
 */
export function composeMiddleware(
  middlewares: IngestMiddleware[]
): IngestMiddleware {
  return async (context: IngestContext): Promise<MiddlewareResult> => {
    for (const middleware of middlewares) {
      const result = await middleware(context)
      if (!result.continue) {
        return result
      }
    }
    return { continue: true }
  }
}

/**
 * Create a middleware that wraps another with error handling.
 * Catches errors and converts them to a standard error response.
 *
 * @param middleware - The middleware to wrap
 * @param errorHandler - Optional custom error handler
 * @returns A wrapped middleware with error handling
 */
export function withErrorHandling(
  middleware: IngestMiddleware,
  errorHandler?: (error: unknown, context: IngestContext) => Promise<MiddlewareResult>
): IngestMiddleware {
  return async (context: IngestContext): Promise<MiddlewareResult> => {
    try {
      return await middleware(context)
    } catch (error) {
      if (errorHandler) {
        return await errorHandler(error, context)
      }
      // Re-throw if no error handler provided
      throw error
    }
  }
}

/**
 * Create a conditional middleware that only runs if a condition is met.
 *
 * @param condition - Function that determines if middleware should run
 * @param middleware - The middleware to conditionally run
 * @returns A middleware that only runs when condition is true
 *
 * @example
 * ```typescript
 * const schemaMiddleware = conditionalMiddleware(
 *   (ctx) => ctx.env.ENABLE_SCHEMA_VALIDATION === 'true',
 *   schemaValidationMiddleware
 * )
 * ```
 */
export function conditionalMiddleware(
  condition: (context: IngestContext) => boolean,
  middleware: IngestMiddleware
): IngestMiddleware {
  return async (context: IngestContext): Promise<MiddlewareResult> => {
    if (condition(context)) {
      return await middleware(context)
    }
    return { continue: true }
  }
}

/**
 * Create a middleware from a simple function that returns void or throws.
 * Useful for middleware that don't need to return a response.
 *
 * @param fn - A function that either completes successfully or throws
 * @returns A middleware that continues on success or throws on error
 */
export function fromSimpleFunction(
  fn: (context: IngestContext) => Promise<void>
): IngestMiddleware {
  return async (context: IngestContext): Promise<MiddlewareResult> => {
    await fn(context)
    return { continue: true }
  }
}
