/**
 * Standard API Response Envelope
 *
 * Provides a consistent response format across all API routes:
 * - Success responses: { success: true, data: T }
 * - Error responses: { success: false, error: { code: string, message: string } }
 *
 * This ensures clients always know how to parse responses and handle errors consistently.
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Standard API response envelope
 */
export interface ApiResponse<T = unknown> {
  /** Whether the request was successful */
  success: boolean
  /** Response data (only present on success) */
  data?: T
  /** Error details (only present on failure) */
  error?: ApiError
}

/**
 * Standard error structure
 */
export interface ApiError {
  /** Machine-readable error code (e.g., 'INVALID_JSON', 'RATE_LIMITED') */
  code: string
  /** Human-readable error message */
  message: string
  /** Optional additional error details */
  details?: Record<string, unknown>
}

// ============================================================================
// Common Error Codes
// ============================================================================

/**
 * Standard error codes used across the API
 */
export const ErrorCodes = {
  // Client errors (4xx)
  INVALID_JSON: 'INVALID_JSON',
  INVALID_REQUEST: 'INVALID_REQUEST',
  INVALID_BATCH: 'INVALID_BATCH',
  INVALID_EVENT: 'INVALID_EVENT',
  INVALID_QUERY: 'INVALID_QUERY',
  MISSING_FIELD: 'MISSING_FIELD',
  INVALID_FIELD: 'INVALID_FIELD',
  UNAUTHORIZED: 'UNAUTHORIZED',
  FORBIDDEN: 'FORBIDDEN',
  NOT_FOUND: 'NOT_FOUND',
  METHOD_NOT_ALLOWED: 'METHOD_NOT_ALLOWED',
  RATE_LIMITED: 'RATE_LIMITED',
  PAYLOAD_TOO_LARGE: 'PAYLOAD_TOO_LARGE',
  DUPLICATE_BATCH: 'DUPLICATE_BATCH',
  VALIDATION_FAILED: 'VALIDATION_FAILED',

  // Server errors (5xx)
  INTERNAL_ERROR: 'INTERNAL_ERROR',
  SERVICE_UNAVAILABLE: 'SERVICE_UNAVAILABLE',
  NOT_CONFIGURED: 'NOT_CONFIGURED',
  TIMEOUT: 'TIMEOUT',
} as const

export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes]

// ============================================================================
// Response Builders
// ============================================================================

/**
 * Create a success response with the standard envelope
 *
 * @param data - The response data
 * @param options - Optional Response init options (headers, status)
 * @returns A JSON Response with the standard envelope
 *
 * @example
 * return successResponse({ events: [...] })
 * return successResponse({ ok: true }, { status: 201 })
 */
export function successResponse<T>(
  data: T,
  options: ResponseInit = {}
): Response {
  const envelope: ApiResponse<T> = {
    success: true,
    data,
  }

  return Response.json(envelope, {
    status: options.status ?? 200,
    headers: options.headers,
  })
}

/**
 * Create an error response with the standard envelope
 *
 * @param code - Machine-readable error code
 * @param message - Human-readable error message
 * @param status - HTTP status code
 * @param options - Optional additional options (headers, details)
 * @returns A JSON Response with the standard envelope
 *
 * @example
 * return errorResponse('INVALID_JSON', 'Request body must be valid JSON', 400)
 * return errorResponse('NOT_FOUND', 'Schema not found', 404, { headers: corsHeaders() })
 */
export function errorResponse(
  code: string,
  message: string,
  status: number,
  options: { headers?: HeadersInit; details?: Record<string, unknown> } = {}
): Response {
  const error: ApiError = { code, message }
  if (options.details) {
    error.details = options.details
  }

  const envelope: ApiResponse<never> = {
    success: false,
    error,
  }

  return Response.json(envelope, {
    status,
    headers: options.headers,
  })
}

// ============================================================================
// Convenience Helpers
// ============================================================================

/**
 * Create a 400 Bad Request response
 */
export function badRequest(
  message: string,
  code: string = ErrorCodes.INVALID_REQUEST,
  options: { headers?: HeadersInit; details?: Record<string, unknown> } = {}
): Response {
  return errorResponse(code, message, 400, options)
}

/**
 * Create a 401 Unauthorized response
 */
export function unauthorized(
  message: string = 'Authentication required',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.UNAUTHORIZED, message, 401, options)
}

/**
 * Create a 403 Forbidden response
 */
export function forbidden(
  message: string = 'Access denied',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.FORBIDDEN, message, 403, options)
}

/**
 * Create a 404 Not Found response
 */
export function notFound(
  message: string = 'Resource not found',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.NOT_FOUND, message, 404, options)
}

/**
 * Create a 405 Method Not Allowed response
 */
export function methodNotAllowed(
  message: string = 'Method not allowed',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.METHOD_NOT_ALLOWED, message, 405, options)
}

/**
 * Create a 429 Rate Limited response
 */
export function rateLimited(
  message: string = 'Rate limit exceeded',
  retryAfterSeconds?: number,
  options: { headers?: HeadersInit } = {}
): Response {
  const headers = new Headers(options.headers)
  if (retryAfterSeconds !== undefined) {
    headers.set('Retry-After', String(retryAfterSeconds))
  }

  return errorResponse(ErrorCodes.RATE_LIMITED, message, 429, {
    headers,
    details: retryAfterSeconds !== undefined ? { retryAfter: retryAfterSeconds } : undefined,
  })
}

/**
 * Create a 500 Internal Server Error response
 */
export function internalError(
  message: string = 'Internal server error',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.INTERNAL_ERROR, message, 500, options)
}

/**
 * Create a 501 Not Implemented / Not Configured response
 */
export function notConfigured(
  message: string = 'Feature not configured',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.NOT_CONFIGURED, message, 501, options)
}

/**
 * Create a 503 Service Unavailable response
 */
export function serviceUnavailable(
  message: string = 'Service temporarily unavailable',
  options: { headers?: HeadersInit } = {}
): Response {
  return errorResponse(ErrorCodes.SERVICE_UNAVAILABLE, message, 503, options)
}
