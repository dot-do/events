/**
 * Standardized Error Handling for @dotdo/events
 *
 * This module provides a unified error handling approach with:
 * - Base error classes with error codes
 * - Error codes enum for categorization
 * - Utility for converting errors to API responses
 * - Type-safe error handling across the codebase
 *
 * @example Basic Usage
 * ```typescript
 * import { EventsError, ErrorCode, toErrorResponse } from '@dotdo/events'
 *
 * // Throw a typed error
 * throw new ValidationError('Invalid event type', { field: 'type' })
 *
 * // Convert to API response
 * try {
 *   await processEvent(event)
 * } catch (error) {
 *   return toErrorResponse(error)
 * }
 * ```
 *
 * @packageDocumentation
 * @module errors
 */

// ============================================================================
// Error Codes
// ============================================================================

/**
 * Standardized error codes for the events system.
 * These codes categorize errors for logging, monitoring, and client handling.
 */
export enum ErrorCode {
  // Validation errors (400)
  VALIDATION_ERROR = 'VALIDATION_ERROR',
  INVALID_JSON = 'INVALID_JSON',
  INVALID_BATCH = 'INVALID_BATCH',
  INVALID_EVENT = 'INVALID_EVENT',
  EVENT_TOO_LARGE = 'EVENT_TOO_LARGE',
  PAYLOAD_TOO_LARGE = 'PAYLOAD_TOO_LARGE',
  SCHEMA_VALIDATION_ERROR = 'SCHEMA_VALIDATION_ERROR',
  INVALID_PATTERN = 'INVALID_PATTERN',
  INVALID_PATH = 'INVALID_PATH',
  INVALID_SIGNATURE = 'INVALID_SIGNATURE',
  INVALID_TIMESTAMP = 'INVALID_TIMESTAMP',
  INVALID_CONFIG = 'INVALID_CONFIG',

  // Authentication/Authorization errors (401/403)
  AUTH_ERROR = 'AUTH_ERROR',
  UNAUTHORIZED = 'UNAUTHORIZED',
  FORBIDDEN = 'FORBIDDEN',
  INVALID_API_KEY = 'INVALID_API_KEY',
  MISSING_API_KEY = 'MISSING_API_KEY',

  // Resource errors (404/409)
  NOT_FOUND = 'NOT_FOUND',
  SUBSCRIPTION_NOT_FOUND = 'SUBSCRIPTION_NOT_FOUND',
  DELIVERY_NOT_FOUND = 'DELIVERY_NOT_FOUND',
  CONFLICT = 'CONFLICT',
  DUPLICATE_ENTRY = 'DUPLICATE_ENTRY',

  // Rate limiting (429)
  RATE_LIMITED = 'RATE_LIMITED',

  // Server errors (500)
  INTERNAL_ERROR = 'INTERNAL_ERROR',
  DATABASE_ERROR = 'DATABASE_ERROR',
  STORAGE_ERROR = 'STORAGE_ERROR',
  DELIVERY_FAILED = 'DELIVERY_FAILED',
  WEBHOOK_ERROR = 'WEBHOOK_ERROR',

  // Service errors (502/503/504)
  SERVICE_UNAVAILABLE = 'SERVICE_UNAVAILABLE',
  TIMEOUT = 'TIMEOUT',
  CIRCUIT_BREAKER_OPEN = 'CIRCUIT_BREAKER_OPEN',
  BUFFER_FULL = 'BUFFER_FULL',
}

/**
 * Maps error codes to HTTP status codes
 */
export const ErrorCodeToStatus: Record<ErrorCode, number> = {
  // 400 Bad Request
  [ErrorCode.VALIDATION_ERROR]: 400,
  [ErrorCode.INVALID_JSON]: 400,
  [ErrorCode.INVALID_BATCH]: 400,
  [ErrorCode.INVALID_EVENT]: 400,
  [ErrorCode.EVENT_TOO_LARGE]: 400,
  [ErrorCode.PAYLOAD_TOO_LARGE]: 413,
  [ErrorCode.SCHEMA_VALIDATION_ERROR]: 400,
  [ErrorCode.INVALID_PATTERN]: 400,
  [ErrorCode.INVALID_PATH]: 400,
  [ErrorCode.INVALID_SIGNATURE]: 400,
  [ErrorCode.INVALID_TIMESTAMP]: 400,
  [ErrorCode.INVALID_CONFIG]: 400,

  // 401 Unauthorized
  [ErrorCode.AUTH_ERROR]: 401,
  [ErrorCode.UNAUTHORIZED]: 401,
  [ErrorCode.INVALID_API_KEY]: 401,
  [ErrorCode.MISSING_API_KEY]: 401,

  // 403 Forbidden
  [ErrorCode.FORBIDDEN]: 403,

  // 404 Not Found
  [ErrorCode.NOT_FOUND]: 404,
  [ErrorCode.SUBSCRIPTION_NOT_FOUND]: 404,
  [ErrorCode.DELIVERY_NOT_FOUND]: 404,

  // 409 Conflict
  [ErrorCode.CONFLICT]: 409,
  [ErrorCode.DUPLICATE_ENTRY]: 409,

  // 429 Too Many Requests
  [ErrorCode.RATE_LIMITED]: 429,

  // 500 Internal Server Error
  [ErrorCode.INTERNAL_ERROR]: 500,
  [ErrorCode.DATABASE_ERROR]: 500,
  [ErrorCode.STORAGE_ERROR]: 500,
  [ErrorCode.DELIVERY_FAILED]: 500,
  [ErrorCode.WEBHOOK_ERROR]: 500,

  // 503 Service Unavailable
  [ErrorCode.SERVICE_UNAVAILABLE]: 503,
  [ErrorCode.TIMEOUT]: 504,
  [ErrorCode.CIRCUIT_BREAKER_OPEN]: 503,
  [ErrorCode.BUFFER_FULL]: 503,
}

// ============================================================================
// Base Error Classes
// ============================================================================

/**
 * Base error class for all events.do errors.
 * Provides error code, metadata, and serialization support.
 */
export class EventsError extends Error {
  /** Error code for categorization */
  readonly code: ErrorCode

  /** HTTP status code */
  readonly status: number

  /** Additional metadata about the error */
  readonly details?: Record<string, unknown> | undefined

  /** Original error that caused this error */
  readonly cause?: Error | undefined

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    options?: {
      details?: Record<string, unknown> | undefined
      cause?: Error | undefined
    }
  ) {
    super(message)
    this.name = 'EventsError'
    this.code = code
    this.status = ErrorCodeToStatus[code]
    if (options?.details !== undefined) {
      this.details = options.details
    }
    if (options?.cause !== undefined) {
      this.cause = options.cause
    }

    // Maintains proper stack trace in V8 environments
    // V8's captureStackTrace is a non-standard extension
    const ErrorWithCapture = Error as typeof Error & { captureStackTrace?: (target: object, constructor: Function) => void }
    if (typeof ErrorWithCapture.captureStackTrace === 'function') {
      ErrorWithCapture.captureStackTrace(this, this.constructor)
    }
  }

  /**
   * Convert error to a JSON-serializable object
   */
  toJSON(): ErrorResponse {
    return {
      error: this.message,
      code: this.code,
      ...(this.details && { details: this.details }),
    }
  }
}

// ============================================================================
// Specialized Error Classes
// ============================================================================

/**
 * Validation error - thrown when input data is invalid
 */
export class ValidationError extends EventsError {
  constructor(
    message: string,
    details?: Record<string, unknown>,
    cause?: Error
  ) {
    super(message, ErrorCode.VALIDATION_ERROR, { details, cause })
    this.name = 'ValidationError'
  }
}

/**
 * Invalid JSON error - thrown when JSON parsing fails
 */
export class InvalidJsonError extends EventsError {
  constructor(message: string = 'Invalid JSON', cause?: Error) {
    super(message, ErrorCode.INVALID_JSON, { cause })
    this.name = 'InvalidJsonError'
  }
}

/**
 * Invalid batch error - thrown when event batch structure is invalid
 */
export class InvalidBatchError extends EventsError {
  constructor(
    message: string = 'Invalid batch: must have events array with max 1000 events',
    details?: Record<string, unknown>
  ) {
    super(message, ErrorCode.INVALID_BATCH, { details })
    this.name = 'InvalidBatchError'
  }
}

/**
 * Invalid event error - thrown when individual events fail validation
 */
export class InvalidEventError extends EventsError {
  constructor(
    message: string,
    details?: { indices?: number[]; field?: string }
  ) {
    super(message, ErrorCode.INVALID_EVENT, { details })
    this.name = 'InvalidEventError'
  }
}

/**
 * Event too large error - thrown when event exceeds size limit
 */
export class EventTooLargeError extends EventsError {
  constructor(
    message: string = 'Event exceeds maximum size',
    details?: { indices?: number[]; maxSize?: number }
  ) {
    super(message, ErrorCode.EVENT_TOO_LARGE, { details })
    this.name = 'EventTooLargeError'
  }
}

/**
 * Payload too large error - thrown when request body exceeds size limit (413)
 */
export class PayloadTooLargeError extends EventsError {
  constructor(
    message: string = 'Payload Too Large',
    details?: { maxSize?: number; contentLength?: number }
  ) {
    super(message, ErrorCode.PAYLOAD_TOO_LARGE, { details })
    this.name = 'PayloadTooLargeError'
  }
}

/**
 * Schema validation error - thrown when events fail JSON schema validation
 */
export class SchemaValidationError extends EventsError {
  constructor(
    message: string = 'Schema validation failed',
    details?: { validationErrors?: unknown[]; summary?: string[] }
  ) {
    super(message, ErrorCode.SCHEMA_VALIDATION_ERROR, { details })
    this.name = 'SchemaValidationError'
  }
}

/**
 * Invalid pattern error - thrown when subscription pattern is invalid
 */
export class InvalidPatternError extends EventsError {
  constructor(message: string, details?: { pattern?: string }) {
    super(message, ErrorCode.INVALID_PATTERN, { details })
    this.name = 'InvalidPatternError'
  }
}

/**
 * Invalid path error - thrown when R2 path is invalid
 */
export class InvalidPathError extends EventsError {
  constructor(message: string, details?: { path?: string }) {
    super(message, ErrorCode.INVALID_PATH, { details })
    this.name = 'InvalidPathError'
  }
}

/**
 * Invalid signature error - thrown when webhook signature verification fails
 */
export class InvalidSignatureError extends EventsError {
  constructor(
    message: string = 'Invalid signature',
    details?: { provider?: string; timestamp?: number }
  ) {
    super(message, ErrorCode.INVALID_SIGNATURE, { details })
    this.name = 'InvalidSignatureError'
  }
}

/**
 * Invalid timestamp error - thrown when timestamp is outside tolerance window
 */
export class InvalidTimestampError extends EventsError {
  constructor(
    message: string = 'Timestamp outside tolerance window',
    details?: { timestamp?: number; tolerance?: number }
  ) {
    super(message, ErrorCode.INVALID_TIMESTAMP, { details })
    this.name = 'InvalidTimestampError'
  }
}

/**
 * Invalid config error - thrown when configuration is invalid
 */
export class InvalidConfigError extends EventsError {
  constructor(
    message: string,
    details?: { provider?: string; field?: string; errors?: string[] }
  ) {
    super(message, ErrorCode.INVALID_CONFIG, { details })
    this.name = 'InvalidConfigError'
  }
}

/**
 * Authentication error - thrown when authentication fails
 */
export class AuthError extends EventsError {
  constructor(
    message: string = 'Authentication required',
    code: ErrorCode = ErrorCode.AUTH_ERROR,
    details?: Record<string, unknown>
  ) {
    super(message, code, { details })
    this.name = 'AuthError'
  }
}

/**
 * Unauthorized error - thrown when API key is invalid
 */
export class UnauthorizedError extends AuthError {
  constructor(message: string = 'Unauthorized', details?: Record<string, unknown>) {
    super(message, ErrorCode.UNAUTHORIZED, details)
    this.name = 'UnauthorizedError'
  }
}

/**
 * Forbidden error - thrown when user lacks permission
 */
export class ForbiddenError extends EventsError {
  constructor(message: string = 'Forbidden', details?: Record<string, unknown>) {
    super(message, ErrorCode.FORBIDDEN, { details })
    this.name = 'ForbiddenError'
  }
}

/**
 * Not found error - thrown when resource doesn't exist
 */
export class NotFoundError extends EventsError {
  constructor(
    message: string = 'Not found',
    code: ErrorCode = ErrorCode.NOT_FOUND,
    details?: Record<string, unknown>
  ) {
    super(message, code, { details })
    this.name = 'NotFoundError'
  }
}

/**
 * Subscription not found error
 */
export class SubscriptionNotFoundError extends NotFoundError {
  constructor(subscriptionId: string) {
    super(`Subscription not found: ${subscriptionId}`, ErrorCode.SUBSCRIPTION_NOT_FOUND, {
      subscriptionId,
    })
    this.name = 'SubscriptionNotFoundError'
  }
}

/**
 * Delivery not found error
 */
export class DeliveryNotFoundError extends NotFoundError {
  constructor(deliveryId: string) {
    super(`Delivery not found: ${deliveryId}`, ErrorCode.DELIVERY_NOT_FOUND, {
      deliveryId,
    })
    this.name = 'DeliveryNotFoundError'
  }
}

/**
 * Conflict error - thrown when operation conflicts with existing state
 */
export class ConflictError extends EventsError {
  constructor(
    message: string = 'Conflict',
    code: ErrorCode = ErrorCode.CONFLICT,
    details?: Record<string, unknown>
  ) {
    super(message, code, { details })
    this.name = 'ConflictError'
  }
}

/**
 * Duplicate entry error - thrown when trying to create duplicate
 */
export class DuplicateEntryError extends ConflictError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, ErrorCode.DUPLICATE_ENTRY, details)
    this.name = 'DuplicateEntryError'
  }
}

/**
 * Rate limited error - thrown when rate limit is exceeded
 */
export class RateLimitedError extends EventsError {
  constructor(
    message: string = 'Rate limit exceeded',
    details?: { retryAfter?: number; limit?: number }
  ) {
    super(message, ErrorCode.RATE_LIMITED, { details })
    this.name = 'RateLimitedError'
  }
}

/**
 * Database error - thrown when database operations fail
 */
export class DatabaseError extends EventsError {
  constructor(message: string, cause?: Error) {
    super(message, ErrorCode.DATABASE_ERROR, { cause })
    this.name = 'DatabaseError'
  }
}

/**
 * Storage error - thrown when R2/storage operations fail
 */
export class StorageError extends EventsError {
  constructor(message: string, details?: Record<string, unknown>, cause?: Error) {
    super(message, ErrorCode.STORAGE_ERROR, { details, cause })
    this.name = 'StorageError'
  }
}

/**
 * Delivery failed error - thrown when event delivery fails
 */
export class DeliveryFailedError extends EventsError {
  constructor(
    message: string,
    details?: { deliveryId?: string; subscriptionId?: string; attemptNumber?: number }
  ) {
    super(message, ErrorCode.DELIVERY_FAILED, { details })
    this.name = 'DeliveryFailedError'
  }
}

/**
 * Webhook error - thrown when webhook processing fails
 */
export class WebhookError extends EventsError {
  constructor(
    message: string,
    details?: { provider?: string; reason?: string }
  ) {
    super(message, ErrorCode.WEBHOOK_ERROR, { details })
    this.name = 'WebhookError'
  }
}

/**
 * Service unavailable error - thrown when service is temporarily unavailable
 */
export class ServiceUnavailableError extends EventsError {
  constructor(message: string = 'Service temporarily unavailable', details?: Record<string, unknown>) {
    super(message, ErrorCode.SERVICE_UNAVAILABLE, { details })
    this.name = 'ServiceUnavailableError'
  }
}

/**
 * Timeout error - thrown when operation times out
 */
export class TimeoutError extends EventsError {
  constructor(message: string = 'Operation timed out', details?: { timeoutMs?: number }) {
    super(message, ErrorCode.TIMEOUT, { details })
    this.name = 'TimeoutError'
  }
}

// ============================================================================
// Error Response Types
// ============================================================================

/**
 * Standard error response format
 */
export interface ErrorResponse {
  /** Human-readable error message */
  error: string
  /** Error code for programmatic handling */
  code: ErrorCode
  /** Additional details about the error */
  details?: Record<string, unknown>
}

/**
 * Error response with optional fields for specific error types
 */
export interface ExtendedErrorResponse extends ErrorResponse {
  /** Validation errors for schema validation failures */
  validationErrors?: unknown[]
  /** Summary of errors */
  summary?: string[]
  /** Retry-After header value in seconds */
  retryAfter?: number
}

// ============================================================================
// Error Conversion Utilities
// ============================================================================

/**
 * Options for error response conversion
 */
export interface ToErrorResponseOptions {
  /** Include stack trace in development */
  includeStack?: boolean
  /** Custom CORS headers */
  headers?: HeadersInit
}

/**
 * Converts any error to a standardized API Response.
 * Handles EventsError subclasses, standard errors, and unknown values.
 *
 * @param error - The error to convert
 * @param options - Optional configuration
 * @returns A Response object with appropriate status and JSON body
 *
 * @example
 * ```typescript
 * try {
 *   await processEvent(event)
 * } catch (error) {
 *   return toErrorResponse(error)
 * }
 * ```
 */
export function toErrorResponse(
  error: unknown,
  options?: ToErrorResponseOptions
): Response {
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...options?.headers,
  }

  // Handle EventsError and subclasses
  if (error instanceof EventsError) {
    const body: ExtendedErrorResponse = error.toJSON()

    // Add specific fields for certain error types
    if (error instanceof SchemaValidationError && error.details) {
      body.validationErrors = error.details.validationErrors as unknown[]
      body.summary = error.details.summary as string[]
    }

    if (error instanceof RateLimitedError && error.details?.retryAfter) {
      body.retryAfter = error.details.retryAfter as number
      headers['Retry-After'] = String(error.details.retryAfter)
    }

    return new Response(JSON.stringify(body), {
      status: error.status,
      headers,
    })
  }

  // Handle standard Error
  if (error instanceof Error) {
    const body: ErrorResponse = {
      error: error.message,
      code: ErrorCode.INTERNAL_ERROR,
    }

    // Include stack in development if requested
    if (options?.includeStack) {
      body.details = { stack: error.stack }
    }

    return new Response(JSON.stringify(body), {
      status: 500,
      headers,
    })
  }

  // Handle unknown error types
  const body: ErrorResponse = {
    error: String(error) || 'Unknown error',
    code: ErrorCode.INTERNAL_ERROR,
  }

  return new Response(JSON.stringify(body), {
    status: 500,
    headers,
  })
}

/**
 * Converts an error to a JSON-serializable object (without Response wrapper).
 * Useful for logging or returning in RPC responses.
 *
 * @param error - The error to convert
 * @returns An ErrorResponse object
 */
export function toErrorObject(error: unknown): ErrorResponse {
  if (error instanceof EventsError) {
    return error.toJSON()
  }

  if (error instanceof Error) {
    return {
      error: error.message,
      code: ErrorCode.INTERNAL_ERROR,
    }
  }

  return {
    error: String(error) || 'Unknown error',
    code: ErrorCode.INTERNAL_ERROR,
  }
}

/**
 * Type guard to check if a value is an EventsError
 */
export function isEventsError(error: unknown): error is EventsError {
  return error instanceof EventsError
}

/**
 * Type guard to check if a value is a validation error
 */
export function isValidationError(error: unknown): error is ValidationError {
  return error instanceof ValidationError || (
    error instanceof EventsError && error.code === ErrorCode.VALIDATION_ERROR
  )
}

/**
 * Type guard to check if an error is retryable
 */
export function isRetryableError(error: unknown): boolean {
  if (error instanceof EventsError) {
    // Service errors are generally retryable
    return [
      ErrorCode.SERVICE_UNAVAILABLE,
      ErrorCode.TIMEOUT,
      ErrorCode.RATE_LIMITED,
      ErrorCode.DATABASE_ERROR,
      ErrorCode.STORAGE_ERROR,
    ].includes(error.code)
  }
  return false
}

/**
 * Wraps an error with an EventsError if it isn't already one.
 * Preserves the original error as the cause.
 *
 * @param error - The error to wrap
 * @param message - Optional message override
 * @param code - Error code (defaults to INTERNAL_ERROR)
 * @returns An EventsError instance
 */
export function wrapError(
  error: unknown,
  message?: string,
  code: ErrorCode = ErrorCode.INTERNAL_ERROR
): EventsError {
  if (error instanceof EventsError) {
    return error
  }

  const cause = error instanceof Error ? error : undefined
  const errorMessage = message ?? (error instanceof Error ? error.message : String(error))

  return new EventsError(errorMessage, code, { cause })
}
