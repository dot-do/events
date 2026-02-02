/**
 * Structured JSON Logger with Correlation IDs and Sensitive Data Sanitization
 *
 * Provides structured JSON logging with support for:
 * - Log levels (debug, info, warn, error)
 * - Timestamps (ISO 8601)
 * - Correlation IDs for distributed tracing
 * - Request context extraction (method, path, headers)
 * - Contextual metadata
 * - Automatic sanitization of sensitive data (tokens, keys, PII)
 *
 * Usage:
 *   import { logger, createRequestLogger, extractRequestContext, correlationIdHeaders, sanitize } from './logger'
 *
 *   // Global logger
 *   logger.info('Server started', { port: 8080 })
 *
 *   // Request-scoped logger with correlation ID
 *   const reqCtx = extractRequestContext(request)
 *   const log = createRequestLogger(reqCtx)
 *   log.info('Processing request')
 *
 *   // Include correlation ID in response headers
 *   return new Response('OK', { headers: correlationIdHeaders(reqCtx.correlationId) })
 *
 *   // Sanitize sensitive values
 *   log.info('Auth callback', { token: sanitize.token(accessToken) })
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogContext {
  /** Unique correlation identifier for distributed tracing */
  correlationId?: string
  /** Unique request identifier (Cloudflare Ray ID or generated) */
  requestId?: string
  /** Tenant namespace for multi-tenant isolation */
  namespace?: string
  /** HTTP method (GET, POST, etc.) */
  method?: string
  /** Request path */
  path?: string
  /** User agent (truncated) */
  userAgent?: string
  /** Component/module name */
  component?: string
  /** Additional contextual fields */
  [key: string]: unknown
}

export interface LogEntry {
  /** ISO 8601 timestamp */
  timestamp: string
  /** Log severity level */
  level: LogLevel
  /** Log message */
  message: string
  /** Correlation ID for distributed tracing (top-level for easy filtering) */
  correlationId?: string
  /** Contextual metadata */
  context?: Record<string, unknown>
}

/**
 * Request context extracted from an incoming request
 */
export interface RequestContext {
  /** Correlation ID for distributed tracing */
  correlationId: string
  /** Request ID (from Cloudflare Ray ID or generated) */
  requestId: string
  /** HTTP method */
  method: string
  /** Request path */
  path: string
  /** Query string (if any) */
  query?: string | undefined
  /** User agent (truncated) */
  userAgent?: string | undefined
  /** Client IP (from CF-Connecting-IP) */
  clientIp?: string | undefined
  /** Cloudflare colo */
  colo?: string | undefined
  /** Cloudflare Ray ID */
  rayId?: string | undefined
}

/** Log level priority for filtering */
const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
}

/** Get minimum log level from environment (defaults to 'info') */
function getMinLogLevel(): LogLevel {
  // In Cloudflare Workers, we can't access env directly here
  // The level can be overridden per-logger instance
  return 'info'
}

export interface Logger {
  debug(message: string, context?: Record<string, unknown>): void
  info(message: string, context?: Record<string, unknown>): void
  warn(message: string, context?: Record<string, unknown>): void
  error(message: string, context?: Record<string, unknown>): void
  child(additionalContext: LogContext): Logger
}

/**
 * Creates a logger instance with optional base context
 */
export function createLogger(baseContext: LogContext = {}, minLevel: LogLevel = 'info'): Logger {
  const minPriority = LOG_LEVEL_PRIORITY[minLevel]

  function log(level: LogLevel, message: string, context?: Record<string, unknown>): void {
    // Skip if below minimum level
    if (LOG_LEVEL_PRIORITY[level] < minPriority) {
      return
    }

    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
    }

    // Extract correlation ID to top level for easy filtering
    const correlationId = baseContext.correlationId || context?.correlationId
    if (correlationId && typeof correlationId === 'string') {
      entry.correlationId = correlationId
    }

    // Merge base context with call-specific context (excluding correlationId since it's top-level)
    const { correlationId: _baseCorrelation, ...restBaseContext } = baseContext
    const { correlationId: _callCorrelation, ...restCallContext } = context || {}
    const mergedContext = { ...restBaseContext, ...restCallContext }
    if (Object.keys(mergedContext).length > 0) {
      entry.context = mergedContext
    }

    // Output as JSON to the appropriate console method
    const output = JSON.stringify(entry)
    switch (level) {
      case 'debug':
        console.debug(output)
        break
      case 'info':
        console.info(output)
        break
      case 'warn':
        console.warn(output)
        break
      case 'error':
        console.error(output)
        break
    }
  }

  return {
    debug: (message: string, context?: Record<string, unknown>) => log('debug', message, context),
    info: (message: string, context?: Record<string, unknown>) => log('info', message, context),
    warn: (message: string, context?: Record<string, unknown>) => log('warn', message, context),
    error: (message: string, context?: Record<string, unknown>) => log('error', message, context),
    child: (additionalContext: LogContext) =>
      createLogger({ ...baseContext, ...additionalContext }, minLevel),
  }
}

/**
 * Creates a request-scoped logger with requestId and optional namespace
 */
export function createRequestLogger(context: {
  requestId?: string
  namespace?: string
  [key: string]: unknown
}): Logger {
  return createLogger(context)
}

/**
 * Generates a unique request ID (short form for logging)
 */
export function generateRequestId(): string {
  // Use crypto.randomUUID if available, otherwise fall back to timestamp + random
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID().slice(0, 8)
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`
}

/**
 * Extracts or generates a request ID from headers
 */
export function getRequestId(request: Request): string {
  // Check common request ID headers
  const headers = [
    'x-request-id',
    'x-correlation-id',
    'cf-ray', // Cloudflare Ray ID
  ]

  for (const header of headers) {
    const value = request.headers.get(header)
    if (value) {
      // Truncate long IDs for readability
      return value.slice(0, 16)
    }
  }

  return generateRequestId()
}

/**
 * Default global logger instance
 */
export const logger = createLogger()

// ============================================================================
// Correlation ID and Request Context Utilities
// ============================================================================

/**
 * Generates a unique correlation ID for distributed tracing.
 * Uses crypto.randomUUID for uniqueness and includes a timestamp prefix for sortability.
 *
 * Format: {timestamp_base36}-{uuid_prefix}
 * Example: "m1abc123-1a2b3c4d"
 */
export function generateCorrelationId(): string {
  const timestamp = Date.now().toString(36)
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    const uuid = crypto.randomUUID().replace(/-/g, '').slice(0, 8)
    return `${timestamp}-${uuid}`
  }
  // Fallback for environments without crypto.randomUUID
  const random = Math.random().toString(36).slice(2, 10)
  return `${timestamp}-${random}`
}

/**
 * Gets an existing correlation ID from request headers or generates a new one.
 * Checks these headers in order: x-correlation-id, x-request-id, traceparent (W3C)
 *
 * @param request - The incoming HTTP request
 * @returns Correlation ID (existing or newly generated)
 */
export function getOrCreateCorrelationId(request: Request): string {
  // Check standard correlation ID headers
  const correlationHeaders = [
    'x-correlation-id',
    'x-request-id',
  ]

  for (const header of correlationHeaders) {
    const value = request.headers.get(header)
    if (value) {
      // Validate and sanitize: allow alphanumeric, dashes, underscores, max 64 chars
      const sanitized = value.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64)
      if (sanitized.length >= 8) {
        return sanitized
      }
    }
  }

  // Check W3C Trace Context traceparent header
  // Format: 00-{trace-id}-{parent-id}-{flags}
  const traceparent = request.headers.get('traceparent')
  if (traceparent) {
    const parts = traceparent.split('-')
    if (parts.length >= 3 && parts[1]) {
      // Use the trace-id portion (32 hex chars), truncate to 16 for logging
      return parts[1].slice(0, 16)
    }
  }

  return generateCorrelationId()
}

/**
 * Extracts comprehensive request context from an incoming HTTP request.
 * Includes correlation ID, request metadata, and Cloudflare-specific headers.
 *
 * @param request - The incoming HTTP request
 * @returns RequestContext object suitable for logging
 */
export function extractRequestContext(request: Request): RequestContext {
  const url = new URL(request.url)
  const correlationId = getOrCreateCorrelationId(request)

  // Get Cloudflare Ray ID if available
  const rayId = request.headers.get('cf-ray')?.split('-')[0] || undefined

  // Use Ray ID as requestId if available, otherwise use part of correlation ID
  const requestId = rayId || correlationId.slice(-8)

  // Truncate user agent for logging (first 100 chars)
  const userAgent = request.headers.get('user-agent')?.slice(0, 100) || undefined

  // Extract Cloudflare-specific context
  const clientIp = request.headers.get('cf-connecting-ip') || undefined
  const colo = request.headers.get('cf-ipcolo') || undefined

  return {
    correlationId,
    requestId,
    method: request.method,
    path: url.pathname,
    query: url.search ? url.search.slice(1) : undefined,
    userAgent,
    clientIp,
    colo,
    rayId,
  }
}

/**
 * Creates HTTP headers for including correlation ID in responses.
 * This allows downstream services and clients to correlate logs.
 *
 * @param correlationId - The correlation ID to include
 * @returns Headers object with x-correlation-id
 */
export function correlationIdHeaders(correlationId: string): HeadersInit {
  return {
    'x-correlation-id': correlationId,
  }
}

/**
 * Merges correlation ID headers with other response headers.
 *
 * @param correlationId - The correlation ID to include
 * @param additionalHeaders - Additional headers to merge
 * @returns Combined headers object
 */
export function withCorrelationId(
  correlationId: string,
  additionalHeaders: HeadersInit = {}
): HeadersInit {
  return {
    ...additionalHeaders,
    'x-correlation-id': correlationId,
  }
}

/**
 * Creates a request-scoped logger from a Request object.
 * Automatically extracts request context including correlation ID.
 *
 * @param request - The incoming HTTP request
 * @param additionalContext - Additional context to include
 * @returns Logger instance with request context
 */
export function createLoggerFromRequest(
  request: Request,
  additionalContext: LogContext = {}
): { logger: Logger; correlationId: string; requestContext: RequestContext } {
  const requestContext = extractRequestContext(request)
  const logContext: LogContext = {
    ...requestContext,
    ...additionalContext,
  }
  return {
    logger: createLogger(logContext),
    correlationId: requestContext.correlationId,
    requestContext,
  }
}

/**
 * Utility to log errors with stack traces (sanitized)
 */
export function logError(
  log: Logger,
  message: string,
  error: unknown,
  context?: Record<string, unknown>
): void {
  const errorContext: Record<string, unknown> = { ...context }

  if (error instanceof Error) {
    errorContext.error = {
      name: error.name,
      // Sanitize error message to avoid leaking sensitive data
      message: sanitize.errorMessage(error.message),
      // Include sanitized stack trace (first 5 lines only)
      stack: sanitize.stackTrace(error.stack),
    }
  } else {
    errorContext.error = sanitize.errorMessage(String(error))
  }

  log.error(message, errorContext)
}

// ============================================================================
// Sensitive Data Sanitization Utilities
// ============================================================================

/**
 * Patterns that indicate sensitive data in keys or values
 */
const SENSITIVE_KEY_PATTERNS = [
  /password/i,
  /secret/i,
  /token/i,
  /api[-_]?key/i,
  /auth/i,
  /bearer/i,
  /credential/i,
  /private/i,
  /session/i,
  /cookie/i,
  /jwt/i,
  /access[-_]?token/i,
  /refresh[-_]?token/i,
]

/**
 * Patterns for sensitive values that should be redacted
 */
const SENSITIVE_VALUE_PATTERNS = [
  // Bearer tokens
  /^Bearer\s+.+$/i,
  // JWT tokens (header.payload.signature)
  /^eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/,
  // API keys with common prefixes
  /^(sk[-_]|pk[-_]|api[-_]|key[-_]|ns[-_])[A-Za-z0-9_-]{16,}$/,
  // Base64-encoded secrets (32+ chars)
  /^[A-Za-z0-9+/]{32,}={0,2}$/,
]

/**
 * Sanitization utilities for common sensitive data types
 */
export const sanitize = {
  /**
   * Redact a token/key, showing only first 4 and last 4 characters
   * @example sanitize.token('sk_live_abc123def456') => 'sk_l***6'
   */
  token(value: string | undefined | null): string {
    if (!value) return '[empty]'
    if (value.length <= 8) return '***'
    return `${value.slice(0, 4)}***${value.slice(-4)}`
  },

  /**
   * Redact a Bearer token header
   * @example sanitize.bearer('Bearer abc123') => 'Bearer ***'
   */
  bearer(value: string | undefined | null): string {
    if (!value) return '[empty]'
    if (value.toLowerCase().startsWith('bearer ')) {
      return 'Bearer ***'
    }
    return sanitize.token(value)
  },

  /**
   * Redact email addresses, keeping domain visible
   * @example sanitize.email('user@example.com') => 'u***@example.com'
   */
  email(value: string | undefined | null): string {
    if (!value) return '[empty]'
    const atIndex = value.indexOf('@')
    if (atIndex <= 0) return '***'
    const localPart = value.slice(0, atIndex)
    const domain = value.slice(atIndex)
    if (localPart.length <= 1) return `*${domain}`
    return `${localPart[0]}***${domain}`
  },

  /**
   * Redact a URL, removing query parameters and fragments
   * @example sanitize.url('https://api.example.com/path?token=secret') => 'https://api.example.com/path'
   */
  url(value: string | undefined | null): string {
    if (!value) return '[empty]'
    try {
      const url = new URL(value)
      // Remove query params and hash
      return `${url.protocol}//${url.host}${url.pathname}`
    } catch {
      // If not a valid URL, truncate and redact
      const queryIndex = value.indexOf('?')
      if (queryIndex > 0) {
        return `${value.slice(0, queryIndex)}?[redacted]`
      }
      return value
    }
  },

  /**
   * Truncate and hash an identifier for correlation without exposing full value
   * @example sanitize.id('user_12345678901234567890') => 'user_1234...a1b2'
   */
  id(value: string | undefined | null, maxLength: number = 16): string {
    if (!value) return '[empty]'
    if (value.length <= maxLength) return value
    // Show prefix and a hash suffix for correlation
    const hash = simpleHash(value).toString(16).slice(0, 4)
    return `${value.slice(0, maxLength - 8)}...${hash}`
  },

  /**
   * Sanitize an error message to remove potential secrets
   * @example sanitize.errorMessage('Auth failed: token=abc123') => 'Auth failed: token=[redacted]'
   */
  errorMessage(message: string | undefined | null): string {
    if (!message) return '[empty]'
    // Redact common patterns of secrets in error messages
    return message
      // Redact anything after common sensitive keywords
      .replace(/(token|key|secret|password|auth|bearer)[\s:=]+\S+/gi, '$1=[redacted]')
      // Redact JWT-like patterns
      .replace(/eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '[jwt-redacted]')
      // Redact long alphanumeric strings that look like secrets (32+ chars)
      .replace(/[A-Za-z0-9_-]{32,}/g, '[redacted]')
  },

  /**
   * Sanitize a stack trace to limit depth and remove file paths
   * @param stack Stack trace string
   * @param maxLines Maximum number of lines to include
   */
  stackTrace(stack: string | undefined | null, maxLines: number = 5): string {
    if (!stack) return '[no stack]'
    const lines = stack.split('\n').slice(0, maxLines + 1) // +1 for the error message line
    // Remove full file paths, keep just filename and line
    return lines
      .map(line => line.replace(/\(\/[^)]+\/([^/]+)\)/g, '($1)'))
      .join('\n')
  },

  /**
   * Sanitize a full payload/object, redacting sensitive fields
   */
  payload(obj: unknown, depth: number = 3): unknown {
    if (depth <= 0) return '[truncated]'
    if (obj === null || obj === undefined) return obj
    if (typeof obj !== 'object') {
      // Check if string value looks sensitive
      if (typeof obj === 'string') {
        for (const pattern of SENSITIVE_VALUE_PATTERNS) {
          if (pattern.test(obj)) {
            return sanitize.token(obj)
          }
        }
      }
      return obj
    }
    if (Array.isArray(obj)) {
      if (obj.length > 10) {
        return [...obj.slice(0, 10).map(item => sanitize.payload(item, depth - 1)), `...(${obj.length - 10} more)`]
      }
      return obj.map(item => sanitize.payload(item, depth - 1))
    }
    const result: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(obj)) {
      // Check if key indicates sensitive data
      const isSensitiveKey = SENSITIVE_KEY_PATTERNS.some(pattern => pattern.test(key))
      if (isSensitiveKey) {
        result[key] = typeof value === 'string' ? sanitize.token(value) : '[redacted]'
      } else {
        result[key] = sanitize.payload(value, depth - 1)
      }
    }
    return result
  },

  /**
   * Create a safe context object for logging, automatically sanitizing known sensitive fields
   */
  context(ctx: Record<string, unknown>): Record<string, unknown> {
    return sanitize.payload(ctx) as Record<string, unknown>
  },
}

/**
 * Simple string hash for ID correlation
 */
function simpleHash(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}
