/**
 * Structured JSON Logger
 *
 * Provides structured JSON logging with support for:
 * - Log levels (debug, info, warn, error)
 * - Timestamps (ISO 8601)
 * - Request context (requestId, namespace)
 * - Contextual metadata
 *
 * Usage:
 *   import { logger, createRequestLogger } from './logger'
 *
 *   // Global logger
 *   logger.info('Server started', { port: 8080 })
 *
 *   // Request-scoped logger
 *   const log = createRequestLogger({ requestId: 'abc123', namespace: 'acme' })
 *   log.info('Processing request', { path: '/ingest' })
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogContext {
  /** Unique request identifier for tracing */
  requestId?: string
  /** Tenant namespace for multi-tenant isolation */
  namespace?: string
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
  /** Contextual metadata */
  context?: Record<string, unknown>
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

    // Merge base context with call-specific context
    const mergedContext = { ...baseContext, ...context }
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

/**
 * Utility to log errors with stack traces
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
      message: error.message,
      stack: error.stack,
    }
  } else {
    errorContext.error = String(error)
  }

  log.error(message, errorContext)
}
