/**
 * Shared utilities for the events.do worker
 */

// ============================================================================
// R2 Path Sanitization - Prevents Path Traversal Attacks
// ============================================================================

/**
 * Error thrown when an R2 path contains invalid or dangerous characters
 */
export class InvalidR2PathError extends Error {
  constructor(message: string, public readonly path: string) {
    super(message)
    this.name = 'InvalidR2PathError'
  }
}

/**
 * Sanitizes a path segment for use in R2 keys.
 * Removes or rejects dangerous path traversal sequences.
 *
 * @param segment - A single path segment (should not contain slashes)
 * @returns Sanitized segment
 * @throws InvalidR2PathError if segment contains dangerous patterns
 *
 * @example
 * sanitizePathSegment('valid-segment') // 'valid-segment'
 * sanitizePathSegment('../etc/passwd') // throws InvalidR2PathError
 * sanitizePathSegment('..') // throws InvalidR2PathError
 */
export function sanitizePathSegment(segment: string): string {
  // Reject empty segments
  if (!segment || segment.length === 0) {
    throw new InvalidR2PathError('Path segment cannot be empty', segment)
  }

  // Reject path traversal sequences
  if (segment === '.' || segment === '..') {
    throw new InvalidR2PathError('Path traversal sequences are not allowed', segment)
  }

  // Reject segments that start or end with dots (hidden files or traversal attempts)
  if (segment.startsWith('.') || segment.endsWith('.')) {
    throw new InvalidR2PathError('Path segments cannot start or end with dots', segment)
  }

  // Reject segments containing path separators
  if (segment.includes('/') || segment.includes('\\')) {
    throw new InvalidR2PathError('Path segments cannot contain path separators', segment)
  }

  // Reject null bytes (can be used to bypass validation)
  if (segment.includes('\0')) {
    throw new InvalidR2PathError('Path segments cannot contain null bytes', segment)
  }

  // Reject control characters
  if (/[\x00-\x1f\x7f]/.test(segment)) {
    throw new InvalidR2PathError('Path segments cannot contain control characters', segment)
  }

  // Maximum segment length (R2 key max is 1024 bytes total)
  if (segment.length > 256) {
    throw new InvalidR2PathError('Path segment exceeds maximum length of 256 characters', segment)
  }

  return segment
}

/**
 * Sanitizes a complete R2 path, validating each segment.
 * Handles paths with or without leading slashes.
 *
 * @param path - The full path to sanitize
 * @returns Sanitized path with normalized slashes
 * @throws InvalidR2PathError if any segment is invalid
 *
 * @example
 * sanitizeR2Path('events/2024/01/file.parquet') // 'events/2024/01/file.parquet'
 * sanitizeR2Path('events/../secrets/file.txt') // throws InvalidR2PathError
 * sanitizeR2Path('events/./file.txt') // throws InvalidR2PathError
 */
export function sanitizeR2Path(path: string): string {
  // Reject empty paths
  if (!path || path.length === 0) {
    throw new InvalidR2PathError('Path cannot be empty', path)
  }

  // Reject paths with backslashes (normalize to forward slashes)
  if (path.includes('\\')) {
    throw new InvalidR2PathError('Paths must use forward slashes', path)
  }

  // Reject null bytes
  if (path.includes('\0')) {
    throw new InvalidR2PathError('Path cannot contain null bytes', path)
  }

  // Reject control characters
  if (/[\x00-\x1f\x7f]/.test(path)) {
    throw new InvalidR2PathError('Path cannot contain control characters', path)
  }

  // Check for path traversal patterns before splitting
  // This catches patterns like '/../', '/..', '../', etc.
  if (/(^|\/)\.\.(\/|$)/.test(path) || /(^|\/)\.($|\/)/.test(path)) {
    throw new InvalidR2PathError('Path traversal sequences are not allowed', path)
  }

  // Maximum total path length (R2 key max is 1024 bytes)
  if (path.length > 1024) {
    throw new InvalidR2PathError('Path exceeds maximum length of 1024 characters', path)
  }

  // Split and validate each segment
  const segments = path.split('/').filter(s => s.length > 0)

  if (segments.length === 0) {
    throw new InvalidR2PathError('Path must contain at least one segment', path)
  }

  // Validate each segment
  for (const segment of segments) {
    sanitizePathSegment(segment)
  }

  // Return normalized path (no leading slash, single slashes between segments)
  return segments.join('/')
}

/**
 * Constructs a safe R2 path from a prefix and additional segments.
 * Validates and sanitizes all parts of the path.
 *
 * @param prefix - The base prefix (e.g., 'events', 'dedup')
 * @param segments - Additional path segments to append
 * @returns Sanitized complete path
 * @throws InvalidR2PathError if any part is invalid
 *
 * @example
 * buildSafeR2Path('dedup', batchId) // 'dedup/safe-batch-id'
 * buildSafeR2Path('events', year, month, day, filename)
 */
export function buildSafeR2Path(prefix: string, ...segments: string[]): string {
  // Validate prefix
  const sanitizedPrefix = sanitizeR2Path(prefix)

  // Validate each additional segment
  const sanitizedSegments = segments.map(seg => sanitizePathSegment(seg))

  // Build and return the complete path
  const fullPath = [sanitizedPrefix, ...sanitizedSegments].join('/')

  // Final validation of complete path
  return sanitizeR2Path(fullPath)
}

// ============================================================================
// CORS Utilities
// ============================================================================

/**
 * CORS headers for public endpoints (webhooks, health, ingest)
 */
export function corsHeaders(): HeadersInit {
  return {
    'Access-Control-Allow-Origin': '*',
  }
}

/** Default allowed origins pattern for authenticated endpoints (*.do domains) */
const DEFAULT_ALLOWED_ORIGIN_PATTERN = /^https?:\/\/([a-z0-9-]+\.)*do$/

/**
 * Check if an origin is allowed for authenticated endpoints.
 * Reads ALLOWED_ORIGINS env var (comma-separated) or defaults to *.do domains.
 */
export function getAllowedOrigin(requestOrigin: string | null, env?: { ALLOWED_ORIGINS?: string }): string | null {
  if (!requestOrigin) return null

  // Check explicit allowed origins from env
  if (env?.ALLOWED_ORIGINS) {
    const allowed = env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
    if (allowed.includes(requestOrigin)) return requestOrigin
    // Also support wildcard patterns like *.do
    for (const pattern of allowed) {
      if (pattern.startsWith('*.')) {
        const suffix = pattern.slice(1) // e.g. ".do"
        try {
          const originHost = new URL(requestOrigin).hostname
          if (originHost.endsWith(suffix) || originHost === suffix.slice(1)) return requestOrigin
        } catch {
          // Invalid origin URL
        }
      }
    }
    return null
  }

  // Default: allow *.do domains
  if (DEFAULT_ALLOWED_ORIGIN_PATTERN.test(requestOrigin)) return requestOrigin
  return null
}

/**
 * CORS headers for authenticated endpoints - restrict to known .do origins.
 * Returns the request's Origin if it matches allowed origins, otherwise 'null'.
 */
export function authCorsHeaders(request: Request, env?: { ALLOWED_ORIGINS?: string }): HeadersInit {
  const requestOrigin = request.headers.get('Origin')
  const origin = getAllowedOrigin(requestOrigin, env) ?? 'null'
  return {
    'Access-Control-Allow-Origin': origin,
    'Vary': 'Origin',
  }
}

// ============================================================================
// Timeout Utilities
// ============================================================================

/**
 * Error thrown when an operation times out
 */
export class TimeoutError extends Error {
  constructor(message: string, public readonly timeoutMs: number) {
    super(message)
    this.name = 'TimeoutError'
  }
}

/**
 * Wraps a promise with a timeout. If the promise doesn't resolve within
 * the specified time, it rejects with a TimeoutError.
 *
 * @param promise - The promise to wrap
 * @param ms - Timeout in milliseconds
 * @param errorMsg - Error message to use if timeout occurs
 * @returns The result of the promise if it resolves in time
 * @throws TimeoutError if the promise doesn't resolve within the timeout
 *
 * @example
 * // Timeout a DO call after 5 seconds
 * const result = await withTimeout(
 *   doStub.process(events),
 *   5000,
 *   'DO call timed out'
 * )
 */
export async function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  errorMsg: string
): Promise<T> {
  if (ms <= 0) {
    return promise
  }

  let timeoutId: ReturnType<typeof setTimeout> | undefined

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new TimeoutError(errorMsg, ms))
    }, ms)
  })

  try {
    const result = await Promise.race([promise, timeoutPromise])
    return result
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId)
    }
  }
}

// ============================================================================
// Circuit Breaker
// ============================================================================

/**
 * Circuit breaker states
 */
export type CircuitState = 'closed' | 'open' | 'half-open'

/**
 * Configuration options for the circuit breaker
 */
export interface CircuitBreakerOptions {
  /** Number of consecutive failures before opening the circuit (default: 5) */
  failureThreshold?: number
  /** Time in ms to wait before attempting to close the circuit (default: 30000) */
  resetTimeoutMs?: number
  /** Number of successful calls needed in half-open state to close circuit (default: 2) */
  successThreshold?: number
}

/**
 * Internal state for a circuit breaker
 */
interface CircuitBreakerState {
  state: CircuitState
  failures: number
  successes: number
  lastFailureTime: number
  lastError?: Error
}

/**
 * Circuit breaker for protecting against cascading failures from slow or failing DOs.
 *
 * The circuit breaker has three states:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Circuit is tripped, requests fail immediately
 * - HALF-OPEN: Testing if the service has recovered
 *
 * @example
 * const breaker = new CircuitBreaker('schema-registry', { failureThreshold: 3 })
 *
 * try {
 *   const result = await breaker.execute(() => registry.validateEvents(events))
 * } catch (err) {
 *   if (err instanceof CircuitBreakerOpenError) {
 *     // Circuit is open, fail fast
 *   }
 * }
 */
export class CircuitBreaker {
  private state: CircuitBreakerState
  private readonly failureThreshold: number
  private readonly resetTimeoutMs: number
  private readonly successThreshold: number

  constructor(
    public readonly name: string,
    options: CircuitBreakerOptions = {}
  ) {
    this.failureThreshold = options.failureThreshold ?? 5
    this.resetTimeoutMs = options.resetTimeoutMs ?? 30000
    this.successThreshold = options.successThreshold ?? 2

    this.state = {
      state: 'closed',
      failures: 0,
      successes: 0,
      lastFailureTime: 0,
    }
  }

  /**
   * Get the current state of the circuit breaker
   */
  getState(): CircuitState {
    this.updateState()
    return this.state.state
  }

  /**
   * Get detailed status of the circuit breaker
   */
  getStatus(): {
    state: CircuitState
    failures: number
    successes: number
    lastError?: string
  } {
    this.updateState()
    return {
      state: this.state.state,
      failures: this.state.failures,
      successes: this.state.successes,
      lastError: this.state.lastError?.message,
    }
  }

  /**
   * Execute a function with circuit breaker protection
   *
   * @param fn - The async function to execute
   * @returns The result of the function
   * @throws CircuitBreakerOpenError if the circuit is open
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    this.updateState()

    if (this.state.state === 'open') {
      throw new CircuitBreakerOpenError(
        `Circuit breaker '${this.name}' is open`,
        this.name,
        this.state.lastError
      )
    }

    try {
      const result = await fn()
      this.onSuccess()
      return result
    } catch (err) {
      this.onFailure(err instanceof Error ? err : new Error(String(err)))
      throw err
    }
  }

  /**
   * Manually reset the circuit breaker to closed state
   */
  reset(): void {
    this.state = {
      state: 'closed',
      failures: 0,
      successes: 0,
      lastFailureTime: 0,
    }
  }

  /**
   * Force the circuit to open (for testing or manual intervention)
   */
  trip(error?: Error): void {
    this.state.state = 'open'
    this.state.failures = this.failureThreshold
    this.state.lastFailureTime = Date.now()
    this.state.lastError = error
  }

  private updateState(): void {
    if (this.state.state === 'open') {
      // Check if we should transition to half-open
      const timeSinceLastFailure = Date.now() - this.state.lastFailureTime
      if (timeSinceLastFailure >= this.resetTimeoutMs) {
        this.state.state = 'half-open'
        this.state.successes = 0
      }
    }
  }

  private onSuccess(): void {
    if (this.state.state === 'half-open') {
      this.state.successes++
      if (this.state.successes >= this.successThreshold) {
        // Enough successes, close the circuit
        this.state.state = 'closed'
        this.state.failures = 0
        this.state.successes = 0
      }
    } else if (this.state.state === 'closed') {
      // Reset failure count on success
      this.state.failures = 0
    }
  }

  private onFailure(error: Error): void {
    this.state.lastError = error
    this.state.lastFailureTime = Date.now()

    if (this.state.state === 'half-open') {
      // Any failure in half-open state reopens the circuit
      this.state.state = 'open'
      this.state.failures = this.failureThreshold
    } else if (this.state.state === 'closed') {
      this.state.failures++
      if (this.state.failures >= this.failureThreshold) {
        this.state.state = 'open'
      }
    }
  }
}

/**
 * Error thrown when attempting to execute through an open circuit breaker
 */
export class CircuitBreakerOpenError extends Error {
  constructor(
    message: string,
    public readonly circuitName: string,
    public readonly lastError?: Error
  ) {
    super(message)
    this.name = 'CircuitBreakerOpenError'
  }
}

// ============================================================================
// Combined Timeout + Circuit Breaker
// ============================================================================

/**
 * Options for protected DO calls
 */
export interface ProtectedCallOptions {
  /** Timeout in milliseconds (default: 5000) */
  timeoutMs?: number
  /** Circuit breaker to use (optional) */
  circuitBreaker?: CircuitBreaker
  /** Error message prefix for timeout errors */
  errorMsg?: string
}

/**
 * Execute a DO call with timeout and optional circuit breaker protection.
 *
 * This is the recommended way to call Durable Objects from middleware to
 * prevent slow DOs from causing cascading failures.
 *
 * @param fn - The async function to execute (typically a DO method call)
 * @param options - Protection options (timeout, circuit breaker)
 * @returns The result of the function
 * @throws TimeoutError if the operation times out
 * @throws CircuitBreakerOpenError if the circuit is open
 *
 * @example
 * // Simple timeout protection
 * const result = await protectedCall(
 *   () => registry.validateEvents(events),
 *   { timeoutMs: 5000, errorMsg: 'Schema validation' }
 * )
 *
 * // With circuit breaker
 * const breaker = new CircuitBreaker('schema-registry')
 * const result = await protectedCall(
 *   () => registry.validateEvents(events),
 *   { timeoutMs: 5000, circuitBreaker: breaker }
 * )
 */
export async function protectedCall<T>(
  fn: () => Promise<T>,
  options: ProtectedCallOptions = {}
): Promise<T> {
  const {
    timeoutMs = 5000,
    circuitBreaker,
    errorMsg = 'Operation',
  } = options

  const wrappedFn = async () => {
    return withTimeout(
      fn(),
      timeoutMs,
      `${errorMsg} timed out after ${timeoutMs}ms`
    )
  }

  if (circuitBreaker) {
    return circuitBreaker.execute(wrappedFn)
  }

  return wrappedFn()
}
