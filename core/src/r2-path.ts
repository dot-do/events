/**
 * R2 Path Sanitization Utilities
 *
 * Provides functions to prevent path traversal attacks when constructing
 * R2 object keys from user input or external data.
 */

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

/**
 * Validates namespace and collection names for R2 path usage.
 * These are commonly used as path prefixes in CDC and compaction operations.
 *
 * Allowed characters: alphanumeric, hyphen, underscore
 * Must start with alphanumeric character
 * Length: 1-64 characters
 *
 * @param name - The namespace or collection name to validate
 * @param type - Label for error messages ('namespace' or 'collection')
 * @returns The validated name
 * @throws InvalidR2PathError if name is invalid
 */
export function validateNamespaceOrCollection(name: string, type: 'namespace' | 'collection' = 'namespace'): string {
  if (!name || name.length === 0) {
    throw new InvalidR2PathError(`${type} cannot be empty`, name)
  }

  if (name.length > 64) {
    throw new InvalidR2PathError(`${type} exceeds maximum length of 64 characters`, name)
  }

  // Must start with alphanumeric
  if (!/^[a-zA-Z0-9]/.test(name)) {
    throw new InvalidR2PathError(`${type} must start with an alphanumeric character`, name)
  }

  // Only allow alphanumeric, hyphen, underscore
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]*$/.test(name)) {
    throw new InvalidR2PathError(`${type} can only contain alphanumeric characters, hyphens, and underscores`, name)
  }

  // Reject reserved names
  const reserved = ['.', '..', 'CON', 'PRN', 'AUX', 'NUL']
  if (reserved.includes(name.toUpperCase())) {
    throw new InvalidR2PathError(`${type} cannot be a reserved name`, name)
  }

  return name
}
