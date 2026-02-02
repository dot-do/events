/**
 * Worker ID and RPC Method Validation
 *
 * Provides strict validation for workerId and rpcMethod to prevent SSRF attacks
 * in subscription delivery. Only allows safe, known patterns that correspond
 * to legitimate Cloudflare Worker names.
 *
 * Security considerations:
 * - Worker IDs are used to construct URLs like `https://${workerId}.workers.dev/...`
 * - Malicious values could potentially:
 *   - Redirect requests to internal services (SSRF)
 *   - Include path traversal (../)
 *   - Include query strings or fragments
 *   - Include protocol schemes
 */

// ============================================================================
// Validation Constants
// ============================================================================

/**
 * Valid worker ID pattern based on Cloudflare Workers naming rules:
 * - Must start with a letter
 * - Can contain lowercase letters, numbers, and hyphens
 * - Cannot start or end with a hyphen
 * - Maximum length of 63 characters (DNS subdomain limit)
 *
 * This pattern is intentionally strict to prevent any SSRF attempts.
 */
export const WORKER_ID_PATTERN = /^[a-z][a-z0-9-]{0,61}[a-z0-9]$/

/**
 * Simpler pattern for single-character worker IDs (edge case)
 * Just a single lowercase letter
 */
export const WORKER_ID_PATTERN_SHORT = /^[a-z]$/

/**
 * Valid RPC method name pattern:
 * - Must start with a letter (lower or upper case)
 * - Can contain letters, numbers, and underscores
 * - Common method naming conventions (camelCase, snake_case)
 * - Maximum length of 128 characters
 */
export const RPC_METHOD_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]{0,127}$/

/**
 * Maximum length for worker IDs (DNS subdomain limit)
 */
export const MAX_WORKER_ID_LENGTH = 63

/**
 * Maximum length for RPC method names
 */
export const MAX_RPC_METHOD_LENGTH = 128

/**
 * Blocklist of patterns that could indicate SSRF attempts.
 * These are checked before regex validation as an additional layer.
 */
export const SSRF_BLOCKLIST_PATTERNS = [
  // Protocol schemes
  'http://',
  'https://',
  'ftp://',
  'file://',
  'data:',
  'javascript:',
  // Path traversal
  '..',
  './',
  '/..',
  // URL components that shouldn't appear in worker IDs
  '?',
  '#',
  '@',
  ':',
  '/',
  '\\',
  // Encoded characters
  '%',
  // Null bytes
  '\x00',
  '\0',
  // Newlines that could enable header injection
  '\n',
  '\r',
  // Internal/localhost attempts
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '[::1]',
  // Octal IP representations (127.0.0.1 variants)
  '0177.0.0.1',
  '0177.0.0.01',
  '0177.0.01.01',
  '0x7f.0.0.1',
  // IPv6 localhost variants
  '::1',
  '0:0:0:0:0:0:0:1',
  '::ffff:127.0.0.1',
  // Cloud metadata endpoints
  '169.254.169.254',
  'metadata.google.internal',
  // Punycode/IDN bypass attempts (xn-- prefix)
  'xn--',
  // Common internal hostnames
  'internal',
  'private',
  'admin',
  'kubernetes',
  'k8s',
  // AWS/cloud internal
  'ec2',
  'imds',
  'instance-data',
]

// ============================================================================
// Validation Result Types
// ============================================================================

export interface ValidationResult {
  valid: boolean
  error?: string
}

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validates a worker ID to ensure it's safe for use in URL construction.
 *
 * @param workerId - The worker ID to validate
 * @returns ValidationResult indicating if the ID is valid and any error message
 *
 * @example
 * ```ts
 * validateWorkerId('my-worker') // { valid: true }
 * validateWorkerId('http://evil.com') // { valid: false, error: '...' }
 * ```
 */
export function validateWorkerId(workerId: unknown): ValidationResult {
  // Type check
  if (typeof workerId !== 'string') {
    return {
      valid: false,
      error: 'Worker ID must be a string',
    }
  }

  // Empty check
  if (workerId.length === 0) {
    return {
      valid: false,
      error: 'Worker ID cannot be empty',
    }
  }

  // Length check
  if (workerId.length > MAX_WORKER_ID_LENGTH) {
    return {
      valid: false,
      error: `Worker ID exceeds maximum length of ${MAX_WORKER_ID_LENGTH} characters`,
    }
  }

  // Blocklist check (case-insensitive)
  const lowerWorkerId = workerId.toLowerCase()
  for (const pattern of SSRF_BLOCKLIST_PATTERNS) {
    if (lowerWorkerId.includes(pattern.toLowerCase())) {
      return {
        valid: false,
        error: `Worker ID contains blocked pattern: ${pattern}`,
      }
    }
  }

  // Pattern validation
  // Handle single-character worker IDs as edge case
  if (workerId.length === 1) {
    if (!WORKER_ID_PATTERN_SHORT.test(workerId)) {
      return {
        valid: false,
        error: 'Single-character worker ID must be a lowercase letter',
      }
    }
    return { valid: true }
  }

  // Two-character IDs need special handling too
  if (workerId.length === 2) {
    // Must be letter + alphanumeric
    if (!/^[a-z][a-z0-9]$/.test(workerId)) {
      return {
        valid: false,
        error: 'Worker ID must start with a lowercase letter and contain only lowercase letters, numbers, and hyphens',
      }
    }
    return { valid: true }
  }

  // Standard pattern validation for 3+ characters
  if (!WORKER_ID_PATTERN.test(workerId)) {
    return {
      valid: false,
      error: 'Worker ID must start with a lowercase letter, contain only lowercase letters, numbers, and hyphens, and not end with a hyphen',
    }
  }

  // Check for consecutive hyphens (not allowed in DNS)
  if (workerId.includes('--')) {
    return {
      valid: false,
      error: 'Worker ID cannot contain consecutive hyphens',
    }
  }

  return { valid: true }
}

/**
 * Validates an RPC method name to ensure it's safe and follows conventions.
 *
 * @param rpcMethod - The RPC method name to validate
 * @returns ValidationResult indicating if the method name is valid
 *
 * @example
 * ```ts
 * validateRpcMethod('handleEvent') // { valid: true }
 * validateRpcMethod('process_webhook') // { valid: true }
 * validateRpcMethod('../etc/passwd') // { valid: false, error: '...' }
 * ```
 */
export function validateRpcMethod(rpcMethod: unknown): ValidationResult {
  // Type check
  if (typeof rpcMethod !== 'string') {
    return {
      valid: false,
      error: 'RPC method must be a string',
    }
  }

  // Empty check
  if (rpcMethod.length === 0) {
    return {
      valid: false,
      error: 'RPC method cannot be empty',
    }
  }

  // Length check
  if (rpcMethod.length > MAX_RPC_METHOD_LENGTH) {
    return {
      valid: false,
      error: `RPC method exceeds maximum length of ${MAX_RPC_METHOD_LENGTH} characters`,
    }
  }

  // Blocklist check for path traversal and URL components
  for (const pattern of SSRF_BLOCKLIST_PATTERNS) {
    if (rpcMethod.includes(pattern)) {
      return {
        valid: false,
        error: `RPC method contains blocked pattern: ${pattern}`,
      }
    }
  }

  // Pattern validation
  if (!RPC_METHOD_PATTERN.test(rpcMethod)) {
    return {
      valid: false,
      error: 'RPC method must start with a letter and contain only letters, numbers, and underscores',
    }
  }

  return { valid: true }
}

/**
 * Validates both worker ID and RPC method for subscription delivery.
 * Throws an error if either is invalid.
 *
 * @param workerId - The worker ID to validate
 * @param rpcMethod - The RPC method to validate
 * @throws Error if validation fails
 *
 * @example
 * ```ts
 * assertValidDeliveryTarget('my-worker', 'handleEvent') // OK
 * assertValidDeliveryTarget('http://evil.com', 'foo') // throws Error
 * ```
 */
export function assertValidDeliveryTarget(workerId: string, rpcMethod: string): void {
  const workerIdResult = validateWorkerId(workerId)
  if (!workerIdResult.valid) {
    throw new Error(`Invalid worker ID: ${workerIdResult.error}`)
  }

  const rpcMethodResult = validateRpcMethod(rpcMethod)
  if (!rpcMethodResult.valid) {
    throw new Error(`Invalid RPC method: ${rpcMethodResult.error}`)
  }
}

/**
 * Constructs a safe delivery URL after validation.
 * This is the only sanctioned way to build delivery URLs.
 *
 * @param workerId - The worker ID (must be pre-validated or will be validated)
 * @param rpcMethod - The RPC method name (must be pre-validated or will be validated)
 * @returns The constructed URL
 * @throws Error if validation fails
 *
 * @example
 * ```ts
 * buildSafeDeliveryUrl('my-worker', 'handleEvent')
 * // => 'https://my-worker.workers.dev/rpc/handleEvent'
 * ```
 */
export function buildSafeDeliveryUrl(workerId: string, rpcMethod: string): string {
  // Always validate before constructing URL
  assertValidDeliveryTarget(workerId, rpcMethod)

  return `https://${workerId}.workers.dev/rpc/${rpcMethod}`
}
