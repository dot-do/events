/**
 * Webhook Signature Verification
 *
 * Provides signature verification for popular webhook providers:
 * - GitHub (sha256=hex format)
 * - Stripe (t=timestamp,v1=sig format with multiple signatures for key rotation)
 * - WorkOS (timestamp=ts,signature=hex format)
 *
 * Uses Web Crypto API for cross-platform compatibility (Node.js, Workers, Deno).
 * All functions use timing-safe comparison to prevent timing attacks.
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Result of webhook signature verification
 */
export interface VerificationResult {
  /** Whether the signature is valid */
  valid: boolean
  /** Error message if validation failed */
  error?: string
  /** Unix timestamp from the signature header (Stripe/WorkOS only) */
  timestamp?: number
}

/**
 * Supported webhook providers
 */
export const WEBHOOK_PROVIDERS = ['github', 'stripe', 'workos', 'slack', 'linear', 'svix'] as const
export type WebhookProvider = (typeof WEBHOOK_PROVIDERS)[number]

/**
 * Base webhook provider configuration
 */
export interface WebhookProviderConfigBase {
  /** The webhook provider type */
  provider: WebhookProvider
  /** Whether this provider is enabled */
  enabled?: boolean
}

/**
 * GitHub webhook provider configuration
 */
export interface GitHubWebhookConfig extends WebhookProviderConfigBase {
  provider: 'github'
  /** The webhook secret configured in GitHub */
  secret: string
}

/**
 * Stripe webhook provider configuration
 */
export interface StripeWebhookConfig extends WebhookProviderConfigBase {
  provider: 'stripe'
  /** The webhook signing secret (starts with whsec_) */
  secret: string
  /** Tolerance window in seconds (default: 300) */
  toleranceSeconds?: number
}

/**
 * WorkOS webhook provider configuration
 */
export interface WorkOSWebhookConfig extends WebhookProviderConfigBase {
  provider: 'workos'
  /** The webhook signing secret */
  secret: string
  /** Tolerance window in seconds (default: 300) */
  toleranceSeconds?: number
}

/**
 * Slack webhook provider configuration
 */
export interface SlackWebhookConfig extends WebhookProviderConfigBase {
  provider: 'slack'
  /** The Slack signing secret */
  secret: string
  /** Tolerance window in seconds (default: 300) */
  toleranceSeconds?: number
}

/**
 * Linear webhook provider configuration
 */
export interface LinearWebhookConfig extends WebhookProviderConfigBase {
  provider: 'linear'
  /** The Linear webhook signing secret */
  secret: string
}

/**
 * Svix webhook provider configuration
 */
export interface SvixWebhookConfig extends WebhookProviderConfigBase {
  provider: 'svix'
  /** The Svix webhook signing secret (with whsec_ prefix, base64 encoded) */
  secret: string
  /** Tolerance window in seconds (default: 300) */
  toleranceSeconds?: number
}

/**
 * Union of all webhook provider configurations
 */
export type WebhookProviderConfig =
  | GitHubWebhookConfig
  | StripeWebhookConfig
  | WorkOSWebhookConfig
  | SlackWebhookConfig
  | LinearWebhookConfig
  | SvixWebhookConfig

/**
 * Error thrown when webhook configuration is invalid
 */
export class WebhookConfigError extends Error {
  constructor(
    message: string,
    public readonly provider?: string,
    public readonly field?: string
  ) {
    super(message)
    this.name = 'WebhookConfigError'
  }
}

/**
 * Error thrown when webhook signature verification fails
 */
export class WebhookSignatureError extends Error {
  constructor(
    message: string,
    public readonly provider: string,
    public readonly timestamp?: number
  ) {
    super(message)
    this.name = 'WebhookSignatureError'
  }
}

// ============================================================================
// Type Guards for Runtime Validation
// ============================================================================

/**
 * Checks if a value is a non-empty string
 */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}

/**
 * Checks if a value is a valid webhook provider
 */
export function isValidProvider(value: unknown): value is WebhookProvider {
  return isNonEmptyString(value) && WEBHOOK_PROVIDERS.includes(value as WebhookProvider)
}

/**
 * Type guard for GitHubWebhookConfig
 */
export function isGitHubConfig(config: unknown): config is GitHubWebhookConfig {
  if (!config || typeof config !== 'object') return false
  const c = config as Record<string, unknown>
  return c.provider === 'github' && isNonEmptyString(c.secret)
}

/**
 * Type guard for StripeWebhookConfig
 */
export function isStripeConfig(config: unknown): config is StripeWebhookConfig {
  if (!config || typeof config !== 'object') return false
  const c = config as Record<string, unknown>
  return (
    c.provider === 'stripe' &&
    isNonEmptyString(c.secret) &&
    (c.toleranceSeconds === undefined || typeof c.toleranceSeconds === 'number')
  )
}

/**
 * Type guard for WorkOSWebhookConfig
 */
export function isWorkOSConfig(config: unknown): config is WorkOSWebhookConfig {
  if (!config || typeof config !== 'object') return false
  const c = config as Record<string, unknown>
  return (
    c.provider === 'workos' &&
    isNonEmptyString(c.secret) &&
    (c.toleranceSeconds === undefined || typeof c.toleranceSeconds === 'number')
  )
}

/**
 * Type guard for SlackWebhookConfig
 */
export function isSlackConfig(config: unknown): config is SlackWebhookConfig {
  if (!config || typeof config !== 'object') return false
  const c = config as Record<string, unknown>
  return (
    c.provider === 'slack' &&
    isNonEmptyString(c.secret) &&
    (c.toleranceSeconds === undefined || typeof c.toleranceSeconds === 'number')
  )
}

/**
 * Type guard for LinearWebhookConfig
 */
export function isLinearConfig(config: unknown): config is LinearWebhookConfig {
  if (!config || typeof config !== 'object') return false
  const c = config as Record<string, unknown>
  return c.provider === 'linear' && isNonEmptyString(c.secret)
}

/**
 * Type guard for SvixWebhookConfig
 */
export function isSvixConfig(config: unknown): config is SvixWebhookConfig {
  if (!config || typeof config !== 'object') return false
  const c = config as Record<string, unknown>
  return (
    c.provider === 'svix' &&
    isNonEmptyString(c.secret) &&
    (c.toleranceSeconds === undefined || typeof c.toleranceSeconds === 'number')
  )
}

/**
 * Type guard for any WebhookProviderConfig
 */
export function isWebhookProviderConfig(config: unknown): config is WebhookProviderConfig {
  return (
    isGitHubConfig(config) ||
    isStripeConfig(config) ||
    isWorkOSConfig(config) ||
    isSlackConfig(config) ||
    isLinearConfig(config) ||
    isSvixConfig(config)
  )
}

/**
 * Validates a webhook provider configuration and returns validation errors
 *
 * @param config - The configuration to validate
 * @returns An array of validation error messages (empty if valid)
 */
export function validateWebhookConfig(config: unknown): string[] {
  const errors: string[] = []

  if (!config || typeof config !== 'object') {
    errors.push('Configuration must be a non-null object')
    return errors
  }

  const c = config as Record<string, unknown>

  // Check provider field
  if (!('provider' in c)) {
    errors.push('Missing required field: provider')
    return errors
  }

  if (!isValidProvider(c.provider)) {
    errors.push(`Invalid provider: "${c.provider}". Must be one of: ${WEBHOOK_PROVIDERS.join(', ')}`)
    return errors
  }

  // Check secret field (required for all providers)
  if (!('secret' in c)) {
    errors.push('Missing required field: secret')
  } else if (!isNonEmptyString(c.secret)) {
    errors.push('Field "secret" must be a non-empty string')
  }

  // Provider-specific validation
  switch (c.provider) {
    case 'stripe':
      if (c.toleranceSeconds !== undefined && typeof c.toleranceSeconds !== 'number') {
        errors.push('Field "toleranceSeconds" must be a number')
      }
      if (typeof c.toleranceSeconds === 'number' && c.toleranceSeconds <= 0) {
        errors.push('Field "toleranceSeconds" must be a positive number')
      }
      break

    case 'svix':
      // Svix secrets must start with whsec_
      if (isNonEmptyString(c.secret) && !c.secret.startsWith('whsec_')) {
        errors.push('Svix webhook secret must start with "whsec_"')
      }
      if (c.toleranceSeconds !== undefined && typeof c.toleranceSeconds !== 'number') {
        errors.push('Field "toleranceSeconds" must be a number')
      }
      if (typeof c.toleranceSeconds === 'number' && c.toleranceSeconds <= 0) {
        errors.push('Field "toleranceSeconds" must be a positive number')
      }
      break

    case 'workos':
    case 'slack':
      if (c.toleranceSeconds !== undefined && typeof c.toleranceSeconds !== 'number') {
        errors.push('Field "toleranceSeconds" must be a number')
      }
      if (typeof c.toleranceSeconds === 'number' && c.toleranceSeconds <= 0) {
        errors.push('Field "toleranceSeconds" must be a positive number')
      }
      break

    // github and linear have no additional validation
  }

  // Validate enabled field if present
  if ('enabled' in c && c.enabled !== undefined && typeof c.enabled !== 'boolean') {
    errors.push('Field "enabled" must be a boolean')
  }

  return errors
}

/**
 * Validates a webhook provider configuration and throws if invalid
 *
 * @param config - The configuration to validate
 * @throws {WebhookConfigError} If the configuration is invalid
 * @returns The validated configuration
 */
export function assertValidWebhookConfig(config: unknown): WebhookProviderConfig {
  const errors = validateWebhookConfig(config)
  if (errors.length > 0) {
    const provider = config && typeof config === 'object' ? (config as Record<string, unknown>).provider : undefined
    throw new WebhookConfigError(
      `Invalid webhook configuration: ${errors.join('; ')}`,
      typeof provider === 'string' ? provider : undefined
    )
  }
  return config as WebhookProviderConfig
}

/**
 * Creates a validated webhook configuration from partial input
 *
 * @param provider - The webhook provider
 * @param secret - The webhook secret
 * @param options - Optional additional configuration
 * @returns The validated configuration
 * @throws {WebhookConfigError} If the configuration is invalid
 */
export function createWebhookConfig(
  provider: WebhookProvider,
  secret: string,
  options?: { enabled?: boolean; toleranceSeconds?: number }
): WebhookProviderConfig {
  const config = {
    provider,
    secret,
    ...options,
  }
  return assertValidWebhookConfig(config)
}

// ============================================================================
// Shared Utilities
// ============================================================================

/**
 * Encodes a string to Uint8Array using UTF-8
 */
function encodeString(str: string): Uint8Array<ArrayBuffer> {
  return new TextEncoder().encode(str) as Uint8Array<ArrayBuffer>
}

/**
 * Converts an ArrayBuffer to hex string
 */
function bufferToHex(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer)
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Computes HMAC-SHA256 of the given message with the secret
 */
async function hmacSha256(secret: string, message: string): Promise<string> {
  const secretBytes = encodeString(secret)
  const messageBytes = encodeString(message)
  const key = await crypto.subtle.importKey('raw', secretBytes.buffer as ArrayBuffer, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])

  const signature = await crypto.subtle.sign('HMAC', key, messageBytes.buffer as ArrayBuffer)

  return bufferToHex(signature)
}

/**
 * Timing-safe string comparison using HMAC verification
 *
 * Compares two strings in constant time to prevent timing attacks.
 * Uses crypto.subtle.verify which provides native timing-safe comparison.
 *
 * The technique works by:
 * 1. Converting both strings to Uint8Array
 * 2. Using crypto.subtle.verify to compare the HMAC of one string against the other
 *
 * This ensures:
 * - No length information leaks (comparison happens on fixed-size HMAC output)
 * - Comparison time is constant regardless of where strings differ
 * - Uses native crypto API for timing safety
 */
async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const aBytes = encodeString(a)
  const bBytes = encodeString(b)

  // Generate a random key for this comparison
  const randomKey = crypto.getRandomValues(new Uint8Array(32)) as Uint8Array<ArrayBuffer>

  // Import key for HMAC with both sign and verify capabilities
  const key = await crypto.subtle.importKey('raw', randomKey, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify'])

  // Sign string 'a' to get its HMAC
  const aHmac = await crypto.subtle.sign('HMAC', key, aBytes)

  // Use crypto.subtle.verify to compare - this is natively timing-safe
  // It computes HMAC(key, b) and compares against aHmac in constant time
  return crypto.subtle.verify('HMAC', key, aHmac, bBytes)
}

/**
 * Validates that a timestamp is within the tolerance window
 */
function isTimestampValid(timestamp: number, toleranceSeconds: number): boolean {
  const now = Math.floor(Date.now() / 1000)
  const diff = Math.abs(now - timestamp)
  return diff <= toleranceSeconds
}

// ============================================================================
// GitHub Webhook Verification
// ============================================================================

/**
 * Verifies a GitHub webhook signature
 *
 * GitHub sends webhooks with the X-Hub-Signature-256 header containing
 * the HMAC-SHA256 signature in the format: sha256={hex}
 *
 * @param secret - The webhook secret configured in GitHub
 * @param payload - The raw request body as a string
 * @param signature - The X-Hub-Signature-256 header value
 * @returns Verification result with valid status and optional error
 *
 * @example
 * ```ts
 * const result = await verifyGitHubSignature(
 *   'my-secret',
 *   request.body,
 *   request.headers.get('X-Hub-Signature-256')
 * )
 * if (!result.valid) {
 *   return new Response('Invalid signature', { status: 401 })
 * }
 * ```
 */
export async function verifyGitHubSignature(secret: string, payload: string, signature: string): Promise<VerificationResult> {
  // Validate signature format
  if (!signature || !signature.startsWith('sha256=')) {
    return { valid: false, error: 'Invalid signature format' }
  }

  const providedHash = signature.slice(7) // Remove 'sha256=' prefix

  // Validate hex format (64 characters for SHA-256)
  if (!/^[a-f0-9]{64}$/i.test(providedHash)) {
    // If it looks like hex but wrong length or invalid chars, it's an invalid signature
    // If it's completely malformed, it's a format error
    if (providedHash.length > 0) {
      return { valid: false, error: 'Invalid signature' }
    }
    return { valid: false, error: 'Invalid signature format' }
  }

  // Normalize to lowercase for comparison
  const normalizedProvidedHash = providedHash.toLowerCase()

  // Compute expected signature
  const expectedHash = await hmacSha256(secret, payload)

  // Timing-safe comparison
  const isValid = await timingSafeEqual(normalizedProvidedHash, expectedHash)

  if (!isValid) {
    return { valid: false, error: 'Invalid signature' }
  }

  return { valid: true }
}

/**
 * Generates a GitHub webhook signature for testing
 *
 * @param secret - The webhook secret
 * @param payload - The payload to sign
 * @returns The signature in sha256={hex} format
 */
export async function generateGitHubSignature(secret: string, payload: string): Promise<string> {
  const hash = await hmacSha256(secret, payload)
  return `sha256=${hash}`
}

// ============================================================================
// Stripe Webhook Verification
// ============================================================================

/**
 * Parses a Stripe signature header
 *
 * Format: t={timestamp},v1={sig1},v1={sig2},...
 */
function parseStripeHeader(header: string): { timestamp: number | null; signatures: string[] } {
  const parts = header.split(',')
  let timestamp: number | null = null
  const signatures: string[] = []

  for (const part of parts) {
    const [key, value] = part.split('=')
    if (key === 't' && value !== undefined) {
      const parsed = parseInt(value, 10)
      timestamp = isNaN(parsed) ? null : parsed
    } else if (key === 'v1' && value) {
      signatures.push(value)
    }
  }

  return { timestamp, signatures }
}

/**
 * Verifies a Stripe webhook signature
 *
 * Stripe sends webhooks with the Stripe-Signature header containing
 * a timestamp and one or more v1 signatures (for key rotation).
 * Format: t={timestamp},v1={sig1},v1={sig2}
 *
 * The signature is computed as: HMAC-SHA256({timestamp}.{payload})
 *
 * @param secret - The webhook signing secret (starts with whsec_)
 * @param payload - The raw request body as a string
 * @param signatureHeader - The Stripe-Signature header value
 * @param toleranceSeconds - Maximum age of the webhook in seconds (default: 300)
 * @returns Verification result with valid status, timestamp, and optional error
 *
 * @example
 * ```ts
 * const result = await verifyStripeSignature(
 *   process.env.STRIPE_WEBHOOK_SECRET,
 *   request.body,
 *   request.headers.get('Stripe-Signature')
 * )
 * if (!result.valid) {
 *   return new Response(result.error, { status: 401 })
 * }
 * ```
 */
export async function verifyStripeSignature(
  secret: string,
  payload: string,
  signatureHeader: string,
  toleranceSeconds: number = 300
): Promise<VerificationResult> {
  // Parse the header
  const { timestamp, signatures } = parseStripeHeader(signatureHeader)

  // Validate timestamp is present and numeric
  if (timestamp === null) {
    const hasT = signatureHeader.includes('t=')
    if (!hasT) {
      return { valid: false, error: 'Invalid header format: missing timestamp' }
    }
    return { valid: false, error: 'Invalid header format: invalid timestamp' }
  }

  // Validate at least one v1 signature
  if (signatures.length === 0) {
    return { valid: false, error: 'Invalid header format: no v1 signatures' }
  }

  // Validate timestamp is within tolerance
  if (!isTimestampValid(timestamp, toleranceSeconds)) {
    return { valid: false, error: 'timestamp outside tolerance window', timestamp }
  }

  // Compute expected signature: HMAC-SHA256({timestamp}.{payload})
  const signedPayload = `${timestamp}.${payload}`
  const expectedSignature = await hmacSha256(secret, signedPayload)

  // Check if any v1 signature matches (supports key rotation)
  for (const sig of signatures) {
    const isValid = await timingSafeEqual(sig, expectedSignature)
    if (isValid) {
      return { valid: true, timestamp }
    }
  }

  return { valid: false, error: 'Invalid signature', timestamp }
}

/**
 * Generates a Stripe webhook signature for testing
 *
 * @param secret - The webhook signing secret
 * @param payload - The payload to sign
 * @param timestamp - Unix timestamp in seconds
 * @returns The signature header in t={timestamp},v1={sig} format
 */
export async function generateStripeSignature(secret: string, payload: string, timestamp: number): Promise<string> {
  const signedPayload = `${timestamp}.${payload}`
  const signature = await hmacSha256(secret, signedPayload)
  return `t=${timestamp},v1=${signature}`
}

// ============================================================================
// WorkOS Webhook Verification
// ============================================================================

/**
 * Parses a WorkOS signature header
 *
 * Format: timestamp={ts},signature={hex}
 */
function parseWorkOSHeader(header: string): { timestamp: number | null; signature: string | null } {
  const parts = header.split(',')
  let timestamp: number | null = null
  let signature: string | null = null

  for (const part of parts) {
    const [key, value] = part.split('=')
    if (key === 'timestamp' && value !== undefined) {
      const parsed = parseInt(value, 10)
      timestamp = isNaN(parsed) ? null : parsed
    } else if (key === 'signature' && value) {
      signature = value
    }
  }

  return { timestamp, signature }
}

/**
 * Verifies a WorkOS webhook signature
 *
 * WorkOS sends webhooks with a signature header containing
 * a timestamp and signature.
 * Format: timestamp={ts},signature={hex}
 *
 * The signature is computed as: HMAC-SHA256({payload}) using the raw body
 *
 * @param secret - The webhook signing secret
 * @param payload - The raw request body as a string
 * @param signatureHeader - The WorkOS-Signature header value
 * @param toleranceSeconds - Maximum age of the webhook in seconds (default: 300)
 * @returns Verification result with valid status, timestamp, and optional error
 *
 * @example
 * ```ts
 * const result = await verifyWorkOSSignature(
 *   process.env.WORKOS_WEBHOOK_SECRET,
 *   request.body,
 *   request.headers.get('WorkOS-Signature')
 * )
 * if (!result.valid) {
 *   return new Response(result.error, { status: 401 })
 * }
 * ```
 */
export async function verifyWorkOSSignature(
  secret: string,
  payload: string,
  signatureHeader: string,
  toleranceSeconds: number = 300
): Promise<VerificationResult> {
  // Parse the header
  const { timestamp, signature } = parseWorkOSHeader(signatureHeader)

  // Validate timestamp is present and numeric
  if (timestamp === null) {
    const hasTimestamp = signatureHeader.includes('timestamp=')
    if (!hasTimestamp) {
      return { valid: false, error: 'Invalid header format: missing timestamp' }
    }
    return { valid: false, error: 'Invalid header format: invalid timestamp' }
  }

  // Validate signature is present
  if (!signature) {
    return { valid: false, error: 'Invalid header format: missing signature' }
  }

  // Validate timestamp is within tolerance
  if (!isTimestampValid(timestamp, toleranceSeconds)) {
    return { valid: false, error: 'timestamp outside tolerance window', timestamp }
  }

  // Compute expected signature: HMAC-SHA256({payload})
  const expectedSignature = await hmacSha256(secret, payload)

  // Timing-safe comparison
  const isValid = await timingSafeEqual(signature, expectedSignature)

  if (!isValid) {
    return { valid: false, error: 'Invalid signature', timestamp }
  }

  return { valid: true, timestamp }
}

/**
 * Generates a WorkOS webhook signature for testing
 *
 * @param secret - The webhook signing secret
 * @param payload - The payload to sign
 * @param timestamp - Unix timestamp in seconds
 * @returns The signature header in timestamp={ts},signature={hex} format
 */
export async function generateWorkOSSignature(secret: string, payload: string, timestamp: number): Promise<string> {
  const signature = await hmacSha256(secret, payload)
  return `timestamp=${timestamp},signature=${signature}`
}

// ============================================================================
// Slack Webhook Verification
// ============================================================================

/**
 * Verifies a Slack webhook signature
 *
 * Slack sends webhooks with X-Slack-Signature and X-Slack-Request-Timestamp headers.
 * The signature format is: v0={hex}
 * The signed string is: v0:{timestamp}:{body}
 *
 * @param secret - The Slack signing secret
 * @param payload - The raw request body as a string
 * @param signature - The X-Slack-Signature header value
 * @param timestamp - The X-Slack-Request-Timestamp header value (Unix seconds as string)
 * @param toleranceSeconds - Maximum age of the webhook in seconds (default: 300)
 * @returns Verification result with valid status, timestamp, and optional error
 *
 * @example
 * ```ts
 * const result = await verifySlackSignature(
 *   process.env.SLACK_SIGNING_SECRET,
 *   request.body,
 *   request.headers.get('X-Slack-Signature'),
 *   request.headers.get('X-Slack-Request-Timestamp')
 * )
 * if (!result.valid) {
 *   return new Response(result.error, { status: 401 })
 * }
 * ```
 */
export async function verifySlackSignature(
  secret: string,
  payload: string,
  signature: string,
  timestamp: string,
  toleranceSeconds: number = 300
): Promise<VerificationResult> {
  // Validate signature format
  if (!signature || !signature.startsWith('v0=')) {
    return { valid: false, error: 'Invalid signature format' }
  }

  // Parse timestamp
  const ts = parseInt(timestamp, 10)
  if (isNaN(ts)) {
    return { valid: false, error: 'Invalid timestamp' }
  }

  // Validate timestamp is within tolerance
  if (!isTimestampValid(ts, toleranceSeconds)) {
    return { valid: false, error: 'timestamp outside tolerance window', timestamp: ts }
  }

  // Compute expected signature: HMAC-SHA256("v0:{timestamp}:{body}")
  const signedPayload = `v0:${timestamp}:${payload}`
  const expectedHash = await hmacSha256(secret, signedPayload)
  const expectedSignature = `v0=${expectedHash}`

  // Timing-safe comparison
  const isValid = await timingSafeEqual(signature, expectedSignature)

  if (!isValid) {
    return { valid: false, error: 'Invalid signature', timestamp: ts }
  }

  return { valid: true, timestamp: ts }
}

/**
 * Generates a Slack webhook signature for testing
 *
 * @param secret - The Slack signing secret
 * @param payload - The payload to sign
 * @param timestamp - Unix timestamp in seconds (or string)
 * @returns The signature in v0={hex} format
 */
export async function generateSlackSignature(
  secret: string,
  payload: string,
  timestamp: string | number
): Promise<string> {
  const signedPayload = `v0:${timestamp}:${payload}`
  const hash = await hmacSha256(secret, signedPayload)
  return `v0=${hash}`
}

// ============================================================================
// Linear Webhook Verification
// ============================================================================

/**
 * Verifies a Linear webhook signature
 *
 * Linear sends webhooks with a Linear-Signature header.
 * The signature is a raw HMAC-SHA256 hex digest of the body.
 *
 * @param secret - The Linear webhook signing secret
 * @param payload - The raw request body as a string
 * @param signature - The Linear-Signature header value
 * @returns Verification result with valid status and optional error
 *
 * @example
 * ```ts
 * const result = await verifyLinearSignature(
 *   process.env.LINEAR_WEBHOOK_SECRET,
 *   request.body,
 *   request.headers.get('Linear-Signature')
 * )
 * if (!result.valid) {
 *   return new Response(result.error, { status: 401 })
 * }
 * ```
 */
export async function verifyLinearSignature(secret: string, payload: string, signature: string): Promise<VerificationResult> {
  // Validate signature is present
  if (!signature) {
    return { valid: false, error: 'Missing signature' }
  }

  // Validate hex format (64 characters for SHA-256)
  if (!/^[a-f0-9]{64}$/i.test(signature)) {
    return { valid: false, error: 'Invalid signature format' }
  }

  // Normalize to lowercase
  const normalizedSignature = signature.toLowerCase()

  // Compute expected signature
  const expectedSignature = await hmacSha256(secret, payload)

  // Timing-safe comparison
  const isValid = await timingSafeEqual(normalizedSignature, expectedSignature)

  if (!isValid) {
    return { valid: false, error: 'Invalid signature' }
  }

  return { valid: true }
}

/**
 * Generates a Linear webhook signature for testing
 *
 * @param secret - The Linear webhook signing secret
 * @param payload - The payload to sign
 * @returns The HMAC-SHA256 hex digest
 */
export async function generateLinearSignature(secret: string, payload: string): Promise<string> {
  return hmacSha256(secret, payload)
}

// ============================================================================
// Svix Webhook Verification
// ============================================================================

/**
 * Parses a Svix signature header
 *
 * Format: v1,{sig1} v1,{sig2} ... (space-separated, each with v1, prefix)
 * Multiple signatures support key rotation
 */
function parseSvixSignatures(header: string): string[] {
  const signatures: string[] = []
  const parts = header.split(' ')

  for (const part of parts) {
    if (part.startsWith('v1,')) {
      signatures.push(part.slice(3)) // Remove 'v1,' prefix, keep the base64 signature
    }
  }

  return signatures
}

/**
 * Verifies a Svix webhook signature
 *
 * Svix sends webhooks with svix-id, svix-timestamp, and svix-signature headers.
 * The signature format is: v1,{base64sig1} {base64sig2}
 * The signed string is: {msg_id}.{timestamp}.{body}
 *
 * @param secret - The Svix webhook signing secret (base64 encoded, may have whsec_ prefix)
 * @param payload - The raw request body as a string
 * @param messageId - The svix-id header value
 * @param timestamp - The svix-timestamp header value (Unix seconds as string)
 * @param signature - The svix-signature header value
 * @param toleranceSeconds - Maximum age of the webhook in seconds (default: 300)
 * @returns Verification result with valid status, timestamp, and optional error
 *
 * @example
 * ```ts
 * const result = await verifySvixSignature(
 *   process.env.SVIX_WEBHOOK_SECRET,
 *   request.body,
 *   request.headers.get('svix-id'),
 *   request.headers.get('svix-timestamp'),
 *   request.headers.get('svix-signature')
 * )
 * if (!result.valid) {
 *   return new Response(result.error, { status: 401 })
 * }
 * ```
 */
export async function verifySvixSignature(
  secret: string,
  payload: string,
  messageId: string,
  timestamp: string,
  signature: string,
  toleranceSeconds: number = 300
): Promise<VerificationResult> {
  // Validate required fields
  if (!messageId) {
    return { valid: false, error: 'Missing message ID' }
  }
  if (!signature) {
    return { valid: false, error: 'Missing signature' }
  }

  // Parse timestamp
  const ts = parseInt(timestamp, 10)
  if (isNaN(ts)) {
    return { valid: false, error: 'Invalid timestamp' }
  }

  // Validate timestamp is within tolerance
  if (!isTimestampValid(ts, toleranceSeconds)) {
    return { valid: false, error: 'timestamp outside tolerance window', timestamp: ts }
  }

  // Parse signatures
  const signatures = parseSvixSignatures(signature)
  if (signatures.length === 0) {
    return { valid: false, error: 'Invalid signature format' }
  }

  // Validate and decode secret (must have whsec_ prefix)
  if (!secret.startsWith('whsec_')) {
    return { valid: false, error: 'Invalid secret format: missing whsec_ prefix' }
  }

  let secretBytes: Uint8Array
  try {
    const secretStr = secret.slice(6) // Remove whsec_ prefix
    // Decode base64 secret
    const binaryStr = atob(secretStr)
    secretBytes = new Uint8Array(binaryStr.length)
    for (let i = 0; i < binaryStr.length; i++) {
      secretBytes[i] = binaryStr.charCodeAt(i)
    }
  } catch {
    return { valid: false, error: 'Invalid secret format' }
  }

  // Compute expected signature: HMAC-SHA256("{msg_id}.{timestamp}.{body}")
  const signedPayload = `${messageId}.${timestamp}.${payload}`
  const signedPayloadBytes = encodeString(signedPayload)

  const key = await crypto.subtle.importKey('raw', secretBytes.buffer as ArrayBuffer, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])

  const expectedSigBuffer = await crypto.subtle.sign('HMAC', key, signedPayloadBytes.buffer as ArrayBuffer)

  // Convert to base64 for comparison
  const expectedSigBytes = new Uint8Array(expectedSigBuffer)
  let binary = ''
  for (let i = 0; i < expectedSigBytes.length; i++) {
    binary += String.fromCharCode(expectedSigBytes[i]!)
  }
  const expectedSigBase64 = btoa(binary)

  // Check if any signature matches (supports key rotation)
  for (const sig of signatures) {
    const isValid = await timingSafeEqual(sig, expectedSigBase64)
    if (isValid) {
      return { valid: true, timestamp: ts }
    }
  }

  return { valid: false, error: 'Invalid signature', timestamp: ts }
}

/**
 * Generates a Svix webhook signature for testing
 *
 * @param secret - The Svix webhook signing secret (with whsec_ prefix)
 * @param messageId - The message ID (svix-id header)
 * @param timestamp - Unix timestamp in seconds (or string)
 * @param payload - The payload to sign
 * @returns The signature in v1,{base64} format
 */
export async function generateSvixSignature(
  secret: string,
  messageId: string,
  timestamp: string | number,
  payload: string
): Promise<string> {
  // Prepare secret (remove whsec_ prefix if present and decode base64)
  const secretStr = secret.startsWith('whsec_') ? secret.slice(6) : secret
  const binaryStr = atob(secretStr)
  const secretBytes = new Uint8Array(binaryStr.length)
  for (let i = 0; i < binaryStr.length; i++) {
    secretBytes[i] = binaryStr.charCodeAt(i)
  }

  // Compute signature
  const signedPayload = `${messageId}.${timestamp}.${payload}`
  const signedPayloadBytes = encodeString(signedPayload)
  const key = await crypto.subtle.importKey('raw', secretBytes.buffer as ArrayBuffer, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const sigBuffer = await crypto.subtle.sign('HMAC', key, signedPayloadBytes.buffer as ArrayBuffer)

  // Convert to base64
  const sigBytes = new Uint8Array(sigBuffer)
  let binary = ''
  for (let i = 0; i < sigBytes.length; i++) {
    binary += String.fromCharCode(sigBytes[i]!)
  }
  const sigBase64 = btoa(binary)

  return `v1,${sigBase64}`
}
