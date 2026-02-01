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

// ============================================================================
// Shared Utilities
// ============================================================================

/**
 * Encodes a string to Uint8Array using UTF-8
 */
function encodeString(str: string): Uint8Array {
  return new TextEncoder().encode(str)
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
  const key = await crypto.subtle.importKey('raw', encodeString(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])

  const signature = await crypto.subtle.sign('HMAC', key, encodeString(message))

  return bufferToHex(signature)
}

/**
 * Timing-safe string comparison using Web Crypto API
 *
 * Compares two strings in constant time to prevent timing attacks.
 * Falls back to a manual constant-time comparison if subtle.timingSafeEqual is unavailable.
 */
async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  // If lengths differ, still compare to maintain constant time
  // We'll pad the shorter one to match lengths
  const maxLength = Math.max(a.length, b.length)
  const aPadded = a.padEnd(maxLength, '\0')
  const bPadded = b.padEnd(maxLength, '\0')

  const aBytes = encodeString(aPadded)
  const bBytes = encodeString(bPadded)

  // XOR all bytes and accumulate - constant time operation
  let result = 0
  for (let i = 0; i < aBytes.length; i++) {
    result |= aBytes[i] ^ bBytes[i]
  }

  // Also check original lengths match
  result |= a.length ^ b.length

  return result === 0
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
    if (key === 't') {
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
    if (key === 'timestamp') {
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

  const key = await crypto.subtle.importKey('raw', secretBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])

  const expectedSigBuffer = await crypto.subtle.sign('HMAC', key, encodeString(signedPayload))

  // Convert to base64 for comparison
  const expectedSigBytes = new Uint8Array(expectedSigBuffer)
  let binary = ''
  for (let i = 0; i < expectedSigBytes.length; i++) {
    binary += String.fromCharCode(expectedSigBytes[i])
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
  const key = await crypto.subtle.importKey('raw', secretBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const sigBuffer = await crypto.subtle.sign('HMAC', key, encodeString(signedPayload))

  // Convert to base64
  const sigBytes = new Uint8Array(sigBuffer)
  let binary = ''
  for (let i = 0; i < sigBytes.length; i++) {
    binary += String.fromCharCode(sigBytes[i])
  }
  const sigBase64 = btoa(binary)

  return `v1,${sigBase64}`
}
