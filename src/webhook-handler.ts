/**
 * Webhook Handler for events.do
 *
 * Receives and verifies webhooks from various providers:
 * - GitHub
 * - Stripe
 * - WorkOS
 * - Slack
 * - Linear
 * - Svix (generic webhook infrastructure)
 *
 * Normalizes all webhooks to a standard event format for the event stream.
 */

// Import from local source (not package) for proper bundling
import {
  verifyGitHubSignature,
  verifyStripeSignature,
  verifyWorkOSSignature,
  verifySlackSignature,
  verifyLinearSignature,
  verifySvixSignature,
  type VerificationResult,
  type WebhookProvider,
  type WebhookProviderConfig,
  WEBHOOK_PROVIDERS,
  isValidProvider,
  validateWebhookConfig,
  WebhookConfigError,
  WebhookSignatureError,
} from '../core/src/webhooks'
import { PayloadTooLargeError, InvalidJsonError, ValidationError } from '../core/src/errors'
import { MAX_WEBHOOK_BODY_SIZE } from './middleware/ingest/types'
import { readBodyWithLimit } from './middleware/ingest/validate'
import { logger, sanitize } from './logger'

const log = logger.child({ component: 'webhook' })

// ============================================================================
// Types
// ============================================================================

export interface WebhookEnv {
  GITHUB_WEBHOOK_SECRET?: string
  STRIPE_WEBHOOK_SECRET?: string
  WORKOS_WEBHOOK_SECRET?: string
  SLACK_SIGNING_SECRET?: string
  LINEAR_WEBHOOK_SECRET?: string
  SVIX_WEBHOOK_SECRET?: string
}

/**
 * Result of webhook processing
 */
export interface WebhookResult {
  success: boolean
  verified: boolean
  event?: NormalizedWebhookEvent
  error?: string
  errorCode?: WebhookErrorCode
}

/**
 * Error codes for webhook processing failures
 */
export type WebhookErrorCode =
  | 'UNSUPPORTED_PROVIDER'
  | 'SECRET_NOT_CONFIGURED'
  | 'INVALID_SIGNATURE'
  | 'INVALID_JSON'
  | 'MISSING_HEADERS'
  | 'TIMESTAMP_EXPIRED'
  | 'CONFIG_ERROR'
  | 'PAYLOAD_TOO_LARGE'
  | 'UNKNOWN_ERROR'

/**
 * Normalized webhook event format for the event stream
 */
export interface NormalizedWebhookEvent {
  /** Event type, e.g., "webhook.github.push", "webhook.stripe.payment_intent.succeeded" */
  type: string
  /** ISO timestamp */
  ts: string
  /** Provider name */
  source: string
  /** Webhook metadata */
  webhook: {
    provider: string
    eventType: string // Original event type from provider
    deliveryId?: string | undefined
    verified: boolean
  }
  /** Original webhook body */
  payload: unknown
}

// Re-export for convenience
export { WEBHOOK_PROVIDERS, isValidProvider, WebhookConfigError, WebhookSignatureError }
export type { WebhookProvider, WebhookProviderConfig }

// ============================================================================
// Secret Mapping
// ============================================================================

/**
 * Gets the webhook secret for a validated provider from environment
 * @param env - The environment containing webhook secrets
 * @param provider - The validated webhook provider
 * @returns The secret if configured, undefined otherwise
 */
function getSecretForProvider(env: WebhookEnv, provider: WebhookProvider): string | undefined {
  const secretMap: Record<WebhookProvider, string | undefined> = {
    github: env.GITHUB_WEBHOOK_SECRET,
    stripe: env.STRIPE_WEBHOOK_SECRET,
    workos: env.WORKOS_WEBHOOK_SECRET,
    slack: env.SLACK_SIGNING_SECRET,
    linear: env.LINEAR_WEBHOOK_SECRET,
    svix: env.SVIX_WEBHOOK_SECRET,
  }
  return secretMap[provider]
}

/**
 * Creates a type-safe provider configuration using an exhaustive switch pattern
 * @param provider - The validated webhook provider
 * @param secret - The webhook secret
 * @returns The properly typed provider configuration
 */
function createProviderConfig(provider: WebhookProvider, secret: string): WebhookProviderConfig {
  switch (provider) {
    case 'github':
      return { provider: 'github', secret, enabled: true }
    case 'stripe':
      return { provider: 'stripe', secret, enabled: true }
    case 'workos':
      return { provider: 'workos', secret, enabled: true }
    case 'slack':
      return { provider: 'slack', secret, enabled: true }
    case 'linear':
      return { provider: 'linear', secret, enabled: true }
    case 'svix':
      return { provider: 'svix', secret, enabled: true }
  }
}

/**
 * Creates a webhook configuration from environment variables
 * @param env - The environment containing webhook secrets
 * @param provider - The validated webhook provider
 * @returns The provider configuration or null if not configured
 */
function createProviderConfigFromEnv(env: WebhookEnv, provider: WebhookProvider): WebhookProviderConfig | null {
  const secret = getSecretForProvider(env, provider)
  if (!secret) {
    return null
  }

  // Build configuration using type-safe builder pattern
  const config = createProviderConfig(provider, secret)

  // Validate the configuration
  const errors = validateWebhookConfig(config)
  if (errors.length > 0) {
    log.error('Invalid webhook configuration', { provider, errorCount: errors.length })
    return null
  }

  return config
}

// ============================================================================
// Event Type Extraction
// ============================================================================

/**
 * Safely extracts a string property from an unknown object
 */
function safeGetString(obj: unknown, key: string): string | undefined {
  if (obj && typeof obj === 'object' && key in obj) {
    const value = (obj as Record<string, unknown>)[key]
    return typeof value === 'string' ? value : undefined
  }
  return undefined
}

/**
 * Extracts the event type from a webhook payload based on provider conventions
 * @param provider - The validated webhook provider
 * @param headers - The request headers
 * @param payload - The parsed JSON payload
 * @returns The extracted event type or 'unknown'
 */
function extractEventType(provider: WebhookProvider, headers: Headers, payload: unknown): string {
  switch (provider) {
    case 'github':
      // GitHub: X-GitHub-Event header (e.g., "push", "pull_request")
      return headers.get('X-GitHub-Event') ?? 'unknown'

    case 'stripe':
      // Stripe: type field in body (e.g., "payment_intent.succeeded")
      return safeGetString(payload, 'type') ?? 'unknown'

    case 'slack': {
      // Slack: type field or event.type in body
      const type = safeGetString(payload, 'type')
      if (type) return type
      const event = payload && typeof payload === 'object' ? (payload as Record<string, unknown>).event : undefined
      return safeGetString(event, 'type') ?? 'unknown'
    }

    case 'linear':
      // Linear: type field (e.g., "Issue", "Comment")
      return safeGetString(payload, 'type') ?? 'unknown'

    case 'workos':
      // WorkOS: event field
      return safeGetString(payload, 'event') ?? 'unknown'

    case 'svix':
      // Svix: type field
      return safeGetString(payload, 'type') ?? 'unknown'
  }
}

/**
 * Extracts the delivery ID from webhook headers based on provider conventions
 * @param provider - The validated webhook provider
 * @param headers - The request headers
 * @returns The delivery ID if available
 */
function extractDeliveryId(provider: WebhookProvider, headers: Headers): string | undefined {
  const deliveryIdHeaders: Partial<Record<WebhookProvider, string>> = {
    github: 'X-GitHub-Delivery',
    stripe: 'Stripe-Idempotency-Key',
    svix: 'svix-id',
  }
  const headerName = deliveryIdHeaders[provider]
  return headerName ? (headers.get(headerName) ?? undefined) : undefined
}

// ============================================================================
// Signature Verification
// ============================================================================

/**
 * Required headers for each webhook provider
 */
const REQUIRED_HEADERS: Record<WebhookProvider, string[]> = {
  github: ['X-Hub-Signature-256'],
  stripe: ['Stripe-Signature'],
  workos: ['WorkOS-Signature'],
  slack: ['X-Slack-Signature', 'X-Slack-Request-Timestamp'],
  linear: ['Linear-Signature'],
  svix: ['svix-id', 'svix-timestamp', 'svix-signature'],
}

/**
 * Validates that all required headers are present for a provider
 * @param provider - The validated webhook provider
 * @param headers - The request headers
 * @returns An array of missing header names, or empty array if all present
 */
function validateRequiredHeaders(provider: WebhookProvider, headers: Headers): string[] {
  const required = REQUIRED_HEADERS[provider]
  return required.filter((header) => !headers.get(header))
}

/**
 * Verifies the webhook signature for a given provider
 * @param provider - The validated webhook provider
 * @param secret - The webhook secret
 * @param rawBody - The raw request body
 * @param headers - The request headers
 * @returns The verification result
 */
async function verifySignature(
  provider: WebhookProvider,
  secret: string,
  rawBody: string,
  headers: Headers
): Promise<VerificationResult> {
  // First, validate that all required headers are present
  const missingHeaders = validateRequiredHeaders(provider, headers)
  if (missingHeaders.length > 0) {
    return {
      valid: false,
      error: `Missing required header${missingHeaders.length > 1 ? 's' : ''}: ${missingHeaders.join(', ')}`,
    }
  }

  switch (provider) {
    case 'github': {
      const signature = headers.get('X-Hub-Signature-256')!
      return verifyGitHubSignature(secret, rawBody, signature)
    }

    case 'stripe': {
      const signature = headers.get('Stripe-Signature')!
      return verifyStripeSignature(secret, rawBody, signature)
    }

    case 'workos': {
      const signature = headers.get('WorkOS-Signature')!
      return verifyWorkOSSignature(secret, rawBody, signature)
    }

    case 'slack': {
      const signature = headers.get('X-Slack-Signature')!
      const timestamp = headers.get('X-Slack-Request-Timestamp')!
      return verifySlackSignature(secret, rawBody, signature, timestamp)
    }

    case 'linear': {
      const signature = headers.get('Linear-Signature')!
      return verifyLinearSignature(secret, rawBody, signature)
    }

    case 'svix': {
      const messageId = headers.get('svix-id')!
      const timestamp = headers.get('svix-timestamp')!
      const signature = headers.get('svix-signature')!
      return verifySvixSignature(secret, rawBody, messageId, timestamp, signature)
    }
  }
}

// ============================================================================
// Main Handler
// ============================================================================

/**
 * Maps verification errors to error codes
 */
function getErrorCodeFromVerification(error: string): WebhookErrorCode {
  if (error.includes('Missing')) {
    return 'MISSING_HEADERS'
  }
  if (error.includes('timestamp')) {
    return 'TIMESTAMP_EXPIRED'
  }
  return 'INVALID_SIGNATURE'
}

/**
 * Creates an error response with consistent structure
 */
function createErrorResponse(
  error: string,
  errorCode: WebhookErrorCode,
  status: number,
  details?: Record<string, unknown>
): Response {
  return Response.json(
    {
      success: false,
      error,
      errorCode,
      ...details,
    },
    { status }
  )
}

/**
 * Handle incoming webhook requests
 *
 * @param request - The incoming request
 * @param env - Environment with webhook secrets
 * @param providerParam - The webhook provider (from query param)
 * @returns Response with normalized event or error
 */
export async function handleWebhook(
  request: Request,
  env: WebhookEnv,
  providerParam: string
): Promise<Response> {
  // Validate provider using type guard
  if (!isValidProvider(providerParam)) {
    log.warn('Unsupported provider', { provider: providerParam })
    return createErrorResponse(
      `Unsupported provider: "${providerParam}"`,
      'UNSUPPORTED_PROVIDER',
      400,
      { supported: [...WEBHOOK_PROVIDERS] }
    )
  }

  const provider: WebhookProvider = providerParam

  // Build and validate provider configuration from environment
  const config = createProviderConfigFromEnv(env, provider)

  // Reject requests if webhook is not properly configured
  if (!config) {
    log.error('Provider not configured or invalid', { provider })
    return createErrorResponse(
      'Webhook not configured for this provider',
      'SECRET_NOT_CONFIGURED',
      500
    )
  }

  // Check Content-Length header first (fast rejection)
  const contentLength = request.headers.get('content-length')
  if (contentLength) {
    const length = parseInt(contentLength, 10)
    if (!isNaN(length) && length > MAX_WEBHOOK_BODY_SIZE) {
      log.warn('Webhook payload too large', { provider, contentLength: length, maxSize: MAX_WEBHOOK_BODY_SIZE })
      return createErrorResponse(
        `Request body too large: ${length} bytes exceeds ${MAX_WEBHOOK_BODY_SIZE} byte limit`,
        'PAYLOAD_TOO_LARGE',
        413,
        { maxSize: MAX_WEBHOOK_BODY_SIZE, contentLength: length }
      )
    }
  }

  // CRITICAL: Get raw body FIRST (before any parsing)
  // Use streaming read with size limit to protect against missing Content-Length
  let rawBody: string
  try {
    rawBody = await readBodyWithLimit(request, MAX_WEBHOOK_BODY_SIZE)
  } catch (error) {
    if (error instanceof PayloadTooLargeError) {
      log.warn('Webhook payload too large during read', { provider })
      return createErrorResponse(
        error.message,
        'PAYLOAD_TOO_LARGE',
        413,
        { maxSize: MAX_WEBHOOK_BODY_SIZE }
      )
    } else if (error instanceof InvalidJsonError) {
      log.warn('Invalid JSON in webhook body', { provider })
      return createErrorResponse(
        error.message,
        'INVALID_JSON',
        400
      )
    } else if (error instanceof ValidationError) {
      log.warn('Validation error reading webhook body', { provider })
      return createErrorResponse(
        error.message,
        'INVALID_JSON',
        400,
        error.details
      )
    } else {
      log.error('Unexpected error reading body', { error })
      return createErrorResponse('Failed to read request body', 'UNKNOWN_ERROR', 500)
    }
  }

  // Verify signature - always required when secret is configured
  const verification = await verifySignature(provider, config.secret, rawBody, request.headers)
  if (!verification.valid) {
    log.warn('Signature verification failed', { provider, errorType: verification.error?.split(':')[0] })
    return createErrorResponse(
      'Signature verification failed',
      getErrorCodeFromVerification(verification.error ?? 'Invalid signature'),
      401,
      { details: verification.error }
    )
  }

  // Parse the body
  let payload: unknown
  try {
    payload = JSON.parse(rawBody)
  } catch (error) {
    const parseError = error instanceof Error ? error.message : 'Unknown parse error'
    log.warn('Invalid JSON body', { provider })
    return createErrorResponse(
      'Invalid JSON body',
      'INVALID_JSON',
      400,
      { details: parseError }
    )
  }

  // Validate that payload is an object (most webhooks send objects)
  if (payload === null || typeof payload !== 'object') {
    log.warn('Payload is not an object', { provider })
    return createErrorResponse(
      'Webhook payload must be a JSON object',
      'INVALID_JSON',
      400
    )
  }

  // Extract event type from payload/headers
  const eventType = extractEventType(provider, request.headers, payload)
  const deliveryId = extractDeliveryId(provider, request.headers)

  // Normalize to standard event format
  const normalizedEvent: NormalizedWebhookEvent = {
    type: `webhook.${provider}.${eventType}`,
    ts: new Date().toISOString(),
    source: provider,
    webhook: {
      provider,
      eventType,
      deliveryId,
      verified: true,
    },
    payload,
  }

  // Log successful webhook receipt (deliveryId is safe - it's a public identifier from the provider)
  log.info('Webhook received', { provider, eventType, deliveryId: deliveryId ? sanitize.id(deliveryId) : undefined })

  // Return the normalized event
  // The caller can decide what to do with it (store in R2, forward to queue, etc.)
  return Response.json({
    success: true,
    accepted: true,
    verified: true,
    event: normalizedEvent,
  })
}

/**
 * Process a webhook with a pre-configured provider config
 * Useful for programmatic webhook handling with explicit configuration
 *
 * @param request - The incoming request
 * @param config - The validated provider configuration
 * @returns The webhook processing result
 */
export async function processWebhookWithConfig(
  request: Request,
  config: WebhookProviderConfig
): Promise<WebhookResult> {
  const provider = config.provider

  // Validate configuration
  const configErrors = validateWebhookConfig(config)
  if (configErrors.length > 0) {
    return {
      success: false,
      verified: false,
      error: `Invalid configuration: ${configErrors.join('; ')}`,
      errorCode: 'CONFIG_ERROR',
    }
  }

  // Check if provider is enabled
  if (config.enabled === false) {
    return {
      success: false,
      verified: false,
      error: 'Provider is disabled',
      errorCode: 'CONFIG_ERROR',
    }
  }

  // Check Content-Length header first (fast rejection)
  const contentLength = request.headers.get('content-length')
  if (contentLength) {
    const length = parseInt(contentLength, 10)
    if (!isNaN(length) && length > MAX_WEBHOOK_BODY_SIZE) {
      return {
        success: false,
        verified: false,
        error: `Request body too large: ${length} bytes exceeds ${MAX_WEBHOOK_BODY_SIZE} byte limit`,
        errorCode: 'PAYLOAD_TOO_LARGE',
      }
    }
  }

  // Get raw body with size limit
  let rawBody: string
  try {
    rawBody = await readBodyWithLimit(request, MAX_WEBHOOK_BODY_SIZE)
  } catch (error) {
    if (error instanceof PayloadTooLargeError) {
      return {
        success: false,
        verified: false,
        error: error.message,
        errorCode: 'PAYLOAD_TOO_LARGE',
      }
    }
    return {
      success: false,
      verified: false,
      error: 'Failed to read request body',
      errorCode: 'UNKNOWN_ERROR',
    }
  }

  // Verify signature
  const verification = await verifySignature(provider, config.secret, rawBody, request.headers)
  if (!verification.valid) {
    return {
      success: false,
      verified: false,
      error: verification.error ?? 'Signature verification failed',
      errorCode: getErrorCodeFromVerification(verification.error ?? 'Invalid signature'),
    }
  }

  // Parse body
  let payload: unknown
  try {
    payload = JSON.parse(rawBody)
  } catch {
    return {
      success: false,
      verified: false,
      error: 'Invalid JSON body',
      errorCode: 'INVALID_JSON',
    }
  }

  if (payload === null || typeof payload !== 'object') {
    return {
      success: false,
      verified: false,
      error: 'Webhook payload must be a JSON object',
      errorCode: 'INVALID_JSON',
    }
  }

  // Extract event info and create normalized event
  const eventType = extractEventType(provider, request.headers, payload)
  const deliveryId = extractDeliveryId(provider, request.headers)

  const event: NormalizedWebhookEvent = {
    type: `webhook.${provider}.${eventType}`,
    ts: new Date().toISOString(),
    source: provider,
    webhook: {
      provider,
      eventType,
      deliveryId,
      verified: true,
    },
    payload,
  }

  return {
    success: true,
    verified: true,
    event,
  }
}
