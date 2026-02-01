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
} from '../core/src/webhooks'

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
    deliveryId?: string
    verified: boolean
  }
  /** Original webhook body */
  payload: unknown
}

// Supported providers
type Provider = 'github' | 'stripe' | 'workos' | 'slack' | 'linear' | 'svix'

const SUPPORTED_PROVIDERS: Provider[] = ['github', 'stripe', 'workos', 'slack', 'linear', 'svix']

// ============================================================================
// Secret Mapping
// ============================================================================

function getSecretForProvider(env: WebhookEnv, provider: string): string | undefined {
  switch (provider) {
    case 'github':
      return env.GITHUB_WEBHOOK_SECRET
    case 'stripe':
      return env.STRIPE_WEBHOOK_SECRET
    case 'workos':
      return env.WORKOS_WEBHOOK_SECRET
    case 'slack':
      return env.SLACK_SIGNING_SECRET
    case 'linear':
      return env.LINEAR_WEBHOOK_SECRET
    case 'svix':
      return env.SVIX_WEBHOOK_SECRET
    default:
      return undefined
  }
}

// ============================================================================
// Event Type Extraction
// ============================================================================

function extractEventType(provider: string, headers: Headers, payload: unknown): string {
  const body = payload as Record<string, unknown>

  switch (provider) {
    case 'github':
      // GitHub: X-GitHub-Event header (e.g., "push", "pull_request")
      return headers.get('X-GitHub-Event') ?? 'unknown'

    case 'stripe':
      // Stripe: type field in body (e.g., "payment_intent.succeeded")
      return typeof body.type === 'string' ? body.type : 'unknown'

    case 'slack':
      // Slack: type field or event.type in body
      if (typeof body.type === 'string') return body.type
      if (body.event && typeof (body.event as Record<string, unknown>).type === 'string') {
        return (body.event as Record<string, unknown>).type as string
      }
      return 'unknown'

    case 'linear':
      // Linear: type field (e.g., "Issue", "Comment")
      return typeof body.type === 'string' ? body.type : 'unknown'

    case 'workos':
      // WorkOS: event field
      return typeof body.event === 'string' ? body.event : 'unknown'

    case 'svix':
      // Svix: type field
      return typeof body.type === 'string' ? body.type : 'unknown'

    default:
      return 'unknown'
  }
}

function extractDeliveryId(provider: string, headers: Headers): string | undefined {
  switch (provider) {
    case 'github':
      return headers.get('X-GitHub-Delivery') ?? undefined
    case 'stripe':
      // Stripe doesn't have a delivery ID header, but has idempotency
      return headers.get('Stripe-Idempotency-Key') ?? undefined
    case 'svix':
      return headers.get('svix-id') ?? undefined
    default:
      return undefined
  }
}

// ============================================================================
// Signature Verification
// ============================================================================

async function verifySignature(
  provider: string,
  secret: string,
  rawBody: string,
  headers: Headers
): Promise<VerificationResult> {
  switch (provider) {
    case 'github': {
      const signature = headers.get('X-Hub-Signature-256')
      if (!signature) {
        return { valid: false, error: 'Missing X-Hub-Signature-256 header' }
      }
      return verifyGitHubSignature(secret, rawBody, signature)
    }

    case 'stripe': {
      const signature = headers.get('Stripe-Signature')
      if (!signature) {
        return { valid: false, error: 'Missing Stripe-Signature header' }
      }
      return verifyStripeSignature(secret, rawBody, signature)
    }

    case 'workos': {
      const signature = headers.get('WorkOS-Signature')
      if (!signature) {
        return { valid: false, error: 'Missing WorkOS-Signature header' }
      }
      return verifyWorkOSSignature(secret, rawBody, signature)
    }

    case 'slack': {
      const signature = headers.get('X-Slack-Signature')
      const timestamp = headers.get('X-Slack-Request-Timestamp')
      if (!signature) {
        return { valid: false, error: 'Missing X-Slack-Signature header' }
      }
      if (!timestamp) {
        return { valid: false, error: 'Missing X-Slack-Request-Timestamp header' }
      }
      return verifySlackSignature(secret, rawBody, signature, timestamp)
    }

    case 'linear': {
      const signature = headers.get('Linear-Signature')
      if (!signature) {
        return { valid: false, error: 'Missing Linear-Signature header' }
      }
      return verifyLinearSignature(secret, rawBody, signature)
    }

    case 'svix': {
      const messageId = headers.get('svix-id')
      const timestamp = headers.get('svix-timestamp')
      const signature = headers.get('svix-signature')
      if (!messageId) {
        return { valid: false, error: 'Missing svix-id header' }
      }
      if (!timestamp) {
        return { valid: false, error: 'Missing svix-timestamp header' }
      }
      if (!signature) {
        return { valid: false, error: 'Missing svix-signature header' }
      }
      return verifySvixSignature(secret, rawBody, messageId, timestamp, signature)
    }

    default:
      return { valid: false, error: `Unsupported provider: ${provider}` }
  }
}

// ============================================================================
// Main Handler
// ============================================================================

/**
 * Handle incoming webhook requests
 *
 * @param request - The incoming request
 * @param env - Environment with webhook secrets
 * @param provider - The webhook provider (from query param)
 * @returns Response with normalized event or error
 */
export async function handleWebhook(
  request: Request,
  env: WebhookEnv,
  provider: string
): Promise<Response> {
  // Validate provider
  if (!SUPPORTED_PROVIDERS.includes(provider as Provider)) {
    return Response.json(
      {
        error: 'Unsupported provider',
        supported: SUPPORTED_PROVIDERS,
      },
      { status: 400 }
    )
  }

  // Get secret for provider (optional - skip verification if not configured)
  const secret = getSecretForProvider(env, provider)

  // CRITICAL: Get raw body FIRST (before any parsing)
  const rawBody = await request.text()

  // Verify signature if secret is configured
  let verified = false
  if (secret) {
    const verification = await verifySignature(provider, secret, rawBody, request.headers)
    if (!verification.valid) {
      console.warn(`[webhook] Signature verification failed for ${provider}: ${verification.error}`)
      return Response.json(
        { error: 'Invalid signature', details: verification.error },
        { status: 401 }
      )
    }
    verified = true
  } else {
    console.warn(`[webhook] Provider ${provider} secret not configured - skipping verification`)
  }

  // Parse the body
  let payload: unknown
  try {
    payload = JSON.parse(rawBody)
  } catch {
    console.warn(`[webhook] Invalid JSON body from ${provider}`)
    return Response.json(
      { error: 'Invalid JSON body' },
      { status: 400 }
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
      verified,
    },
    payload,
  }

  // Log successful webhook receipt
  console.log(`[webhook] Received ${provider}.${eventType}${deliveryId ? ` (${deliveryId})` : ''}${verified ? '' : ' (unverified)'}`)

  // Return the normalized event
  // The caller can decide what to do with it (store in R2, forward to queue, etc.)
  return Response.json({
    accepted: true,
    verified,
    event: normalizedEvent,
  })
}
