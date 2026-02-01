/**
 * Webhook Signature Generators for E2E Testing
 *
 * These generate real, valid signatures that will pass verification
 * against production webhook handlers. Uses the same signature functions
 * from the core package that the worker uses for verification.
 */

import {
  generateGitHubSignature,
  generateStripeSignature,
  generateSlackSignature,
  generateLinearSignature,
  generateSvixSignature,
  generateWorkOSSignature,
} from '../../core/src/webhooks'

// Re-export the generate functions for direct use
export {
  generateGitHubSignature,
  generateStripeSignature,
  generateSlackSignature,
  generateLinearSignature,
  generateSvixSignature,
  generateWorkOSSignature,
}

/**
 * Payload structure for webhook test requests
 */
export interface WebhookTestPayload {
  /** The webhook provider name */
  provider: string
  /** HTTP headers to send with the request */
  headers: Record<string, string>
  /** The JSON body as a string */
  body: string
}

/**
 * Creates a signed GitHub webhook test payload
 *
 * @param secret - The GitHub webhook secret
 * @param eventType - The GitHub event type (e.g., 'push', 'pull_request')
 * @param payload - The event payload object
 * @returns Complete webhook test payload with headers and signed body
 *
 * @example
 * ```ts
 * const webhook = await createGitHubWebhook(
 *   'my-secret',
 *   'push',
 *   { ref: 'refs/heads/main', repository: { full_name: 'org/repo' } }
 * )
 * await fetch(endpoint, { method: 'POST', headers: webhook.headers, body: webhook.body })
 * ```
 */
export async function createGitHubWebhook(
  secret: string,
  eventType: string,
  payload: Record<string, unknown>
): Promise<WebhookTestPayload> {
  const body = JSON.stringify(payload)
  const signature = await generateGitHubSignature(secret, body)

  return {
    provider: 'github',
    headers: {
      'Content-Type': 'application/json',
      'X-GitHub-Event': eventType,
      'X-Hub-Signature-256': signature,
      'X-GitHub-Delivery': crypto.randomUUID(),
    },
    body,
  }
}

/**
 * Creates a signed Stripe webhook test payload
 *
 * @param secret - The Stripe webhook signing secret (whsec_...)
 * @param eventType - The Stripe event type (e.g., 'payment_intent.succeeded')
 * @param payload - Additional payload data (type is added automatically)
 * @param timestamp - Optional Unix timestamp (defaults to current time)
 * @returns Complete webhook test payload with headers and signed body
 *
 * @example
 * ```ts
 * const webhook = await createStripeWebhook(
 *   'whsec_test_secret',
 *   'payment_intent.succeeded',
 *   { id: 'pi_123', amount: 1000 }
 * )
 * ```
 */
export async function createStripeWebhook(
  secret: string,
  eventType: string,
  payload: Record<string, unknown>,
  timestamp?: number
): Promise<WebhookTestPayload> {
  const ts = timestamp ?? Math.floor(Date.now() / 1000)
  const fullPayload = {
    id: `evt_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`,
    object: 'event',
    type: eventType,
    created: ts,
    data: { object: payload },
    ...payload,
  }
  const body = JSON.stringify(fullPayload)
  const signature = await generateStripeSignature(secret, body, ts)

  return {
    provider: 'stripe',
    headers: {
      'Content-Type': 'application/json',
      'Stripe-Signature': signature,
    },
    body,
  }
}

/**
 * Creates a signed Slack webhook test payload
 *
 * @param secret - The Slack signing secret
 * @param eventType - The Slack event type
 * @param payload - The event payload
 * @param timestamp - Optional Unix timestamp (defaults to current time)
 * @returns Complete webhook test payload with headers and signed body
 *
 * @example
 * ```ts
 * const webhook = await createSlackWebhook(
 *   'signing-secret',
 *   'message',
 *   { channel: 'C123', text: 'Hello' }
 * )
 * ```
 */
export async function createSlackWebhook(
  secret: string,
  eventType: string,
  payload: Record<string, unknown>,
  timestamp?: number
): Promise<WebhookTestPayload> {
  const ts = timestamp ?? Math.floor(Date.now() / 1000)
  const fullPayload = {
    type: eventType,
    event_id: `Ev${crypto.randomUUID().replace(/-/g, '').slice(0, 10).toUpperCase()}`,
    event_time: ts,
    ...payload,
  }
  const body = JSON.stringify(fullPayload)
  const signature = await generateSlackSignature(secret, body, ts)

  return {
    provider: 'slack',
    headers: {
      'Content-Type': 'application/json',
      'X-Slack-Signature': signature,
      'X-Slack-Request-Timestamp': ts.toString(),
    },
    body,
  }
}

/**
 * Creates a signed Linear webhook test payload
 *
 * @param secret - The Linear webhook signing secret
 * @param eventType - The Linear event type (e.g., 'Issue', 'Comment')
 * @param action - The action type (e.g., 'create', 'update')
 * @param payload - The event payload
 * @returns Complete webhook test payload with headers and signed body
 *
 * @example
 * ```ts
 * const webhook = await createLinearWebhook(
 *   'linear-secret',
 *   'Issue',
 *   'create',
 *   { id: 'ISS-123', title: 'Bug fix' }
 * )
 * ```
 */
export async function createLinearWebhook(
  secret: string,
  eventType: string,
  action: string,
  payload: Record<string, unknown>
): Promise<WebhookTestPayload> {
  const fullPayload = {
    type: eventType,
    action,
    createdAt: new Date().toISOString(),
    data: payload,
    ...payload,
  }
  const body = JSON.stringify(fullPayload)
  const signature = await generateLinearSignature(secret, body)

  return {
    provider: 'linear',
    headers: {
      'Content-Type': 'application/json',
      'Linear-Signature': signature,
      'Linear-Event': eventType,
    },
    body,
  }
}

/**
 * Creates a signed Svix webhook test payload
 *
 * @param secret - The Svix webhook signing secret (with or without whsec_ prefix)
 * @param eventType - The event type
 * @param payload - The event payload
 * @param timestamp - Optional Unix timestamp (defaults to current time)
 * @returns Complete webhook test payload with headers and signed body
 *
 * @example
 * ```ts
 * const webhook = await createSvixWebhook(
 *   'whsec_base64secret',
 *   'user.created',
 *   { userId: '123', email: 'user@example.com' }
 * )
 * ```
 */
export async function createSvixWebhook(
  secret: string,
  eventType: string,
  payload: Record<string, unknown>,
  timestamp?: number
): Promise<WebhookTestPayload> {
  const ts = timestamp ?? Math.floor(Date.now() / 1000)
  const messageId = `msg_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`
  const fullPayload = {
    type: eventType,
    timestamp: new Date(ts * 1000).toISOString(),
    data: payload,
    ...payload,
  }
  const body = JSON.stringify(fullPayload)
  const signature = await generateSvixSignature(secret, messageId, ts, body)

  return {
    provider: 'svix',
    headers: {
      'Content-Type': 'application/json',
      'svix-id': messageId,
      'svix-timestamp': ts.toString(),
      'svix-signature': signature,
    },
    body,
  }
}

/**
 * Creates a signed WorkOS webhook test payload
 *
 * @param secret - The WorkOS webhook signing secret
 * @param eventType - The WorkOS event type (e.g., 'user.created')
 * @param payload - The event payload
 * @param timestamp - Optional Unix timestamp (defaults to current time)
 * @returns Complete webhook test payload with headers and signed body
 *
 * @example
 * ```ts
 * const webhook = await createWorkOSWebhook(
 *   'workos-secret',
 *   'user.created',
 *   { id: 'user_123', email: 'user@example.com' }
 * )
 * ```
 */
export async function createWorkOSWebhook(
  secret: string,
  eventType: string,
  payload: Record<string, unknown>,
  timestamp?: number
): Promise<WebhookTestPayload> {
  const ts = timestamp ?? Math.floor(Date.now() / 1000)
  const fullPayload = {
    id: `evt_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`,
    event: eventType,
    created_at: new Date(ts * 1000).toISOString(),
    data: payload,
    ...payload,
  }
  const body = JSON.stringify(fullPayload)
  const signature = await generateWorkOSSignature(secret, body, ts)

  return {
    provider: 'workos',
    headers: {
      'Content-Type': 'application/json',
      'WorkOS-Signature': signature,
    },
    body,
  }
}
