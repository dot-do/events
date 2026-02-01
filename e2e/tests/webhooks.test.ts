/**
 * Webhook Route E2E Tests
 *
 * Tests POST /webhooks?provider=xxx endpoint with signature verification.
 * Tests all supported providers: GitHub, Stripe, WorkOS, Slack, Linear, Svix.
 */

import { env, SELF } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'
import {
  generateGitHubSignature,
  generateStripeSignature,
  generateWorkOSSignature,
  generateSlackSignature,
  generateLinearSignature,
  generateSvixSignature,
} from '../../core/src/webhooks'

/** Test secrets matching the vitest.config.ts bindings */
const TEST_SECRETS = {
  github: 'test-github-secret',
  stripe: 'test-stripe-secret',
  workos: 'test-workos-secret',
  slack: 'test-slack-secret',
  linear: 'test-linear-secret',
  svix: 'whsec_dGVzdC1zdml4LXNlY3JldC1rZXktMzJieXRlcyEh', // base64 encoded
}

/** Response type for successful webhook */
interface WebhookResponse {
  accepted: boolean
  verified: boolean
  event?: {
    type: string
    ts: string
    source: string
    webhook: {
      provider: string
      eventType: string
      deliveryId?: string
      verified: boolean
    }
    payload: unknown
  }
}

/** Response type for webhook errors */
interface WebhookErrorResponse {
  error: string
  details?: string
  supported?: string[]
}

// ============================================================================
// GitHub Webhooks
// ============================================================================

describe('POST /webhooks?provider=github', () => {
  const webhookUrl = 'http://localhost/webhooks?provider=github'

  it('accepts valid GitHub webhook with correct signature', async () => {
    const payload = {
      ref: 'refs/heads/main',
      repository: { full_name: 'test-org/test-repo' },
      pusher: { name: 'test-user' },
    }
    const body = JSON.stringify(payload)
    const signature = await generateGitHubSignature(TEST_SECRETS.github, body)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-GitHub-Event': 'push',
        'X-Hub-Signature-256': signature,
        'X-GitHub-Delivery': crypto.randomUUID(),
      },
      body,
    })

    expect(response.status).toBe(200)
    const data = (await response.json()) as WebhookResponse
    expect(data.accepted).toBe(true)
    expect(data.verified).toBe(true)
    expect(data.event?.type).toBe('webhook.github.push')
    expect(data.event?.webhook.provider).toBe('github')
  })

  it('rejects GitHub webhook with invalid signature with 401', async () => {
    const payload = { ref: 'refs/heads/main' }
    const body = JSON.stringify(payload)
    // Use a wrong secret to generate an invalid signature
    const invalidSignature = await generateGitHubSignature('wrong-secret', body)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-GitHub-Event': 'push',
        'X-Hub-Signature-256': invalidSignature,
        'X-GitHub-Delivery': crypto.randomUUID(),
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects GitHub webhook with missing signature header with 401', async () => {
    const payload = { ref: 'refs/heads/main' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-GitHub-Event': 'push',
        'X-GitHub-Delivery': crypto.randomUUID(),
        // Missing X-Hub-Signature-256
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
    expect(data.details).toContain('Missing')
  })

  it('rejects GitHub webhook with malformed signature with 401', async () => {
    const payload = { ref: 'refs/heads/main' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-GitHub-Event': 'push',
        'X-Hub-Signature-256': 'not-a-valid-signature-format',
        'X-GitHub-Delivery': crypto.randomUUID(),
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })
})

// ============================================================================
// Stripe Webhooks
// ============================================================================

describe('POST /webhooks?provider=stripe', () => {
  const webhookUrl = 'http://localhost/webhooks?provider=stripe'

  it('accepts valid Stripe webhook with correct signature', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = {
      id: 'evt_test123',
      object: 'event',
      type: 'payment_intent.succeeded',
      data: { object: { id: 'pi_test123', amount: 2000 } },
    }
    const body = JSON.stringify(payload)
    const signature = await generateStripeSignature(TEST_SECRETS.stripe, body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Stripe-Signature': signature,
      },
      body,
    })

    expect(response.status).toBe(200)
    const data = (await response.json()) as WebhookResponse
    expect(data.accepted).toBe(true)
    expect(data.verified).toBe(true)
    expect(data.event?.type).toBe('webhook.stripe.payment_intent.succeeded')
    expect(data.event?.webhook.provider).toBe('stripe')
  })

  it('rejects Stripe webhook with invalid signature with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { type: 'payment_intent.succeeded' }
    const body = JSON.stringify(payload)
    const invalidSignature = await generateStripeSignature('wrong-secret', body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Stripe-Signature': invalidSignature,
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Stripe webhook with missing signature header with 401', async () => {
    const payload = { type: 'payment_intent.succeeded' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // Missing Stripe-Signature
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Stripe webhook with expired timestamp with 401', async () => {
    // 10 minutes ago - beyond 5 minute tolerance
    const expiredTimestamp = Math.floor(Date.now() / 1000) - 600
    const payload = { type: 'payment_intent.succeeded' }
    const body = JSON.stringify(payload)
    const signature = await generateStripeSignature(TEST_SECRETS.stripe, body, expiredTimestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Stripe-Signature': signature,
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
    expect(data.details).toContain('timestamp')
  })
})

// ============================================================================
// WorkOS Webhooks
// ============================================================================

describe('POST /webhooks?provider=workos', () => {
  const webhookUrl = 'http://localhost/webhooks?provider=workos'

  it('accepts valid WorkOS webhook with correct signature', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = {
      id: 'evt_test123',
      event: 'user.created',
      data: { id: 'user_123', email: 'test@example.com' },
    }
    const body = JSON.stringify(payload)
    const signature = await generateWorkOSSignature(TEST_SECRETS.workos, body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'WorkOS-Signature': signature,
      },
      body,
    })

    expect(response.status).toBe(200)
    const data = (await response.json()) as WebhookResponse
    expect(data.accepted).toBe(true)
    expect(data.verified).toBe(true)
    expect(data.event?.type).toBe('webhook.workos.user.created')
    expect(data.event?.webhook.provider).toBe('workos')
  })

  it('rejects WorkOS webhook with invalid signature with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { event: 'user.created' }
    const body = JSON.stringify(payload)
    const invalidSignature = await generateWorkOSSignature('wrong-secret', body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'WorkOS-Signature': invalidSignature,
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects WorkOS webhook with missing signature header with 401', async () => {
    const payload = { event: 'user.created' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // Missing WorkOS-Signature
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })
})

// ============================================================================
// Slack Webhooks
// ============================================================================

describe('POST /webhooks?provider=slack', () => {
  const webhookUrl = 'http://localhost/webhooks?provider=slack'

  it('accepts valid Slack webhook with correct signature', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = {
      type: 'event_callback',
      event: { type: 'message', channel: 'C123', text: 'Hello' },
    }
    const body = JSON.stringify(payload)
    const signature = await generateSlackSignature(TEST_SECRETS.slack, body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Slack-Signature': signature,
        'X-Slack-Request-Timestamp': timestamp.toString(),
      },
      body,
    })

    expect(response.status).toBe(200)
    const data = (await response.json()) as WebhookResponse
    expect(data.accepted).toBe(true)
    expect(data.verified).toBe(true)
    expect(data.event?.type).toBe('webhook.slack.event_callback')
    expect(data.event?.webhook.provider).toBe('slack')
  })

  it('rejects Slack webhook with invalid signature with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { type: 'event_callback' }
    const body = JSON.stringify(payload)
    const invalidSignature = await generateSlackSignature('wrong-secret', body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Slack-Signature': invalidSignature,
        'X-Slack-Request-Timestamp': timestamp.toString(),
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Slack webhook with missing signature header with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { type: 'event_callback' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Slack-Request-Timestamp': timestamp.toString(),
        // Missing X-Slack-Signature
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Slack webhook with missing timestamp header with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { type: 'event_callback' }
    const body = JSON.stringify(payload)
    const signature = await generateSlackSignature(TEST_SECRETS.slack, body, timestamp)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Slack-Signature': signature,
        // Missing X-Slack-Request-Timestamp
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })
})

// ============================================================================
// Linear Webhooks
// ============================================================================

describe('POST /webhooks?provider=linear', () => {
  const webhookUrl = 'http://localhost/webhooks?provider=linear'

  it('accepts valid Linear webhook with correct signature', async () => {
    const payload = {
      type: 'Issue',
      action: 'create',
      data: { id: 'issue_123', title: 'Test Issue' },
    }
    const body = JSON.stringify(payload)
    const signature = await generateLinearSignature(TEST_SECRETS.linear, body)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Linear-Signature': signature,
        'Linear-Event': 'Issue',
      },
      body,
    })

    expect(response.status).toBe(200)
    const data = (await response.json()) as WebhookResponse
    expect(data.accepted).toBe(true)
    expect(data.verified).toBe(true)
    expect(data.event?.type).toBe('webhook.linear.Issue')
    expect(data.event?.webhook.provider).toBe('linear')
  })

  it('rejects Linear webhook with invalid signature with 401', async () => {
    const payload = { type: 'Issue', action: 'create' }
    const body = JSON.stringify(payload)
    const invalidSignature = await generateLinearSignature('wrong-secret', body)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Linear-Signature': invalidSignature,
        'Linear-Event': 'Issue',
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Linear webhook with missing signature header with 401', async () => {
    const payload = { type: 'Issue', action: 'create' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Linear-Event': 'Issue',
        // Missing Linear-Signature
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })
})

// ============================================================================
// Svix Webhooks
// ============================================================================

describe('POST /webhooks?provider=svix', () => {
  const webhookUrl = 'http://localhost/webhooks?provider=svix'

  it('accepts valid Svix webhook with correct signature', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const messageId = `msg_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`
    const payload = {
      type: 'user.created',
      data: { userId: 'user_123', email: 'test@example.com' },
    }
    const body = JSON.stringify(payload)
    const signature = await generateSvixSignature(TEST_SECRETS.svix, messageId, timestamp, body)

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'svix-id': messageId,
        'svix-timestamp': timestamp.toString(),
        'svix-signature': signature,
      },
      body,
    })

    expect(response.status).toBe(200)
    const data = (await response.json()) as WebhookResponse
    expect(data.accepted).toBe(true)
    expect(data.verified).toBe(true)
    expect(data.event?.type).toBe('webhook.svix.user.created')
    expect(data.event?.webhook.provider).toBe('svix')
  })

  it('rejects Svix webhook with invalid signature with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const messageId = 'msg_test123'
    const payload = { type: 'user.created' }
    const body = JSON.stringify(payload)
    // Use the wrong secret format to generate invalid signature
    const invalidSignature = 'v1,aW52YWxpZHNpZ25hdHVyZQ==' // Invalid base64 signature

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'svix-id': messageId,
        'svix-timestamp': timestamp.toString(),
        'svix-signature': invalidSignature,
      },
      body,
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Svix webhook with missing svix-id header with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { type: 'user.created' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'svix-timestamp': timestamp.toString(),
        'svix-signature': 'v1,test',
        // Missing svix-id
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Svix webhook with missing svix-timestamp header with 401', async () => {
    const payload = { type: 'user.created' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'svix-id': 'msg_test123',
        'svix-signature': 'v1,test',
        // Missing svix-timestamp
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })

  it('rejects Svix webhook with missing svix-signature header with 401', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const payload = { type: 'user.created' }

    const response = await SELF.fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'svix-id': 'msg_test123',
        'svix-timestamp': timestamp.toString(),
        // Missing svix-signature
      },
      body: JSON.stringify(payload),
    })

    expect(response.status).toBe(401)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid signature')
  })
})

// ============================================================================
// General Webhook Endpoint Tests
// ============================================================================

describe('POST /webhooks - General behavior', () => {
  it('returns 400 for missing provider query parameter', async () => {
    const response = await SELF.fetch('http://localhost/webhooks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ test: true }),
    })

    expect(response.status).toBe(400)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toContain('provider')
  })

  it('returns 400 for unsupported provider', async () => {
    const response = await SELF.fetch('http://localhost/webhooks?provider=unknown', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ test: true }),
    })

    expect(response.status).toBe(400)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Unsupported provider')
    expect(data.supported).toContain('github')
    expect(data.supported).toContain('stripe')
  })

  it('returns 400 for invalid JSON body', async () => {
    const timestamp = Math.floor(Date.now() / 1000)
    const body = 'not-valid-json{{{'
    const signature = await generateGitHubSignature(TEST_SECRETS.github, body)

    const response = await SELF.fetch('http://localhost/webhooks?provider=github', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-GitHub-Event': 'push',
        'X-Hub-Signature-256': signature,
        'X-GitHub-Delivery': crypto.randomUUID(),
      },
      body,
    })

    // After signature verification passes, JSON parsing will fail
    expect(response.status).toBe(400)
    const data = (await response.json()) as WebhookErrorResponse
    expect(data.error).toBe('Invalid JSON body')
  })
})
