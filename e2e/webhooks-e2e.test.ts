/**
 * Webhook E2E Tests
 *
 * These tests run against actual production webhook endpoints.
 * No mocking - real signatures are generated and verified by real workers.
 *
 * Run with: pnpm test:e2e:prod
 *
 * Prerequisites:
 * - events-do worker deployed with webhook endpoints
 * - Test secrets configured (or use defaults for basic testing)
 *
 * Environment variables for production secrets:
 * - TEST_GITHUB_SECRET: GitHub webhook secret
 * - TEST_STRIPE_SECRET: Stripe webhook signing secret
 * - TEST_SLACK_SECRET: Slack signing secret
 * - TEST_LINEAR_SECRET: Linear webhook secret
 * - TEST_SVIX_SECRET: Svix webhook secret (with whsec_ prefix)
 * - TEST_WORKOS_SECRET: WorkOS webhook secret
 */

import { describe, it, expect, beforeAll } from 'vitest'
import {
  createGitHubWebhook,
  createStripeWebhook,
  createSlackWebhook,
  createLinearWebhook,
  createSvixWebhook,
  createWorkOSWebhook,
} from './helpers/webhook-signer'
import {
  ENDPOINTS,
  TEST_SECRETS,
  generateTestId,
  createTestContext,
  safeFetch,
  checkSecretConfigured,
} from './helpers/test-utils'

describe('Webhook E2E Tests', () => {
  const testId = generateTestId()

  beforeAll(() => {
    console.log(`Test run ID: ${testId}`)
    console.log(`Events DO endpoint: ${ENDPOINTS.EVENTS_DO}`)
  })

  describe('GitHub Webhooks', () => {
    const webhookUrl = `${ENDPOINTS.EVENTS_DO}/webhooks?provider=github`

    it('accepts valid GitHub push webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createGitHubWebhook(TEST_SECRETS.github, 'push', {
        ref: 'refs/heads/main',
        before: '0000000000000000000000000000000000000000',
        after: 'abc123def456',
        repository: {
          id: 123456789,
          full_name: 'test-org/test-repo',
          name: 'test-repo',
          private: false,
        },
        pusher: {
          name: 'test-user',
          email: 'test@example.com',
        },
        commits: [
          {
            id: 'abc123def456',
            message: 'Test commit',
            timestamp: new Date().toISOString(),
          },
        ],
        testId: ctx.testId,
      })

      ctx.log('Sending GitHub push webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean; error?: string }
      expect(result.accepted).toBe(true)
      ctx.log('GitHub push webhook accepted')
    })

    it('accepts valid GitHub pull_request webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createGitHubWebhook(TEST_SECRETS.github, 'pull_request', {
        action: 'opened',
        number: 42,
        pull_request: {
          id: 987654321,
          number: 42,
          title: 'Test PR',
          state: 'open',
          user: { login: 'test-user' },
          head: { ref: 'feature-branch', sha: 'abc123' },
          base: { ref: 'main', sha: 'def456' },
        },
        repository: {
          id: 123456789,
          full_name: 'test-org/test-repo',
          name: 'test-repo',
        },
        testId: ctx.testId,
      })

      ctx.log('Sending GitHub pull_request webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('rejects webhook with invalid signature (when secret configured)', async () => {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-GitHub-Event': 'push',
          'X-Hub-Signature-256': 'sha256=0000000000000000000000000000000000000000000000000000000000000000',
          'X-GitHub-Delivery': crypto.randomUUID(),
        },
        body: JSON.stringify({ test: true, timestamp: Date.now() }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })

    it('handles webhook with missing signature header', async () => {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-GitHub-Event': 'push',
          'X-GitHub-Delivery': crypto.randomUUID(),
        },
        body: JSON.stringify({ test: true }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })

    it('handles webhook with malformed signature', async () => {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-GitHub-Event': 'push',
          'X-Hub-Signature-256': 'invalid-signature-format',
          'X-GitHub-Delivery': crypto.randomUUID(),
        },
        body: JSON.stringify({ test: true }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })
  })

  describe('Stripe Webhooks', () => {
    const webhookUrl = `${ENDPOINTS.EVENTS_DO}/webhooks?provider=stripe`

    it('accepts valid Stripe payment_intent.succeeded webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createStripeWebhook(
        TEST_SECRETS.stripe,
        'payment_intent.succeeded',
        {
          id: 'pi_test_' + ctx.testId.slice(0, 20),
          amount: 2000,
          currency: 'usd',
          status: 'succeeded',
          customer: 'cus_test123',
          metadata: ctx.createMetadata(),
        }
      )

      ctx.log('Sending Stripe payment_intent.succeeded webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('accepts valid Stripe customer.subscription.created webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createStripeWebhook(
        TEST_SECRETS.stripe,
        'customer.subscription.created',
        {
          id: 'sub_test_' + ctx.testId.slice(0, 20),
          customer: 'cus_test123',
          status: 'active',
          items: {
            data: [{ price: { id: 'price_test123', product: 'prod_test123' } }],
          },
          metadata: ctx.createMetadata(),
        }
      )

      ctx.log('Sending Stripe customer.subscription.created webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('handles webhook with invalid signature (when secret configured)', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Stripe-Signature': `t=${timestamp},v1=0000000000000000000000000000000000000000000000000000000000000000`,
        },
        body: JSON.stringify({ type: 'test.event', data: {} }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })

    it('handles webhook with expired timestamp (when secret configured)', async () => {
      // Create a webhook with a timestamp from 10 minutes ago (beyond 5 min tolerance)
      const expiredTimestamp = Math.floor(Date.now() / 1000) - 600
      const webhook = await createStripeWebhook(
        TEST_SECRETS.stripe,
        'payment_intent.succeeded',
        { id: 'pi_test_expired', amount: 1000 },
        expiredTimestamp
      )

      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })
  })

  describe('Slack Webhooks', () => {
    const webhookUrl = `${ENDPOINTS.EVENTS_DO}/webhooks?provider=slack`

    it('accepts valid Slack event callback', async () => {
      const ctx = createTestContext()
      const webhook = await createSlackWebhook(TEST_SECRETS.slack, 'event_callback', {
        token: 'verification_token',
        team_id: 'T123ABC',
        api_app_id: 'A123ABC',
        event: {
          type: 'message',
          channel: 'C123ABC',
          user: 'U123ABC',
          text: 'Hello from E2E test',
          ts: Date.now().toString(),
        },
        event_id: `Ev${ctx.testId.slice(0, 10).toUpperCase()}`,
        testId: ctx.testId,
      })

      ctx.log('Sending Slack event_callback webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('handles webhook with invalid signature (when secret configured)', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Slack-Signature': 'v0=0000000000000000000000000000000000000000000000000000000000000000',
          'X-Slack-Request-Timestamp': timestamp,
        },
        body: JSON.stringify({ type: 'event_callback', event: {} }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })

    it('handles webhook with missing timestamp header (when secret configured)', async () => {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Slack-Signature': 'v0=test',
        },
        body: JSON.stringify({ type: 'event_callback' }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })
  })

  describe('Linear Webhooks', () => {
    const webhookUrl = `${ENDPOINTS.EVENTS_DO}/webhooks?provider=linear`

    it('accepts valid Linear issue.create webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createLinearWebhook(TEST_SECRETS.linear, 'Issue', 'create', {
        id: 'issue_' + ctx.testId.slice(0, 16),
        title: 'E2E Test Issue',
        description: 'Created by E2E test',
        state: { name: 'Todo' },
        team: { key: 'TEST' },
        createdAt: new Date().toISOString(),
        testId: ctx.testId,
      })

      ctx.log('Sending Linear Issue create webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('accepts valid Linear comment.create webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createLinearWebhook(TEST_SECRETS.linear, 'Comment', 'create', {
        id: 'comment_' + ctx.testId.slice(0, 16),
        body: 'E2E test comment',
        issue: { id: 'issue_123', title: 'Test Issue' },
        user: { name: 'Test User' },
        createdAt: new Date().toISOString(),
        testId: ctx.testId,
      })

      ctx.log('Sending Linear Comment create webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('handles webhook with invalid signature (when secret configured)', async () => {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Linear-Signature': '0'.repeat(64),
          'Linear-Event': 'Issue',
        },
        body: JSON.stringify({ type: 'Issue', action: 'create', data: {} }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })
  })

  describe('Svix Webhooks', () => {
    const webhookUrl = `${ENDPOINTS.EVENTS_DO}/webhooks?provider=svix`

    it('accepts valid Svix webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createSvixWebhook(TEST_SECRETS.svix, 'user.created', {
        userId: 'user_' + ctx.testId.slice(0, 16),
        email: 'test@example.com',
        name: 'Test User',
        testId: ctx.testId,
      })

      ctx.log('Sending Svix user.created webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('handles webhook with invalid signature (when secret configured)', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'svix-id': 'msg_test123',
          'svix-timestamp': timestamp,
          'svix-signature': 'v1,aW52YWxpZA==', // Invalid base64 signature
        },
        body: JSON.stringify({ type: 'test.event', data: {} }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })

    it('handles webhook with missing headers (when secret configured)', async () => {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          // Missing svix-id, svix-timestamp, svix-signature
        },
        body: JSON.stringify({ type: 'test.event' }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })
  })

  describe('WorkOS Webhooks', () => {
    const webhookUrl = `${ENDPOINTS.EVENTS_DO}/webhooks?provider=workos`

    it('accepts valid WorkOS user.created webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createWorkOSWebhook(TEST_SECRETS.workos, 'user.created', {
        id: 'user_' + ctx.testId.slice(0, 16),
        email: 'test@example.com',
        first_name: 'Test',
        last_name: 'User',
        email_verified: true,
        profile_picture_url: null,
        testId: ctx.testId,
      })

      ctx.log('Sending WorkOS user.created webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('accepts valid WorkOS dsync.user.created webhook', async () => {
      const ctx = createTestContext()
      const webhook = await createWorkOSWebhook(TEST_SECRETS.workos, 'dsync.user.created', {
        id: 'directory_user_' + ctx.testId.slice(0, 12),
        directory_id: 'directory_123',
        organization_id: 'org_123',
        first_name: 'Directory',
        last_name: 'User',
        emails: [{ primary: true, value: 'dsync@example.com' }],
        testId: ctx.testId,
      })

      ctx.log('Sending WorkOS dsync.user.created webhook')
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: webhook.headers,
        body: webhook.body,
      })

      expect(response.status).toBe(200)
      const result = (await response.json()) as { accepted?: boolean }
      expect(result.accepted).toBe(true)
    })

    it('handles webhook with invalid signature (when secret configured)', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'WorkOS-Signature': `timestamp=${timestamp},signature=${'0'.repeat(64)}`,
        },
        body: JSON.stringify({ event: 'user.created', data: {} }),
      })

      // 401 if secret configured, 200 with verified:false if not
      if (response.status === 200) {
        const result = (await response.json()) as { verified?: boolean }
        expect(result.verified).toBe(false)
      } else {
        expect(response.status).toBe(401)
      }
    })
  })

  describe('Webhook Endpoint Behavior', () => {
    it('returns 400 for unknown provider', async () => {
      const response = await fetch(`${ENDPOINTS.EVENTS_DO}/webhooks?provider=unknown`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test: true }),
      })

      expect(response.status).toBe(400)
    })

    it('returns 400 for missing provider parameter', async () => {
      const response = await fetch(`${ENDPOINTS.EVENTS_DO}/webhooks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test: true }),
      })

      expect(response.status).toBe(400)
    })

    it('handles OPTIONS preflight request', async () => {
      const response = await fetch(`${ENDPOINTS.EVENTS_DO}/webhooks?provider=github`, {
        method: 'OPTIONS',
        headers: {
          Origin: 'https://example.com',
          'Access-Control-Request-Method': 'POST',
        },
      })

      expect(response.headers.get('Access-Control-Allow-Origin')).toBeTruthy()
      expect(response.headers.get('Access-Control-Allow-Methods')).toContain('POST')
    })

    it('returns JSON error for invalid JSON body', async () => {
      const response = await fetch(`${ENDPOINTS.EVENTS_DO}/webhooks?provider=github`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-GitHub-Event': 'push',
          'X-Hub-Signature-256': 'sha256=test',
          'X-GitHub-Delivery': crypto.randomUUID(),
        },
        body: 'not-valid-json{{{',
      })

      // 400 for invalid JSON (if no secret configured, parse happens first)
      // 401 if secret configured and signature check happens first
      expect([400, 401]).toContain(response.status)
    })
  })

  describe('Rate Limiting (if enabled)', () => {
    it.skip('applies rate limiting to repeated requests', async () => {
      const ctx = createTestContext()
      const requests: Promise<Response>[] = []

      // Send 100 requests rapidly
      for (let i = 0; i < 100; i++) {
        const webhook = await createGitHubWebhook(TEST_SECRETS.github, 'ping', {
          zen: `Request ${i}`,
          testId: ctx.testId,
        })

        requests.push(
          fetch(`${ENDPOINTS.EVENTS_DO}/webhooks?provider=github`, {
            method: 'POST',
            headers: webhook.headers,
            body: webhook.body,
          })
        )
      }

      const responses = await Promise.all(requests)
      const statuses = responses.map((r) => r.status)

      // If rate limiting is enabled, we should see some 429 responses
      const rateLimited = statuses.filter((s) => s === 429).length
      const successful = statuses.filter((s) => s === 200).length

      ctx.log(`Rate limiting results: ${successful} successful, ${rateLimited} rate limited`)

      // At least some should succeed
      expect(successful).toBeGreaterThan(0)
    })
  })
})

// Export for use in other test files
export { ENDPOINTS, TEST_SECRETS, generateTestId }
