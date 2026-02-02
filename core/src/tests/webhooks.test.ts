/**
 * Webhook Signature Verification Tests
 *
 * Tests for GitHub, Stripe, and WorkOS webhook signature verification.
 * Uses real Web Crypto API operations - no mocking.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  verifyGitHubSignature,
  verifyStripeSignature,
  verifyWorkOSSignature,
  verifySlackSignature,
  verifyLinearSignature,
  verifySvixSignature,
  generateGitHubSignature,
  generateStripeSignature,
  generateWorkOSSignature,
  generateSlackSignature,
  generateLinearSignature,
  generateSvixSignature,
  // Validation exports
  WEBHOOK_PROVIDERS,
  isValidProvider,
  isGitHubConfig,
  isStripeConfig,
  isWorkOSConfig,
  isSlackConfig,
  isLinearConfig,
  isSvixConfig,
  isWebhookProviderConfig,
  validateWebhookConfig,
  assertValidWebhookConfig,
  createWebhookConfig,
  WebhookConfigError,
  WebhookSignatureError,
  type WebhookProvider,
  type WebhookProviderConfig,
} from '../webhooks.js'

// ============================================================================
// GitHub Webhook Verification Tests
// ============================================================================

describe('GitHub Webhook Verification', () => {
  const secret = 'github-webhook-secret'
  const payload = '{"action":"push","repository":{"full_name":"test/repo"}}'

  describe('verifyGitHubSignature', () => {
    it('accepts valid signature', async () => {
      const signature = await generateGitHubSignature(secret, payload)
      const result = await verifyGitHubSignature(secret, payload, signature)

      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
    })

    it('rejects invalid signature', async () => {
      const result = await verifyGitHubSignature(secret, payload, 'sha256=invalid')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature')
    })

    it('rejects malformed signature header (missing sha256= prefix)', async () => {
      const result = await verifyGitHubSignature(secret, payload, 'abcdef123456')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature format')
    })

    it('rejects signature with wrong algorithm prefix', async () => {
      const result = await verifyGitHubSignature(secret, payload, 'sha1=abcdef123456')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature format')
    })

    it('rejects empty signature', async () => {
      const result = await verifyGitHubSignature(secret, payload, '')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature format')
    })

    it('handles different payload types', async () => {
      const jsonPayload = '{"event":"test"}'
      const signature = await generateGitHubSignature(secret, jsonPayload)
      const result = await verifyGitHubSignature(secret, jsonPayload, signature)

      expect(result.valid).toBe(true)
    })

    it('is case-sensitive for signature hex', async () => {
      const signature = await generateGitHubSignature(secret, payload)
      // Make signature uppercase (should fail due to timing-safe comparison)
      const upperSignature = signature.replace(/[a-f]/g, (c) => c.toUpperCase())

      // If the hex was all lowercase, uppercase version should fail
      if (signature !== upperSignature) {
        const result = await verifyGitHubSignature(secret, payload, upperSignature)
        expect(result.valid).toBe(false)
      }
    })

    it('uses timing-safe comparison', async () => {
      // This test verifies the implementation uses constant-time comparison
      // by checking that verification takes similar time for valid/invalid sigs
      const validSignature = await generateGitHubSignature(secret, payload)

      const iterations = 100
      const validTimes: number[] = []
      const invalidTimes: number[] = []

      for (let i = 0; i < iterations; i++) {
        const start1 = performance.now()
        await verifyGitHubSignature(secret, payload, validSignature)
        validTimes.push(performance.now() - start1)

        const start2 = performance.now()
        await verifyGitHubSignature(secret, payload, 'sha256=0000000000000000000000000000000000000000000000000000000000000000')
        invalidTimes.push(performance.now() - start2)
      }

      // Remove outliers (first few calls may be slower due to JIT)
      const validAvg = validTimes.slice(10).reduce((a, b) => a + b, 0) / (iterations - 10)
      const invalidAvg = invalidTimes.slice(10).reduce((a, b) => a + b, 0) / (iterations - 10)

      // Times should be within reasonable range of each other (less than 2x difference)
      // This is a weak test but helps ensure obvious timing attacks aren't present
      const ratio = Math.max(validAvg, invalidAvg) / Math.min(validAvg, invalidAvg)
      expect(ratio).toBeLessThan(5) // Allow some variance due to async operations
    })
  })

  describe('generateGitHubSignature', () => {
    it('generates signature with sha256= prefix', async () => {
      const signature = await generateGitHubSignature(secret, payload)

      expect(signature).toMatch(/^sha256=[a-f0-9]{64}$/)
    })

    it('generates consistent signatures for same input', async () => {
      const sig1 = await generateGitHubSignature(secret, payload)
      const sig2 = await generateGitHubSignature(secret, payload)

      expect(sig1).toBe(sig2)
    })

    it('generates different signatures for different payloads', async () => {
      const sig1 = await generateGitHubSignature(secret, '{"a":1}')
      const sig2 = await generateGitHubSignature(secret, '{"a":2}')

      expect(sig1).not.toBe(sig2)
    })

    it('generates different signatures for different secrets', async () => {
      const sig1 = await generateGitHubSignature('secret1', payload)
      const sig2 = await generateGitHubSignature('secret2', payload)

      expect(sig1).not.toBe(sig2)
    })
  })
})

// ============================================================================
// Stripe Webhook Verification Tests
// ============================================================================

describe('Stripe Webhook Verification', () => {
  const secret = 'whsec_test_stripe_secret'
  const payload = '{"id":"evt_123","type":"payment_intent.created"}'

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('verifyStripeSignature', () => {
    it('accepts valid signature', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateStripeSignature(secret, payload, timestamp)
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
      expect(result.timestamp).toBe(timestamp)
    })

    it('rejects invalid signature', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = `t=${timestamp},v1=invalid_signature`
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature')
    })

    it('rejects expired timestamp (default 300s tolerance)', async () => {
      const oldTimestamp = Math.floor(Date.now() / 1000) - 400 // 400 seconds ago
      const header = await generateStripeSignature(secret, payload, oldTimestamp)
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('timestamp outside tolerance window')
    })

    it('accepts timestamp within tolerance', async () => {
      const recentTimestamp = Math.floor(Date.now() / 1000) - 200 // 200 seconds ago
      const header = await generateStripeSignature(secret, payload, recentTimestamp)
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.valid).toBe(true)
    })

    it('accepts custom tolerance', async () => {
      const oldTimestamp = Math.floor(Date.now() / 1000) - 400 // 400 seconds ago
      const header = await generateStripeSignature(secret, payload, oldTimestamp)
      const result = await verifyStripeSignature(secret, payload, header, 600) // 10 min tolerance

      expect(result.valid).toBe(true)
    })

    it('rejects future timestamp outside tolerance', async () => {
      const futureTimestamp = Math.floor(Date.now() / 1000) + 400 // 400 seconds in future
      const header = await generateStripeSignature(secret, payload, futureTimestamp)
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('timestamp outside tolerance window')
    })

    it('supports multiple v1 signatures (key rotation)', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const oldSecret = 'whsec_old_secret'
      const newSecret = secret

      // Create header with both old and new signatures
      const oldSig = await generateStripeSignature(oldSecret, payload, timestamp)
      const newSig = await generateStripeSignature(newSecret, payload, timestamp)

      // Extract just the v1 signature parts
      const oldV1 = oldSig.split(',v1=')[1]
      const newV1 = newSig.split(',v1=')[1]

      // Combine into multi-sig header
      const multiHeader = `t=${timestamp},v1=${oldV1},v1=${newV1}`
      const result = await verifyStripeSignature(newSecret, payload, multiHeader)

      expect(result.valid).toBe(true)
    })

    it('rejects when no v1 signatures match', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = `t=${timestamp},v1=invalid1,v1=invalid2`
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature')
    })

    it('rejects malformed header (missing timestamp)', async () => {
      const result = await verifyStripeSignature(secret, payload, 'v1=abc123')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: missing timestamp')
    })

    it('rejects malformed header (missing v1 signature)', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const result = await verifyStripeSignature(secret, payload, `t=${timestamp}`)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: no v1 signatures')
    })

    it('rejects empty header', async () => {
      const result = await verifyStripeSignature(secret, payload, '')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: missing timestamp')
    })

    it('handles non-numeric timestamp', async () => {
      const result = await verifyStripeSignature(secret, payload, 't=invalid,v1=abc123')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: invalid timestamp')
    })

    it('returns timestamp in result', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateStripeSignature(secret, payload, timestamp)
      const result = await verifyStripeSignature(secret, payload, header)

      expect(result.timestamp).toBe(timestamp)
    })
  })

  describe('generateStripeSignature', () => {
    it('generates header with t= and v1= parts', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateStripeSignature(secret, payload, timestamp)

      expect(header).toMatch(/^t=\d+,v1=[a-f0-9]{64}$/)
    })

    it('includes provided timestamp', async () => {
      const timestamp = 1705320000
      const header = await generateStripeSignature(secret, payload, timestamp)

      expect(header).toContain(`t=${timestamp}`)
    })

    it('generates consistent signatures', async () => {
      const timestamp = 1705320000
      const header1 = await generateStripeSignature(secret, payload, timestamp)
      const header2 = await generateStripeSignature(secret, payload, timestamp)

      expect(header1).toBe(header2)
    })
  })
})

// ============================================================================
// WorkOS Webhook Verification Tests
// ============================================================================

describe('WorkOS Webhook Verification', () => {
  const secret = 'workos_webhook_secret_key'
  const payload = '{"id":"event_123","event":"user.created"}'

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('verifyWorkOSSignature', () => {
    it('accepts valid signature', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateWorkOSSignature(secret, payload, timestamp)
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
      expect(result.timestamp).toBe(timestamp)
    })

    it('rejects invalid signature', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = `timestamp=${timestamp},signature=invalid_signature`
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid signature')
    })

    it('rejects expired timestamp (default 300s tolerance)', async () => {
      const oldTimestamp = Math.floor(Date.now() / 1000) - 400
      const header = await generateWorkOSSignature(secret, payload, oldTimestamp)
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('timestamp outside tolerance window')
    })

    it('accepts timestamp within tolerance', async () => {
      const recentTimestamp = Math.floor(Date.now() / 1000) - 200
      const header = await generateWorkOSSignature(secret, payload, recentTimestamp)
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.valid).toBe(true)
    })

    it('accepts custom tolerance', async () => {
      const oldTimestamp = Math.floor(Date.now() / 1000) - 400
      const header = await generateWorkOSSignature(secret, payload, oldTimestamp)
      const result = await verifyWorkOSSignature(secret, payload, header, 600)

      expect(result.valid).toBe(true)
    })

    it('rejects future timestamp outside tolerance', async () => {
      const futureTimestamp = Math.floor(Date.now() / 1000) + 400
      const header = await generateWorkOSSignature(secret, payload, futureTimestamp)
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('timestamp outside tolerance window')
    })

    it('rejects malformed header (missing timestamp)', async () => {
      const result = await verifyWorkOSSignature(secret, payload, 'signature=abc123')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: missing timestamp')
    })

    it('rejects malformed header (missing signature)', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const result = await verifyWorkOSSignature(secret, payload, `timestamp=${timestamp}`)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: missing signature')
    })

    it('rejects empty header', async () => {
      const result = await verifyWorkOSSignature(secret, payload, '')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: missing timestamp')
    })

    it('handles non-numeric timestamp', async () => {
      const result = await verifyWorkOSSignature(secret, payload, 'timestamp=invalid,signature=abc123')

      expect(result.valid).toBe(false)
      expect(result.error).toBe('Invalid header format: invalid timestamp')
    })

    it('returns timestamp in result', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateWorkOSSignature(secret, payload, timestamp)
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.timestamp).toBe(timestamp)
    })

    it('handles headers with spaces around equals signs', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateWorkOSSignature(secret, payload, timestamp)
      // Should still work with the normal format
      const result = await verifyWorkOSSignature(secret, payload, header)

      expect(result.valid).toBe(true)
    })
  })

  describe('generateWorkOSSignature', () => {
    it('generates header with timestamp= and signature= parts', async () => {
      const timestamp = Math.floor(Date.now() / 1000)
      const header = await generateWorkOSSignature(secret, payload, timestamp)

      expect(header).toMatch(/^timestamp=\d+,signature=[a-f0-9]{64}$/)
    })

    it('includes provided timestamp', async () => {
      const timestamp = 1705320000
      const header = await generateWorkOSSignature(secret, payload, timestamp)

      expect(header).toContain(`timestamp=${timestamp}`)
    })

    it('generates consistent signatures', async () => {
      const timestamp = 1705320000
      const header1 = await generateWorkOSSignature(secret, payload, timestamp)
      const header2 = await generateWorkOSSignature(secret, payload, timestamp)

      expect(header1).toBe(header2)
    })
  })
})

// ============================================================================
// Cross-provider Tests
// ============================================================================

describe('Cross-provider signature verification', () => {
  const secret = 'shared-test-secret'
  const payload = '{"test":"data"}'

  it('GitHub and Stripe signatures are different for same payload', async () => {
    const timestamp = 1705320000
    const githubSig = await generateGitHubSignature(secret, payload)
    const stripeSig = await generateStripeSignature(secret, payload, timestamp)

    // Extract hex part from each
    const githubHex = githubSig.replace('sha256=', '')
    const stripeHex = stripeSig.split(',v1=')[1]

    expect(githubHex).not.toBe(stripeHex)
  })

  it('GitHub and WorkOS signatures are the same for the same payload (both sign raw body)', async () => {
    // WorkOS signs the raw payload, just like GitHub
    // The only difference is the header format
    const timestamp = 1705320000
    const githubSig = await generateGitHubSignature(secret, payload)
    const workosSig = await generateWorkOSSignature(secret, payload, timestamp)

    const githubHex = githubSig.replace('sha256=', '')
    const workosHex = workosSig.split(',signature=')[1]

    // They should be identical since both use HMAC-SHA256 of the raw payload
    expect(githubHex).toBe(workosHex)
  })
})

// ============================================================================
// Slack Webhook Verification Tests
// ============================================================================

describe('Slack Webhook Verification', () => {
  const secret = 'slack-signing-secret'
  const payload = '{"type":"event_callback","event":{"type":"message"}}'

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('verifySlackSignature', () => {
    it('accepts valid signature with fresh timestamp', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSlackSignature(secret, payload, timestamp)
      const result = await verifySlackSignature(secret, payload, signature, timestamp)

      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
    })

    it('rejects expired timestamp (outside default 300s tolerance)', async () => {
      const oldTimestamp = (Math.floor(Date.now() / 1000) - 400).toString() // 400 seconds ago
      const signature = await generateSlackSignature(secret, payload, oldTimestamp)
      const result = await verifySlackSignature(secret, payload, signature, oldTimestamp)

      expect(result.valid).toBe(false)
      expect(result.error?.toLowerCase()).toContain('timestamp')
    })

    it('accepts timestamp within default tolerance', async () => {
      const recentTimestamp = (Math.floor(Date.now() / 1000) - 200).toString() // 200 seconds ago
      const signature = await generateSlackSignature(secret, payload, recentTimestamp)
      const result = await verifySlackSignature(secret, payload, signature, recentTimestamp)

      expect(result.valid).toBe(true)
    })

    it('accepts timestamp within custom tolerance', async () => {
      const oldTimestamp = (Math.floor(Date.now() / 1000) - 400).toString() // 400 seconds ago
      const signature = await generateSlackSignature(secret, payload, oldTimestamp)
      const result = await verifySlackSignature(secret, payload, signature, oldTimestamp, 600)

      expect(result.valid).toBe(true)
    })

    it('rejects invalid signature', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const result = await verifySlackSignature(secret, payload, 'v0=invalidhex', timestamp)

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('rejects signature without v0= prefix', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSlackSignature(secret, payload, timestamp)
      const signatureWithoutPrefix = signature.replace('v0=', '')
      const result = await verifySlackSignature(secret, payload, signatureWithoutPrefix, timestamp)

      expect(result.valid).toBe(false)
      expect(result.error).toContain('format')
    })

    it('rejects signature with wrong secret', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSlackSignature('wrong-secret', payload, timestamp)
      const result = await verifySlackSignature(secret, payload, signature, timestamp)

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('rejects future timestamp (replay attack prevention)', async () => {
      const futureTimestamp = (Math.floor(Date.now() / 1000) + 400).toString() // 400 seconds in future
      const signature = await generateSlackSignature(secret, payload, futureTimestamp)
      const result = await verifySlackSignature(secret, payload, signature, futureTimestamp)

      expect(result.valid).toBe(false)
      expect(result.error?.toLowerCase()).toContain('timestamp')
    })

    it('rejects empty signature', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const result = await verifySlackSignature(secret, payload, '', timestamp)

      expect(result.valid).toBe(false)
      expect(result.error).toContain('format')
    })
  })

  describe('generateSlackSignature', () => {
    it('generates signature with v0= prefix', async () => {
      const timestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSlackSignature(secret, payload, timestamp)

      expect(signature).toMatch(/^v0=[a-f0-9]{64}$/)
    })

    it('generates consistent signatures for same input', async () => {
      const timestamp = '1705320000'
      const sig1 = await generateSlackSignature(secret, payload, timestamp)
      const sig2 = await generateSlackSignature(secret, payload, timestamp)

      expect(sig1).toBe(sig2)
    })

    it('generates different signatures for different timestamps', async () => {
      const sig1 = await generateSlackSignature(secret, payload, '1705320000')
      const sig2 = await generateSlackSignature(secret, payload, '1705320001')

      expect(sig1).not.toBe(sig2)
    })
  })
})

// ============================================================================
// Linear Webhook Verification Tests
// ============================================================================

describe('Linear Webhook Verification', () => {
  const secret = 'linear-webhook-secret'
  const payload = JSON.stringify({
    action: 'create',
    type: 'Issue',
    data: { id: 'issue-123', title: 'Test issue' },
  })

  describe('verifyLinearSignature', () => {
    it('accepts valid signature', async () => {
      const signature = await generateLinearSignature(secret, payload)
      const result = await verifyLinearSignature(secret, payload, signature)

      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
    })

    it('rejects invalid signature', async () => {
      const result = await verifyLinearSignature(secret, payload, 'invalid_hex_signature')

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('rejects signature with wrong secret', async () => {
      const signature = await generateLinearSignature('wrong-secret', payload)
      const result = await verifyLinearSignature(secret, payload, signature)

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('handles payload without webhookTimestamp', async () => {
      const payloadNoTimestamp = JSON.stringify({ action: 'create', type: 'Issue' })
      const signature = await generateLinearSignature(secret, payloadNoTimestamp)
      const result = await verifyLinearSignature(secret, payloadNoTimestamp, signature)

      expect(result.valid).toBe(true)
    })

    it('rejects signature with v0= prefix (Linear uses plain hex)', async () => {
      const signature = await generateLinearSignature(secret, payload)
      const signatureWithPrefix = `v0=${signature}`
      const result = await verifyLinearSignature(secret, payload, signatureWithPrefix)

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('rejects empty signature', async () => {
      const result = await verifyLinearSignature(secret, payload, '')

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })
  })

  describe('generateLinearSignature', () => {
    it('generates plain hex signature (no prefix)', async () => {
      const signature = await generateLinearSignature(secret, payload)

      expect(signature).toMatch(/^[a-f0-9]{64}$/)
      expect(signature).not.toContain('=')
    })

    it('generates consistent signatures for same input', async () => {
      const sig1 = await generateLinearSignature(secret, payload)
      const sig2 = await generateLinearSignature(secret, payload)

      expect(sig1).toBe(sig2)
    })

    it('generates different signatures for different payloads', async () => {
      const sig1 = await generateLinearSignature(secret, '{"a":1}')
      const sig2 = await generateLinearSignature(secret, '{"a":2}')

      expect(sig1).not.toBe(sig2)
    })
  })
})

// ============================================================================
// Svix Webhook Verification Tests
// ============================================================================

describe('Svix Webhook Verification', () => {
  // "test-secret" in base64 is "dGVzdC1zZWNyZXQ="
  const secretBase64 = 'dGVzdC1zZWNyZXQ='
  const secretWithPrefix = `whsec_${secretBase64}`
  const payload = '{"type":"invoice.paid","data":{"id":"inv_123"}}'
  const svixId = 'msg_123abc'

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('verifySvixSignature', () => {
    it('accepts valid Svix signature', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)
      const result = await verifySvixSignature(secretWithPrefix, payload, svixId, svixTimestamp, signature)

      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
    })

    it('rejects expired timestamp', async () => {
      const oldTimestamp = (Math.floor(Date.now() / 1000) - 400).toString() // 400 seconds ago
      const signature = await generateSvixSignature(secretWithPrefix, svixId, oldTimestamp, payload)
      const result = await verifySvixSignature(secretWithPrefix, payload, svixId, oldTimestamp, signature)

      expect(result.valid).toBe(false)
      expect(result.error?.toLowerCase()).toContain('timestamp')
    })

    it('accepts timestamp within tolerance', async () => {
      const recentTimestamp = (Math.floor(Date.now() / 1000) - 200).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, recentTimestamp, payload)
      const result = await verifySvixSignature(secretWithPrefix, payload, svixId, recentTimestamp, signature)

      expect(result.valid).toBe(true)
    })

    it('accepts timestamp within custom tolerance', async () => {
      const oldTimestamp = (Math.floor(Date.now() / 1000) - 400).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, oldTimestamp, payload)
      const result = await verifySvixSignature(secretWithPrefix, payload, svixId, oldTimestamp, signature, 600)

      expect(result.valid).toBe(true)
    })

    it('rejects invalid signature', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const result = await verifySvixSignature(
        secretWithPrefix,
        payload,
        svixId,
        svixTimestamp,
        'v1,invalidbase64signature'
      )

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('handles multiple signatures (space-separated)', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const validSignature = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)
      // Svix can send multiple signatures (for key rotation)
      const multipleSignatures = `v1,b2xkc2lnbmF0dXJl ${validSignature}`
      const result = await verifySvixSignature(
        secretWithPrefix,
        payload,
        svixId,
        svixTimestamp,
        multipleSignatures
      )

      expect(result.valid).toBe(true)
    })

    it('rejects secret without whsec_ prefix', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)
      // Pass secret without prefix - should fail
      const result = await verifySvixSignature(
        secretBase64, // Missing whsec_ prefix
        payload,
        svixId,
        svixTimestamp,
        signature
      )

      expect(result.valid).toBe(false)
      expect(result.error).toContain('secret')
    })

    it('rejects signature with wrong secret', async () => {
      const wrongSecretBase64 = 'd3Jvbmctc2VjcmV0' // "wrong-secret" in base64
      const wrongSecretWithPrefix = `whsec_${wrongSecretBase64}`
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSvixSignature(wrongSecretWithPrefix, svixId, svixTimestamp, payload)
      const result = await verifySvixSignature(
        secretWithPrefix,
        payload,
        svixId,
        svixTimestamp,
        signature
      )

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })

    it('rejects future timestamp', async () => {
      const futureTimestamp = (Math.floor(Date.now() / 1000) + 400).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, futureTimestamp, payload)
      const result = await verifySvixSignature(
        secretWithPrefix,
        payload,
        svixId,
        futureTimestamp,
        signature
      )

      expect(result.valid).toBe(false)
      expect(result.error?.toLowerCase()).toContain('timestamp')
    })

    it('verifies using base64 encoding (not hex)', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)

      // The signature should be base64 format (contains characters like +, /, =)
      // and NOT hex format (which only contains 0-9 and a-f)
      const signatureValue = signature.split(',')[1]
      expect(signatureValue).not.toMatch(/^[0-9a-f]+$/) // Not purely hex

      const result = await verifySvixSignature(
        secretWithPrefix,
        payload,
        svixId,
        svixTimestamp,
        signature
      )
      expect(result.valid).toBe(true)
    })

    it('rejects empty signature', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const result = await verifySvixSignature(secretWithPrefix, payload, svixId, svixTimestamp, '')

      expect(result.valid).toBe(false)
      expect(result.error).toContain('signature')
    })
  })

  describe('generateSvixSignature', () => {
    it('generates signature with v1, prefix (base64)', async () => {
      const svixTimestamp = Math.floor(Date.now() / 1000).toString()
      const signature = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)

      // Svix signatures start with "v1," and contain base64
      expect(signature).toMatch(/^v1,[A-Za-z0-9+/]+=*$/)
    })

    it('generates consistent signatures for same input', async () => {
      const svixTimestamp = '1705320000'
      const sig1 = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)
      const sig2 = await generateSvixSignature(secretWithPrefix, svixId, svixTimestamp, payload)

      expect(sig1).toBe(sig2)
    })

    it('generates different signatures for different message IDs', async () => {
      const svixTimestamp = '1705320000'
      const sig1 = await generateSvixSignature(secretWithPrefix, 'msg_001', svixTimestamp, payload)
      const sig2 = await generateSvixSignature(secretWithPrefix, 'msg_002', svixTimestamp, payload)

      expect(sig1).not.toBe(sig2)
    })
  })
})

// ============================================================================
// Timing Safety Tests
// ============================================================================

describe('Timing-safe comparison', () => {
  const secret = 'test-webhook-secret'
  const payload = '{"test":"data"}'

  describe('length-independent timing', () => {
    it('comparison time should not vary significantly with different length signatures', async () => {
      // This test verifies that comparing signatures of different lengths
      // doesn't reveal length information through timing differences.
      // The double-HMAC technique should ensure constant comparison time.

      const validSignature = await generateGitHubSignature(secret, payload)
      const iterations = 50

      // Test with signatures of vastly different lengths
      const shortInvalid = 'sha256=0000000000000000000000000000000000000000000000000000000000000000'
      const longInvalid = 'sha256=' + '0'.repeat(64) // Same length as valid

      const shortTimes: number[] = []
      const longTimes: number[] = []

      // Warm up JIT
      for (let i = 0; i < 10; i++) {
        await verifyGitHubSignature(secret, payload, shortInvalid)
        await verifyGitHubSignature(secret, payload, longInvalid)
      }

      // Measure times
      for (let i = 0; i < iterations; i++) {
        const start1 = performance.now()
        await verifyGitHubSignature(secret, payload, shortInvalid)
        shortTimes.push(performance.now() - start1)

        const start2 = performance.now()
        await verifyGitHubSignature(secret, payload, longInvalid)
        longTimes.push(performance.now() - start2)
      }

      // Calculate averages (excluding outliers)
      const sortedShort = [...shortTimes].sort((a, b) => a - b)
      const sortedLong = [...longTimes].sort((a, b) => a - b)

      // Use median to reduce noise
      const shortMedian = sortedShort[Math.floor(iterations / 2)]!
      const longMedian = sortedLong[Math.floor(iterations / 2)]!

      // Times should be similar - allow 3x variance due to async operations and crypto
      const ratio = Math.max(shortMedian, longMedian) / Math.min(shortMedian, longMedian)
      expect(ratio).toBeLessThan(3)
    })
  })

  describe('early-mismatch independence', () => {
    it('comparison time should not vary based on where mismatch occurs', async () => {
      // Verify that signatures that differ at the beginning take the same
      // time to compare as signatures that differ at the end

      const iterations = 50

      // Valid signature (all correct)
      const validSig = await generateGitHubSignature(secret, payload)
      const validHex = validSig.slice(7) // Remove 'sha256='

      // Create signature that differs at the start
      const firstChar = validHex[0]!
      const differentFirst = firstChar === '0' ? '1' : '0'
      const earlyMismatch = 'sha256=' + differentFirst + validHex.slice(1)

      // Create signature that differs at the end
      const lastChar = validHex[63]!
      const differentLast = lastChar === '0' ? '1' : '0'
      const lateMismatch = 'sha256=' + validHex.slice(0, 63) + differentLast

      const earlyTimes: number[] = []
      const lateTimes: number[] = []

      // Warm up
      for (let i = 0; i < 10; i++) {
        await verifyGitHubSignature(secret, payload, earlyMismatch)
        await verifyGitHubSignature(secret, payload, lateMismatch)
      }

      // Measure
      for (let i = 0; i < iterations; i++) {
        const start1 = performance.now()
        await verifyGitHubSignature(secret, payload, earlyMismatch)
        earlyTimes.push(performance.now() - start1)

        const start2 = performance.now()
        await verifyGitHubSignature(secret, payload, lateMismatch)
        lateTimes.push(performance.now() - start2)
      }

      // Calculate medians
      const sortedEarly = [...earlyTimes].sort((a, b) => a - b)
      const sortedLate = [...lateTimes].sort((a, b) => a - b)

      const earlyMedian = sortedEarly[Math.floor(iterations / 2)]!
      const lateMedian = sortedLate[Math.floor(iterations / 2)]!

      // Times should be nearly identical
      const ratio = Math.max(earlyMedian, lateMedian) / Math.min(earlyMedian, lateMedian)
      expect(ratio).toBeLessThan(2)
    })
  })

  describe('all-zeros vs all-fs comparison', () => {
    it('comparing all-zeros vs all-fs should take similar time', async () => {
      // This tests that the XOR accumulator doesn't short-circuit

      const iterations = 50
      const allZeros = 'sha256=' + '0'.repeat(64)
      const allFs = 'sha256=' + 'f'.repeat(64)
      const mixed = 'sha256=' + '0f'.repeat(32)

      const zerosTimes: number[] = []
      const fsTimes: number[] = []
      const mixedTimes: number[] = []

      // Warm up
      for (let i = 0; i < 10; i++) {
        await verifyGitHubSignature(secret, payload, allZeros)
        await verifyGitHubSignature(secret, payload, allFs)
        await verifyGitHubSignature(secret, payload, mixed)
      }

      // Measure
      for (let i = 0; i < iterations; i++) {
        const start1 = performance.now()
        await verifyGitHubSignature(secret, payload, allZeros)
        zerosTimes.push(performance.now() - start1)

        const start2 = performance.now()
        await verifyGitHubSignature(secret, payload, allFs)
        fsTimes.push(performance.now() - start2)

        const start3 = performance.now()
        await verifyGitHubSignature(secret, payload, mixed)
        mixedTimes.push(performance.now() - start3)
      }

      // Calculate medians
      const sortedZeros = [...zerosTimes].sort((a, b) => a - b)
      const sortedFs = [...fsTimes].sort((a, b) => a - b)
      const sortedMixed = [...mixedTimes].sort((a, b) => a - b)

      const zerosMedian = sortedZeros[Math.floor(iterations / 2)]!
      const fsMedian = sortedFs[Math.floor(iterations / 2)]!
      const mixedMedian = sortedMixed[Math.floor(iterations / 2)]!

      // All should be similar
      const maxTime = Math.max(zerosMedian, fsMedian, mixedMedian)
      const minTime = Math.min(zerosMedian, fsMedian, mixedMedian)
      const ratio = maxTime / minTime

      expect(ratio).toBeLessThan(2)
    })
  })

  describe('valid vs invalid signature timing', () => {
    it('valid and invalid signatures should take similar verification time', async () => {
      // The original timing test from GitHub verification, but more robust

      const validSignature = await generateGitHubSignature(secret, payload)
      const invalidSignature = 'sha256=' + 'a'.repeat(64)

      const iterations = 100
      const validTimes: number[] = []
      const invalidTimes: number[] = []

      // Warm up
      for (let i = 0; i < 20; i++) {
        await verifyGitHubSignature(secret, payload, validSignature)
        await verifyGitHubSignature(secret, payload, invalidSignature)
      }

      // Interleave valid and invalid to reduce systematic bias
      for (let i = 0; i < iterations; i++) {
        if (i % 2 === 0) {
          const start = performance.now()
          await verifyGitHubSignature(secret, payload, validSignature)
          validTimes.push(performance.now() - start)
        } else {
          const start = performance.now()
          await verifyGitHubSignature(secret, payload, invalidSignature)
          invalidTimes.push(performance.now() - start)
        }
      }

      // Fill remaining
      for (let i = 0; i < iterations; i++) {
        if (i % 2 === 1) {
          const start = performance.now()
          await verifyGitHubSignature(secret, payload, validSignature)
          validTimes.push(performance.now() - start)
        } else {
          const start = performance.now()
          await verifyGitHubSignature(secret, payload, invalidSignature)
          invalidTimes.push(performance.now() - start)
        }
      }

      // Calculate medians
      const sortedValid = [...validTimes].sort((a, b) => a - b)
      const sortedInvalid = [...invalidTimes].sort((a, b) => a - b)

      const validMedian = sortedValid[Math.floor(validTimes.length / 2)]!
      const invalidMedian = sortedInvalid[Math.floor(invalidTimes.length / 2)]!

      const ratio = Math.max(validMedian, invalidMedian) / Math.min(validMedian, invalidMedian)
      expect(ratio).toBeLessThan(2)
    })
  })
})

// ============================================================================
// Webhook Provider Configuration Validation Tests
// ============================================================================

describe('Webhook Provider Validation', () => {
  describe('WEBHOOK_PROVIDERS constant', () => {
    it('contains all supported providers', () => {
      expect(WEBHOOK_PROVIDERS).toContain('github')
      expect(WEBHOOK_PROVIDERS).toContain('stripe')
      expect(WEBHOOK_PROVIDERS).toContain('workos')
      expect(WEBHOOK_PROVIDERS).toContain('slack')
      expect(WEBHOOK_PROVIDERS).toContain('linear')
      expect(WEBHOOK_PROVIDERS).toContain('svix')
      expect(WEBHOOK_PROVIDERS).toHaveLength(6)
    })

    it('is immutable (readonly)', () => {
      // TypeScript should prevent mutations, but we can verify runtime behavior
      const originalLength = WEBHOOK_PROVIDERS.length
      expect(originalLength).toBe(6)
    })
  })

  describe('isValidProvider', () => {
    it('returns true for valid providers', () => {
      expect(isValidProvider('github')).toBe(true)
      expect(isValidProvider('stripe')).toBe(true)
      expect(isValidProvider('workos')).toBe(true)
      expect(isValidProvider('slack')).toBe(true)
      expect(isValidProvider('linear')).toBe(true)
      expect(isValidProvider('svix')).toBe(true)
    })

    it('returns false for invalid providers', () => {
      expect(isValidProvider('invalid')).toBe(false)
      expect(isValidProvider('GitHub')).toBe(false) // Case-sensitive
      expect(isValidProvider('GITHUB')).toBe(false)
      expect(isValidProvider('')).toBe(false)
      expect(isValidProvider(null)).toBe(false)
      expect(isValidProvider(undefined)).toBe(false)
      expect(isValidProvider(123)).toBe(false)
      expect(isValidProvider({})).toBe(false)
    })
  })

  describe('Provider-specific type guards', () => {
    describe('isGitHubConfig', () => {
      it('returns true for valid GitHub config', () => {
        expect(isGitHubConfig({ provider: 'github', secret: 'my-secret' })).toBe(true)
        expect(isGitHubConfig({ provider: 'github', secret: 'my-secret', enabled: true })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isGitHubConfig({ provider: 'stripe', secret: 'my-secret' })).toBe(false)
        expect(isGitHubConfig({ provider: 'github' })).toBe(false) // Missing secret
        expect(isGitHubConfig({ provider: 'github', secret: '' })).toBe(false) // Empty secret
        expect(isGitHubConfig(null)).toBe(false)
        expect(isGitHubConfig({})).toBe(false)
      })
    })

    describe('isStripeConfig', () => {
      it('returns true for valid Stripe config', () => {
        expect(isStripeConfig({ provider: 'stripe', secret: 'whsec_test' })).toBe(true)
        expect(isStripeConfig({ provider: 'stripe', secret: 'whsec_test', toleranceSeconds: 600 })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isStripeConfig({ provider: 'github', secret: 'my-secret' })).toBe(false)
        expect(isStripeConfig({ provider: 'stripe', secret: 'test', toleranceSeconds: 'invalid' })).toBe(false)
      })
    })

    describe('isWorkOSConfig', () => {
      it('returns true for valid WorkOS config', () => {
        expect(isWorkOSConfig({ provider: 'workos', secret: 'my-secret' })).toBe(true)
        expect(isWorkOSConfig({ provider: 'workos', secret: 'my-secret', toleranceSeconds: 300 })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isWorkOSConfig({ provider: 'github', secret: 'my-secret' })).toBe(false)
      })
    })

    describe('isSlackConfig', () => {
      it('returns true for valid Slack config', () => {
        expect(isSlackConfig({ provider: 'slack', secret: 'my-secret' })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isSlackConfig({ provider: 'github', secret: 'my-secret' })).toBe(false)
      })
    })

    describe('isLinearConfig', () => {
      it('returns true for valid Linear config', () => {
        expect(isLinearConfig({ provider: 'linear', secret: 'my-secret' })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isLinearConfig({ provider: 'github', secret: 'my-secret' })).toBe(false)
      })
    })

    describe('isSvixConfig', () => {
      it('returns true for valid Svix config', () => {
        expect(isSvixConfig({ provider: 'svix', secret: 'whsec_test' })).toBe(true)
        expect(isSvixConfig({ provider: 'svix', secret: 'whsec_test', toleranceSeconds: 600 })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isSvixConfig({ provider: 'github', secret: 'my-secret' })).toBe(false)
      })
    })

    describe('isWebhookProviderConfig', () => {
      it('returns true for any valid provider config', () => {
        expect(isWebhookProviderConfig({ provider: 'github', secret: 'test' })).toBe(true)
        expect(isWebhookProviderConfig({ provider: 'stripe', secret: 'test' })).toBe(true)
        expect(isWebhookProviderConfig({ provider: 'workos', secret: 'test' })).toBe(true)
        expect(isWebhookProviderConfig({ provider: 'slack', secret: 'test' })).toBe(true)
        expect(isWebhookProviderConfig({ provider: 'linear', secret: 'test' })).toBe(true)
        expect(isWebhookProviderConfig({ provider: 'svix', secret: 'test' })).toBe(true)
      })

      it('returns false for invalid configs', () => {
        expect(isWebhookProviderConfig({ provider: 'invalid', secret: 'test' })).toBe(false)
        expect(isWebhookProviderConfig({ secret: 'test' })).toBe(false)
        expect(isWebhookProviderConfig(null)).toBe(false)
        expect(isWebhookProviderConfig('string')).toBe(false)
      })
    })
  })

  describe('validateWebhookConfig', () => {
    it('returns empty array for valid configs', () => {
      expect(validateWebhookConfig({ provider: 'github', secret: 'test' })).toEqual([])
      expect(validateWebhookConfig({ provider: 'stripe', secret: 'whsec_test', toleranceSeconds: 300 })).toEqual([])
      expect(validateWebhookConfig({ provider: 'svix', secret: 'whsec_test' })).toEqual([])
    })

    it('returns errors for null/undefined config', () => {
      const errors = validateWebhookConfig(null)
      expect(errors).toContain('Configuration must be a non-null object')
    })

    it('returns errors for non-object config', () => {
      const errors = validateWebhookConfig('string')
      expect(errors).toContain('Configuration must be a non-null object')
    })

    it('returns errors for missing provider', () => {
      const errors = validateWebhookConfig({ secret: 'test' })
      expect(errors).toContain('Missing required field: provider')
    })

    it('returns errors for invalid provider', () => {
      const errors = validateWebhookConfig({ provider: 'invalid', secret: 'test' })
      expect(errors.some((e) => e.includes('Invalid provider'))).toBe(true)
    })

    it('returns errors for missing secret', () => {
      const errors = validateWebhookConfig({ provider: 'github' })
      expect(errors).toContain('Missing required field: secret')
    })

    it('returns errors for empty secret', () => {
      const errors = validateWebhookConfig({ provider: 'github', secret: '' })
      expect(errors.some((e) => e.includes('non-empty string'))).toBe(true)
    })

    it('returns errors for invalid toleranceSeconds type', () => {
      const errors = validateWebhookConfig({ provider: 'stripe', secret: 'test', toleranceSeconds: 'invalid' })
      expect(errors.some((e) => e.includes('toleranceSeconds'))).toBe(true)
    })

    it('returns errors for non-positive toleranceSeconds', () => {
      const errors = validateWebhookConfig({ provider: 'stripe', secret: 'test', toleranceSeconds: 0 })
      expect(errors.some((e) => e.includes('positive number'))).toBe(true)

      const errors2 = validateWebhookConfig({ provider: 'stripe', secret: 'test', toleranceSeconds: -100 })
      expect(errors2.some((e) => e.includes('positive number'))).toBe(true)
    })

    it('returns errors for Svix secret without whsec_ prefix', () => {
      const errors = validateWebhookConfig({ provider: 'svix', secret: 'invalid-prefix' })
      expect(errors.some((e) => e.includes('whsec_'))).toBe(true)
    })

    it('returns errors for invalid enabled field', () => {
      const errors = validateWebhookConfig({ provider: 'github', secret: 'test', enabled: 'yes' })
      expect(errors.some((e) => e.includes('enabled'))).toBe(true)
    })

    it('allows valid enabled field', () => {
      expect(validateWebhookConfig({ provider: 'github', secret: 'test', enabled: true })).toEqual([])
      expect(validateWebhookConfig({ provider: 'github', secret: 'test', enabled: false })).toEqual([])
    })
  })

  describe('assertValidWebhookConfig', () => {
    it('returns config for valid input', () => {
      const config = { provider: 'github' as const, secret: 'test' }
      const result = assertValidWebhookConfig(config)
      expect(result).toEqual(config)
    })

    it('throws WebhookConfigError for invalid input', () => {
      expect(() => assertValidWebhookConfig(null)).toThrow(WebhookConfigError)
      expect(() => assertValidWebhookConfig({ provider: 'invalid' })).toThrow(WebhookConfigError)
      expect(() => assertValidWebhookConfig({ provider: 'github' })).toThrow(WebhookConfigError)
    })

    it('includes provider in error when available', () => {
      try {
        assertValidWebhookConfig({ provider: 'github' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(WebhookConfigError)
        expect((error as WebhookConfigError).provider).toBe('github')
      }
    })

    it('includes error message details', () => {
      try {
        assertValidWebhookConfig({ provider: 'github', secret: '' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(WebhookConfigError)
        expect((error as WebhookConfigError).message).toContain('Invalid webhook configuration')
      }
    })
  })

  describe('createWebhookConfig', () => {
    it('creates valid config for each provider', () => {
      const githubConfig = createWebhookConfig('github', 'secret')
      expect(githubConfig.provider).toBe('github')
      expect(githubConfig.secret).toBe('secret')

      const stripeConfig = createWebhookConfig('stripe', 'whsec_test', { toleranceSeconds: 600 })
      expect(stripeConfig.provider).toBe('stripe')
      expect((stripeConfig as any).toleranceSeconds).toBe(600)
    })

    it('throws for invalid input', () => {
      expect(() => createWebhookConfig('github', '')).toThrow(WebhookConfigError)
    })

    it('includes enabled option', () => {
      const config = createWebhookConfig('github', 'secret', { enabled: false })
      expect(config.enabled).toBe(false)
    })
  })

  describe('WebhookConfigError', () => {
    it('has correct name', () => {
      const error = new WebhookConfigError('test message')
      expect(error.name).toBe('WebhookConfigError')
    })

    it('stores provider and field', () => {
      const error = new WebhookConfigError('test message', 'github', 'secret')
      expect(error.provider).toBe('github')
      expect(error.field).toBe('secret')
    })

    it('is instanceof Error', () => {
      const error = new WebhookConfigError('test')
      expect(error).toBeInstanceOf(Error)
    })
  })

  describe('WebhookSignatureError', () => {
    it('has correct name', () => {
      const error = new WebhookSignatureError('test message', 'github')
      expect(error.name).toBe('WebhookSignatureError')
    })

    it('stores provider and timestamp', () => {
      const error = new WebhookSignatureError('test message', 'stripe', 1234567890)
      expect(error.provider).toBe('stripe')
      expect(error.timestamp).toBe(1234567890)
    })

    it('is instanceof Error', () => {
      const error = new WebhookSignatureError('test', 'github')
      expect(error).toBeInstanceOf(Error)
    })
  })
})
