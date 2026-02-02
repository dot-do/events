/**
 * End-to-End Encryption Tests
 *
 * Tests for AES-256-GCM encryption of event payloads.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  generateEncryptionKey,
  generateEncryptionKeyInfo,
  encryptPayload,
  decryptPayload,
  encryptFields,
  decryptFields,
  isEncryptedPayload,
  shouldEncryptEvent,
  createKeyStore,
  initializeKeyStore,
  rotateEncryptionKey,
  reencryptPayload,
  constantTimeCompare,
  ENCRYPTED_MARKER,
  type EncryptionKeyStore,
  type PayloadEncryptionConfig,
  type EncryptedPayload,
} from '../encryption'

describe('Key Generation', () => {
  it('generates a valid base64-encoded key', async () => {
    const key = await generateEncryptionKey()

    expect(typeof key).toBe('string')
    // Base64 of 32 bytes = 44 characters
    expect(key.length).toBe(44)
    // Should be valid base64
    expect(() => atob(key)).not.toThrow()
  })

  it('generates unique keys', async () => {
    const key1 = await generateEncryptionKey()
    const key2 = await generateEncryptionKey()

    expect(key1).not.toBe(key2)
  })

  it('generates key info with metadata', async () => {
    const keyInfo = await generateEncryptionKeyInfo('v1', 30)

    expect(keyInfo.keyId).toBe('v1')
    expect(keyInfo.keyBase64.length).toBe(44)
    expect(keyInfo.isActive).toBe(true)
    expect(keyInfo.createdAt).toBeDefined()
    expect(keyInfo.expiresAt).toBeDefined()

    // Expiration should be ~30 days in the future
    const created = new Date(keyInfo.createdAt).getTime()
    const expires = new Date(keyInfo.expiresAt!).getTime()
    const diff = expires - created
    expect(diff).toBeCloseTo(30 * 24 * 60 * 60 * 1000, -5)
  })
})

describe('Encryption/Decryption', () => {
  let testKey: string
  const testKeyId = 'test-key-v1'

  beforeEach(async () => {
    testKey = await generateEncryptionKey()
  })

  it('encrypts and decrypts a simple string', async () => {
    const payload = 'Hello, World!'

    const encrypted = await encryptPayload(payload, testKey, testKeyId)
    const decrypted = await decryptPayload<string>(encrypted, testKey)

    expect(decrypted).toBe(payload)
  })

  it('encrypts and decrypts an object', async () => {
    const payload = {
      user: { id: 123, email: 'test@example.com' },
      secret: 'api-key-12345',
    }

    const encrypted = await encryptPayload(payload, testKey, testKeyId)
    const decrypted = await decryptPayload<typeof payload>(encrypted, testKey)

    expect(decrypted).toEqual(payload)
  })

  it('encrypts and decrypts an array', async () => {
    const payload = [1, 'two', { three: 3 }]

    const encrypted = await encryptPayload(payload, testKey, testKeyId)
    const decrypted = await decryptPayload<typeof payload>(encrypted, testKey)

    expect(decrypted).toEqual(payload)
  })

  it('produces valid encrypted payload structure', async () => {
    const payload = { test: true }
    const encrypted = await encryptPayload(payload, testKey, testKeyId)

    expect(encrypted.v).toBe(1)
    expect(encrypted.kid).toBe(testKeyId)
    expect(typeof encrypted.iv).toBe('string')
    expect(typeof encrypted.ct).toBe('string')
    // IV should be 12 bytes = 16 chars base64
    expect(encrypted.iv.length).toBe(16)
  })

  it('produces different ciphertext for same plaintext', async () => {
    const payload = { same: 'data' }

    const encrypted1 = await encryptPayload(payload, testKey, testKeyId)
    const encrypted2 = await encryptPayload(payload, testKey, testKeyId)

    // Same key and plaintext should produce different ciphertext (random IV)
    expect(encrypted1.ct).not.toBe(encrypted2.ct)
    expect(encrypted1.iv).not.toBe(encrypted2.iv)

    // But both should decrypt to the same value
    const decrypted1 = await decryptPayload(encrypted1, testKey)
    const decrypted2 = await decryptPayload(encrypted2, testKey)
    expect(decrypted1).toEqual(decrypted2)
  })

  it('fails to decrypt with wrong key', async () => {
    const payload = 'secret data'
    const wrongKey = await generateEncryptionKey()

    const encrypted = await encryptPayload(payload, testKey, testKeyId)

    await expect(decryptPayload(encrypted, wrongKey)).rejects.toThrow()
  })

  it('fails to decrypt tampered ciphertext', async () => {
    const payload = 'important data'
    const encrypted = await encryptPayload(payload, testKey, testKeyId)

    // Tamper with ciphertext
    const tampered = { ...encrypted }
    const ctBytes = new Uint8Array(
      atob(tampered.ct)
        .split('')
        .map(c => c.charCodeAt(0))
    )
    ctBytes[0] = ctBytes[0] ^ 0xff // Flip bits
    tampered.ct = btoa(String.fromCharCode(...ctBytes))

    await expect(decryptPayload(tampered, testKey)).rejects.toThrow()
  })

  it('handles null and undefined values', async () => {
    const payloadWithNull = { value: null }
    const encrypted = await encryptPayload(payloadWithNull, testKey, testKeyId)
    const decrypted = await decryptPayload<typeof payloadWithNull>(encrypted, testKey)
    expect(decrypted.value).toBeNull()
  })

  it('handles special characters and unicode', async () => {
    const payload = {
      emoji: '🔐🔑🛡️',
      unicode: '你好世界',
      special: '<script>alert("xss")</script>',
    }

    const encrypted = await encryptPayload(payload, testKey, testKeyId)
    const decrypted = await decryptPayload<typeof payload>(encrypted, testKey)

    expect(decrypted).toEqual(payload)
  })
})

describe('Field-Level Encryption', () => {
  let keyStore: EncryptionKeyStore

  beforeEach(async () => {
    keyStore = await initializeKeyStore('key-v1', 90)
  })

  it('encrypts specific fields', async () => {
    const payload = {
      username: 'publicuser',
      email: 'secret@example.com',
      profile: {
        bio: 'public bio',
        phone: '555-1234',
      },
    }

    const encrypted = await encryptFields(
      payload,
      ['email', 'profile.phone'],
      keyStore.keys[keyStore.activeKeyId].keyBase64,
      keyStore.activeKeyId
    )

    // Non-encrypted fields should be unchanged
    expect(encrypted.username).toBe('publicuser')
    expect((encrypted.profile as { bio: string }).bio).toBe('public bio')

    // Encrypted fields should have marker
    expect(encrypted.email).toHaveProperty(ENCRYPTED_MARKER)
    expect((encrypted.profile as { phone: unknown }).phone).toHaveProperty(ENCRYPTED_MARKER)
  })

  it('decrypts specific fields', async () => {
    const payload = {
      username: 'publicuser',
      email: 'secret@example.com',
      profile: {
        bio: 'public bio',
        phone: '555-1234',
      },
    }

    const encrypted = await encryptFields(
      payload,
      ['email', 'profile.phone'],
      keyStore.keys[keyStore.activeKeyId].keyBase64,
      keyStore.activeKeyId
    )

    const decrypted = await decryptFields(encrypted, keyStore)

    expect(decrypted).toEqual(payload)
  })

  it('handles missing fields gracefully', async () => {
    const payload = {
      existingField: 'value',
    }

    const encrypted = await encryptFields(
      payload,
      ['nonexistent', 'deeply.nested.missing'],
      keyStore.keys[keyStore.activeKeyId].keyBase64,
      keyStore.activeKeyId
    )

    // Should not add markers for missing fields
    expect(encrypted).toEqual(payload)
  })
})

describe('isEncryptedPayload', () => {
  it('returns true for valid encrypted payload', () => {
    const encrypted: EncryptedPayload = {
      v: 1,
      kid: 'key-1',
      iv: 'AAAAAAAAAAAAAAAA',
      ct: 'encrypted-data',
    }

    expect(isEncryptedPayload(encrypted)).toBe(true)
  })

  it('returns false for non-objects', () => {
    expect(isEncryptedPayload(null)).toBe(false)
    expect(isEncryptedPayload(undefined)).toBe(false)
    expect(isEncryptedPayload('string')).toBe(false)
    expect(isEncryptedPayload(123)).toBe(false)
  })

  it('returns false for objects missing required fields', () => {
    expect(isEncryptedPayload({ v: 1, kid: 'key' })).toBe(false)
    expect(isEncryptedPayload({ v: 1, iv: 'iv', ct: 'ct' })).toBe(false)
    expect(isEncryptedPayload({ kid: 'key', iv: 'iv', ct: 'ct' })).toBe(false)
  })

  it('returns false for objects with wrong field types', () => {
    expect(isEncryptedPayload({ v: '1', kid: 'key', iv: 'iv', ct: 'ct' })).toBe(false)
    expect(isEncryptedPayload({ v: 1, kid: 123, iv: 'iv', ct: 'ct' })).toBe(false)
  })
})

describe('shouldEncryptEvent', () => {
  it('returns false when encryption disabled', () => {
    const config: PayloadEncryptionConfig = { enabled: false }
    expect(shouldEncryptEvent('any.event', config)).toBe(false)
  })

  it('returns true for all events when no types specified', () => {
    const config: PayloadEncryptionConfig = { enabled: true }
    expect(shouldEncryptEvent('any.event', config)).toBe(true)
    expect(shouldEncryptEvent('custom.sensitive', config)).toBe(true)
  })

  it('returns true only for matching event types', () => {
    const config: PayloadEncryptionConfig = {
      enabled: true,
      eventTypes: ['user.created', 'user.updated'],
    }

    expect(shouldEncryptEvent('user.created', config)).toBe(true)
    expect(shouldEncryptEvent('user.updated', config)).toBe(true)
    expect(shouldEncryptEvent('user.deleted', config)).toBe(false)
    expect(shouldEncryptEvent('order.created', config)).toBe(false)
  })

  it('supports wildcard patterns', () => {
    const config: PayloadEncryptionConfig = {
      enabled: true,
      eventTypes: ['user.*', 'payment.*.sensitive'],
    }

    expect(shouldEncryptEvent('user.created', config)).toBe(true)
    expect(shouldEncryptEvent('user.profile.updated', config)).toBe(false) // * matches single segment
    expect(shouldEncryptEvent('payment.card.sensitive', config)).toBe(true)
    expect(shouldEncryptEvent('order.created', config)).toBe(false)
  })

  it('supports * to match all', () => {
    const config: PayloadEncryptionConfig = {
      enabled: true,
      eventTypes: ['*'],
    }

    expect(shouldEncryptEvent('any.event', config)).toBe(true)
  })
})

describe('Key Store Management', () => {
  it('creates empty key store', () => {
    const store = createKeyStore()

    expect(store.keys).toEqual({})
    expect(store.activeKeyId).toBe('')
  })

  it('initializes key store with first key', async () => {
    const store = await initializeKeyStore('initial-key')

    expect(store.activeKeyId).toBe('initial-key')
    expect(store.keys['initial-key']).toBeDefined()
    expect(store.keys['initial-key'].isActive).toBe(true)
  })

  it('rotates to new key while keeping old', async () => {
    const store = await initializeKeyStore('v1')
    const rotated = await rotateEncryptionKey(store, 'v2')

    expect(rotated.activeKeyId).toBe('v2')
    expect(rotated.keys['v2'].isActive).toBe(true)
    expect(rotated.keys['v1'].isActive).toBe(false)
    // Both keys should be present
    expect(Object.keys(rotated.keys)).toHaveLength(2)
  })
})

describe('Key Rotation', () => {
  it('re-encrypts data with new key', async () => {
    // Create initial key store
    let keyStore = await initializeKeyStore('v1')
    const originalPayload = { secret: 'data' }

    // Encrypt with v1
    const encryptedV1 = await encryptPayload(
      originalPayload,
      keyStore.keys['v1'].keyBase64,
      'v1'
    )
    expect(encryptedV1.kid).toBe('v1')

    // Rotate to v2
    keyStore = await rotateEncryptionKey(keyStore, 'v2')

    // Re-encrypt with v2
    const encryptedV2 = await reencryptPayload(encryptedV1, keyStore, 'v2')
    expect(encryptedV2.kid).toBe('v2')
    expect(encryptedV2.ct).not.toBe(encryptedV1.ct)

    // Both should decrypt to same value
    const decrypted = await decryptPayload(encryptedV2, keyStore.keys['v2'].keyBase64)
    expect(decrypted).toEqual(originalPayload)
  })

  it('can still decrypt with old key after rotation', async () => {
    let keyStore = await initializeKeyStore('v1')
    const payload = { data: 'encrypted with v1' }

    // Encrypt with v1
    const encrypted = await encryptPayload(
      payload,
      keyStore.keys['v1'].keyBase64,
      'v1'
    )

    // Rotate to v2
    keyStore = await rotateEncryptionKey(keyStore, 'v2')

    // Should still decrypt v1 data
    const decrypted = await decryptPayload(encrypted, keyStore.keys['v1'].keyBase64)
    expect(decrypted).toEqual(payload)
  })
})

describe('constantTimeCompare', () => {
  it('returns true for identical strings', () => {
    expect(constantTimeCompare('test', 'test')).toBe(true)
    expect(constantTimeCompare('', '')).toBe(true)
    expect(constantTimeCompare('a longer string', 'a longer string')).toBe(true)
  })

  it('returns false for different strings', () => {
    expect(constantTimeCompare('test', 'TEST')).toBe(false)
    expect(constantTimeCompare('test', 'test!')).toBe(false)
    expect(constantTimeCompare('abc', 'abd')).toBe(false)
  })

  it('returns false for different lengths', () => {
    expect(constantTimeCompare('short', 'longer string')).toBe(false)
    expect(constantTimeCompare('', 'not empty')).toBe(false)
  })
})

describe('Edge Cases', () => {
  it('handles empty objects', async () => {
    const keyStore = await initializeKeyStore('key')
    const key = keyStore.keys[keyStore.activeKeyId].keyBase64

    const encrypted = await encryptPayload({}, key, keyStore.activeKeyId)
    const decrypted = await decryptPayload(encrypted, key)

    expect(decrypted).toEqual({})
  })

  it('handles large payloads', async () => {
    const keyStore = await initializeKeyStore('key')
    const key = keyStore.keys[keyStore.activeKeyId].keyBase64

    // Create a 100KB payload
    const largePayload = {
      data: 'x'.repeat(100 * 1024),
      nested: {
        array: Array(1000)
          .fill(null)
          .map((_, i) => ({ id: i, value: `item-${i}` })),
      },
    }

    const encrypted = await encryptPayload(largePayload, key, keyStore.activeKeyId)
    const decrypted = await decryptPayload<typeof largePayload>(encrypted, key)

    expect(decrypted).toEqual(largePayload)
  })

  it('handles deeply nested structures', async () => {
    const keyStore = await initializeKeyStore('key')
    const key = keyStore.keys[keyStore.activeKeyId].keyBase64

    let nested: Record<string, unknown> = { value: 'deep' }
    for (let i = 0; i < 20; i++) {
      nested = { level: i, child: nested }
    }

    const encrypted = await encryptPayload(nested, key, keyStore.activeKeyId)
    const decrypted = await decryptPayload(encrypted, key)

    expect(decrypted).toEqual(nested)
  })

  it('rejects unsupported encryption version', async () => {
    const keyStore = await initializeKeyStore('key')
    const key = keyStore.keys[keyStore.activeKeyId].keyBase64

    const invalidPayload: EncryptedPayload = {
      v: 99 as 1, // Invalid version
      kid: 'key',
      iv: 'AAAAAAAAAAAAAAAA',
      ct: 'some-ciphertext',
    }

    await expect(decryptPayload(invalidPayload, key)).rejects.toThrow(
      'Unsupported encryption version'
    )
  })
})
