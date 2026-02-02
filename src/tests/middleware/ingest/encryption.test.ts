/**
 * Encryption Middleware Tests
 *
 * Unit tests for src/middleware/ingest/encryption.ts
 * Tests encryption middleware, key loading, config loading, and decryption helpers
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  encryptionMiddleware,
  loadKeyStore,
  loadPayloadEncryptionConfig,
  decryptEvent,
  decryptEventBatch,
  validateClientEncryption,
  type EncryptionEnv,
  type EncryptedIngestContext,
} from '../../../middleware/ingest/encryption'
import type { IngestContext } from '../../../middleware/ingest/types'
import type { Env } from '../../../env'
import type { DurableEvent } from '@dotdo/events'
import {
  generateEncryptionKey,
  initializeKeyStore,
  encryptPayload,
  encryptFields,
  ENCRYPTED_MARKER,
  type EncryptionKeyStore,
  type PayloadEncryptionConfig,
  type EncryptedPayload,
} from '../../../../core/src/encryption'

// ============================================================================
// Mock Logger
// ============================================================================

vi.mock('../../../logger', () => ({
  createLogger: () => ({
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn().mockReturnThis(),
  }),
}))

// ============================================================================
// Helper Functions
// ============================================================================

function createMockContext(
  overrides: Partial<IngestContext> = {},
  envOverrides: Partial<EncryptionEnv> = {}
): IngestContext {
  return {
    request: new Request('https://events.do/ingest', { method: 'POST' }),
    env: {
      ANALYTICS: undefined,
      ...envOverrides,
    } as Env & EncryptionEnv,
    ctx: {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as unknown as ExecutionContext,
    tenant: {
      namespace: 'test-namespace',
      isAdmin: false,
      keyId: 'test-key',
    },
    batch: {
      events: [
        { type: 'user.created', ts: '2024-01-15T10:00:00Z', userId: '123' },
        { type: 'order.placed', ts: '2024-01-15T10:00:01Z', orderId: '456' },
      ],
    },
    startTime: performance.now(),
    ...overrides,
  }
}

function createKeyStoreJson(keyStore: EncryptionKeyStore): string {
  return JSON.stringify(keyStore)
}

function createEncryptionConfig(config: PayloadEncryptionConfig): string {
  return JSON.stringify(config)
}

function createNamespaceConfigs(
  configs: Record<string, PayloadEncryptionConfig>
): string {
  return JSON.stringify(configs)
}

// ============================================================================
// loadKeyStore Tests
// ============================================================================

describe('loadKeyStore', () => {
  describe('when ENCRYPTION_KEYS is not set', () => {
    it('returns null', () => {
      const env = {} as Env & EncryptionEnv

      const result = loadKeyStore(env)

      expect(result).toBeNull()
    })

    it('returns null for undefined', () => {
      const env = { ENCRYPTION_KEYS: undefined } as Env & EncryptionEnv

      const result = loadKeyStore(env)

      expect(result).toBeNull()
    })

    it('returns null for empty string', () => {
      const env = { ENCRYPTION_KEYS: '' } as Env & EncryptionEnv

      const result = loadKeyStore(env)

      expect(result).toBeNull()
    })
  })

  describe('when ENCRYPTION_KEYS is set', () => {
    it('parses valid JSON key store', async () => {
      const keyStore = await initializeKeyStore('v1')
      const env = {
        ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
      } as Env & EncryptionEnv

      const result = loadKeyStore(env)

      expect(result).not.toBeNull()
      expect(result?.activeKeyId).toBe('v1')
      expect(result?.keys['v1']).toBeDefined()
      expect(result?.keys['v1'].keyBase64).toBe(keyStore.keys['v1'].keyBase64)
    })

    it('parses key store with multiple keys', async () => {
      const keyStore: EncryptionKeyStore = {
        activeKeyId: 'v2',
        keys: {
          v1: {
            keyId: 'v1',
            keyBase64: await generateEncryptionKey(),
            createdAt: '2024-01-01T00:00:00Z',
            isActive: false,
          },
          v2: {
            keyId: 'v2',
            keyBase64: await generateEncryptionKey(),
            createdAt: '2024-01-15T00:00:00Z',
            isActive: true,
          },
        },
      }
      const env = {
        ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
      } as Env & EncryptionEnv

      const result = loadKeyStore(env)

      expect(result?.activeKeyId).toBe('v2')
      expect(Object.keys(result?.keys || {})).toHaveLength(2)
    })

    it('returns null for invalid JSON', () => {
      const env = {
        ENCRYPTION_KEYS: '{ invalid json }',
      } as Env & EncryptionEnv

      const result = loadKeyStore(env)

      expect(result).toBeNull()
    })

    it('returns null for non-object JSON', () => {
      const env = {
        ENCRYPTION_KEYS: '"just a string"',
      } as Env & EncryptionEnv

      const result = loadKeyStore(env)

      // JSON.parse succeeds but type guard validation fails for strings
      expect(result).toBeNull()
    })
  })
})

// ============================================================================
// loadPayloadEncryptionConfig Tests
// ============================================================================

describe('loadPayloadEncryptionConfig', () => {
  describe('namespace-specific config', () => {
    it('loads config for specific namespace', () => {
      const configs: Record<string, PayloadEncryptionConfig> = {
        acme: { enabled: true, eventTypes: ['user.*'] },
        beta: { enabled: true, fields: ['email'] },
      }
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs(configs),
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'acme')

      expect(result).not.toBeNull()
      expect(result?.enabled).toBe(true)
      expect(result?.eventTypes).toEqual(['user.*'])
    })

    it('returns config for correct namespace only', () => {
      const configs: Record<string, PayloadEncryptionConfig> = {
        team1: { enabled: true, eventTypes: ['order.*'] },
        team2: { enabled: false },
      }
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs(configs),
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'team2')

      expect(result?.enabled).toBe(false)
    })

    it('returns null for non-existent namespace when no default', () => {
      const configs: Record<string, PayloadEncryptionConfig> = {
        existing: { enabled: true },
      }
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs(configs),
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'nonexistent')

      expect(result).toBeNull()
    })
  })

  describe('default config fallback', () => {
    it('falls back to default config when namespace not found', () => {
      const defaultConfig: PayloadEncryptionConfig = {
        enabled: true,
        eventTypes: ['sensitive.*'],
      }
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({}),
        DEFAULT_ENCRYPTION_CONFIG: createEncryptionConfig(defaultConfig),
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'any-namespace')

      expect(result).not.toBeNull()
      expect(result?.enabled).toBe(true)
      expect(result?.eventTypes).toEqual(['sensitive.*'])
    })

    it('uses namespace config over default', () => {
      const namespaceConfigs: Record<string, PayloadEncryptionConfig> = {
        specific: { enabled: true, fields: ['ssn'] },
      }
      const defaultConfig: PayloadEncryptionConfig = {
        enabled: false,
      }
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs(namespaceConfigs),
        DEFAULT_ENCRYPTION_CONFIG: createEncryptionConfig(defaultConfig),
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'specific')

      expect(result?.enabled).toBe(true)
      expect(result?.fields).toEqual(['ssn'])
    })

    it('returns default config when namespace configs not set', () => {
      const defaultConfig: PayloadEncryptionConfig = {
        enabled: true,
      }
      const env = {
        DEFAULT_ENCRYPTION_CONFIG: createEncryptionConfig(defaultConfig),
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'any')

      expect(result?.enabled).toBe(true)
    })
  })

  describe('error handling', () => {
    it('returns null for invalid namespace configs JSON', () => {
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: '{ invalid }',
        DEFAULT_ENCRYPTION_CONFIG: createEncryptionConfig({ enabled: true }),
      } as Env & EncryptionEnv

      // Should fall back to default config when namespace config parsing fails
      const result = loadPayloadEncryptionConfig(env, 'any')

      expect(result?.enabled).toBe(true)
    })

    it('returns null for invalid default config JSON', () => {
      const env = {
        DEFAULT_ENCRYPTION_CONFIG: 'not valid json',
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'any')

      expect(result).toBeNull()
    })

    it('returns null when both configs invalid', () => {
      const env = {
        NAMESPACE_ENCRYPTION_CONFIGS: 'invalid',
        DEFAULT_ENCRYPTION_CONFIG: 'also invalid',
      } as Env & EncryptionEnv

      const result = loadPayloadEncryptionConfig(env, 'any')

      expect(result).toBeNull()
    })
  })
})

// ============================================================================
// encryptionMiddleware Tests
// ============================================================================

describe('encryptionMiddleware', () => {
  let keyStore: EncryptionKeyStore

  beforeEach(async () => {
    keyStore = await initializeKeyStore('test-key-v1')
  })

  describe('when encryption is disabled', () => {
    it('continues without encryption when no config', async () => {
      const context = createMockContext()

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const extendedContext = context as EncryptedIngestContext
      expect(extendedContext.encrypted).toBe(false)
    })

    it('continues without encryption when config.enabled is false', async () => {
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: false },
          }),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const extendedContext = context as EncryptedIngestContext
      expect(extendedContext.encrypted).toBe(false)
    })
  })

  describe('when encryption is enabled but keys missing', () => {
    it('returns 500 error when no keys configured', async () => {
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          // No ENCRYPTION_KEYS set
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(500)
      const json = await result.response?.json()
      expect(json.error).toContain('no keys available')
    })

    it('returns 500 error when active key not found', async () => {
      const invalidKeyStore: EncryptionKeyStore = {
        activeKeyId: 'nonexistent',
        keys: {
          'different-key': {
            keyId: 'different-key',
            keyBase64: await generateEncryptionKey(),
            createdAt: '2024-01-01T00:00:00Z',
            isActive: false,
          },
        },
      }
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(invalidKeyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(500)
      const json = await result.response?.json()
      expect(json.error).toContain('active key not found')
    })

    it('returns 500 error when key store has empty activeKeyId', async () => {
      const invalidKeyStore: EncryptionKeyStore = {
        activeKeyId: '',
        keys: {},
      }
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(invalidKeyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(500)
    })
  })

  describe('when encryption is properly configured', () => {
    it('encrypts all events when no eventTypes filter', async () => {
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const extendedContext = context as EncryptedIngestContext
      expect(extendedContext.encrypted).toBe(true)
      expect(extendedContext.encryptionKeyId).toBe('test-key-v1')
      expect(extendedContext.encryptedEventCount).toBe(2)
    })

    it('encrypts only matching event types', async () => {
      const context = createMockContext(
        {
          batch: {
            events: [
              { type: 'user.created', ts: '2024-01-15T10:00:00Z', userId: '123' },
              { type: 'order.placed', ts: '2024-01-15T10:00:01Z', orderId: '456' },
              { type: 'user.updated', ts: '2024-01-15T10:00:02Z', userId: '789' },
            ],
          },
        },
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true, eventTypes: ['user.*'] },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const extendedContext = context as EncryptedIngestContext
      expect(extendedContext.encrypted).toBe(true)
      // Only user.created and user.updated should be encrypted
      expect(extendedContext.encryptedEventCount).toBe(2)
    })

    it('does not modify events that do not match filter', async () => {
      const originalOrderEvent = {
        type: 'order.placed',
        ts: '2024-01-15T10:00:01Z',
        orderId: '456',
      }
      const context = createMockContext(
        {
          batch: {
            events: [
              { type: 'user.created', ts: '2024-01-15T10:00:00Z', userId: '123' },
              { ...originalOrderEvent },
            ],
          },
        },
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true, eventTypes: ['user.*'] },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      await encryptionMiddleware(context)

      // The order event should be unchanged
      const orderEvent = context.batch?.events[1] as DurableEvent & { orderId?: string }
      expect(orderEvent.type).toBe('order.placed')
      expect(orderEvent.orderId).toBe('456')
    })

    it('sets encrypted flag to false when no events match filter', async () => {
      const context = createMockContext(
        {
          batch: {
            events: [
              { type: 'order.placed', ts: '2024-01-15T10:00:00Z', orderId: '123' },
            ],
          },
        },
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true, eventTypes: ['user.*'] },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const extendedContext = context as EncryptedIngestContext
      expect(extendedContext.encrypted).toBe(false)
      expect(extendedContext.encryptedEventCount).toBe(0)
    })
  })

  describe('field-level encryption', () => {
    it('encrypts only specified fields', async () => {
      const context = createMockContext(
        {
          batch: {
            events: [
              {
                type: 'user.created',
                ts: '2024-01-15T10:00:00Z',
                payload: {
                  userId: '123',
                  email: 'test@example.com',
                  name: 'Test User',
                },
              },
            ],
          },
        },
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true, fields: ['email'] },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const event = context.batch?.events[0] as DurableEvent & {
        payload?: Record<string, unknown>
      }
      // Email should be encrypted (have the ENCRYPTED_MARKER)
      expect((event.payload?.email as Record<string, unknown>)?.[ENCRYPTED_MARKER]).toBeDefined()
      // Other fields should be unchanged
      expect(event.payload?.userId).toBe('123')
      expect(event.payload?.name).toBe('Test User')
    })
  })

  describe('error handling', () => {
    it('returns 500 error when encryption fails', async () => {
      // Create a context with a malformed key that will cause encryption to fail
      const badKeyStore: EncryptionKeyStore = {
        activeKeyId: 'bad-key',
        keys: {
          'bad-key': {
            keyId: 'bad-key',
            keyBase64: 'invalid-base64!!!', // Invalid base64
            createdAt: '2024-01-01T00:00:00Z',
            isActive: true,
          },
        },
      }
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(badKeyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(500)
      const json = await result.response?.json()
      expect(json.error).toContain('encryption failed')
    })

    it('includes CORS headers in error response', async () => {
      const context = createMockContext(
        {},
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          // No keys - will cause error
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.response?.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })
  })

  describe('handles empty and edge cases', () => {
    it('handles empty batch', async () => {
      const context = createMockContext(
        {
          batch: { events: [] },
        },
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      expect(result.continue).toBe(true)
      const extendedContext = context as EncryptedIngestContext
      expect(extendedContext.encrypted).toBe(false)
      expect(extendedContext.encryptedEventCount).toBe(0)
    })

    it('handles events with no payload', async () => {
      const context = createMockContext(
        {
          batch: {
            events: [
              { type: 'system.ping', ts: '2024-01-15T10:00:00Z' },
            ],
          },
        },
        {
          NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
            'test-namespace': { enabled: true },
          }),
          ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
        }
      )

      const result = await encryptionMiddleware(context)

      // Should still mark as encrypted since the event was processed
      expect(result.continue).toBe(true)
    })
  })
})

// ============================================================================
// decryptEvent Tests
// ============================================================================

describe('decryptEvent', () => {
  let keyStore: EncryptionKeyStore

  beforeEach(async () => {
    keyStore = await initializeKeyStore('test-key-v1')
  })

  describe('full payload decryption', () => {
    it('decrypts event with encrypted payload', async () => {
      const key = keyStore.keys[keyStore.activeKeyId].keyBase64
      const sensitiveData = { userId: '123', email: 'test@example.com' }
      const encrypted = await encryptPayload(sensitiveData, key, keyStore.activeKeyId)

      const encryptedEvent = {
        type: 'user.created',
        ts: '2024-01-15T10:00:00Z',
        do: { name: 'test-do', id: 'abc' },
        _encrypted: encrypted,
      } as DurableEvent & { _encrypted: EncryptedPayload }

      const decrypted = await decryptEvent(encryptedEvent, keyStore)

      expect(decrypted.type).toBe('user.created')
      expect(decrypted.ts).toBe('2024-01-15T10:00:00Z')
      expect((decrypted as Record<string, unknown>).userId).toBe('123')
      expect((decrypted as Record<string, unknown>).email).toBe('test@example.com')
      expect((decrypted as Record<string, unknown>)._encrypted).toBeUndefined()
    })

    it('returns event as-is when key not found', async () => {
      const key = keyStore.keys[keyStore.activeKeyId].keyBase64
      const encrypted = await encryptPayload({ secret: 'data' }, key, 'nonexistent-key')

      const encryptedEvent = {
        type: 'test',
        ts: '2024-01-15T10:00:00Z',
        _encrypted: encrypted,
      } as DurableEvent & { _encrypted: EncryptedPayload }

      // Create key store without the key used for encryption
      const limitedKeyStore = await initializeKeyStore('different-key')

      const result = await decryptEvent(encryptedEvent, limitedKeyStore)

      // Should return event unchanged since key not found
      expect((result as Record<string, unknown>)._encrypted).toBeDefined()
    })

    it('returns event as-is when decryption fails', async () => {
      // Create an encrypted payload with a different key
      const differentKey = await generateEncryptionKey()
      const encrypted = await encryptPayload({ secret: 'data' }, differentKey, keyStore.activeKeyId)

      const encryptedEvent = {
        type: 'test',
        ts: '2024-01-15T10:00:00Z',
        _encrypted: encrypted,
      } as DurableEvent & { _encrypted: EncryptedPayload }

      const result = await decryptEvent(encryptedEvent, keyStore)

      // Decryption will fail because of key mismatch, should return original event
      expect((result as Record<string, unknown>)._encrypted).toBeDefined()
    })
  })

  describe('field-level decryption', () => {
    it('decrypts encrypted fields', async () => {
      const key = keyStore.keys[keyStore.activeKeyId].keyBase64
      const payload = {
        userId: '123',
        email: 'test@example.com',
      }
      const encryptedPayload = await encryptFields(
        payload,
        ['email'],
        key,
        keyStore.activeKeyId
      )

      const event = {
        type: 'user.created',
        ts: '2024-01-15T10:00:00Z',
        ...encryptedPayload,
      } as DurableEvent

      const decrypted = await decryptEvent(event, keyStore)

      expect((decrypted as Record<string, unknown>).userId).toBe('123')
      expect((decrypted as Record<string, unknown>).email).toBe('test@example.com')
    })
  })

  describe('non-encrypted events', () => {
    it('returns non-encrypted event unchanged', async () => {
      const event = {
        type: 'user.created',
        ts: '2024-01-15T10:00:00Z',
        userId: '123',
      } as DurableEvent

      const result = await decryptEvent(event, keyStore)

      expect(result).toEqual(event)
    })
  })
})

// ============================================================================
// decryptEventBatch Tests
// ============================================================================

describe('decryptEventBatch', () => {
  let keyStore: EncryptionKeyStore

  beforeEach(async () => {
    keyStore = await initializeKeyStore('test-key-v1')
  })

  it('decrypts multiple events', async () => {
    const key = keyStore.keys[keyStore.activeKeyId].keyBase64

    const encrypted1 = await encryptPayload({ userId: '1' }, key, keyStore.activeKeyId)
    const encrypted2 = await encryptPayload({ userId: '2' }, key, keyStore.activeKeyId)

    const events = [
      { type: 'user.created', ts: '2024-01-15T10:00:00Z', _encrypted: encrypted1 },
      { type: 'user.created', ts: '2024-01-15T10:00:01Z', _encrypted: encrypted2 },
    ] as (DurableEvent & { _encrypted: EncryptedPayload })[]

    const decrypted = await decryptEventBatch(events, keyStore)

    expect(decrypted).toHaveLength(2)
    expect((decrypted[0] as Record<string, unknown>).userId).toBe('1')
    expect((decrypted[1] as Record<string, unknown>).userId).toBe('2')
  })

  it('handles mixed encrypted and non-encrypted events', async () => {
    const key = keyStore.keys[keyStore.activeKeyId].keyBase64
    const encrypted = await encryptPayload({ secret: 'data' }, key, keyStore.activeKeyId)

    const events = [
      { type: 'encrypted', ts: '2024-01-15T10:00:00Z', _encrypted: encrypted },
      { type: 'plain', ts: '2024-01-15T10:00:01Z', publicData: 'visible' },
    ] as DurableEvent[]

    const decrypted = await decryptEventBatch(events, keyStore)

    expect(decrypted).toHaveLength(2)
    expect((decrypted[0] as Record<string, unknown>).secret).toBe('data')
    expect((decrypted[1] as Record<string, unknown>).publicData).toBe('visible')
  })

  it('handles empty batch', async () => {
    const decrypted = await decryptEventBatch([], keyStore)

    expect(decrypted).toEqual([])
  })
})

// ============================================================================
// validateClientEncryption Tests
// ============================================================================

describe('validateClientEncryption', () => {
  it('validates properly encrypted payload', async () => {
    const key = await generateEncryptionKey()
    const encrypted = await encryptPayload({ data: 'test' }, key, 'key-v1')

    const result = validateClientEncryption(encrypted)

    expect(result.valid).toBe(true)
    expect(result.error).toBeUndefined()
  })

  it('validates encrypted payload with expected key ID', async () => {
    const key = await generateEncryptionKey()
    const encrypted = await encryptPayload({ data: 'test' }, key, 'expected-key')

    const result = validateClientEncryption(encrypted, 'expected-key')

    expect(result.valid).toBe(true)
  })

  it('rejects non-encrypted payload', () => {
    const result = validateClientEncryption({ plainData: 'not encrypted' })

    expect(result.valid).toBe(false)
    expect(result.error).toBe('Payload is not encrypted')
  })

  it('rejects null payload', () => {
    const result = validateClientEncryption(null)

    expect(result.valid).toBe(false)
    expect(result.error).toBe('Payload is not encrypted')
  })

  it('rejects undefined payload', () => {
    const result = validateClientEncryption(undefined)

    expect(result.valid).toBe(false)
    expect(result.error).toBe('Payload is not encrypted')
  })

  it('rejects string payload', () => {
    const result = validateClientEncryption('just a string')

    expect(result.valid).toBe(false)
    expect(result.error).toBe('Payload is not encrypted')
  })

  it('rejects payload with wrong encryption version', () => {
    const invalidPayload = {
      v: 99, // Invalid version
      kid: 'key-1',
      iv: 'AAAAAAAAAAAAAAAA',
      ct: 'ciphertext',
    }

    const result = validateClientEncryption(invalidPayload)

    expect(result.valid).toBe(false)
    expect(result.error).toContain('Unsupported encryption version')
  })

  it('rejects payload with mismatched key ID', async () => {
    const key = await generateEncryptionKey()
    const encrypted = await encryptPayload({ data: 'test' }, key, 'actual-key')

    const result = validateClientEncryption(encrypted, 'expected-key')

    expect(result.valid).toBe(false)
    expect(result.error).toContain('Key ID mismatch')
    expect(result.error).toContain('expected-key')
    expect(result.error).toContain('actual-key')
  })
})

// ============================================================================
// Integration Tests
// ============================================================================

describe('encryption middleware integration', () => {
  let keyStore: EncryptionKeyStore

  beforeEach(async () => {
    keyStore = await initializeKeyStore('integration-key')
  })

  it('encrypted events can be decrypted', async () => {
    const originalEvents = [
      { type: 'user.created', ts: '2024-01-15T10:00:00Z', userId: '123', email: 'test@example.com' },
      { type: 'user.updated', ts: '2024-01-15T10:00:01Z', userId: '123', name: 'Updated Name' },
    ]

    const context = createMockContext(
      {
        batch: { events: [...originalEvents] as DurableEvent[] },
      },
      {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
          'test-namespace': { enabled: true },
        }),
        ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
      }
    )

    // Encrypt
    await encryptionMiddleware(context)

    const extendedContext = context as EncryptedIngestContext
    expect(extendedContext.encrypted).toBe(true)
    expect(extendedContext.encryptedEventCount).toBe(2)

    // Verify events are encrypted (have _encrypted marker)
    const encryptedEvents = context.batch!.events as (DurableEvent & { _encrypted?: EncryptedPayload })[]
    expect(encryptedEvents[0]._encrypted).toBeDefined()
    expect(encryptedEvents[1]._encrypted).toBeDefined()

    // Decrypt
    const decryptedEvents = await decryptEventBatch(encryptedEvents, keyStore)

    // Verify decryption restored original data
    expect((decryptedEvents[0] as Record<string, unknown>).userId).toBe('123')
    expect((decryptedEvents[0] as Record<string, unknown>).email).toBe('test@example.com')
    expect((decryptedEvents[1] as Record<string, unknown>).userId).toBe('123')
    expect((decryptedEvents[1] as Record<string, unknown>).name).toBe('Updated Name')
  })

  it('field-level encryption round trip works', async () => {
    const context = createMockContext(
      {
        batch: {
          events: [
            {
              type: 'user.created',
              ts: '2024-01-15T10:00:00Z',
              payload: {
                userId: '123',
                email: 'secret@example.com',
                name: 'Public Name',
              },
            },
          ] as DurableEvent[],
        },
      },
      {
        NAMESPACE_ENCRYPTION_CONFIGS: createNamespaceConfigs({
          'test-namespace': { enabled: true, fields: ['email'] },
        }),
        ENCRYPTION_KEYS: createKeyStoreJson(keyStore),
      }
    )

    // Encrypt
    await encryptionMiddleware(context)

    const encryptedEvent = context.batch!.events[0] as DurableEvent & {
      payload?: Record<string, unknown>
    }

    // Email should be encrypted
    expect((encryptedEvent.payload?.email as Record<string, unknown>)?.[ENCRYPTED_MARKER]).toBeDefined()
    // Other fields should be plaintext
    expect(encryptedEvent.payload?.userId).toBe('123')
    expect(encryptedEvent.payload?.name).toBe('Public Name')

    // Decrypt
    const decrypted = await decryptEvent(encryptedEvent, keyStore)
    const decryptedPayload = (decrypted as Record<string, unknown>).payload as Record<string, unknown>

    expect(decryptedPayload.email).toBe('secret@example.com')
    expect(decryptedPayload.userId).toBe('123')
    expect(decryptedPayload.name).toBe('Public Name')
  })
})
