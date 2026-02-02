/**
 * Encryption Middleware for Event Ingest
 *
 * Provides optional end-to-end encryption for event payloads during ingestion.
 * Encryption is opt-in per namespace or event type.
 *
 * @example
 * ```typescript
 * // In the ingest middleware chain:
 * const pipeline = composeMiddleware([
 *   parseJsonMiddleware,
 *   validateBatchMiddleware,
 *   encryptionMiddleware,  // Add encryption before storage
 *   // ... rest of pipeline
 * ])
 * ```
 */

import type { DurableEvent, EventBatch, BaseEvent } from '@dotdo/events'
import type { IngestContext, IngestMiddleware, MiddlewareResult } from './types'
import type { Env } from '../../env'
import { corsHeaders } from '../../utils'
import { createLogger } from '../../logger'
import {
  encryptPayload,
  encryptFields,
  shouldEncryptEvent,
  isEncryptedPayload,
  type PayloadEncryptionConfig,
  type EncryptionKeyStore,
  type EncryptionKeyInfo,
  type EncryptedPayload,
} from '../../../core/src/encryption'

const log = createLogger({ component: 'encryption-middleware' })

// ============================================================================
// Encrypted Event Types
// ============================================================================

/**
 * Base structure for an encrypted event - core fields preserved, payload encrypted.
 * Used as an intermediate type to avoid double casts.
 */
interface EncryptedEventBase {
  type: string
  ts: string
  do?: BaseEvent['do']
  _encrypted: EncryptedPayload
}

/**
 * Event with encrypted payload - union compatible with DurableEvent for storage.
 * The _encrypted field contains the encrypted sensitive data.
 */
type EncryptedEvent = EncryptedEventBase & Omit<DurableEvent, 'type' | 'ts' | 'do'>

/**
 * Event that may have either encrypted or decrypted payload.
 */
type MaybeEncryptedEvent = DurableEvent | (DurableEvent & { _encrypted?: EncryptedPayload })

// ============================================================================
// Type Validators
// ============================================================================

/**
 * Type guard for EncryptionKeyInfo
 */
function isEncryptionKeyInfo(value: unknown): value is EncryptionKeyInfo {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  return (
    typeof v.keyId === 'string' &&
    typeof v.keyBase64 === 'string' &&
    typeof v.createdAt === 'string' &&
    typeof v.isActive === 'boolean' &&
    (v.expiresAt === undefined || typeof v.expiresAt === 'string')
  )
}

/**
 * Type guard for EncryptionKeyStore
 */
function isEncryptionKeyStore(value: unknown): value is EncryptionKeyStore {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  if (typeof v.activeKeyId !== 'string') return false
  if (typeof v.keys !== 'object' || v.keys === null) return false

  // Validate each key in the store
  const keys = v.keys as Record<string, unknown>
  for (const keyInfo of Object.values(keys)) {
    if (!isEncryptionKeyInfo(keyInfo)) return false
  }
  return true
}

/**
 * Type guard for PayloadEncryptionConfig
 */
function isPayloadEncryptionConfig(value: unknown): value is PayloadEncryptionConfig {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  if (typeof v.enabled !== 'boolean') return false

  // Optional fields
  if (v.eventTypes !== undefined) {
    if (!Array.isArray(v.eventTypes)) return false
    if (!v.eventTypes.every((t) => typeof t === 'string')) return false
  }
  if (v.fields !== undefined) {
    if (!Array.isArray(v.fields)) return false
    if (!v.fields.every((f) => typeof f === 'string')) return false
  }
  if (v.keyId !== undefined && typeof v.keyId !== 'string') return false

  return true
}

/**
 * Type guard for namespace encryption configs map
 */
function isNamespaceEncryptionConfigsMap(value: unknown): value is Record<string, PayloadEncryptionConfig> {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>

  for (const config of Object.values(v)) {
    if (!isPayloadEncryptionConfig(config)) return false
  }
  return true
}

// ============================================================================
// Types
// ============================================================================

/**
 * Extended Env interface for encryption bindings
 */
export interface EncryptionEnv {
  /** JSON-encoded EncryptionKeyStore */
  ENCRYPTION_KEYS?: string
  /** JSON-encoded namespace encryption configs: { "namespace": PayloadEncryptionConfig } */
  NAMESPACE_ENCRYPTION_CONFIGS?: string
  /** Default encryption config (JSON) - applied when namespace has no specific config */
  DEFAULT_ENCRYPTION_CONFIG?: string
}

/**
 * Extended IngestContext with encryption state
 */
export interface EncryptedIngestContext extends IngestContext {
  /** Whether encryption was applied to this batch */
  encrypted?: boolean
  /** Key ID used for encryption (if any) */
  encryptionKeyId?: string
  /** Number of events encrypted */
  encryptedEventCount?: number
}

// ============================================================================
// Configuration Loading
// ============================================================================

/**
 * Load encryption key store from environment
 */
export function loadKeyStore(env: Env & EncryptionEnv): EncryptionKeyStore | null {
  if (!env.ENCRYPTION_KEYS) {
    return null
  }

  try {
    const parsed: unknown = JSON.parse(env.ENCRYPTION_KEYS)
    if (!isEncryptionKeyStore(parsed)) {
      log.error('ENCRYPTION_KEYS failed validation: invalid structure')
      return null
    }
    return parsed
  } catch (error) {
    log.error('Failed to parse ENCRYPTION_KEYS', { error })
    return null
  }
}

/**
 * Load encryption configuration for a namespace
 * @alias loadEncryptionConfig - exported for backwards compatibility
 */
export function loadPayloadEncryptionConfig(
  env: Env & EncryptionEnv,
  namespace: string
): PayloadEncryptionConfig | null {
  // Check namespace-specific config first
  if (env.NAMESPACE_ENCRYPTION_CONFIGS) {
    try {
      const parsed: unknown = JSON.parse(env.NAMESPACE_ENCRYPTION_CONFIGS)
      if (!isNamespaceEncryptionConfigsMap(parsed)) {
        log.error('NAMESPACE_ENCRYPTION_CONFIGS failed validation: invalid structure')
      } else if (parsed[namespace]) {
        return parsed[namespace]
      }
    } catch (error) {
      log.error('Failed to parse NAMESPACE_ENCRYPTION_CONFIGS', { error })
    }
  }

  // Fall back to default config
  if (env.DEFAULT_ENCRYPTION_CONFIG) {
    try {
      const parsed: unknown = JSON.parse(env.DEFAULT_ENCRYPTION_CONFIG)
      if (!isPayloadEncryptionConfig(parsed)) {
        log.error('DEFAULT_ENCRYPTION_CONFIG failed validation: invalid structure')
        return null
      }
      return parsed
    } catch (error) {
      log.error('Failed to parse DEFAULT_ENCRYPTION_CONFIG', { error })
    }
  }

  return null
}

// ============================================================================
// Encryption Middleware
// ============================================================================

/**
 * Encryption middleware for ingest pipeline
 *
 * Encrypts event payloads based on namespace/event type configuration.
 * Must be placed after validation but before storage.
 */
export const encryptionMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const extendedEnv = context.env as Env & EncryptionEnv
  const encryptedContext = context as EncryptedIngestContext

  // Load encryption config for this namespace
  const config = loadPayloadEncryptionConfig(extendedEnv, context.tenant.namespace)

  // If no config or encryption disabled, pass through
  if (!config || !config.enabled) {
    encryptedContext.encrypted = false
    return { continue: true }
  }

  // Load key store
  const keyStore = loadKeyStore(extendedEnv)
  if (!keyStore || !keyStore.activeKeyId) {
    log.error('Encryption enabled but no keys configured', {
      namespace: context.tenant.namespace,
    })
    return {
      continue: false,
      response: Response.json(
        { error: 'Encryption configuration error: no keys available' },
        { status: 500, headers: corsHeaders() }
      ),
    }
  }

  const activeKey = keyStore.keys[keyStore.activeKeyId]
  if (!activeKey) {
    log.error('Active key not found in key store', {
      activeKeyId: keyStore.activeKeyId,
    })
    return {
      continue: false,
      response: Response.json(
        { error: 'Encryption configuration error: active key not found' },
        { status: 500, headers: corsHeaders() }
      ),
    }
  }

  // Process events and encrypt as needed
  const batch = context.batch!
  const encryptedEvents: DurableEvent[] = []
  let encryptedCount = 0

  try {
    for (const event of batch.events) {
      if (shouldEncryptEvent(event.type, config)) {
        const encryptedEvent = await encryptEvent(event, activeKey.keyBase64, keyStore.activeKeyId, config)
        encryptedEvents.push(encryptedEvent)
        encryptedCount++
      } else {
        encryptedEvents.push(event)
      }
    }

    // Update batch with encrypted events
    context.batch = { events: encryptedEvents }

    // Track encryption state
    encryptedContext.encrypted = encryptedCount > 0
    encryptedContext.encryptionKeyId = keyStore.activeKeyId
    encryptedContext.encryptedEventCount = encryptedCount

    log.info('Events encrypted', {
      namespace: context.tenant.namespace,
      totalEvents: batch.events.length,
      encryptedEvents: encryptedCount,
      keyId: keyStore.activeKeyId,
    })

    return { continue: true }
  } catch (error) {
    log.error('Encryption failed', {
      namespace: context.tenant.namespace,
      error: error instanceof Error ? error.message : String(error),
    })
    return {
      continue: false,
      response: Response.json(
        { error: 'Event encryption failed' },
        { status: 500, headers: corsHeaders() }
      ),
    }
  }
}

/**
 * Encrypt a single event's payload.
 * Returns either the original event (for field-level encryption) or
 * an event with `_encrypted` field (for full payload encryption).
 */
async function encryptEvent(
  event: DurableEvent,
  keyBase64: string,
  keyId: string,
  config: PayloadEncryptionConfig
): Promise<MaybeEncryptedEvent> {
  // Determine what to encrypt
  const eventWithPayload = event as DurableEvent & { payload?: unknown; data?: unknown }

  // For field-level encryption
  if (config.fields && config.fields.length > 0) {
    // Only encrypt specified fields
    const payload = eventWithPayload.payload ?? eventWithPayload.data ?? {}
    if (typeof payload === 'object' && payload !== null) {
      const encryptedPayload = await encryptFields(
        payload as Record<string, unknown>,
        config.fields,
        keyBase64,
        keyId
      )
      return {
        ...event,
        ...(eventWithPayload.payload !== undefined ? { payload: encryptedPayload } : {}),
        ...(eventWithPayload.data !== undefined ? { data: encryptedPayload } : {}),
      } as DurableEvent
    }
    return event
  }

  // Full payload encryption
  // Extract the sensitive data (everything except core event fields)
  const { type, ts, do: doInfo, ...sensitiveData } = event as DurableEvent & Record<string, unknown>

  // If there's no sensitive data, no need to encrypt
  if (Object.keys(sensitiveData).length === 0) {
    return event
  }

  // Encrypt the sensitive portion
  const encrypted = await encryptPayload(sensitiveData, keyBase64, keyId)

  // Return event with encrypted payload marker
  const encryptedEvent: EncryptedEventBase = {
    type,
    ts,
    do: doInfo,
    _encrypted: encrypted,
  }
  return encryptedEvent as MaybeEncryptedEvent
}

// ============================================================================
// Decryption Helpers (for retrieval)
// ============================================================================

/**
 * Decrypt an event's encrypted payload
 *
 * @param event - Event that may have encrypted data
 * @param keyStore - Key store for looking up decryption keys
 * @returns Decrypted event
 */
export async function decryptEvent(
  event: DurableEvent & { _encrypted?: EncryptedPayload },
  keyStore: EncryptionKeyStore
): Promise<DurableEvent> {
  // Check for full payload encryption
  if (event._encrypted && isEncryptedPayload(event._encrypted)) {
    const encrypted = event._encrypted
    const keyInfo = keyStore.keys[encrypted.kid]

    if (!keyInfo) {
      log.warn('Decryption key not found', { keyId: encrypted.kid })
      // Return event as-is if we can't decrypt
      return event
    }

    try {
      const { decryptPayload } = await import('../../../core/src/encryption')
      const decrypted = await decryptPayload<Record<string, unknown>>(encrypted, keyInfo.keyBase64)

      // Reconstruct the full event
      const { _encrypted, ...rest } = event
      return {
        ...rest,
        ...decrypted,
      } as DurableEvent
    } catch (error) {
      log.error('Decryption failed', {
        keyId: encrypted.kid,
        error: error instanceof Error ? error.message : String(error),
      })
      return event
    }
  }

  // Check for field-level encryption (recursive)
  // Cast event to Record for field decryption - DurableEvent fields are compatible
  const { decryptFields } = await import('../../../core/src/encryption')
  const eventAsRecord = event as DurableEvent & Record<string, unknown>
  const decrypted = await decryptFields(eventAsRecord, keyStore)
  return decrypted as DurableEvent
}

/**
 * Decrypt a batch of events
 *
 * @param events - Array of events that may have encrypted data
 * @param keyStore - Key store for decryption
 * @returns Array of decrypted events
 */
export async function decryptEventBatch(
  events: DurableEvent[],
  keyStore: EncryptionKeyStore
): Promise<DurableEvent[]> {
  return Promise.all(
    events.map(event => decryptEvent(event as DurableEvent & { _encrypted?: EncryptedPayload }, keyStore))
  )
}

// ============================================================================
// Client-Side Encryption Helpers
// ============================================================================

/**
 * Validate that a payload was encrypted client-side
 * Used when clients encrypt before sending to ensure E2E encryption
 */
export function validateClientEncryption(
  payload: unknown,
  expectedKeyId?: string
): { valid: boolean; error?: string } {
  if (!isEncryptedPayload(payload)) {
    return { valid: false, error: 'Payload is not encrypted' }
  }

  const encrypted = payload as EncryptedPayload

  if (encrypted.v !== 1) {
    return { valid: false, error: `Unsupported encryption version: ${encrypted.v}` }
  }

  if (expectedKeyId && encrypted.kid !== expectedKeyId) {
    return {
      valid: false,
      error: `Key ID mismatch: expected ${expectedKeyId}, got ${encrypted.kid}`,
    }
  }

  return { valid: true }
}

/**
 * Alias for loadPayloadEncryptionConfig - exported for backwards compatibility
 */
export const loadEncryptionConfig = loadPayloadEncryptionConfig
