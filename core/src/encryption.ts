/**
 * End-to-End Encryption for Event Payloads
 *
 * Provides AES-256-GCM encryption for sensitive event data using the Web Crypto API.
 * Designed for Cloudflare Workers runtime compatibility.
 *
 * Features:
 * - AES-256-GCM authenticated encryption
 * - Key rotation support via versioned keys
 * - Namespace/event type scoped encryption configuration
 * - Constant-time key comparison to prevent timing attacks
 *
 * @example
 * ```typescript
 * // Generate a new encryption key
 * const key = await generateEncryptionKey()
 *
 * // Encrypt payload
 * const encrypted = await encryptPayload(payload, key)
 *
 * // Decrypt payload
 * const decrypted = await decryptPayload(encrypted, key)
 * ```
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Encrypted payload structure stored in events
 */
export interface EncryptedPayload {
  /** Encryption version for future algorithm changes */
  v: 1
  /** Key version/ID for key rotation */
  kid: string
  /** Base64-encoded initialization vector (12 bytes for AES-GCM) */
  iv: string
  /** Base64-encoded ciphertext */
  ct: string
  /** Base64-encoded authentication tag (included in ciphertext for Web Crypto) */
  tag?: string | undefined
}

/**
 * Encryption key with metadata for rotation
 */
export interface EncryptionKeyInfo {
  /** Unique key identifier (e.g., "v1", "2024-01-key") */
  keyId: string
  /** Raw key material (256-bit = 32 bytes, base64 encoded for storage) */
  keyBase64: string
  /** When this key was created */
  createdAt: string
  /** When this key expires (for rotation planning) */
  expiresAt?: string | undefined
  /** Whether this key is the current active key for encryption */
  isActive: boolean
}

/**
 * Key store for managing multiple encryption keys (for rotation)
 */
export interface EncryptionKeyStore {
  /** All available keys, keyed by keyId */
  keys: Record<string, EncryptionKeyInfo>
  /** The current active key ID for encryption */
  activeKeyId: string
}

/**
 * Encryption configuration for a namespace or event type
 */
export interface PayloadEncryptionConfig {
  /** Whether encryption is enabled */
  enabled: boolean
  /** Event types to encrypt (empty = all events when enabled) */
  eventTypes?: string[] | undefined
  /** Fields within payload to encrypt (empty = entire payload) */
  fields?: string[] | undefined
  /** Key ID to use (defaults to active key) */
  keyId?: string | undefined
}

/**
 * Encryption options for the encrypt function
 */
export interface EncryptOptions {
  /** Key ID to use (for key rotation) */
  keyId?: string | undefined
}

// ============================================================================
// Constants
// ============================================================================

/** AES-GCM IV size in bytes */
const IV_SIZE = 12

/** AES key size in bits */
const KEY_SIZE = 256

/** Encryption algorithm identifier */
const ALGORITHM = 'AES-GCM'

/** Current encryption format version */
const ENCRYPTION_VERSION = 1

/** Marker to identify encrypted payloads */
export const ENCRYPTED_MARKER = '__encrypted__'

// ============================================================================
// Key Generation
// ============================================================================

/**
 * Generate a new AES-256 encryption key
 *
 * @returns Base64-encoded key material
 */
export async function generateEncryptionKey(): Promise<string> {
  const key = await crypto.subtle.generateKey(
    { name: ALGORITHM, length: KEY_SIZE },
    true, // extractable
    ['encrypt', 'decrypt']
  ) as CryptoKey

  const raw = await crypto.subtle.exportKey('raw', key) as ArrayBuffer
  return base64Encode(new Uint8Array(raw))
}

/**
 * Generate a new encryption key with metadata
 *
 * @param keyId - Unique identifier for this key
 * @param expiresInDays - Optional expiration in days
 * @returns Full key info object
 */
export async function generateEncryptionKeyInfo(
  keyId: string,
  expiresInDays?: number
): Promise<EncryptionKeyInfo> {
  const keyBase64 = await generateEncryptionKey()
  const now = new Date()

  return {
    keyId,
    keyBase64,
    createdAt: now.toISOString(),
    expiresAt: expiresInDays
      ? new Date(now.getTime() + expiresInDays * 24 * 60 * 60 * 1000).toISOString()
      : undefined,
    isActive: true,
  }
}

/**
 * Import a raw key for use with Web Crypto
 *
 * @param keyBase64 - Base64-encoded key material
 * @param usage - Key usage ('encrypt' or 'decrypt' or both)
 * @returns CryptoKey ready for use
 */
async function importKey(
  keyBase64: string,
  usage: ('encrypt' | 'decrypt')[]
): Promise<CryptoKey> {
  const raw = base64Decode(keyBase64)
  return crypto.subtle.importKey(
    'raw',
    raw,
    { name: ALGORITHM, length: KEY_SIZE },
    false, // not extractable
    usage
  )
}

// ============================================================================
// Encryption / Decryption
// ============================================================================

/**
 * Encrypt a payload using AES-256-GCM
 *
 * @param payload - The data to encrypt (will be JSON serialized)
 * @param keyBase64 - Base64-encoded encryption key
 * @param keyId - Key identifier for tracking/rotation
 * @returns Encrypted payload structure
 */
export async function encryptPayload(
  payload: unknown,
  keyBase64: string,
  keyId: string
): Promise<EncryptedPayload> {
  // Serialize payload to JSON
  const plaintext = JSON.stringify(payload)
  const plaintextBytes = new TextEncoder().encode(plaintext)

  // Generate random IV
  const iv = crypto.getRandomValues(new Uint8Array(IV_SIZE))

  // Import key for encryption
  const key = await importKey(keyBase64, ['encrypt'])

  // Encrypt with AES-GCM (includes authentication tag)
  const ciphertext = await crypto.subtle.encrypt(
    { name: ALGORITHM, iv },
    key,
    plaintextBytes
  )

  return {
    v: ENCRYPTION_VERSION,
    kid: keyId,
    iv: base64Encode(iv),
    ct: base64Encode(new Uint8Array(ciphertext)),
  }
}

/**
 * Decrypt an encrypted payload
 *
 * @param encrypted - The encrypted payload structure
 * @param keyBase64 - Base64-encoded encryption key
 * @returns Decrypted and parsed payload
 * @throws Error if decryption fails (wrong key, tampered data, etc.)
 */
export async function decryptPayload<T = unknown>(
  encrypted: EncryptedPayload,
  keyBase64: string
): Promise<T> {
  // Validate version
  if (encrypted.v !== ENCRYPTION_VERSION) {
    throw new Error(`Unsupported encryption version: ${encrypted.v}`)
  }

  // Decode IV and ciphertext
  const iv = base64Decode(encrypted.iv)
  const ciphertext = base64Decode(encrypted.ct)

  // Import key for decryption
  const key = await importKey(keyBase64, ['decrypt'])

  // Decrypt with AES-GCM (verifies authentication tag)
  const plaintext = await crypto.subtle.decrypt(
    { name: ALGORITHM, iv },
    key,
    ciphertext
  )

  // Parse and return
  const text = new TextDecoder().decode(plaintext)
  return JSON.parse(text) as T
}

/**
 * Check if a payload is encrypted
 *
 * @param payload - The payload to check
 * @returns True if payload appears to be encrypted
 */
export function isEncryptedPayload(payload: unknown): payload is EncryptedPayload {
  if (typeof payload !== 'object' || payload === null) {
    return false
  }

  const p = payload as Record<string, unknown>
  return (
    typeof p.v === 'number' &&
    typeof p.kid === 'string' &&
    typeof p.iv === 'string' &&
    typeof p.ct === 'string'
  )
}

// ============================================================================
// Selective Field Encryption
// ============================================================================

/**
 * Encrypt specific fields within a payload
 *
 * @param payload - The payload with fields to encrypt
 * @param fields - Array of field paths to encrypt (e.g., ['user.email', 'creditCard'])
 * @param keyBase64 - Base64-encoded encryption key
 * @param keyId - Key identifier
 * @returns Payload with specified fields encrypted
 */
export async function encryptFields(
  payload: Record<string, unknown>,
  fields: string[],
  keyBase64: string,
  keyId: string
): Promise<Record<string, unknown>> {
  // Deep clone to avoid mutating original
  const result = deepClone(payload)

  for (const fieldPath of fields) {
    const value = getNestedValue(payload, fieldPath)
    if (value !== undefined) {
      const encrypted = await encryptPayload(value, keyBase64, keyId)
      setNestedValue(result, fieldPath, { [ENCRYPTED_MARKER]: encrypted })
    }
  }

  return result
}

/**
 * Decrypt specific fields within a payload
 *
 * @param payload - The payload with encrypted fields
 * @param keyStore - Key store for looking up keys by ID
 * @returns Payload with fields decrypted
 */
export async function decryptFields(
  payload: Record<string, unknown>,
  keyStore: EncryptionKeyStore
): Promise<Record<string, unknown>> {
  const result = await decryptFieldsRecursive(payload, keyStore)
  return result as Record<string, unknown>
}

/**
 * Recursively decrypt encrypted fields in a payload
 */
async function decryptFieldsRecursive(
  value: unknown,
  keyStore: EncryptionKeyStore
): Promise<unknown> {
  if (value === null || value === undefined) {
    return value
  }

  if (typeof value !== 'object') {
    return value
  }

  if (Array.isArray(value)) {
    return Promise.all(value.map(v => decryptFieldsRecursive(v, keyStore)))
  }

  const obj = value as Record<string, unknown>

  // Check if this is an encrypted field marker
  if (ENCRYPTED_MARKER in obj && isEncryptedPayload(obj[ENCRYPTED_MARKER])) {
    const encrypted = obj[ENCRYPTED_MARKER] as EncryptedPayload
    const keyInfo = keyStore.keys[encrypted.kid]
    if (!keyInfo) {
      throw new Error(`Encryption key not found: ${encrypted.kid}`)
    }
    return decryptPayload(encrypted, keyInfo.keyBase64)
  }

  // Recursively process object fields
  const result: Record<string, unknown> = {}
  for (const [key, val] of Object.entries(obj)) {
    result[key] = await decryptFieldsRecursive(val, keyStore)
  }
  return result
}

// ============================================================================
// Key Rotation
// ============================================================================

/**
 * Rotate encryption key - add new key and mark as active
 *
 * @param keyStore - Existing key store
 * @param newKeyId - ID for the new key
 * @param expiresInDays - Optional expiration for the new key
 * @returns Updated key store with new active key
 */
export async function rotateEncryptionKey(
  keyStore: EncryptionKeyStore,
  newKeyId: string,
  expiresInDays?: number
): Promise<EncryptionKeyStore> {
  // Generate new key
  const newKeyInfo = await generateEncryptionKeyInfo(newKeyId, expiresInDays)

  // Mark old active key as inactive
  const updatedKeys: Record<string, EncryptionKeyInfo> = { ...keyStore.keys }
  const oldActiveKey = keyStore.activeKeyId ? updatedKeys[keyStore.activeKeyId] : undefined
  if (oldActiveKey) {
    updatedKeys[keyStore.activeKeyId] = {
      ...oldActiveKey,
      isActive: false,
    }
  }

  // Add new key
  updatedKeys[newKeyId] = newKeyInfo

  return {
    keys: updatedKeys,
    activeKeyId: newKeyId,
  }
}

/**
 * Re-encrypt a payload with a new key (for key rotation migration)
 *
 * @param encrypted - Currently encrypted payload
 * @param keyStore - Key store with both old and new keys
 * @param newKeyId - ID of the new key to encrypt with
 * @returns Re-encrypted payload
 */
export async function reencryptPayload(
  encrypted: EncryptedPayload,
  keyStore: EncryptionKeyStore,
  newKeyId: string
): Promise<EncryptedPayload> {
  // Get old key
  const oldKeyInfo = keyStore.keys[encrypted.kid]
  if (!oldKeyInfo) {
    throw new Error(`Old encryption key not found: ${encrypted.kid}`)
  }

  // Get new key
  const newKeyInfo = keyStore.keys[newKeyId]
  if (!newKeyInfo) {
    throw new Error(`New encryption key not found: ${newKeyId}`)
  }

  // Decrypt with old key
  const plaintext = await decryptPayload(encrypted, oldKeyInfo.keyBase64)

  // Re-encrypt with new key
  return encryptPayload(plaintext, newKeyInfo.keyBase64, newKeyId)
}

// ============================================================================
// Configuration Helpers
// ============================================================================

/**
 * Check if an event should be encrypted based on configuration
 *
 * @param eventType - The event type
 * @param config - Encryption configuration
 * @returns True if the event should be encrypted
 */
export function shouldEncryptEvent(
  eventType: string,
  config: PayloadEncryptionConfig
): boolean {
  if (!config.enabled) {
    return false
  }

  // If no event types specified, encrypt all
  if (!config.eventTypes || config.eventTypes.length === 0) {
    return true
  }

  // Check if event type matches any pattern
  return config.eventTypes.some(pattern => matchEventType(pattern, eventType))
}

/**
 * Match event type against a pattern (supports * wildcard)
 */
function matchEventType(pattern: string, eventType: string): boolean {
  if (pattern === '*') return true
  if (!pattern.includes('*')) return pattern === eventType

  const regex = new RegExp(
    '^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '[^.]*') + '$'
  )
  return regex.test(eventType)
}

/**
 * Create a new empty key store
 */
export function createKeyStore(): EncryptionKeyStore {
  return {
    keys: {},
    activeKeyId: '',
  }
}

/**
 * Initialize a key store with a first key
 *
 * @param keyId - ID for the initial key
 * @param expiresInDays - Optional expiration
 * @returns Initialized key store
 */
export async function initializeKeyStore(
  keyId: string,
  expiresInDays?: number
): Promise<EncryptionKeyStore> {
  const keyInfo = await generateEncryptionKeyInfo(keyId, expiresInDays)

  return {
    keys: { [keyId]: keyInfo },
    activeKeyId: keyId,
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Deep clone an object (simple implementation for JSON-serializable data)
 */
function deepClone<T>(obj: T): T {
  if (obj === null || typeof obj !== 'object') {
    return obj
  }
  if (Array.isArray(obj)) {
    return obj.map(item => deepClone(item)) as unknown as T
  }
  const result: Record<string, unknown> = {}
  for (const key of Object.keys(obj)) {
    result[key] = deepClone((obj as Record<string, unknown>)[key])
  }
  return result as T
}

/**
 * Base64 encode a Uint8Array
 */
function base64Encode(bytes: Uint8Array): string {
  let binary = ''
  for (let i = 0; i < bytes.length; i++) {
    const byte = bytes[i]
    if (byte !== undefined) {
      binary += String.fromCharCode(byte)
    }
  }
  return btoa(binary)
}

/**
 * Base64 decode to Uint8Array
 */
function base64Decode(str: string): Uint8Array {
  const binary = atob(str)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

/**
 * Get a nested value from an object using dot notation
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]
    if (part === undefined || current === null || current === undefined || typeof current !== 'object') {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }
  return current
}

/**
 * Set a nested value in an object using dot notation
 */
function setNestedValue(
  obj: Record<string, unknown>,
  path: string,
  value: unknown
): void {
  const parts = path.split('.')
  let current: Record<string, unknown> = obj

  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]
    if (part === undefined) continue
    if (!(part in current) || typeof current[part] !== 'object') {
      current[part] = {}
    }
    current = current[part] as Record<string, unknown>
  }

  const lastPart = parts[parts.length - 1]
  if (lastPart !== undefined) {
    current[lastPart] = value
  }
}

/**
 * Constant-time string comparison to prevent timing attacks
 */
export function constantTimeCompare(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false
  }

  let result = 0
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i)
  }
  return result === 0
}
