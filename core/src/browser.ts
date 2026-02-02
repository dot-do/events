/// <reference lib="dom" />
/**
 * @dotdo/events Browser SDK
 * Minimal analytics SDK for browser-side event tracking
 */

/** Configuration for localStorage encryption */
export interface EncryptionConfig {
  /** Enable encryption for localStorage data (default: false) */
  enabled: boolean
  /**
   * Encryption key - if not provided, a key will be generated and stored.
   * For better security, provide a key derived from user credentials or server.
   * Must be a 256-bit key (32 bytes) encoded as base64.
   */
  key?: string
  /**
   * Key storage method when key is auto-generated:
   * - 'localStorage': Store in localStorage (default, less secure but persistent)
   * - 'sessionStorage': Store in sessionStorage (more secure but lost on close)
   * - 'memory': Keep only in memory (most secure but lost on refresh)
   */
  keyStorage?: 'localStorage' | 'sessionStorage' | 'memory'
}

export interface BrowserConfig {
  /** Endpoint URL (default: /e) */
  endpoint?: string
  /** Batch size before auto-flush (default: 10) */
  batchSize?: number
  /** Flush interval in ms (default: 5000) */
  flushInterval?: number
  /** Max retry attempts for failed requests (default: 3) */
  maxRetries?: number
  /** Base delay for exponential backoff in ms (default: 1000) */
  retryDelay?: number
  /** Max events to queue for retry (default: 100) */
  maxRetryQueueSize?: number
  /** Error callback for surfacing failures to callers */
  onError?: (error: Error, events: Ev[]) => void
  /** Callback when events are successfully sent */
  onSuccess?: (events: Ev[]) => void
  /** Optional encryption configuration for localStorage data */
  encryption?: EncryptionConfig
}

interface Ev {
  type: 'page' | 'track' | 'identify'
  ts: string
  event?: string
  properties?: Record<string, unknown>
  traits?: Record<string, unknown>
  userId?: string
  anonymousId: string
  sessionId: string
  url: string
  path: string
  referrer: string
  ua: string
}

const P = 'events_'
const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 8)
const get = (k: string, s: Storage) => { try { let v = s.getItem(P + k); if (!v) s.setItem(P + k, v = uid()); return v } catch { return uid() } }

// ============================================================================
// Encryption utilities using Web Crypto API
// ============================================================================

const ALGORITHM = 'AES-GCM'
const KEY_LENGTH = 256
const IV_LENGTH = 12 // 96 bits for GCM
const KEY_STORAGE_KEY = P + 'enc_key'
const ENCRYPTED_PREFIX = 'enc:' // Prefix to identify encrypted values

/** Convert ArrayBuffer to base64 string */
function bufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer)
  let binary = ''
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i])
  }
  return btoa(binary)
}

/** Convert base64 string to ArrayBuffer */
function base64ToBuffer(base64: string): ArrayBuffer {
  const binary = atob(base64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes.buffer
}

/** Storage encryption helper using Web Crypto API */
export class StorageEncryption {
  private cryptoKey: CryptoKey | null = null
  private keyStorage: 'localStorage' | 'sessionStorage' | 'memory'
  private memoryKey: string | null = null
  private initPromise: Promise<void> | null = null

  constructor(private config: EncryptionConfig) {
    this.keyStorage = config.keyStorage ?? 'localStorage'
  }

  /** Initialize the encryption key */
  async initialize(): Promise<void> {
    // Avoid multiple concurrent initializations
    if (this.initPromise) return this.initPromise
    if (this.cryptoKey) return

    this.initPromise = this._initialize()
    await this.initPromise
    this.initPromise = null
  }

  private async _initialize(): Promise<void> {
    if (!this.isSupported()) {
      throw new Error('Web Crypto API not supported')
    }

    if (this.config.key) {
      // Use provided key
      const keyData = base64ToBuffer(this.config.key)
      this.cryptoKey = await crypto.subtle.importKey(
        'raw',
        keyData,
        { name: ALGORITHM },
        false,
        ['encrypt', 'decrypt']
      )
    } else {
      // Try to load existing key or generate new one
      await this.loadOrGenerateKey()
    }
  }

  /** Check if Web Crypto API is available */
  isSupported(): boolean {
    return typeof crypto !== 'undefined' &&
           typeof crypto.subtle !== 'undefined' &&
           typeof crypto.getRandomValues !== 'undefined'
  }

  /** Check if encryption is ready to use */
  isReady(): boolean {
    return this.cryptoKey !== null
  }

  private async loadOrGenerateKey(): Promise<void> {
    let keyBase64: string | null = null

    // Try to load existing key
    if (this.keyStorage === 'localStorage') {
      try { keyBase64 = localStorage.getItem(KEY_STORAGE_KEY) } catch { /* ignore */ }
    } else if (this.keyStorage === 'sessionStorage') {
      try { keyBase64 = sessionStorage.getItem(KEY_STORAGE_KEY) } catch { /* ignore */ }
    } else {
      keyBase64 = this.memoryKey
    }

    if (keyBase64) {
      // Import existing key
      const keyData = base64ToBuffer(keyBase64)
      this.cryptoKey = await crypto.subtle.importKey(
        'raw',
        keyData,
        { name: ALGORITHM },
        true,
        ['encrypt', 'decrypt']
      )
    } else {
      // Generate new key
      this.cryptoKey = await crypto.subtle.generateKey(
        { name: ALGORITHM, length: KEY_LENGTH },
        true,
        ['encrypt', 'decrypt']
      )

      // Export and store the key
      const rawKey = await crypto.subtle.exportKey('raw', this.cryptoKey)
      keyBase64 = bufferToBase64(rawKey)

      if (this.keyStorage === 'localStorage') {
        try { localStorage.setItem(KEY_STORAGE_KEY, keyBase64) } catch { /* ignore */ }
      } else if (this.keyStorage === 'sessionStorage') {
        try { sessionStorage.setItem(KEY_STORAGE_KEY, keyBase64) } catch { /* ignore */ }
      } else {
        this.memoryKey = keyBase64
      }
    }
  }

  /** Encrypt a string value */
  async encrypt(plaintext: string): Promise<string> {
    if (!this.cryptoKey) {
      throw new Error('Encryption not initialized')
    }

    const encoder = new TextEncoder()
    const data = encoder.encode(plaintext)

    // Generate random IV
    const iv = crypto.getRandomValues(new Uint8Array(IV_LENGTH))

    // Encrypt
    const ciphertext = await crypto.subtle.encrypt(
      { name: ALGORITHM, iv },
      this.cryptoKey,
      data
    )

    // Combine IV + ciphertext and encode as base64
    const combined = new Uint8Array(iv.length + ciphertext.byteLength)
    combined.set(iv, 0)
    combined.set(new Uint8Array(ciphertext), iv.length)

    return ENCRYPTED_PREFIX + bufferToBase64(combined.buffer)
  }

  /** Decrypt an encrypted string value */
  async decrypt(encrypted: string): Promise<string> {
    if (!this.cryptoKey) {
      throw new Error('Encryption not initialized')
    }

    // Remove prefix if present
    const data = encrypted.startsWith(ENCRYPTED_PREFIX)
      ? encrypted.slice(ENCRYPTED_PREFIX.length)
      : encrypted

    const combined = new Uint8Array(base64ToBuffer(data))

    // Extract IV and ciphertext
    const iv = combined.slice(0, IV_LENGTH)
    const ciphertext = combined.slice(IV_LENGTH)

    // Decrypt
    const decrypted = await crypto.subtle.decrypt(
      { name: ALGORITHM, iv },
      this.cryptoKey,
      ciphertext
    )

    const decoder = new TextDecoder()
    return decoder.decode(decrypted)
  }

  /** Check if a value is encrypted (has the encryption prefix) */
  isEncrypted(value: string): boolean {
    return value.startsWith(ENCRYPTED_PREFIX)
  }

  /** Get the current key as base64 (for backup/export purposes) */
  async exportKey(): Promise<string | null> {
    if (!this.cryptoKey) return null
    const rawKey = await crypto.subtle.exportKey('raw', this.cryptoKey)
    return bufferToBase64(rawKey)
  }
}

/** Pending retry batch with attempt count */
interface RetryBatch {
  events: Ev[]
  attempts: number
  nextRetry: number
}

export class EventsSDK {
  private q: Ev[] = []
  private e: string
  private b: number
  private t?: number
  private u?: string
  private a: string
  private s: string
  private maxRetries: number
  private retryDelay: number
  private maxRetryQueueSize: number
  private retryQueue: RetryBatch[] = []
  private retryTimer?: number
  private onError?: (error: Error, events: Ev[]) => void
  private onSuccess?: (events: Ev[]) => void
  private encryption?: StorageEncryption
  private encryptionReady: Promise<void> | null = null

  constructor(c: BrowserConfig = {}) {
    this.e = c.endpoint || '/e'
    this.b = c.batchSize || 10
    this.maxRetries = c.maxRetries ?? 3
    this.retryDelay = c.retryDelay ?? 1000
    this.maxRetryQueueSize = c.maxRetryQueueSize ?? 100
    this.onError = c.onError
    this.onSuccess = c.onSuccess

    // Initialize encryption if configured
    if (c.encryption?.enabled) {
      const enc = new StorageEncryption(c.encryption)
      this.encryption = enc
      this.encryptionReady = enc.initialize().catch(err => {
        // If encryption fails to initialize, fall back to unencrypted storage
        // We leave this.encryption set but it won't be ready, so isReady() will return false
        this.onError?.(new Error(`Encryption initialization failed: ${err.message}`), [])
      })
    }

    this.a = get('a', localStorage)
    this.s = get('s', sessionStorage)
    if (typeof window !== 'undefined') {
      this.t = setInterval(() => this.flush(), c.flushInterval || 5000) as unknown as number
      addEventListener('visibilitychange', () => document.visibilityState === 'hidden' && this.flush(1))
      addEventListener('pagehide', () => this.flush(1))
    }
  }

  /** Check if encryption is enabled and ready */
  get encryptionEnabled(): boolean {
    return this.encryption?.isReady() ?? false
  }

  /** Wait for encryption to be ready (if enabled) */
  async waitForEncryption(): Promise<void> {
    if (this.encryptionReady) {
      await this.encryptionReady
    }
  }

  /** Export the encryption key (for backup purposes) */
  async exportEncryptionKey(): Promise<string | null> {
    return this.encryption?.exportKey() ?? null
  }

  private add(ev: Ev) { this.q.push(ev); this.q.length >= this.b && this.flush() }
  private ctx() { return { url: location.href, path: location.pathname, referrer: document.referrer, ua: navigator.userAgent, anonymousId: this.a, sessionId: this.s, userId: this.u } }

  page(props?: Record<string, unknown>) { this.add({ type: 'page', ts: new Date().toISOString(), properties: props, ...this.ctx() }) }
  track(event: string, props?: Record<string, unknown>) { this.add({ type: 'track', ts: new Date().toISOString(), event, properties: props, ...this.ctx() }) }
  identify(userId: string, traits?: Record<string, unknown>) { this.u = userId; const c = this.ctx(); this.add({ type: 'identify', ts: new Date().toISOString(), traits, ...c, userId }) }

  /** Get count of events pending retry */
  get pendingRetryCount(): number {
    return this.retryQueue.reduce((sum, batch) => sum + batch.events.length, 0)
  }

  /** Get count of events in main queue */
  get queuedCount(): number {
    return this.q.length
  }

  private scheduleRetry() {
    if (this.retryTimer || !this.retryQueue.length) return
    const now = Date.now()
    const nextBatch = this.retryQueue[0]
    if (!nextBatch) return
    const delay = Math.max(0, nextBatch.nextRetry - now)
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      this.processRetryQueue()
    }, delay) as unknown as number
  }

  private async processRetryQueue() {
    if (!this.retryQueue.length) return

    const now = Date.now()
    const batch = this.retryQueue[0]
    if (!batch) return

    if (batch.nextRetry > now) {
      this.scheduleRetry()
      return
    }

    this.retryQueue.shift()
    await this.sendEvents(batch.events, batch.attempts)
  }

  private queueForRetry(events: Ev[], attempts: number, error: Error) {
    if (attempts >= this.maxRetries) {
      // Max retries exceeded, notify via callback and drop events
      this.onError?.(new Error(`Failed to send events after ${attempts} attempts: ${error.message}`), events)
      return
    }

    // Calculate total events in retry queue
    const currentRetryCount = this.pendingRetryCount

    // Enforce max queue size - drop oldest events if needed
    let eventsToQueue = events
    if (currentRetryCount + events.length > this.maxRetryQueueSize) {
      const available = Math.max(0, this.maxRetryQueueSize - currentRetryCount)
      if (available === 0) {
        // Queue is full, drop these events and notify
        this.onError?.(new Error(`Retry queue full, dropping ${events.length} events`), events)
        return
      }
      // Keep only as many events as we have room for (keep newest)
      const dropped = events.slice(0, events.length - available)
      eventsToQueue = events.slice(-available)
      this.onError?.(new Error(`Retry queue nearly full, dropping ${dropped.length} oldest events`), dropped)
    }

    // Exponential backoff: delay * 2^attempts (e.g., 1s, 2s, 4s)
    const delay = this.retryDelay * Math.pow(2, attempts)
    const nextRetry = Date.now() + delay

    this.retryQueue.push({ events: eventsToQueue, attempts: attempts + 1, nextRetry })
    this.scheduleRetry()
  }

  private async sendEvents(events: Ev[], attempts = 0): Promise<void> {
    const body = JSON.stringify({ events })

    try {
      const response = await fetch(this.e, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        keepalive: true
      } as globalThis.RequestInit)

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      // Success - notify callback
      this.onSuccess?.(events)
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err))
      this.queueForRetry(events, attempts, error)
    }
  }

  async flush(beacon?: number): Promise<void> {
    // Also process any pending retries
    if (!beacon) {
      this.processRetryQueue()
    }

    if (!this.q.length) return

    const events = this.q.splice(0)

    // Use sendBeacon for page unload scenarios (best effort, no retry possible)
    if (beacon && navigator.sendBeacon) {
      const body = JSON.stringify({ events })
      const success = navigator.sendBeacon(this.e, body)
      if (!success) {
        // sendBeacon failed, try to persist for next session
        this.persistFailedEvents(events)
      }
      return
    }

    // Regular flush with retry support
    await this.sendEvents(events)
  }

  private persistFailedEvents(events: Ev[]) {
    // Best effort: store failed events in localStorage for recovery on next page load
    // Note: During page unload, we can't use async encryption, so we store unencrypted
    // and encrypt on recovery if encryption is available
    try {
      const key = P + 'failed'
      const existing = localStorage.getItem(key)
      let failed: Ev[] = []

      if (existing) {
        // Handle both encrypted and unencrypted existing data
        if (existing.startsWith(ENCRYPTED_PREFIX)) {
          // Can't decrypt synchronously during page unload, so we'll need to recover separately
          // Just append to a separate key for now
          const unencryptedKey = key + '_unenc'
          const existingUnenc = localStorage.getItem(unencryptedKey)
          failed = existingUnenc ? JSON.parse(existingUnenc) : []
          const combined = [...failed, ...events].slice(-this.maxRetryQueueSize)
          localStorage.setItem(unencryptedKey, JSON.stringify(combined))
          return
        } else {
          failed = JSON.parse(existing)
        }
      }

      // Limit stored events to prevent localStorage bloat
      const combined = [...failed, ...events].slice(-this.maxRetryQueueSize)
      localStorage.setItem(key, JSON.stringify(combined))
    } catch {
      // localStorage unavailable or full, drop events
      this.onError?.(new Error('Failed to persist events to localStorage'), events)
    }
  }

  /** Recover any events that failed to send in a previous session */
  recoverPersistedEvents(): Ev[] {
    // Synchronous recovery for unencrypted data
    const recovered: Ev[] = []
    try {
      const key = P + 'failed'
      const stored = localStorage.getItem(key)
      if (stored && !stored.startsWith(ENCRYPTED_PREFIX)) {
        localStorage.removeItem(key)
        const events: Ev[] = JSON.parse(stored)
        this.q.unshift(...events)
        recovered.push(...events)
      }

      // Also check for unencrypted overflow from page unload
      const unencKey = key + '_unenc'
      const unencStored = localStorage.getItem(unencKey)
      if (unencStored) {
        localStorage.removeItem(unencKey)
        const events: Ev[] = JSON.parse(unencStored)
        this.q.unshift(...events)
        recovered.push(...events)
      }
    } catch {
      // Ignore parse errors
    }
    return recovered
  }

  /** Recover any encrypted events that failed to send in a previous session (async) */
  async recoverEncryptedEvents(): Promise<Ev[]> {
    if (!this.encryption?.isReady()) {
      return []
    }

    const recovered: Ev[] = []
    try {
      const key = P + 'failed'
      const stored = localStorage.getItem(key)
      if (stored && stored.startsWith(ENCRYPTED_PREFIX)) {
        localStorage.removeItem(key)
        const decrypted = await this.encryption.decrypt(stored)
        const events: Ev[] = JSON.parse(decrypted)
        this.q.unshift(...events)
        recovered.push(...events)
      }
    } catch {
      // Ignore decrypt/parse errors - data may be corrupted or key may have changed
    }
    return recovered
  }

  /** Persist events with encryption (async version for non-unload scenarios) */
  async persistEventsEncrypted(events: Ev[]): Promise<void> {
    if (!this.encryption?.isReady()) {
      // Fall back to unencrypted storage
      this.persistFailedEvents(events)
      return
    }

    try {
      const key = P + 'failed'

      // Read and decrypt existing events
      let failed: Ev[] = []
      const existing = localStorage.getItem(key)
      if (existing) {
        if (existing.startsWith(ENCRYPTED_PREFIX)) {
          const decrypted = await this.encryption.decrypt(existing)
          failed = JSON.parse(decrypted)
        } else {
          failed = JSON.parse(existing)
        }
      }

      // Also merge any unencrypted overflow
      const unencKey = key + '_unenc'
      const unencStored = localStorage.getItem(unencKey)
      if (unencStored) {
        localStorage.removeItem(unencKey)
        const unencEvents: Ev[] = JSON.parse(unencStored)
        failed.push(...unencEvents)
      }

      // Combine and limit
      const combined = [...failed, ...events].slice(-this.maxRetryQueueSize)

      // Encrypt and store
      const encrypted = await this.encryption.encrypt(JSON.stringify(combined))
      localStorage.setItem(key, encrypted)
    } catch (err) {
      // Fall back to unencrypted on error
      this.onError?.(new Error(`Encryption failed, storing unencrypted: ${err}`), events)
      this.persistFailedEvents(events)
    }
  }

  destroy() {
    this.retryTimer && clearTimeout(this.retryTimer)
    this.t && clearInterval(this.t)
    this.flush(1)
  }
}

let sdk: EventsSDK | null = null

/**
 * Initialize the SDK with optional configuration.
 * If encryption is enabled, also recovers any encrypted events from previous sessions.
 */
export const init = (c?: BrowserConfig) => {
  sdk ||= new EventsSDK(c)
  sdk.recoverPersistedEvents()
  // If encryption is enabled, also recover encrypted events asynchronously
  if (c?.encryption?.enabled) {
    sdk.waitForEncryption().then(() => sdk?.recoverEncryptedEvents())
  }
  return sdk
}

/**
 * Initialize the SDK and wait for encryption to be ready (if enabled).
 * Use this instead of init() when you need to ensure encryption is ready before tracking.
 */
export const initAsync = async (c?: BrowserConfig): Promise<EventsSDK> => {
  sdk ||= new EventsSDK(c)
  sdk.recoverPersistedEvents()
  if (c?.encryption?.enabled) {
    await sdk.waitForEncryption()
    await sdk.recoverEncryptedEvents()
  }
  return sdk
}

export const page = (p?: Record<string, unknown>) => (sdk ||= new EventsSDK()).page(p)
export const track = (e: string, p?: Record<string, unknown>) => (sdk ||= new EventsSDK()).track(e, p)
export const identify = (u: string, t?: Record<string, unknown>) => (sdk ||= new EventsSDK()).identify(u, t)
export const flush = () => sdk?.flush() ?? Promise.resolve()
/** Get the current SDK instance (or null if not initialized) */
export const getInstance = () => sdk
