/// <reference lib="dom" />
/**
 * @dotdo/events Browser SDK
 * Minimal analytics SDK for browser-side event tracking
 */

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
  private flushing = false

  constructor(c: BrowserConfig = {}) {
    this.e = c.endpoint || '/e'
    this.b = c.batchSize || 10
    this.maxRetries = c.maxRetries ?? 3
    this.retryDelay = c.retryDelay ?? 1000
    this.maxRetryQueueSize = c.maxRetryQueueSize ?? 100
    this.onError = c.onError
    this.onSuccess = c.onSuccess
    this.a = get('a', localStorage)
    this.s = get('s', sessionStorage)
    if (typeof window !== 'undefined') {
      this.t = setInterval(() => this.flush(), c.flushInterval || 5000) as unknown as number
      addEventListener('visibilitychange', () => document.visibilityState === 'hidden' && this.flush(1))
      addEventListener('pagehide', () => this.flush(1))
    }
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
    try {
      const key = P + 'failed'
      const existing = localStorage.getItem(key)
      const failed: Ev[] = existing ? JSON.parse(existing) : []
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
    try {
      const key = P + 'failed'
      const stored = localStorage.getItem(key)
      if (stored) {
        localStorage.removeItem(key)
        const events: Ev[] = JSON.parse(stored)
        // Add recovered events to main queue for sending
        this.q.unshift(...events)
        return events
      }
    } catch {
      // Ignore parse errors
    }
    return []
  }

  destroy() {
    this.retryTimer && clearTimeout(this.retryTimer)
    this.t && clearInterval(this.t)
    this.flush(1)
  }
}

let sdk: EventsSDK | null = null
export const init = (c?: BrowserConfig) => { sdk ||= new EventsSDK(c); sdk.recoverPersistedEvents(); return sdk }
export const page = (p?: Record<string, unknown>) => (sdk ||= new EventsSDK()).page(p)
export const track = (e: string, p?: Record<string, unknown>) => (sdk ||= new EventsSDK()).track(e, p)
export const identify = (u: string, t?: Record<string, unknown>) => (sdk ||= new EventsSDK()).identify(u, t)
export const flush = () => sdk?.flush() ?? Promise.resolve()
/** Get the current SDK instance (or null if not initialized) */
export const getInstance = () => sdk
