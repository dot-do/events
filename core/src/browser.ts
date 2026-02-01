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

export class EventsSDK {
  private q: Ev[] = []
  private e: string
  private b: number
  private t?: number
  private u?: string
  private a: string
  private s: string

  constructor(c: BrowserConfig = {}) {
    this.e = c.endpoint || '/e'
    this.b = c.batchSize || 10
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

  async flush(beacon?: number) {
    if (!this.q.length) return
    const body = JSON.stringify({ events: this.q.splice(0) })
    beacon && navigator.sendBeacon ? navigator.sendBeacon(this.e, body) : fetch(this.e, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body, keepalive: true } as globalThis.RequestInit).catch(() => {})
  }

  destroy() { this.t && clearInterval(this.t); this.flush(1) }
}

let sdk: EventsSDK | null = null
export const init = (c?: BrowserConfig) => sdk ||= new EventsSDK(c)
export const page = (p?: Record<string, unknown>) => (sdk ||= new EventsSDK()).page(p)
export const track = (e: string, p?: Record<string, unknown>) => (sdk ||= new EventsSDK()).track(e, p)
export const identify = (u: string, t?: Record<string, unknown>) => (sdk ||= new EventsSDK()).identify(u, t)
export const flush = () => sdk?.flush() ?? Promise.resolve()
