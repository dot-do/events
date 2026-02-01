/**
 * EventEmitter - Batched event emission for Durable Objects
 */

import type { DurableEvent, EmitInput, EventEmitterOptions, CollectionChangeEvent, EventBatch } from './types.js'

/**
 * Extended Request interface with Cloudflare properties
 */
interface CfRequest extends Request {
  cf?: IncomingRequestCfProperties
}

const DEFAULT_ENDPOINT = 'https://events.workers.do/ingest'
const RETRY_KEY = '_events:retry'
const RETRY_COUNT_KEY = '_events:retryCount'
const BATCH_KEY = '_events:batch'

/**
 * Lightweight event emitter for Durable Objects
 *
 * Features:
 * - Batched emission to events.do
 * - Alarm-based retries for reliability
 * - CDC (Change Data Capture) with PITR bookmarks
 * - R2 lakehouse streaming
 *
 * @example
 * ```typescript
 * export class MyDO extends DurableRPC {
 *   events = new EventEmitter(this.ctx, this.env, {
 *     cdc: true,
 *     r2Bucket: this.env.EVENTS_BUCKET
 *   })
 *
 *   async doSomething() {
 *     this.events.emit({ type: 'custom.event', data: 123 })
 *   }
 *
 *   async alarm() {
 *     await this.events.handleAlarm()
 *   }
 * }
 * ```
 */
export class EventEmitter {
  private batch: DurableEvent[] = []
  private flushTimeout?: ReturnType<typeof setTimeout>
  private identity: DurableEvent['do']
  private options: Required<Omit<EventEmitterOptions, 'r2Bucket' | 'apiKey'>> & { r2Bucket?: R2Bucket; apiKey?: string }

  constructor(
    private ctx: DurableObjectState,
    private env: Record<string, unknown>,
    options: EventEmitterOptions = {}
  ) {
    this.options = {
      endpoint: options.endpoint ?? DEFAULT_ENDPOINT,
      batchSize: options.batchSize ?? 100,
      flushIntervalMs: options.flushIntervalMs ?? 1000,
      cdc: options.cdc ?? false,
      trackPrevious: options.trackPrevious ?? false,
      r2Bucket: options.r2Bucket,
      apiKey: options.apiKey,
    }

    // Identity will be enriched on first request
    this.identity = {
      id: ctx.id.toString(),
      name: ctx.id.name,
    }

    // Restore any pending batch from storage (for hibernation recovery)
    this.restoreBatch()
  }

  /**
   * Enrich identity from incoming request
   * Call this in fetch() to capture DO context
   */
  enrichFromRequest(request: Request): void {
    const cf = (request as CfRequest).cf
    this.identity = {
      ...this.identity,
      colo: cf?.colo,
      worker: request.headers.get('cf-worker') ?? undefined,
      class: request.headers.get('X-DO-Class') ?? undefined,
    }
  }

  /**
   * Emit an event (batched, non-blocking)
   */
  emit(event: EmitInput): void {
    // Safe cast: EmitInput is already constrained to valid event shapes,
    // adding ts + do completes it to a DurableEvent. TypeScript can't verify
    // union reconstitution through spread, so the cast is necessary but safe.
    const fullEvent = {
      ...event,
      ts: new Date().toISOString(),
      do: this.identity,
    } as DurableEvent

    this.batch.push(fullEvent)

    // Auto-flush on batch size
    if (this.batch.length >= this.options.batchSize) {
      this.flush()
    } else if (!this.flushTimeout) {
      // Schedule flush
      this.flushTimeout = setTimeout(() => this.flush(), this.options.flushIntervalMs)
    }
  }

  /**
   * Emit CDC event for collection change
   * Captures SQLite bookmark for PITR (point-in-time recovery)
   */
  emitChange(
    type: 'insert' | 'update' | 'delete',
    collection: string,
    docId: string,
    doc?: Record<string, unknown>,
    prev?: Record<string, unknown>
  ): void {
    if (!this.options.cdc) return

    // Capture SQLite bookmark for PITR (async, but we emit without waiting)
    // The bookmark is optional - if we can't get it, we still emit the event
    this.getBookmarkAndEmit(type, collection, docId, doc, prev)
  }

  /**
   * Internal helper to get bookmark and emit CDC event
   */
  private async getBookmarkAndEmit(
    type: 'insert' | 'update' | 'delete',
    collection: string,
    docId: string,
    doc?: Record<string, unknown>,
    prev?: Record<string, unknown>
  ): Promise<void> {
    let bookmark: string | undefined
    try {
      // getCurrentBookmark() returns the current SQLite replication bookmark
      // This can be used to restore the DO to this exact point in time
      bookmark = await this.ctx.storage.getCurrentBookmark()
    } catch (error) {
      console.warn('[events] Failed to get SQLite bookmark:', error instanceof Error ? error.message : error)
    }

    this.emit({
      type: `collection.${type}` as CollectionChangeEvent['type'],
      collection,
      docId,
      doc,
      prev: this.options.trackPrevious ? prev : undefined,
      bookmark,
    })
  }

  /**
   * Flush events to endpoint
   */
  async flush(): Promise<void> {
    if (this.flushTimeout) {
      clearTimeout(this.flushTimeout)
      this.flushTimeout = undefined
    }

    if (this.batch.length === 0) return

    const events = this.batch
    this.batch = []

    try {
      // Build headers
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      }
      if (this.options.apiKey) {
        headers['Authorization'] = `Bearer ${this.options.apiKey}`
      }

      // Send to events endpoint
      const response = await fetch(this.options.endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify({ events } satisfies EventBatch),
      })

      if (!response.ok) {
        throw new Error(`Event flush failed: ${response.status}`)
      }

      // Also stream to R2 if configured (for lakehouse)
      if (this.options.r2Bucket) {
        await this.streamToR2(events)
      }

      // Clear retry state on success
      await this.ctx.storage.delete(RETRY_KEY)
    } catch (error) {
      // Store for retry via alarm
      await this.scheduleRetry(events)
    }
  }

  /**
   * Calculate retry delay with exponential backoff and jitter
   */
  private getRetryDelay(retryCount: number): number {
    const baseDelay = 1000
    const maxDelay = 60000
    const exponentialDelay = Math.min(baseDelay * Math.pow(2, retryCount), maxDelay)
    const jitter = Math.random() * 1000
    return exponentialDelay + jitter
  }

  /**
   * Schedule retry via alarm
   */
  private async scheduleRetry(events: DurableEvent[]): Promise<void> {
    // Get existing retry queue
    const existing = await this.ctx.storage.get<DurableEvent[]>(RETRY_KEY) ?? []
    const combined = [...existing, ...events]

    // Cap retry queue to prevent unbounded growth
    const maxRetry = 10000
    const toRetry = combined.slice(-maxRetry)

    await this.ctx.storage.put(RETRY_KEY, toRetry)

    // Schedule alarm for retry with exponential backoff
    const currentAlarm = await this.ctx.storage.getAlarm()
    if (!currentAlarm) {
      const retryCount = await this.ctx.storage.get<number>(RETRY_COUNT_KEY) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(RETRY_COUNT_KEY, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }
  }

  /**
   * Handle alarm - retry failed events
   * Call this from your DO's alarm() method
   */
  async handleAlarm(): Promise<void> {
    const events = await this.ctx.storage.get<DurableEvent[]>(RETRY_KEY)
    if (!events || events.length === 0) return

    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      }
      if (this.options.apiKey) {
        headers['Authorization'] = `Bearer ${this.options.apiKey}`
      }

      const response = await fetch(this.options.endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify({ events } satisfies EventBatch),
      })

      if (response.ok) {
        await this.ctx.storage.delete(RETRY_KEY)
        await this.ctx.storage.delete(RETRY_COUNT_KEY)

        // Stream to R2 on successful retry
        if (this.options.r2Bucket) {
          await this.streamToR2(events)
        }
      } else {
        // Retry again later with exponential backoff
        const retryCount = await this.ctx.storage.get<number>(RETRY_COUNT_KEY) ?? 0
        const delay = this.getRetryDelay(retryCount)
        await this.ctx.storage.put(RETRY_COUNT_KEY, retryCount + 1)
        await this.ctx.storage.setAlarm(Date.now() + delay)
      }
    } catch {
      // Retry again later with exponential backoff
      const retryCount = await this.ctx.storage.get<number>(RETRY_COUNT_KEY) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(RETRY_COUNT_KEY, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }
  }

  /**
   * Restore batch from storage (after hibernation)
   */
  private async restoreBatch(): Promise<void> {
    try {
      const stored = await this.ctx.storage.get<DurableEvent[]>(BATCH_KEY)
      if (stored) {
        this.batch = stored
        await this.ctx.storage.delete(BATCH_KEY)
      }
    } catch (error) {
      console.warn('[events] Failed to restore batch:', error instanceof Error ? error.message : error)
    }
  }

  /**
   * Persist batch before hibernation
   * Call this in webSocketClose or when expecting hibernation
   */
  async persistBatch(): Promise<void> {
    if (this.batch.length > 0) {
      await this.ctx.storage.put(BATCH_KEY, this.batch)
    }
  }

  /**
   * Stream events to R2 in Parquet-friendly JSON Lines format
   * Organized by: /{year}/{month}/{day}/{hour}/{do_id}_{timestamp}.jsonl
   */
  private async streamToR2(events: DurableEvent[]): Promise<void> {
    if (!this.options.r2Bucket || events.length === 0) return

    const now = new Date()
    const path = [
      now.getUTCFullYear(),
      String(now.getUTCMonth() + 1).padStart(2, '0'),
      String(now.getUTCDate()).padStart(2, '0'),
      String(now.getUTCHours()).padStart(2, '0'),
      `${this.identity.id}_${now.getTime()}.jsonl`
    ].join('/')

    const body = events.map(e => JSON.stringify(e)).join('\n')

    await this.options.r2Bucket.put(`events/${path}`, body, {
      httpMetadata: { contentType: 'application/x-ndjson' },
      customMetadata: {
        doId: this.identity.id,
        doName: this.identity.name ?? '',
        doColo: this.identity.colo ?? '',
        eventCount: String(events.length),
      },
    })
  }

  /**
   * Get current batch size (for debugging)
   */
  get pendingCount(): number {
    return this.batch.length
  }

  /**
   * Get identity info
   */
  get doIdentity(): DurableEvent['do'] {
    return { ...this.identity }
  }
}
