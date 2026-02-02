/**
 * EventEmitter - Batched event emission for Durable Objects
 */

import type { DurableEvent, EmitInput, EventEmitterOptions, CollectionChangeEvent, EventBatch } from './types.js'
import { EventBufferFullError, CircuitBreakerOpenError } from './types.js'
import {
  DEFAULT_EMITTER_ENDPOINT,
  DEFAULT_BATCH_SIZE,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_RETRY_QUEUE_SIZE,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_CIRCUIT_BREAKER_RESET_MS,
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  RETRY_JITTER_MS,
  STORAGE_KEY_RETRY,
  STORAGE_KEY_RETRY_COUNT,
  STORAGE_KEY_BATCH,
  STORAGE_KEY_CIRCUIT_BREAKER,
} from './config.js'

/**
 * Extended Request interface with Cloudflare properties
 */
interface CfRequest extends Request {
  cf?: IncomingRequestCfProperties
}

/** Circuit breaker state stored in DO storage */
interface CircuitBreakerState {
  /** Number of consecutive failures */
  consecutiveFailures: number
  /** Timestamp when circuit breaker opened (ISO string) */
  openedAt?: string | undefined
  /** Whether circuit breaker is currently open */
  isOpen: boolean
}

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
  /** Cached circuit breaker state for fast checks */
  private circuitBreakerCache: CircuitBreakerState | null = null
  /** Total events dropped due to buffer overflow */
  private droppedEventCount = 0

  constructor(
    private ctx: DurableObjectState,
    private env: Record<string, unknown>,
    options: EventEmitterOptions = {}
  ) {
    this.options = {
      endpoint: options.endpoint ?? DEFAULT_EMITTER_ENDPOINT,
      batchSize: options.batchSize ?? DEFAULT_BATCH_SIZE,
      flushIntervalMs: options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      cdc: options.cdc ?? false,
      trackPrevious: options.trackPrevious ?? false,
      r2Bucket: options.r2Bucket,
      apiKey: options.apiKey,
      maxRetryQueueSize: options.maxRetryQueueSize ?? DEFAULT_MAX_RETRY_QUEUE_SIZE,
      maxConsecutiveFailures: options.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_FAILURES,
      circuitBreakerResetMs: options.circuitBreakerResetMs ?? DEFAULT_CIRCUIT_BREAKER_RESET_MS,
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
   *
   * @throws {CircuitBreakerOpenError} When circuit breaker is open due to repeated failures
   */
  emit(event: EmitInput): void {
    // Check circuit breaker before accepting events
    if (this.isCircuitBreakerOpen()) {
      const state = this.circuitBreakerCache!
      const resetAt = new Date(new Date(state.openedAt!).getTime() + this.options.circuitBreakerResetMs)
      throw new CircuitBreakerOpenError(
        `Circuit breaker is open after ${state.consecutiveFailures} consecutive failures. Will reset at ${resetAt.toISOString()}`,
        state.consecutiveFailures,
        resetAt
      )
    }

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
   * Check if circuit breaker is currently open
   */
  private isCircuitBreakerOpen(): boolean {
    if (!this.circuitBreakerCache || !this.circuitBreakerCache.isOpen) {
      return false
    }

    // Check if enough time has passed to attempt reset (half-open state)
    if (this.circuitBreakerCache.openedAt) {
      const openedAt = new Date(this.circuitBreakerCache.openedAt).getTime()
      const elapsed = Date.now() - openedAt
      if (elapsed >= this.options.circuitBreakerResetMs) {
        // Allow a retry attempt (half-open state)
        return false
      }
    }

    return true
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

      // Record success for circuit breaker
      await this.recordSuccess()

      // Also stream to R2 if configured (for lakehouse)
      if (this.options.r2Bucket) {
        await this.streamToR2(events)
      }

      // Clear retry state on success
      await this.ctx.storage.delete(STORAGE_KEY_RETRY)
    } catch (error) {
      // Store for retry via alarm
      await this.scheduleRetry(events)
    }
  }

  /**
   * Calculate retry delay with exponential backoff and jitter
   */
  private getRetryDelay(retryCount: number): number {
    const exponentialDelay = Math.min(RETRY_BASE_DELAY_MS * Math.pow(2, retryCount), RETRY_MAX_DELAY_MS)
    const jitter = Math.random() * RETRY_JITTER_MS
    return exponentialDelay + jitter
  }

  /**
   * Schedule retry via alarm
   *
   * @throws {EventBufferFullError} When buffer is full and events are dropped
   */
  private async scheduleRetry(events: DurableEvent[]): Promise<void> {
    // Update circuit breaker state on failure
    await this.recordFailure()

    // Check if circuit breaker should open
    const cbState = await this.getCircuitBreakerState()
    if (this.options.maxConsecutiveFailures > 0 && cbState.consecutiveFailures >= this.options.maxConsecutiveFailures) {
      // Open circuit breaker
      cbState.isOpen = true
      cbState.openedAt = new Date().toISOString()
      await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, cbState)
      this.circuitBreakerCache = cbState
      console.warn(`[events] Circuit breaker opened after ${cbState.consecutiveFailures} consecutive failures`)
    }

    // Get existing retry queue
    const existing = await this.ctx.storage.get<DurableEvent[]>(STORAGE_KEY_RETRY) ?? []
    const combined = [...existing, ...events]

    // Cap retry queue to prevent unbounded growth
    const maxRetry = this.options.maxRetryQueueSize
    let droppedCount = 0
    let toRetry = combined

    if (combined.length > maxRetry) {
      droppedCount = combined.length - maxRetry
      toRetry = combined.slice(-maxRetry)
      this.droppedEventCount += droppedCount
      console.warn(`[events] Retry buffer full: dropped ${droppedCount} oldest events (total dropped: ${this.droppedEventCount})`)
    }

    await this.ctx.storage.put(STORAGE_KEY_RETRY, toRetry)

    // Schedule alarm for retry with exponential backoff
    const currentAlarm = await this.ctx.storage.getAlarm()
    if (!currentAlarm) {
      const retryCount = await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }

    // Throw error to signal backpressure if events were dropped
    if (droppedCount > 0) {
      throw new EventBufferFullError(
        `Event buffer full: ${droppedCount} events dropped. Consider reducing event volume or increasing maxRetryQueueSize.`,
        droppedCount
      )
    }
  }

  /**
   * Record a failure for circuit breaker tracking
   */
  private async recordFailure(): Promise<void> {
    const state = await this.getCircuitBreakerState()
    state.consecutiveFailures++
    await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, state)
    this.circuitBreakerCache = state
  }

  /**
   * Record a success and potentially close circuit breaker
   */
  private async recordSuccess(): Promise<void> {
    const state = await this.getCircuitBreakerState()
    if (state.consecutiveFailures > 0 || state.isOpen) {
      state.consecutiveFailures = 0
      state.isOpen = false
      state.openedAt = undefined
      await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, state)
      this.circuitBreakerCache = state
      console.log('[events] Circuit breaker closed after successful delivery')
    }
  }

  /**
   * Get circuit breaker state from storage or cache
   */
  private async getCircuitBreakerState(): Promise<CircuitBreakerState> {
    if (this.circuitBreakerCache) {
      return this.circuitBreakerCache
    }
    const stored = await this.ctx.storage.get<CircuitBreakerState>(STORAGE_KEY_CIRCUIT_BREAKER)
    this.circuitBreakerCache = stored ?? { consecutiveFailures: 0, isOpen: false }
    return this.circuitBreakerCache
  }

  /**
   * Handle alarm - retry failed events
   * Call this from your DO's alarm() method
   */
  async handleAlarm(): Promise<void> {
    // Load circuit breaker state
    await this.getCircuitBreakerState()

    const events = await this.ctx.storage.get<DurableEvent[]>(STORAGE_KEY_RETRY)
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
        await this.ctx.storage.delete(STORAGE_KEY_RETRY)
        await this.ctx.storage.delete(STORAGE_KEY_RETRY_COUNT)

        // Record success and potentially close circuit breaker
        await this.recordSuccess()

        // Stream to R2 on successful retry
        if (this.options.r2Bucket) {
          await this.streamToR2(events)
        }
      } else {
        // Record failure for circuit breaker
        await this.recordFailure()

        // Check if circuit breaker should open
        const cbState = await this.getCircuitBreakerState()
        if (this.options.maxConsecutiveFailures > 0 && cbState.consecutiveFailures >= this.options.maxConsecutiveFailures) {
          cbState.isOpen = true
          cbState.openedAt = new Date().toISOString()
          await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, cbState)
          this.circuitBreakerCache = cbState
          console.warn(`[events] Circuit breaker opened after ${cbState.consecutiveFailures} consecutive failures during retry`)

          // Schedule alarm far in the future for circuit breaker reset
          await this.ctx.storage.setAlarm(Date.now() + this.options.circuitBreakerResetMs)
          return
        }

        // Retry again later with exponential backoff
        const retryCount = await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT) ?? 0
        const delay = this.getRetryDelay(retryCount)
        await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
        await this.ctx.storage.setAlarm(Date.now() + delay)
      }
    } catch {
      // Record failure for circuit breaker
      await this.recordFailure()

      // Check if circuit breaker should open
      const cbState = await this.getCircuitBreakerState()
      if (this.options.maxConsecutiveFailures > 0 && cbState.consecutiveFailures >= this.options.maxConsecutiveFailures) {
        cbState.isOpen = true
        cbState.openedAt = new Date().toISOString()
        await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, cbState)
        this.circuitBreakerCache = cbState
        console.warn(`[events] Circuit breaker opened after ${cbState.consecutiveFailures} consecutive failures during retry`)

        // Schedule alarm far in the future for circuit breaker reset
        await this.ctx.storage.setAlarm(Date.now() + this.options.circuitBreakerResetMs)
        return
      }

      // Retry again later with exponential backoff
      const retryCount = await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }
  }

  /**
   * Restore batch from storage (after hibernation)
   */
  private async restoreBatch(): Promise<void> {
    try {
      const stored = await this.ctx.storage.get<DurableEvent[]>(STORAGE_KEY_BATCH)
      if (stored) {
        this.batch = stored
        await this.ctx.storage.delete(STORAGE_KEY_BATCH)
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
      await this.ctx.storage.put(STORAGE_KEY_BATCH, this.batch)
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

  /**
   * Get the total count of dropped events due to buffer overflow
   */
  get droppedEvents(): number {
    return this.droppedEventCount
  }

  /**
   * Check if circuit breaker is currently open (read from cache, may not be current)
   * For accurate state, use getCircuitBreakerInfo()
   */
  get circuitBreakerOpen(): boolean {
    return this.isCircuitBreakerOpen()
  }

  /**
   * Get detailed circuit breaker information
   */
  async getCircuitBreakerInfo(): Promise<{
    isOpen: boolean
    consecutiveFailures: number
    openedAt?: Date
    resetAt?: Date
  }> {
    const state = await this.getCircuitBreakerState()
    const result: {
      isOpen: boolean
      consecutiveFailures: number
      openedAt?: Date
      resetAt?: Date
    } = {
      isOpen: state.isOpen,
      consecutiveFailures: state.consecutiveFailures,
    }

    if (state.openedAt) {
      result.openedAt = new Date(state.openedAt)
      result.resetAt = new Date(new Date(state.openedAt).getTime() + this.options.circuitBreakerResetMs)
    }

    return result
  }

  /**
   * Manually reset the circuit breaker
   * Use this to force retry after fixing the underlying issue
   */
  async resetCircuitBreaker(): Promise<void> {
    const newState: CircuitBreakerState = {
      consecutiveFailures: 0,
      isOpen: false,
      openedAt: undefined,
    }
    await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, newState)
    await this.ctx.storage.delete(STORAGE_KEY_RETRY_COUNT)
    this.circuitBreakerCache = newState
    console.log('[events] Circuit breaker manually reset')

    // Trigger retry if there are pending events
    const events = await this.ctx.storage.get<DurableEvent[]>(STORAGE_KEY_RETRY)
    if (events && events.length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + 1000) // Retry in 1 second
    }
  }

  /**
   * Get retry queue statistics
   */
  async getRetryQueueStats(): Promise<{
    queueSize: number
    maxSize: number
    droppedTotal: number
    retryCount: number
  }> {
    const events = await this.ctx.storage.get<DurableEvent[]>(STORAGE_KEY_RETRY) ?? []
    const retryCount = await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT) ?? 0
    return {
      queueSize: events.length,
      maxSize: this.options.maxRetryQueueSize,
      droppedTotal: this.droppedEventCount,
      retryCount,
    }
  }
}
