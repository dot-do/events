/**
 * EventEmitter - Batched event emission for Durable Objects
 */

import type { DurableEvent, EmitInput, EventEmitterOptions, CollectionChangeEvent, EventBatch, EventLogger } from './types.js'
import { EventBufferFullError, CircuitBreakerOpenError } from './types.js'
import {
  DEFAULT_EMITTER_ENDPOINT,
  DEFAULT_BATCH_SIZE,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_RETRY_QUEUE_SIZE,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_CIRCUIT_BREAKER_RESET_MS,
  MAX_CIRCUIT_BREAKER_RESET_MS,
  DEFAULT_FETCH_TIMEOUT_MS,
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
 * Fetch with timeout using AbortController
 * @param url - The URL to fetch
 * @param options - Fetch options (RequestInit)
 * @param timeoutMs - Timeout in milliseconds (default: 30000)
 * @returns Promise<Response>
 * @throws Error when request times out or fetch fails
 */
async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs: number = DEFAULT_FETCH_TIMEOUT_MS
): Promise<Response> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    })
    return response
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms`)
    }
    throw error
  } finally {
    clearTimeout(timeoutId)
  }
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
/**
 * Default console-based logger for when no logger is injected.
 * Outputs structured JSON for consistency.
 */
const defaultLogger: EventLogger = {
  debug: (message, context) => console.debug(JSON.stringify({ timestamp: new Date().toISOString(), level: 'debug', message, context })),
  info: (message, context) => console.info(JSON.stringify({ timestamp: new Date().toISOString(), level: 'info', message, context })),
  warn: (message, context) => console.warn(JSON.stringify({ timestamp: new Date().toISOString(), level: 'warn', message, context })),
  error: (message, context) => console.error(JSON.stringify({ timestamp: new Date().toISOString(), level: 'error', message, context })),
}

export class EventEmitter {
  private batch: DurableEvent[] = []
  private flushTimeout?: ReturnType<typeof setTimeout>
  private identity: DurableEvent['do']
  private options: Required<Omit<EventEmitterOptions, 'r2Bucket' | 'apiKey' | 'logger'>> & { r2Bucket?: R2Bucket; apiKey?: string }
  private log: EventLogger
  /** Cached circuit breaker state for fast checks */
  private circuitBreakerCache: CircuitBreakerState | null = null
  /** Total events dropped due to buffer overflow */
  private droppedEventCount = 0

  constructor(
    private ctx: DurableObjectState,
    private env: Record<string, unknown>,
    options: EventEmitterOptions = {}
  ) {
    this.log = options.logger ?? defaultLogger
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
      circuitBreakerResetMs: Math.min(
        options.circuitBreakerResetMs ?? DEFAULT_CIRCUIT_BREAKER_RESET_MS,
        MAX_CIRCUIT_BREAKER_RESET_MS
      ),
      fetchTimeoutMs: options.fetchTimeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS,
    }

    // Identity will be enriched on first request
    this.identity = {
      id: ctx.id.toString(),
      name: ctx.id.name,
    }

    // Restore any pending batch from storage (for hibernation recovery)
    this.restoreBatch().catch((error) => {
      this.log.error('Failed to restore batch from storage', { error: error instanceof Error ? error.message : String(error) })
    })
  }

  /**
   * Enrich identity from incoming request.
   * Call this in fetch() to capture DO context like colo, worker name, and DO class.
   *
   * @param request - The incoming HTTP request containing CF properties
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
   * Emit an event to be batched and sent to the events endpoint.
   * Events are batched based on `batchSize` and `flushIntervalMs` options.
   * This method is non-blocking; events are queued and flushed asynchronously.
   *
   * @param event - The event data to emit (type, collection, docId, etc.)
   * @throws {CircuitBreakerOpenError} When circuit breaker is open due to repeated delivery failures
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
      this.flush().catch((error) => {
        this.log.error('Failed to flush events', { error: error instanceof Error ? error.message : String(error) })
      })
    } else if (!this.flushTimeout) {
      // Schedule flush
      this.flushTimeout = setTimeout(() => {
        this.flush().catch((error) => {
          this.log.error('Failed to flush events', { error: error instanceof Error ? error.message : String(error) })
        })
      }, this.options.flushIntervalMs)
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
   * Emit a CDC (Change Data Capture) event for a collection change.
   * Automatically captures the SQLite bookmark for PITR (point-in-time recovery).
   * Only emits if the `cdc` option is enabled.
   *
   * @param type - The type of change: 'insert', 'update', or 'delete'
   * @param collection - The name of the collection being changed
   * @param docId - The unique identifier of the document being changed
   * @param doc - The current document state (for insert/update operations)
   * @param prev - The previous document state (for update/delete, if trackPrevious is enabled)
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
    this.getBookmarkAndEmit(type, collection, docId, doc, prev).catch((error) => {
      this.log.error('Failed to emit CDC event', { error: error instanceof Error ? error.message : String(error), collection, docId })
    })
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
      this.log.warn('Failed to get SQLite bookmark', { error: error instanceof Error ? error.message : String(error) })
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
   * Immediately flush all pending events to the events endpoint.
   * Clears the current batch and sends events via HTTP POST.
   * On failure, events are queued for retry via the alarm system.
   *
   * @returns A promise that resolves when the flush attempt completes
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

      // Send to events endpoint with timeout
      const response = await fetchWithTimeout(
        this.options.endpoint,
        {
          method: 'POST',
          headers,
          body: JSON.stringify({ events } satisfies EventBatch),
        },
        this.options.fetchTimeoutMs
      )

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
      this.log.warn('Circuit breaker opened', { consecutiveFailures: cbState.consecutiveFailures })
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
      this.log.warn('Retry buffer full', { droppedCount, totalDropped: this.droppedEventCount })
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
      this.log.info('Circuit breaker closed after successful delivery')
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
   * Handle the Durable Object alarm to retry failed event deliveries.
   * Must be called from your DO's alarm() method to enable automatic retries.
   * Uses exponential backoff with jitter for retry scheduling.
   *
   * @returns A promise that resolves when retry processing completes
   *
   * @example
   * ```typescript
   * async alarm() {
   *   await this.events.handleAlarm()
   * }
   * ```
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

      const response = await fetchWithTimeout(
        this.options.endpoint,
        {
          method: 'POST',
          headers,
          body: JSON.stringify({ events } satisfies EventBatch),
        },
        this.options.fetchTimeoutMs
      )

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
          this.log.warn('Circuit breaker opened during retry', { consecutiveFailures: cbState.consecutiveFailures })

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
        this.log.warn('Circuit breaker opened during retry', { consecutiveFailures: cbState.consecutiveFailures })

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
      this.log.warn('Failed to restore batch', { error: error instanceof Error ? error.message : String(error) })
    }
  }

  /**
   * Persist the current event batch to storage before hibernation.
   * Call this in webSocketClose or when expecting the DO to hibernate.
   * The batch will be restored automatically when the DO wakes up.
   *
   * @returns A promise that resolves when the batch is persisted
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
   * Get detailed information about the circuit breaker state.
   *
   * @returns A promise resolving to circuit breaker status including:
   *   - isOpen: whether the circuit breaker is currently open
   *   - consecutiveFailures: number of consecutive delivery failures
   *   - openedAt: when the circuit breaker opened (if open)
   *   - resetAt: when the circuit breaker will attempt to reset (if open)
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
   * Manually reset the circuit breaker to allow event delivery to resume.
   * Use this after fixing the underlying issue that caused repeated failures.
   * If there are pending events in the retry queue, schedules an immediate retry.
   *
   * @returns A promise that resolves when the circuit breaker is reset
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
    this.log.info('Circuit breaker manually reset')

    // Trigger retry if there are pending events
    const events = await this.ctx.storage.get<DurableEvent[]>(STORAGE_KEY_RETRY)
    if (events && events.length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + 1000) // Retry in 1 second
    }
  }

  /**
   * Get statistics about the retry queue for monitoring and debugging.
   *
   * @returns A promise resolving to retry queue statistics including:
   *   - queueSize: current number of events in the retry queue
   *   - maxSize: maximum allowed queue size (from options)
   *   - droppedTotal: total events dropped due to buffer overflow
   *   - retryCount: number of retry attempts made
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
