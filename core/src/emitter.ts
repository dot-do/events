/**
 * EventEmitter - Batched event emission for Durable Objects
 */

import type { Event, EventBatch, EventEmitterOptions, EventLogger } from './types.js'
import { EventBufferFullError, CircuitBreakerOpenError } from './types.js'
import { ulid } from './ulid.js'
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

/** Input type for emit() — type, event, and data are required; everything else has defaults */
export type EmitInput = {
  type: string
  event: string
  data: Record<string, unknown>
  ns?: string | undefined
  url?: string | undefined
  source?: string | undefined
  actor?: string | undefined
  meta?: Record<string, unknown> | undefined
  id?: string | undefined
  ts?: string | undefined
}

/** DO identity info stored in meta.do */
interface DoIdentity {
  id: string
  name?: string | undefined
  class?: string | undefined
  colo?: string | undefined
  worker?: string | undefined
}

/** Extended Request interface with Cloudflare properties */
interface CfRequest extends Request {
  cf?: IncomingRequestCfProperties
}

/** Circuit breaker state stored in DO storage */
interface CircuitBreakerState {
  consecutiveFailures: number
  openedAt?: string | undefined
  isOpen: boolean
}

async function fetchWithTimeout(url: string, options: RequestInit, timeoutMs: number = DEFAULT_FETCH_TIMEOUT_MS): Promise<Response> {
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

const defaultLogger: EventLogger = {
  debug: (message, context) => console.debug(JSON.stringify({ timestamp: new Date().toISOString(), level: 'debug', message, context })),
  info: (message, context) => console.info(JSON.stringify({ timestamp: new Date().toISOString(), level: 'info', message, context })),
  warn: (message, context) => console.warn(JSON.stringify({ timestamp: new Date().toISOString(), level: 'warn', message, context })),
  error: (message, context) => console.error(JSON.stringify({ timestamp: new Date().toISOString(), level: 'error', message, context })),
}

export class EventEmitter {
  private batch: Event[] = []
  private flushTimeout?: ReturnType<typeof setTimeout>
  private doIdentity: DoIdentity
  private namespace: string
  private options: Required<Omit<EventEmitterOptions, 'apiKey' | 'logger'>> & { apiKey?: string }
  private log: EventLogger
  private circuitBreakerCache: CircuitBreakerState | null = null
  private droppedEventCount = 0

  constructor(
    private ctx: DurableObjectState,
    private env: Record<string, unknown>,
    options: EventEmitterOptions = {},
  ) {
    this.log = options.logger ?? defaultLogger
    this.options = {
      endpoint: options.endpoint ?? DEFAULT_EMITTER_ENDPOINT,
      batchSize: options.batchSize ?? DEFAULT_BATCH_SIZE,
      flushIntervalMs: options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      cdc: options.cdc ?? false,
      trackPrevious: options.trackPrevious ?? false,
      apiKey: options.apiKey,
      maxRetryQueueSize: options.maxRetryQueueSize ?? DEFAULT_MAX_RETRY_QUEUE_SIZE,
      maxConsecutiveFailures: options.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_FAILURES,
      circuitBreakerResetMs: Math.min(options.circuitBreakerResetMs ?? DEFAULT_CIRCUIT_BREAKER_RESET_MS, MAX_CIRCUIT_BREAKER_RESET_MS),
      fetchTimeoutMs: options.fetchTimeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS,
    }

    this.doIdentity = {
      id: ctx.id.toString(),
      name: ctx.id.name,
    }
    this.namespace = ctx.id.name ?? 'default'

    this.restoreBatch().catch((error) => {
      this.log.error('Failed to restore batch from storage', { error: error instanceof Error ? error.message : String(error) })
    })
  }

  /**
   * Enrich identity from incoming request.
   * Call this in fetch() to capture DO context like colo, worker name, and DO class.
   */
  enrichFromRequest(request: Request): void {
    const cf = (request as CfRequest).cf
    this.doIdentity = {
      ...this.doIdentity,
      colo: cf?.colo,
      worker: request.headers.get('cf-worker') ?? undefined,
      class: request.headers.get('X-DO-Class') ?? undefined,
    }
  }

  /**
   * Emit an event to be batched and sent to the events endpoint.
   */
  emit(input: EmitInput): void {
    if (this.isCircuitBreakerOpen()) {
      const state = this.circuitBreakerCache!
      const resetAt = new Date(new Date(state.openedAt!).getTime() + this.options.circuitBreakerResetMs)
      throw new CircuitBreakerOpenError(
        `Circuit breaker is open after ${state.consecutiveFailures} consecutive failures. Will reset at ${resetAt.toISOString()}`,
        state.consecutiveFailures,
        resetAt,
      )
    }

    const event: Event = {
      id: input.id ?? ulid(),
      ns: input.ns ?? this.namespace,
      ts: input.ts ?? new Date().toISOString(),
      type: input.type,
      event: input.event,
      url: input.url ?? '',
      source: input.source ?? 'events.do',
      actor: input.actor ?? '',
      data: input.data,
      meta: { ...input.meta, do: this.doIdentity },
    } as Event

    this.batch.push(event)

    if (this.batch.length >= this.options.batchSize) {
      this.flush().catch((error) => {
        this.log.error('Failed to flush events', { error: error instanceof Error ? error.message : String(error) })
      })
    } else if (!this.flushTimeout) {
      this.flushTimeout = setTimeout(() => {
        this.flush().catch((error) => {
          this.log.error('Failed to flush events', { error: error instanceof Error ? error.message : String(error) })
        })
      }, this.options.flushIntervalMs)
    }
  }

  private isCircuitBreakerOpen(): boolean {
    if (!this.circuitBreakerCache || !this.circuitBreakerCache.isOpen) {
      return false
    }

    if (this.circuitBreakerCache.openedAt) {
      const openedAt = new Date(this.circuitBreakerCache.openedAt).getTime()
      const elapsed = Date.now() - openedAt
      if (elapsed >= this.options.circuitBreakerResetMs) {
        return false
      }
    }

    return true
  }

  /**
   * Emit a CDC (Change Data Capture) event for a collection change.
   * Automatically captures the SQLite bookmark for PITR (point-in-time recovery).
   * Only emits if the `cdc` option is enabled.
   */
  emitChange(type: 'insert' | 'update' | 'delete', collection: string, docId: string, doc?: Record<string, unknown>, prev?: Record<string, unknown>): void {
    if (!this.options.cdc) return

    this.getBookmarkAndEmit(type, collection, docId, doc, prev).catch((error) => {
      this.log.error('Failed to emit CDC event', { error: error instanceof Error ? error.message : String(error), collection, docId })
    })
  }

  private async getBookmarkAndEmit(
    type: 'insert' | 'update' | 'delete',
    collection: string,
    docId: string,
    doc?: Record<string, unknown>,
    prev?: Record<string, unknown>,
  ): Promise<void> {
    let bookmark: string | undefined
    try {
      bookmark = await this.ctx.storage.getCurrentBookmark()
    } catch (error) {
      this.log.warn('Failed to get SQLite bookmark', { error: error instanceof Error ? error.message : String(error) })
    }

    this.emit({
      type: 'cdc',
      event: `${collection}.${type}`,
      data: { type: collection, id: docId, ...(doc ?? {}) },
      meta: {
        ...(this.options.trackPrevious && prev ? { prev } : {}),
        ...(bookmark ? { bookmark } : {}),
      },
    })
  }

  /**
   * Immediately flush all pending events to the events endpoint.
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
        this.options.fetchTimeoutMs,
      )

      if (!response.ok) {
        throw new Error(`Event flush failed: ${response.status}`)
      }

      await this.recordSuccess()
      await this.ctx.storage.delete(STORAGE_KEY_RETRY)
    } catch {
      await this.scheduleRetry(events)
    }
  }

  private getRetryDelay(retryCount: number): number {
    const exponentialDelay = Math.min(RETRY_BASE_DELAY_MS * Math.pow(2, retryCount), RETRY_MAX_DELAY_MS)
    const jitter = Math.random() * RETRY_JITTER_MS
    return exponentialDelay + jitter
  }

  private async scheduleRetry(events: Event[]): Promise<void> {
    await this.recordFailure()

    const cbState = await this.getCircuitBreakerState()
    if (this.options.maxConsecutiveFailures > 0 && cbState.consecutiveFailures >= this.options.maxConsecutiveFailures) {
      cbState.isOpen = true
      cbState.openedAt = new Date().toISOString()
      await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, cbState)
      this.circuitBreakerCache = cbState
      this.log.warn('Circuit breaker opened', { consecutiveFailures: cbState.consecutiveFailures })
    }

    const existing = (await this.ctx.storage.get<Event[]>(STORAGE_KEY_RETRY)) ?? []
    const combined = [...existing, ...events]

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

    const currentAlarm = await this.ctx.storage.getAlarm()
    if (!currentAlarm) {
      const retryCount = (await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT)) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }

    if (droppedCount > 0) {
      throw new EventBufferFullError(
        `Event buffer full: ${droppedCount} events dropped. Consider reducing event volume or increasing maxRetryQueueSize.`,
        droppedCount,
      )
    }
  }

  private async recordFailure(): Promise<void> {
    const state = await this.getCircuitBreakerState()
    state.consecutiveFailures++
    await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, state)
    this.circuitBreakerCache = state
  }

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
   */
  async handleAlarm(): Promise<void> {
    await this.getCircuitBreakerState()

    const events = await this.ctx.storage.get<Event[]>(STORAGE_KEY_RETRY)
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
        this.options.fetchTimeoutMs,
      )

      if (response.ok) {
        await this.ctx.storage.delete(STORAGE_KEY_RETRY)
        await this.ctx.storage.delete(STORAGE_KEY_RETRY_COUNT)
        await this.recordSuccess()
      } else {
        await this.recordFailure()

        const cbState = await this.getCircuitBreakerState()
        if (this.options.maxConsecutiveFailures > 0 && cbState.consecutiveFailures >= this.options.maxConsecutiveFailures) {
          cbState.isOpen = true
          cbState.openedAt = new Date().toISOString()
          await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, cbState)
          this.circuitBreakerCache = cbState
          this.log.warn('Circuit breaker opened during retry', { consecutiveFailures: cbState.consecutiveFailures })
          await this.ctx.storage.setAlarm(Date.now() + this.options.circuitBreakerResetMs)
          return
        }

        const retryCount = (await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT)) ?? 0
        const delay = this.getRetryDelay(retryCount)
        await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
        await this.ctx.storage.setAlarm(Date.now() + delay)
      }
    } catch {
      await this.recordFailure()

      const cbState = await this.getCircuitBreakerState()
      if (this.options.maxConsecutiveFailures > 0 && cbState.consecutiveFailures >= this.options.maxConsecutiveFailures) {
        cbState.isOpen = true
        cbState.openedAt = new Date().toISOString()
        await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, cbState)
        this.circuitBreakerCache = cbState
        this.log.warn('Circuit breaker opened during retry', { consecutiveFailures: cbState.consecutiveFailures })
        await this.ctx.storage.setAlarm(Date.now() + this.options.circuitBreakerResetMs)
        return
      }

      const retryCount = (await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT)) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }
  }

  private async restoreBatch(): Promise<void> {
    try {
      const stored = await this.ctx.storage.get<Event[]>(STORAGE_KEY_BATCH)
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
   */
  async persistBatch(): Promise<void> {
    if (this.batch.length > 0) {
      await this.ctx.storage.put(STORAGE_KEY_BATCH, this.batch)
    }
  }

  get pendingCount(): number {
    return this.batch.length
  }

  get identity(): DoIdentity {
    return { ...this.doIdentity }
  }

  get droppedEvents(): number {
    return this.droppedEventCount
  }

  get circuitBreakerOpen(): boolean {
    return this.isCircuitBreakerOpen()
  }

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

    const events = await this.ctx.storage.get<Event[]>(STORAGE_KEY_RETRY)
    if (events && events.length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + 1000)
    }
  }

  async getRetryQueueStats(): Promise<{
    queueSize: number
    maxSize: number
    droppedTotal: number
    retryCount: number
  }> {
    const events = (await this.ctx.storage.get<Event[]>(STORAGE_KEY_RETRY)) ?? []
    const retryCount = (await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT)) ?? 0
    return {
      queueSize: events.length,
      maxSize: this.options.maxRetryQueueSize,
      droppedTotal: this.droppedEventCount,
      retryCount,
    }
  }
}
