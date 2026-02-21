/**
 * EventEmitter - Pipeline-first batched event emission for Durable Objects
 *
 * Happy path: emit → buffer → pipeline.send() → done (zero storage I/O)
 * Failure path: pipeline.send() fails → persist to DO storage → alarm retry
 */

import type { Event, EventEmitterOptions, EventLogger, PipelineLike, ResolvedEmitterOptions } from './types.js'
import { EventBufferFullError, CircuitBreakerOpenError } from './types.js'
import { ulid } from './ulid.js'
import {
  DEFAULT_BATCH_SIZE,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_RETRY_QUEUE_SIZE,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_CIRCUIT_BREAKER_RESET_MS,
  MAX_CIRCUIT_BREAKER_RESET_MS,
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  RETRY_JITTER_MS,
  STORAGE_KEY_RETRY,
  STORAGE_KEY_RETRY_COUNT,
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

/** Circuit breaker state stored in DO storage */
interface CircuitBreakerState {
  consecutiveFailures: number
  openedAt?: string | undefined
  isOpen: boolean
}

const defaultLogger: EventLogger = {
  debug: (message, context) => console.debug(JSON.stringify({ timestamp: new Date().toISOString(), level: 'debug', message, context })),
  info: (message, context) => console.info(JSON.stringify({ timestamp: new Date().toISOString(), level: 'info', message, context })),
  warn: (message, context) => console.warn(JSON.stringify({ timestamp: new Date().toISOString(), level: 'warn', message, context })),
  error: (message, context) => console.error(JSON.stringify({ timestamp: new Date().toISOString(), level: 'error', message, context })),
}

export class EventEmitter {
  private buffer: Event[] = []
  private flushTimer: ReturnType<typeof setTimeout> | undefined = undefined
  private doIdentity: DoIdentity
  private namespace: string
  private circuitBreakerCache: CircuitBreakerState | null = null
  private droppedEventCount = 0

  private pipeline: PipelineLike
  private ctx: DurableObjectState | undefined
  private options: ResolvedEmitterOptions
  private log: EventLogger

  constructor(
    pipeline: PipelineLike,
    options?: Omit<EventEmitterOptions, 'pipeline'>,
    ctx?: DurableObjectState,
  ) {
    this.pipeline = pipeline
    this.ctx = ctx
    this.log = options?.logger ?? defaultLogger
    this.options = {
      batchSize: options?.batchSize ?? DEFAULT_BATCH_SIZE,
      flushIntervalMs: options?.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      cdc: options?.cdc ?? false,
      trackPrevious: options?.trackPrevious ?? false,
      maxRetryQueueSize: options?.maxRetryQueueSize ?? DEFAULT_MAX_RETRY_QUEUE_SIZE,
      maxConsecutiveFailures: options?.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_FAILURES,
      circuitBreakerResetMs: Math.min(options?.circuitBreakerResetMs ?? DEFAULT_CIRCUIT_BREAKER_RESET_MS, MAX_CIRCUIT_BREAKER_RESET_MS),
    }

    this.doIdentity = ctx
      ? { id: ctx.id.toString(), name: ctx.id.name }
      : { id: 'unknown' }
    this.namespace = ctx?.id.name ?? 'default'
  }

  /**
   * Emit an event to be batched and sent via Pipeline.
   */
  emit(input: EmitInput): void {
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

    this.buffer.push(event)

    if (this.buffer.length >= this.options.batchSize) {
      this.flush().catch((error) => {
        this.log.error('Failed to flush events', { error: error instanceof Error ? error.message : String(error) })
      })
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flush().catch((error) => {
          this.log.error('Failed to flush events', { error: error instanceof Error ? error.message : String(error) })
        })
      }, this.options.flushIntervalMs)
    }
  }

  /**
   * Emit a CDC (Change Data Capture) event for an entity mutation.
   * Only emits if the `cdc` option is enabled.
   */
  emitChange(op: 'created' | 'updated' | 'deleted', noun: string, docId: string, doc?: Record<string, unknown>, prev?: Record<string, unknown>): void {
    if (!this.options.cdc) return

    this.getBookmarkAndEmit(op, noun, docId, doc, prev).catch((error) => {
      this.log.error('Failed to emit CDC event', { error: error instanceof Error ? error.message : String(error), noun, docId })
    })
  }

  private async getBookmarkAndEmit(
    op: 'created' | 'updated' | 'deleted',
    noun: string,
    docId: string,
    doc?: Record<string, unknown>,
    prev?: Record<string, unknown>,
  ): Promise<void> {
    let bookmark: string | undefined
    if (this.ctx) {
      try {
        bookmark = await this.ctx.storage.getCurrentBookmark()
      } catch (error) {
        this.log.warn('Failed to get SQLite bookmark', { error: error instanceof Error ? error.message : String(error) })
      }
    }

    this.emit({
      type: 'cdc',
      event: `${noun}.${op}`,
      data: { type: noun, id: docId, ...(doc ?? {}) },
      meta: {
        ...(this.options.trackPrevious && prev ? { prev } : {}),
        ...(bookmark ? { bookmark } : {}),
      },
    })
  }

  /**
   * Immediately flush all pending events via Pipeline.
   * On success: zero storage I/O.
   * On failure: schedule retry via DO storage + alarm (if ctx available).
   */
  async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }

    if (this.buffer.length === 0) return

    if (this.isCircuitOpen()) {
      const state = this.circuitBreakerCache!
      const resetAt = new Date(new Date(state.openedAt!).getTime() + this.options.circuitBreakerResetMs)
      throw new CircuitBreakerOpenError(
        `Circuit breaker is open after ${state.consecutiveFailures} consecutive failures. Will reset at ${resetAt.toISOString()}`,
        state.consecutiveFailures,
        resetAt,
      )
    }

    const events = this.buffer
    this.buffer = []

    try {
      await this.sendToPipeline(events)
      this.recordSuccess()
    } catch {
      if (this.ctx) {
        await this.scheduleRetry(events)
      } else {
        // No DO context — put events back in buffer for next attempt
        this.buffer.unshift(...events)
        this.log.error('Pipeline send failed (no DO context for retry)', { count: events.length })
      }
    }
  }

  /**
   * Handle the Durable Object alarm to retry failed Pipeline sends.
   */
  async handleAlarm(): Promise<void> {
    if (!this.ctx) return

    const events = await this.ctx.storage.get<Event[]>(STORAGE_KEY_RETRY)
    if (!events || events.length === 0) return

    try {
      await this.sendToPipeline(events)
      await this.ctx.storage.delete(STORAGE_KEY_RETRY)
      await this.ctx.storage.delete(STORAGE_KEY_RETRY_COUNT)
      this.recordSuccess()
    } catch {
      await this.recordFailure()

      if (this.options.maxConsecutiveFailures > 0 && this.getFailureCount() >= this.options.maxConsecutiveFailures) {
        this.circuitBreakerCache = {
          consecutiveFailures: this.getFailureCount(),
          isOpen: true,
          openedAt: new Date().toISOString(),
        }
        await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, this.circuitBreakerCache)
        this.log.warn('Circuit breaker opened during retry', { consecutiveFailures: this.getFailureCount() })
        await this.ctx.storage.setAlarm(Date.now() + this.options.circuitBreakerResetMs)
        return
      }

      const retryCount = (await this.ctx.storage.get<number>(STORAGE_KEY_RETRY_COUNT)) ?? 0
      const delay = this.getRetryDelay(retryCount)
      await this.ctx.storage.put(STORAGE_KEY_RETRY_COUNT, retryCount + 1)
      await this.ctx.storage.setAlarm(Date.now() + delay)
    }
  }

  // ---------------------------------------------------------------------------
  // State accessors
  // ---------------------------------------------------------------------------

  get pendingCount(): number {
    return this.buffer.length
  }

  get identity(): DoIdentity {
    return { ...this.doIdentity }
  }

  get circuitBreakerOpen(): boolean {
    return this.isCircuitOpen()
  }

  // ---------------------------------------------------------------------------
  // Private
  // ---------------------------------------------------------------------------

  private async sendToPipeline(events: Event[]): Promise<void> {
    await this.pipeline.send(events as unknown as Record<string, unknown>[])
  }

  private async scheduleRetry(events: Event[]): Promise<void> {
    if (!this.ctx) return

    await this.recordFailure()

    if (this.options.maxConsecutiveFailures > 0 && this.getFailureCount() >= this.options.maxConsecutiveFailures) {
      this.circuitBreakerCache = {
        consecutiveFailures: this.getFailureCount(),
        isOpen: true,
        openedAt: new Date().toISOString(),
      }
      await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, this.circuitBreakerCache)
      this.log.warn('Circuit breaker opened', { consecutiveFailures: this.getFailureCount() })
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

  private getRetryDelay(retryCount: number): number {
    const exponentialDelay = Math.min(RETRY_BASE_DELAY_MS * Math.pow(2, retryCount), RETRY_MAX_DELAY_MS)
    const jitter = Math.random() * RETRY_JITTER_MS
    return exponentialDelay + jitter
  }

  private isCircuitOpen(): boolean {
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

  private getFailureCount(): number {
    return this.circuitBreakerCache?.consecutiveFailures ?? 0
  }

  private recordSuccess(): void {
    if (this.circuitBreakerCache && (this.circuitBreakerCache.consecutiveFailures > 0 || this.circuitBreakerCache.isOpen)) {
      this.circuitBreakerCache = { consecutiveFailures: 0, isOpen: false }
      if (this.ctx) {
        this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, this.circuitBreakerCache).catch(() => {})
      }
      this.log.info('Circuit breaker closed after successful delivery')
    }
  }

  private async recordFailure(): Promise<void> {
    if (!this.circuitBreakerCache) {
      if (this.ctx) {
        const stored = await this.ctx.storage.get<CircuitBreakerState>(STORAGE_KEY_CIRCUIT_BREAKER)
        this.circuitBreakerCache = stored ?? { consecutiveFailures: 0, isOpen: false }
      } else {
        this.circuitBreakerCache = { consecutiveFailures: 0, isOpen: false }
      }
    }
    this.circuitBreakerCache.consecutiveFailures++
    if (this.ctx) {
      await this.ctx.storage.put(STORAGE_KEY_CIRCUIT_BREAKER, this.circuitBreakerCache)
    }
  }
}
