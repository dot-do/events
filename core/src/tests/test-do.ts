/**
 * Test Durable Object for EventEmitter — provides real DurableObjectState
 *
 * Uses vitest-pool-workers (workerd runtime) instead of mocks.
 * The TestPipeline is a simple test double for PipelineLike,
 * NOT a Cloudflare runtime mock — it implements the application-level interface.
 */

import { DurableObject } from 'cloudflare:workers'
import { EventEmitter, type EmitInput } from '../emitter.js'
import type { PipelineLike, EventEmitterOptions } from '../types.js'
import { EventBufferFullError, CircuitBreakerOpenError } from '../types.js'

export interface TestEnv {
  EVENT_EMITTER_TEST: DurableObjectNamespace<EventEmitterTestDO>
}

// ---------------------------------------------------------------------------
// TestPipeline — controllable PipelineLike for test scenarios
// ---------------------------------------------------------------------------

class TestPipeline implements PipelineLike {
  sentBatches: Record<string, unknown>[][] = []
  sendCallCount = 0
  private _error: Error | null = null

  async send(records: Record<string, unknown>[]): Promise<void> {
    this.sendCallCount++
    if (this._error) throw this._error
    this.sentBatches.push(records.map((r) => ({ ...r })))
  }

  setError(error: Error | null) {
    this._error = error
  }

  clear() {
    this.sentBatches = []
    this.sendCallCount = 0
    this._error = null
  }
}

// ---------------------------------------------------------------------------
// EventEmitterTestDO — wraps EventEmitter with real DurableObjectState
// ---------------------------------------------------------------------------

export class EventEmitterTestDO extends DurableObject<TestEnv> {
  private emitter!: EventEmitter
  private pipeline: TestPipeline = new TestPipeline()

  /** Create a fresh EventEmitter with the given options */
  setup(options?: Partial<Omit<EventEmitterOptions, 'pipeline'>>): { id: string; name?: string } {
    this.pipeline.clear()
    this.emitter = new EventEmitter(this.pipeline, options, this.ctx)
    return { id: this.ctx.id.toString(), name: this.ctx.id.name }
  }

  // -- Core operations ------------------------------------------------------

  emit(input: EmitInput): number {
    this.emitter.emit(input)
    return this.emitter.pendingCount
  }

  async flush(): Promise<void> {
    await this.emitter.flush()
  }

  /** Flush without throwing — returns error info for assertions */
  async tryFlush(): Promise<{
    ok: boolean
    errorType?: string
    message?: string
    droppedCount?: number
    consecutiveFailures?: number
    resetAt?: string
  }> {
    try {
      await this.emitter.flush()
      return { ok: true }
    } catch (e) {
      if (e instanceof CircuitBreakerOpenError) {
        return {
          ok: false,
          errorType: 'CircuitBreakerOpenError',
          message: e.message,
          consecutiveFailures: e.consecutiveFailures,
          resetAt: e.resetAt.toISOString(),
        }
      }
      if (e instanceof EventBufferFullError) {
        return {
          ok: false,
          errorType: 'EventBufferFullError',
          message: e.message,
          droppedCount: e.droppedCount,
        }
      }
      return { ok: false, errorType: 'Error', message: (e as Error).message }
    }
  }

  async handleAlarm(): Promise<void> {
    await this.emitter.handleAlarm()
  }

  /** DO alarm handler — delegates to EventEmitter.handleAlarm() */
  override async alarm(): Promise<void> {
    if (this.emitter) {
      await this.emitter.handleAlarm()
    }
  }

  // -- State accessors ------------------------------------------------------

  getPendingCount(): number {
    return this.emitter.pendingCount
  }

  getIdentity(): { id: string; name?: string; class?: string; colo?: string; worker?: string } {
    return this.emitter.identity
  }

  isCircuitBreakerOpen(): boolean {
    return this.emitter.circuitBreakerOpen
  }

  // -- Pipeline inspection --------------------------------------------------

  getSentBatches(): Record<string, unknown>[][] {
    return this.pipeline.sentBatches
  }

  getSendCallCount(): number {
    return this.pipeline.sendCallCount
  }

  setPipelineError(message: string | null): void {
    this.pipeline.setError(message ? new Error(message) : null)
  }

  clearPipeline(): void {
    this.pipeline.clear()
  }

  // -- CDC ------------------------------------------------------------------

  async emitChange(
    op: 'created' | 'updated' | 'deleted',
    noun: string,
    docId: string,
    doc?: Record<string, unknown>,
    prev?: Record<string, unknown>,
  ): Promise<void> {
    this.emitter.emitChange(op, noun, docId, doc, prev)
    // emitChange internally fires getBookmarkAndEmit() which does
    // `await ctx.storage.getCurrentBookmark()` then `this.emit()`.
    // We must wait for that to complete so the storage access stays
    // within this RPC call's isolated storage frame.
    await new Promise<void>((resolve) => setTimeout(resolve, 10))
  }

  // -- Cleanup (must be called in afterEach to prevent isolated storage errors)

  async cleanup(): Promise<void> {
    // Clear pipeline errors so flush won't fail
    this.pipeline.setError(null)
    // Flush pending events — this clears the internal flush timer
    if (this.emitter && this.emitter.pendingCount > 0) {
      try {
        await this.emitter.flush()
      } catch {
        // Ignore errors during cleanup
      }
    }
    // Cancel any pending alarms to prevent cross-test storage access
    await this.ctx.storage.deleteAlarm()
  }

  // -- Storage inspection (for retry/circuit-breaker assertions) -------------

  async getStorageValue<T = unknown>(key: string): Promise<T | undefined> {
    return await this.ctx.storage.get<T>(key)
  }

  async setStorageValue(key: string, value: unknown): Promise<void> {
    await this.ctx.storage.put(key, value)
  }

  async getAlarmTime(): Promise<number | null> {
    return await this.ctx.storage.getAlarm()
  }
}
