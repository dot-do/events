/**
 * EventsClient - Node.js SDK for events.do
 *
 * HTTP client with batching, retry logic, and backpressure handling
 */

import type {
  Event,
  TrackEvent,
  PageEvent,
  IdentifyEvent,
  EventBatch,
  EventsClientConfig,
  SendResult,
  RetryBatch,
  ClientStats,
} from './types.js'
import { EventBufferFullError } from './types.js'

/** Default configuration values */
const DEFAULTS = {
  endpoint: 'https://events.workers.do/e',
  batchSize: 100,
  flushIntervalMs: 1000,
  maxRetries: 3,
  retryDelayMs: 1000,
  maxRetryDelayMs: 60000,
  maxRetryQueueSize: 10000,
  timeoutMs: 30000,
  debug: false,
} as const

/**
 * EventsClient - Main SDK class for tracking events
 *
 * @example
 * ```typescript
 * import { EventsClient } from '@dotdo/events-node'
 *
 * const client = new EventsClient({
 *   apiKey: 'your-api-key',
 *   onError: (err, events) => console.error('Failed to send:', err)
 * })
 *
 * // Track a custom event
 * client.track('button_click', { buttonId: 'signup' })
 *
 * // Identify a user
 * client.identify('user-123', { email: 'user@example.com', plan: 'pro' })
 *
 * // Graceful shutdown
 * await client.shutdown()
 * ```
 */
export class EventsClient {
  private queue: Event[] = []
  private retryQueue: RetryBatch[] = []
  private flushTimer: ReturnType<typeof setInterval> | null = null
  private retryTimer: ReturnType<typeof setTimeout> | null = null
  private isShuttingDown = false

  // Statistics
  private sentCount = 0
  private droppedCount = 0
  private totalRetries = 0

  // Configuration with defaults
  private readonly config: Required<
    Omit<EventsClientConfig, 'apiKey' | 'onSuccess' | 'onError' | 'onDrop'>
  > & Pick<EventsClientConfig, 'apiKey' | 'onSuccess' | 'onError' | 'onDrop'>

  constructor(config: EventsClientConfig = {}) {
    this.config = {
      endpoint: config.endpoint ?? DEFAULTS.endpoint,
      apiKey: config.apiKey,
      batchSize: config.batchSize ?? DEFAULTS.batchSize,
      flushIntervalMs: config.flushIntervalMs ?? DEFAULTS.flushIntervalMs,
      maxRetries: config.maxRetries ?? DEFAULTS.maxRetries,
      retryDelayMs: config.retryDelayMs ?? DEFAULTS.retryDelayMs,
      maxRetryDelayMs: config.maxRetryDelayMs ?? DEFAULTS.maxRetryDelayMs,
      maxRetryQueueSize: config.maxRetryQueueSize ?? DEFAULTS.maxRetryQueueSize,
      timeoutMs: config.timeoutMs ?? DEFAULTS.timeoutMs,
      onSuccess: config.onSuccess,
      onError: config.onError,
      onDrop: config.onDrop,
      debug: config.debug ?? DEFAULTS.debug,
    }

    // Start auto-flush interval
    this.flushTimer = setInterval(() => {
      this.flush().catch((err) => this.log('Flush error:', err))
    }, this.config.flushIntervalMs)

    // Ensure timer doesn't prevent process exit
    if (this.flushTimer.unref) {
      this.flushTimer.unref()
    }
  }

  /**
   * Track a custom analytics event
   *
   * @param event - Event name
   * @param properties - Event properties
   * @param userId - Optional user ID
   * @param anonymousId - Optional anonymous ID
   */
  track(
    event: string,
    properties?: Record<string, unknown>,
    userId?: string,
    anonymousId?: string
  ): void {
    const trackEvent: TrackEvent = {
      type: 'track',
      event,
      properties,
      userId,
      anonymousId,
      ts: new Date().toISOString(),
    }
    this.enqueue(trackEvent)
  }

  /**
   * Track a page view
   *
   * @param name - Page name
   * @param properties - Page properties (url, title, etc.)
   * @param userId - Optional user ID
   * @param anonymousId - Optional anonymous ID
   */
  page(
    name?: string,
    properties?: Record<string, unknown>,
    userId?: string,
    anonymousId?: string
  ): void {
    const pageEvent: PageEvent = {
      type: 'page',
      name,
      properties,
      userId,
      anonymousId,
      ts: new Date().toISOString(),
    }
    this.enqueue(pageEvent)
  }

  /**
   * Identify a user with traits
   *
   * @param userId - User ID
   * @param traits - User traits (email, name, plan, etc.)
   */
  identify(userId: string, traits?: Record<string, unknown>): void {
    const identifyEvent: IdentifyEvent = {
      type: 'identify',
      userId,
      traits,
      ts: new Date().toISOString(),
    }
    this.enqueue(identifyEvent)
  }

  /**
   * Track a raw event object
   *
   * @param event - Event object
   */
  trackRaw(event: Event): void {
    const eventWithTs = {
      ...event,
      ts: event.ts ?? new Date().toISOString(),
    }
    this.enqueue(eventWithTs)
  }

  /**
   * Manually flush all queued events
   *
   * @returns Promise that resolves when flush completes
   */
  async flush(): Promise<void> {
    // Also process any pending retries
    this.processRetryQueue()

    if (this.queue.length === 0) {
      return
    }

    const events = this.queue.splice(0)
    await this.sendEvents(events)
  }

  /**
   * Get client statistics
   */
  getStats(): ClientStats {
    return {
      queuedCount: this.queue.length,
      retryCount: this.retryQueue.reduce((sum, batch) => sum + batch.events.length, 0),
      sentCount: this.sentCount,
      droppedCount: this.droppedCount,
      totalRetries: this.totalRetries,
    }
  }

  /**
   * Gracefully shutdown the client
   * Flushes all pending events and waits for retries to complete
   *
   * @param timeoutMs - Maximum time to wait for shutdown (default: 5000ms)
   */
  async shutdown(timeoutMs = 5000): Promise<void> {
    this.isShuttingDown = true

    // Stop auto-flush timer
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }

    // Stop retry timer
    if (this.retryTimer) {
      clearTimeout(this.retryTimer)
      this.retryTimer = null
    }

    // Final flush
    await this.flush()

    // Wait for retry queue to drain (with timeout)
    const startTime = Date.now()
    while (this.retryQueue.length > 0 && Date.now() - startTime < timeoutMs) {
      await new Promise((resolve) => setTimeout(resolve, 100))
      this.processRetryQueue()
    }

    // Drop any remaining events
    if (this.retryQueue.length > 0) {
      const remainingCount = this.retryQueue.reduce((sum, b) => sum + b.events.length, 0)
      this.log(`Shutdown timeout: dropping ${remainingCount} events`)
      for (const batch of this.retryQueue) {
        this.config.onDrop?.(batch.events, 'shutdown_timeout')
        this.droppedCount += batch.events.length
      }
      this.retryQueue = []
    }
  }

  /**
   * Add an event to the queue
   */
  private enqueue(event: Event): void {
    if (this.isShuttingDown) {
      this.log('Client is shutting down, dropping event')
      this.config.onDrop?.([event], 'shutting_down')
      this.droppedCount++
      return
    }

    this.queue.push(event)

    // Auto-flush when batch size is reached
    if (this.queue.length >= this.config.batchSize) {
      this.flush().catch((err) => this.log('Auto-flush error:', err))
    }
  }

  /**
   * Send events to the server
   */
  private async sendEvents(events: Event[], attempts = 0): Promise<void> {
    if (events.length === 0) return

    const batch: EventBatch = { events }
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.config.timeoutMs)

    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      }
      if (this.config.apiKey) {
        headers['Authorization'] = `Bearer ${this.config.apiKey}`
      }

      const response = await fetch(this.config.endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify(batch),
        signal: controller.signal,
      })

      clearTimeout(timeout)

      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unknown error')
        throw new Error(`HTTP ${response.status}: ${errorText}`)
      }

      const result: SendResult = await response.json()
      this.sentCount += events.length
      this.log(`Sent ${events.length} events successfully`)
      this.config.onSuccess?.(events, result)
    } catch (err) {
      clearTimeout(timeout)
      const error = err instanceof Error ? err : new Error(String(err))

      // Handle abort (timeout)
      if (error.name === 'AbortError') {
        error.message = `Request timeout after ${this.config.timeoutMs}ms`
      }

      this.log(`Send failed (attempt ${attempts + 1}):`, error.message)
      this.queueForRetry(events, attempts, error)
    }
  }

  /**
   * Queue events for retry with exponential backoff
   */
  private queueForRetry(events: Event[], attempts: number, error: Error): void {
    if (attempts >= this.config.maxRetries) {
      this.log(`Max retries exceeded, dropping ${events.length} events`)
      this.config.onError?.(
        new Error(`Failed to send events after ${attempts} attempts: ${error.message}`),
        events
      )
      this.droppedCount += events.length
      return
    }

    // Check retry queue capacity
    const currentRetryCount = this.retryQueue.reduce((sum, b) => sum + b.events.length, 0)
    const available = this.config.maxRetryQueueSize - currentRetryCount

    if (available <= 0) {
      this.log(`Retry queue full, dropping ${events.length} events`)
      this.config.onDrop?.(events, 'retry_queue_full')
      this.droppedCount += events.length
      throw new EventBufferFullError(
        `Event buffer full: ${events.length} events dropped`,
        events.length
      )
    }

    let eventsToQueue = events
    if (events.length > available) {
      // Keep newest events, drop oldest
      const dropped = events.slice(0, events.length - available)
      eventsToQueue = events.slice(-available)
      this.log(`Retry queue nearly full, dropping ${dropped.length} oldest events`)
      this.config.onDrop?.(dropped, 'retry_queue_overflow')
      this.droppedCount += dropped.length
    }

    // Calculate exponential backoff delay
    const delay = Math.min(
      this.config.retryDelayMs * Math.pow(2, attempts),
      this.config.maxRetryDelayMs
    )
    // Add jitter (10% of delay)
    const jitter = Math.random() * delay * 0.1
    const nextRetry = Date.now() + delay + jitter

    this.retryQueue.push({
      events: eventsToQueue,
      attempts: attempts + 1,
      nextRetry,
    })

    this.totalRetries++
    this.scheduleRetry()
  }

  /**
   * Schedule the next retry timer
   */
  private scheduleRetry(): void {
    if (this.retryTimer || this.retryQueue.length === 0) {
      return
    }

    const now = Date.now()
    const nextBatch = this.retryQueue[0]
    if (!nextBatch) return

    const delay = Math.max(0, nextBatch.nextRetry - now)

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null
      this.processRetryQueue()
    }, delay)

    // Ensure timer doesn't prevent process exit
    if (this.retryTimer.unref) {
      this.retryTimer.unref()
    }
  }

  /**
   * Process pending retry batches
   */
  private processRetryQueue(): void {
    if (this.retryQueue.length === 0) return

    const now = Date.now()
    const readyBatches: RetryBatch[] = []

    // Find all batches ready to retry
    while (this.retryQueue.length > 0 && this.retryQueue[0].nextRetry <= now) {
      const batch = this.retryQueue.shift()
      if (batch) readyBatches.push(batch)
    }

    // Process ready batches
    for (const batch of readyBatches) {
      this.sendEvents(batch.events, batch.attempts).catch((err) =>
        this.log('Retry send error:', err)
      )
    }

    // Schedule next retry if queue not empty
    if (this.retryQueue.length > 0) {
      this.scheduleRetry()
    }
  }

  /**
   * Debug logging
   */
  private log(...args: unknown[]): void {
    if (this.config.debug) {
      console.log('[events-node]', ...args)
    }
  }
}
