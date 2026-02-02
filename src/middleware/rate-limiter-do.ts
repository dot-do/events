/**
 * RateLimiterDO - Durable Object for rate limiting state
 *
 * Uses SQLite storage with a sliding window algorithm.
 * Each DO instance handles rate limiting for a single key (API key or IP).
 *
 * Schema:
 * - rate_limit_windows: stores request/event counts per minute window
 *
 * The sliding window approach:
 * - Uses 1-minute buckets keyed by timestamp (floor to minute)
 * - On each check, sums counts from current and previous minute
 * - Weights previous minute by how much of current minute has passed
 * - Provides smooth rate limiting without hard cliff edges
 */

import { DurableObject } from 'cloudflare:workers'

// Use Record type to satisfy SqlStorage.exec constraints
type RateLimitWindowRow = Record<string, SqlStorageValue> & {
  windowStart: number // Unix timestamp (ms) of window start
  requestCount: number
  eventCount: number
}

export interface RateLimitCheckResult {
  allowed: boolean
  requestsRemaining: number
  eventsRemaining: number
  resetAt: number
  retryAfterSeconds?: number
}

export class RateLimiterDO extends DurableObject<Record<string, unknown>> {
  private sql!: SqlStorage
  private initialized = false

  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env)
    this.sql = ctx.storage.sql
  }

  private ensureInitialized(): void {
    if (this.initialized) return

    // Create the rate limit windows table
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS rate_limit_windows (
        window_start INTEGER PRIMARY KEY,
        request_count INTEGER NOT NULL DEFAULT 0,
        event_count INTEGER NOT NULL DEFAULT 0
      )
    `)

    // Create index for efficient cleanup
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_window_start ON rate_limit_windows(window_start)
    `)

    this.initialized = true
  }

  /**
   * Get the start of the current minute window
   */
  private getCurrentWindowStart(): number {
    const now = Date.now()
    return Math.floor(now / 60000) * 60000
  }

  /**
   * Get rate limit counts using sliding window algorithm.
   * Returns weighted sum of current and previous window.
   */
  private getSlidingWindowCounts(): { requestCount: number; eventCount: number } {
    const now = Date.now()
    const currentWindowStart = this.getCurrentWindowStart()
    const prevWindowStart = currentWindowStart - 60000

    // Get current and previous window data
    const result = this.sql.exec<RateLimitWindowRow>(`
      SELECT window_start as windowStart, request_count as requestCount, event_count as eventCount
      FROM rate_limit_windows
      WHERE window_start IN (?, ?)
    `, currentWindowStart, prevWindowStart)

    let currentWindow: RateLimitWindowRow | undefined
    let prevWindow: RateLimitWindowRow | undefined

    for (const row of result) {
      if (row.windowStart === currentWindowStart) {
        currentWindow = row
      } else if (row.windowStart === prevWindowStart) {
        prevWindow = row
      }
    }

    // Calculate weight for sliding window
    // Weight is how much of the current minute has passed (0 to 1)
    const elapsedInCurrentMinute = now - currentWindowStart
    const weight = elapsedInCurrentMinute / 60000

    // Sliding window: current + (1 - weight) * previous
    const currentRequests = currentWindow?.requestCount || 0
    const prevRequests = prevWindow?.requestCount || 0
    const currentEvents = currentWindow?.eventCount || 0
    const prevEvents = prevWindow?.eventCount || 0

    return {
      requestCount: currentRequests + Math.floor((1 - weight) * prevRequests),
      eventCount: currentEvents + Math.floor((1 - weight) * prevEvents),
    }
  }

  /**
   * Check rate limit and increment counters if allowed.
   *
   * @param requestsPerMinute - Maximum requests allowed per minute
   * @param eventsPerMinute - Maximum events allowed per minute
   * @param eventCount - Number of events in this batch
   * @returns Rate limit check result
   */
  async checkAndIncrement(
    requestsPerMinute: number,
    eventsPerMinute: number,
    eventCount: number,
  ): Promise<RateLimitCheckResult> {
    this.ensureInitialized()

    const currentWindowStart = this.getCurrentWindowStart()
    const resetAt = currentWindowStart + 60000

    // Get current sliding window counts
    const { requestCount, eventCount: currentEventCount } = this.getSlidingWindowCounts()

    // Check if rate limit would be exceeded
    const wouldExceedRequests = requestCount + 1 > requestsPerMinute
    const wouldExceedEvents = currentEventCount + eventCount > eventsPerMinute

    if (wouldExceedRequests || wouldExceedEvents) {
      // Calculate retry after (seconds until window reset)
      const now = Date.now()
      const retryAfterSeconds = Math.ceil((resetAt - now) / 1000)

      return {
        allowed: false,
        requestsRemaining: Math.max(0, requestsPerMinute - requestCount),
        eventsRemaining: Math.max(0, eventsPerMinute - currentEventCount),
        resetAt,
        retryAfterSeconds: Math.max(1, retryAfterSeconds),
      }
    }

    // Increment counters in current window
    this.sql.exec(`
      INSERT INTO rate_limit_windows (window_start, request_count, event_count)
      VALUES (?, 1, ?)
      ON CONFLICT(window_start) DO UPDATE SET
        request_count = request_count + 1,
        event_count = event_count + ?
    `, currentWindowStart, eventCount, eventCount)

    // Cleanup old windows (keep last 5 minutes for safety)
    const cleanupThreshold = currentWindowStart - 5 * 60000
    this.sql.exec(`
      DELETE FROM rate_limit_windows WHERE window_start < ?
    `, cleanupThreshold)

    return {
      allowed: true,
      requestsRemaining: Math.max(0, requestsPerMinute - requestCount - 1),
      eventsRemaining: Math.max(0, eventsPerMinute - currentEventCount - eventCount),
      resetAt,
    }
  }

  /**
   * Get current rate limit status without incrementing.
   */
  async getStatus(
    requestsPerMinute: number,
    eventsPerMinute: number,
  ): Promise<RateLimitCheckResult> {
    this.ensureInitialized()

    const currentWindowStart = this.getCurrentWindowStart()
    const resetAt = currentWindowStart + 60000
    const { requestCount, eventCount } = this.getSlidingWindowCounts()

    return {
      allowed: requestCount < requestsPerMinute && eventCount < eventsPerMinute,
      requestsRemaining: Math.max(0, requestsPerMinute - requestCount),
      eventsRemaining: Math.max(0, eventsPerMinute - eventCount),
      resetAt,
    }
  }

  /**
   * Reset rate limit counters (for testing/admin purposes).
   */
  async reset(): Promise<void> {
    this.ensureInitialized()
    this.sql.exec(`DELETE FROM rate_limit_windows`)
  }

  // ──────────────────────────────────────────────────────────────────────────
  // HTTP Fetch Handler - Health Check
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Handle HTTP requests to the DO
   * GET /health - Returns health diagnostics with internal state metrics
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname === '/health' || url.pathname === '/diagnostics') {
      this.ensureInitialized()

      const now = Date.now()
      const currentWindowStart = this.getCurrentWindowStart()
      const resetAt = currentWindowStart + 60000

      // Get current sliding window counts
      const { requestCount, eventCount } = this.getSlidingWindowCounts()

      // Count total windows stored
      const windowCountResult = this.sql.exec<Record<string, SqlStorageValue> & { count: number }>(`
        SELECT COUNT(*) as count FROM rate_limit_windows
      `)
      const windowCount = Array.from(windowCountResult)[0]?.count ?? 0

      // Get the oldest and newest window timestamps
      const windowRangeResult = this.sql.exec<Record<string, SqlStorageValue> & {
        minWindow: number | null
        maxWindow: number | null
      }>(`
        SELECT MIN(window_start) as minWindow, MAX(window_start) as maxWindow FROM rate_limit_windows
      `)
      const windowRange = Array.from(windowRangeResult)[0]

      // Calculate total requests and events across all windows
      const totalsResult = this.sql.exec<Record<string, SqlStorageValue> & {
        totalRequests: number
        totalEvents: number
      }>(`
        SELECT
          COALESCE(SUM(request_count), 0) as totalRequests,
          COALESCE(SUM(event_count), 0) as totalEvents
        FROM rate_limit_windows
      `)
      const totals = Array.from(totalsResult)[0]

      const health = {
        status: 'healthy',
        initialized: this.initialized,
        currentWindow: {
          start: new Date(currentWindowStart).toISOString(),
          requestCount,
          eventCount,
          resetAt: new Date(resetAt).toISOString(),
          timeUntilResetMs: Math.max(0, resetAt - now),
        },
        storage: {
          windowCount,
          oldestWindow: windowRange?.minWindow
            ? new Date(windowRange.minWindow).toISOString()
            : null,
          newestWindow: windowRange?.maxWindow
            ? new Date(windowRange.maxWindow).toISOString()
            : null,
        },
        totals: {
          allTimeRequests: totals?.totalRequests ?? 0,
          allTimeEvents: totals?.totalEvents ?? 0,
        },
        timestamp: new Date(now).toISOString(),
      }

      return new Response(JSON.stringify(health, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response('Not Found', { status: 404 })
  }
}
