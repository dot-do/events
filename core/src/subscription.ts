/**
 * SubscriptionDO - Event Subscription and Delivery Management
 *
 * A Durable Object that manages event subscriptions and delivery tracking.
 * Uses SQLite for persistence with proper indexes for performance.
 *
 * Key concepts:
 * - Subscriptions: Workers subscribing to event patterns via RPC or HTTP
 * - Deliveries: Tracking of event delivery attempts to subscribers
 * - Dead Letters: Failed deliveries after max retries
 * - Pattern Matching: Glob-style patterns for event type matching
 */

import { DurableObject } from 'cloudflare:workers'
import { findMatchingSubscriptions } from './pattern-matcher.js'
import { ulid } from './ulid.js'

// ============================================================================
// Types
// ============================================================================

export interface Subscription {
  id: string
  workerId: string
  workerBinding?: string  // null = HTTP fallback
  pattern: string
  patternPrefix: string   // for indexing
  rpcMethod: string
  maxRetries: number
  timeoutMs: number
  active: boolean
  createdAt: number
  updatedAt: number
}

export interface Delivery {
  id: string
  subscriptionId: string
  eventId: string
  eventType: string
  eventPayload: string  // JSON
  status: 'pending' | 'delivered' | 'failed' | 'dead'
  attemptCount: number
  nextAttemptAt?: number
  lastError?: string
  createdAt: number
  deliveredAt?: number
}

export interface DeliveryLog {
  id: string
  deliveryId: string
  subscriptionId: string
  attemptNumber: number
  status: string
  durationMs?: number
  errorMessage?: string
  workerResponse?: string
  createdAt: number
}

export interface DeadLetter {
  id: string
  deliveryId: string
  subscriptionId: string
  eventId: string
  eventPayload: string
  reason: string
  lastError?: string
  createdAt: number
}

export interface SubscriptionStats {
  pendingDeliveries: number
  failedDeliveries: number
  deadLetters: number
  successRate: number
  totalDelivered: number
  totalAttempts: number
}

// Env type - users should extend this for their specific bindings
// eslint-disable-next-line @typescript-eslint/no-empty-interface
interface Env {
  // Service bindings will be added dynamically
  [key: string]: unknown
}

// ============================================================================
// SubscriptionDO
// ============================================================================

export class SubscriptionDO extends DurableObject<Env> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.initSchema()
  }

  /**
   * Initialize SQLite schema with tables and indexes
   */
  private initSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS subscriptions (
        id TEXT PRIMARY KEY,
        worker_id TEXT NOT NULL,
        worker_binding TEXT,
        pattern TEXT NOT NULL,
        pattern_prefix TEXT NOT NULL,
        rpc_method TEXT NOT NULL,
        max_retries INTEGER DEFAULT 5,
        timeout_ms INTEGER DEFAULT 30000,
        active INTEGER DEFAULT 1,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        UNIQUE(worker_id, pattern, rpc_method)
      );

      CREATE INDEX IF NOT EXISTS idx_subscriptions_active
        ON subscriptions(active) WHERE active = 1;

      CREATE INDEX IF NOT EXISTS idx_subscriptions_prefix
        ON subscriptions(pattern_prefix);

      CREATE INDEX IF NOT EXISTS idx_subscriptions_worker
        ON subscriptions(worker_id);

      CREATE TABLE IF NOT EXISTS deliveries (
        id TEXT PRIMARY KEY,
        subscription_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_payload TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        attempt_count INTEGER DEFAULT 0,
        next_attempt_at INTEGER,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        delivered_at INTEGER,
        FOREIGN KEY (subscription_id) REFERENCES subscriptions(id),
        UNIQUE(subscription_id, event_id)
      );

      CREATE INDEX IF NOT EXISTS idx_deliveries_pending
        ON deliveries(status, next_attempt_at)
        WHERE status IN ('pending', 'failed');

      CREATE INDEX IF NOT EXISTS idx_deliveries_subscription
        ON deliveries(subscription_id);

      CREATE TABLE IF NOT EXISTS delivery_log (
        id TEXT PRIMARY KEY,
        delivery_id TEXT NOT NULL,
        subscription_id TEXT NOT NULL,
        attempt_number INTEGER NOT NULL,
        status TEXT NOT NULL,
        duration_ms INTEGER,
        error_message TEXT,
        worker_response TEXT,
        created_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_delivery_log_delivery
        ON delivery_log(delivery_id);

      CREATE TABLE IF NOT EXISTS dead_letters (
        id TEXT PRIMARY KEY,
        delivery_id TEXT NOT NULL,
        subscription_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        event_payload TEXT NOT NULL,
        reason TEXT NOT NULL,
        last_error TEXT,
        created_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_dead_letters_subscription
        ON dead_letters(subscription_id);
    `)
  }

  // ---------------------------------------------------------------------------
  // Subscription Management
  // ---------------------------------------------------------------------------

  /**
   * Create a new subscription
   */
  async subscribe(params: {
    workerId: string
    workerBinding?: string
    pattern: string
    rpcMethod: string
    maxRetries?: number
    timeoutMs?: number
  }): Promise<{ ok: true; subscriptionId: string } | { ok: false; error: string }> {
    const patternPrefix = this.extractPrefix(params.pattern)
    const id = ulid()
    const now = Date.now()

    try {
      this.sql.exec(
        `INSERT INTO subscriptions
         (id, worker_id, worker_binding, pattern, pattern_prefix, rpc_method, max_retries, timeout_ms, active, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
        id,
        params.workerId,
        params.workerBinding ?? null,
        params.pattern,
        patternPrefix,
        params.rpcMethod,
        params.maxRetries ?? 5,
        params.timeoutMs ?? 30000,
        now,
        now
      )
      return { ok: true, subscriptionId: id }
    } catch (e) {
      const error = e instanceof Error ? e.message : 'Unknown error'
      // Check for unique constraint violation
      if (error.includes('UNIQUE constraint')) {
        return { ok: false, error: 'Subscription already exists for this worker/pattern/method combination' }
      }
      return { ok: false, error }
    }
  }

  /**
   * Deactivate a subscription (soft delete)
   */
  async unsubscribe(subscriptionId: string): Promise<{ ok: boolean }> {
    this.sql.exec(
      `UPDATE subscriptions SET active = 0, updated_at = ? WHERE id = ?`,
      Date.now(),
      subscriptionId
    )
    return { ok: true }
  }

  /**
   * Reactivate a previously deactivated subscription
   */
  async reactivate(subscriptionId: string): Promise<{ ok: boolean }> {
    this.sql.exec(
      `UPDATE subscriptions SET active = 1, updated_at = ? WHERE id = ?`,
      Date.now(),
      subscriptionId
    )
    return { ok: true }
  }

  /**
   * Permanently delete a subscription and all related data
   */
  async deleteSubscription(subscriptionId: string): Promise<{ ok: boolean; deleted: { deliveries: number; logs: number; deadLetters: number } }> {
    // Delete in order due to foreign key-like relationships
    const deadLettersDeleted = this.sql.exec(
      `DELETE FROM dead_letters WHERE subscription_id = ?`,
      subscriptionId
    ).rowsWritten

    const logsDeleted = this.sql.exec(
      `DELETE FROM delivery_log WHERE subscription_id = ?`,
      subscriptionId
    ).rowsWritten

    const deliveriesDeleted = this.sql.exec(
      `DELETE FROM deliveries WHERE subscription_id = ?`,
      subscriptionId
    ).rowsWritten

    this.sql.exec(
      `DELETE FROM subscriptions WHERE id = ?`,
      subscriptionId
    )

    return {
      ok: true,
      deleted: {
        deliveries: deliveriesDeleted,
        logs: logsDeleted,
        deadLetters: deadLettersDeleted,
      },
    }
  }

  /**
   * Update subscription settings
   */
  async updateSubscription(
    subscriptionId: string,
    updates: {
      maxRetries?: number
      timeoutMs?: number
      rpcMethod?: string
    }
  ): Promise<{ ok: boolean }> {
    const setClauses: string[] = ['updated_at = ?']
    const params: unknown[] = [Date.now()]

    if (updates.maxRetries !== undefined) {
      setClauses.push('max_retries = ?')
      params.push(updates.maxRetries)
    }
    if (updates.timeoutMs !== undefined) {
      setClauses.push('timeout_ms = ?')
      params.push(updates.timeoutMs)
    }
    if (updates.rpcMethod !== undefined) {
      setClauses.push('rpc_method = ?')
      params.push(updates.rpcMethod)
    }

    params.push(subscriptionId)

    this.sql.exec(
      `UPDATE subscriptions SET ${setClauses.join(', ')} WHERE id = ?`,
      ...params
    )
    return { ok: true }
  }

  /**
   * List subscriptions with optional filters
   */
  async listSubscriptions(options?: {
    active?: boolean
    workerId?: string
    patternPrefix?: string
    limit?: number
    offset?: number
  }): Promise<Subscription[]> {
    let query = 'SELECT * FROM subscriptions WHERE 1=1'
    const params: unknown[] = []

    if (options?.active !== undefined) {
      query += ' AND active = ?'
      params.push(options.active ? 1 : 0)
    }
    if (options?.workerId) {
      query += ' AND worker_id = ?'
      params.push(options.workerId)
    }
    if (options?.patternPrefix) {
      query += ' AND pattern_prefix LIKE ?'
      params.push(options.patternPrefix + '%')
    }

    query += ' ORDER BY created_at DESC'

    if (options?.limit) {
      query += ' LIMIT ?'
      params.push(options.limit)
    }
    if (options?.offset) {
      query += ' OFFSET ?'
      params.push(options.offset)
    }

    return this.sql.exec(query, ...params).toArray().map(row => this.rowToSubscription(row))
  }

  /**
   * Get a single subscription by ID
   */
  async getSubscription(subscriptionId: string): Promise<Subscription | null> {
    const row = this.sql.exec(
      `SELECT * FROM subscriptions WHERE id = ?`,
      subscriptionId
    ).one()
    return row ? this.rowToSubscription(row) : null
  }

  /**
   * Find subscriptions matching an event type
   */
  async findMatchingSubscriptions(eventType: string): Promise<Subscription[]> {
    // Get all active subscriptions and filter by pattern match
    // For efficiency, first filter by prefix
    const prefix = this.extractPrefix(eventType)

    const rows = this.sql.exec(
      `SELECT * FROM subscriptions
       WHERE active = 1
       AND (pattern_prefix = ? OR pattern_prefix = '' OR ? LIKE pattern_prefix || '%')
       ORDER BY created_at`,
      prefix,
      eventType
    ).toArray()

    // Filter by full pattern match
    return rows
      .map(row => this.rowToSubscription(row))
      .filter(sub => this.matchesPattern(eventType, sub.pattern))
  }

  // ---------------------------------------------------------------------------
  // Delivery Management
  // ---------------------------------------------------------------------------

  /**
   * Create a new delivery record
   */
  async createDelivery(params: {
    subscriptionId: string
    eventId: string
    eventType: string
    eventPayload: unknown
  }): Promise<{ ok: true; deliveryId: string } | { ok: false; error: string }> {
    const id = ulid()
    const now = Date.now()

    try {
      this.sql.exec(
        `INSERT INTO deliveries
         (id, subscription_id, event_id, event_type, event_payload, status, attempt_count, next_attempt_at, created_at)
         VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)`,
        id,
        params.subscriptionId,
        params.eventId,
        params.eventType,
        JSON.stringify(params.eventPayload),
        now,  // next_attempt_at = now (ready immediately)
        now
      )
      return { ok: true, deliveryId: id }
    } catch (e) {
      const error = e instanceof Error ? e.message : 'Unknown error'
      if (error.includes('UNIQUE constraint')) {
        return { ok: false, error: 'Delivery already exists for this subscription/event combination' }
      }
      return { ok: false, error }
    }
  }

  /**
   * Get pending deliveries ready for processing
   */
  async getPendingDeliveries(limit = 100): Promise<Delivery[]> {
    const now = Date.now()
    const rows = this.sql.exec(
      `SELECT * FROM deliveries
       WHERE status IN ('pending', 'failed')
       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
       ORDER BY next_attempt_at ASC
       LIMIT ?`,
      now,
      limit
    ).toArray()

    return rows.map(row => this.rowToDelivery(row))
  }

  /**
   * Mark a delivery as successful
   */
  async markDelivered(deliveryId: string, durationMs: number, response?: string): Promise<void> {
    const now = Date.now()

    // Update delivery status
    this.sql.exec(
      `UPDATE deliveries
       SET status = 'delivered', delivered_at = ?, attempt_count = attempt_count + 1
       WHERE id = ?`,
      now,
      deliveryId
    )

    // Get delivery info for log
    const delivery = this.sql.exec(
      `SELECT subscription_id, attempt_count FROM deliveries WHERE id = ?`,
      deliveryId
    ).one()

    if (delivery) {
      // Log the successful attempt
      this.sql.exec(
        `INSERT INTO delivery_log
         (id, delivery_id, subscription_id, attempt_number, status, duration_ms, worker_response, created_at)
         VALUES (?, ?, ?, ?, 'delivered', ?, ?, ?)`,
        ulid(),
        deliveryId,
        delivery.subscription_id,
        delivery.attempt_count,
        durationMs,
        response ?? null,
        now
      )
    }
  }

  /**
   * Mark a delivery attempt as failed, schedule retry or move to dead letter
   */
  async markFailed(
    deliveryId: string,
    error: string,
    durationMs?: number
  ): Promise<{ retrying: boolean; deadLettered: boolean }> {
    const now = Date.now()

    // Get current delivery and subscription info
    const deliveryRow = this.sql.exec(
      `SELECT d.*, s.max_retries
       FROM deliveries d
       JOIN subscriptions s ON d.subscription_id = s.id
       WHERE d.id = ?`,
      deliveryId
    ).one()

    if (!deliveryRow) {
      return { retrying: false, deadLettered: false }
    }

    const delivery = this.rowToDelivery(deliveryRow)
    const maxRetries = deliveryRow.max_retries as number
    const newAttemptCount = delivery.attemptCount + 1

    // Log the failed attempt
    this.sql.exec(
      `INSERT INTO delivery_log
       (id, delivery_id, subscription_id, attempt_number, status, duration_ms, error_message, created_at)
       VALUES (?, ?, ?, ?, 'failed', ?, ?, ?)`,
      ulid(),
      deliveryId,
      delivery.subscriptionId,
      newAttemptCount,
      durationMs ?? null,
      error,
      now
    )

    if (newAttemptCount >= maxRetries) {
      // Move to dead letter queue
      this.sql.exec(
        `UPDATE deliveries
         SET status = 'dead', attempt_count = ?, last_error = ?
         WHERE id = ?`,
        newAttemptCount,
        error,
        deliveryId
      )

      this.sql.exec(
        `INSERT INTO dead_letters
         (id, delivery_id, subscription_id, event_id, event_payload, reason, last_error, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        ulid(),
        deliveryId,
        delivery.subscriptionId,
        delivery.eventId,
        delivery.eventPayload,
        'Max retries exceeded',
        error,
        now
      )

      return { retrying: false, deadLettered: true }
    }

    // Schedule retry with exponential backoff
    const backoffMs = Math.min(1000 * Math.pow(2, newAttemptCount), 300000) // Max 5 minutes
    const nextAttemptAt = now + backoffMs

    this.sql.exec(
      `UPDATE deliveries
       SET status = 'failed', attempt_count = ?, last_error = ?, next_attempt_at = ?
       WHERE id = ?`,
      newAttemptCount,
      error,
      nextAttemptAt,
      deliveryId
    )

    return { retrying: true, deadLettered: false }
  }

  // ---------------------------------------------------------------------------
  // Status and Stats
  // ---------------------------------------------------------------------------

  /**
   * Get subscription status with delivery stats
   */
  async getSubscriptionStatus(subscriptionId: string): Promise<{
    subscription: Subscription | null
    stats: SubscriptionStats
  }> {
    const subscription = await this.getSubscription(subscriptionId)

    // Get stats
    const pendingRow = this.sql.exec(
      `SELECT COUNT(*) as count FROM deliveries
       WHERE subscription_id = ? AND status = 'pending'`,
      subscriptionId
    ).one()

    const failedRow = this.sql.exec(
      `SELECT COUNT(*) as count FROM deliveries
       WHERE subscription_id = ? AND status = 'failed'`,
      subscriptionId
    ).one()

    const deadRow = this.sql.exec(
      `SELECT COUNT(*) as count FROM dead_letters
       WHERE subscription_id = ?`,
      subscriptionId
    ).one()

    const deliveredRow = this.sql.exec(
      `SELECT COUNT(*) as count FROM deliveries
       WHERE subscription_id = ? AND status = 'delivered'`,
      subscriptionId
    ).one()

    const totalAttemptsRow = this.sql.exec(
      `SELECT SUM(attempt_count) as total FROM deliveries
       WHERE subscription_id = ?`,
      subscriptionId
    ).one()

    const totalDelivered = (deliveredRow?.count as number) || 0
    const totalAttempts = (totalAttemptsRow?.total as number) || 0

    return {
      subscription,
      stats: {
        pendingDeliveries: (pendingRow?.count as number) || 0,
        failedDeliveries: (failedRow?.count as number) || 0,
        deadLetters: (deadRow?.count as number) || 0,
        successRate: totalAttempts > 0 ? totalDelivered / totalAttempts : 0,
        totalDelivered,
        totalAttempts,
      },
    }
  }

  /**
   * Get dead letters for a subscription
   */
  async getDeadLetters(subscriptionId: string, limit = 100): Promise<DeadLetter[]> {
    const rows = this.sql.exec(
      `SELECT * FROM dead_letters
       WHERE subscription_id = ?
       ORDER BY created_at DESC
       LIMIT ?`,
      subscriptionId,
      limit
    ).toArray()

    return rows.map(row => ({
      id: row.id as string,
      deliveryId: row.delivery_id as string,
      subscriptionId: row.subscription_id as string,
      eventId: row.event_id as string,
      eventPayload: row.event_payload as string,
      reason: row.reason as string,
      lastError: row.last_error as string | undefined,
      createdAt: row.created_at as number,
    }))
  }

  /**
   * Retry a dead letter (create new delivery)
   */
  async retryDeadLetter(deadLetterId: string): Promise<{ ok: boolean; deliveryId?: string }> {
    const row = this.sql.exec(
      `SELECT * FROM dead_letters WHERE id = ?`,
      deadLetterId
    ).one()

    if (!row) {
      return { ok: false }
    }

    // Get the original event type from the delivery
    const deliveryRow = this.sql.exec(
      `SELECT event_type FROM deliveries WHERE id = ?`,
      row.delivery_id
    ).one()

    if (!deliveryRow) {
      return { ok: false }
    }

    // Create a new delivery
    const result = await this.createDelivery({
      subscriptionId: row.subscription_id as string,
      eventId: `${row.event_id}-retry-${Date.now()}`,
      eventType: deliveryRow.event_type as string,
      eventPayload: JSON.parse(row.event_payload as string),
    })

    if (result.ok) {
      // Remove from dead letters
      this.sql.exec(`DELETE FROM dead_letters WHERE id = ?`, deadLetterId)
      return { ok: true, deliveryId: result.deliveryId }
    }

    return { ok: false }
  }

  /**
   * Get delivery logs for debugging
   */
  async getDeliveryLogs(deliveryId: string): Promise<DeliveryLog[]> {
    const rows = this.sql.exec(
      `SELECT * FROM delivery_log
       WHERE delivery_id = ?
       ORDER BY created_at ASC`,
      deliveryId
    ).toArray()

    return rows.map(row => ({
      id: row.id as string,
      deliveryId: row.delivery_id as string,
      subscriptionId: row.subscription_id as string,
      attemptNumber: row.attempt_number as number,
      status: row.status as string,
      durationMs: row.duration_ms as number | undefined,
      errorMessage: row.error_message as string | undefined,
      workerResponse: row.worker_response as string | undefined,
      createdAt: row.created_at as number,
    }))
  }

  // ---------------------------------------------------------------------------
  // Helper Methods
  // ---------------------------------------------------------------------------

  /**
   * Extract prefix from pattern for indexing
   * "webhook.github.*" -> "webhook.github"
   * "webhook.github.push" -> "webhook.github.push"
   * "**" -> ""
   */
  private extractPrefix(pattern: string): string {
    const parts = pattern.split('.')
    const wildcardIdx = parts.findIndex(p => p === '*' || p === '**')
    if (wildcardIdx === -1) {
      return pattern
    }
    if (wildcardIdx === 0) {
      return ''
    }
    return parts.slice(0, wildcardIdx).join('.')
  }

  /**
   * Check if an event type matches a subscription pattern
   * Supports:
   * - Exact match: "webhook.github.push"
   * - Single wildcard: "webhook.github.*" matches "webhook.github.push" but not "webhook.github.push.v1"
   * - Double wildcard: "webhook.**" matches "webhook", "webhook.github.push" and "webhook.github.push.v1"
   */
  private matchesPattern(eventType: string, pattern: string): boolean {
    // Handle special root patterns
    if (pattern === '**') {
      return true
    }
    if (pattern === '*') {
      // * at root matches only single-segment event types
      return !eventType.includes('.')
    }

    const eventParts = eventType.split('.')
    const patternParts = pattern.split('.')

    let eventIdx = 0
    let patternIdx = 0

    while (patternIdx < patternParts.length) {
      const p = patternParts[patternIdx]

      if (p === '**') {
        // ** matches zero or more segments
        // If it's the last pattern part, match everything remaining (including nothing)
        if (patternIdx === patternParts.length - 1) {
          return true
        }
        // Try matching remaining pattern against rest of event (including empty)
        for (let i = eventIdx; i <= eventParts.length; i++) {
          const remainingEvent = eventParts.slice(i).join('.')
          const remainingPattern = patternParts.slice(patternIdx + 1).join('.')
          if (this.matchesPattern(remainingEvent, remainingPattern)) {
            return true
          }
        }
        return false
      } else if (eventIdx >= eventParts.length) {
        // No more event parts but pattern still has non-** parts
        return false
      } else if (p === '*') {
        // * matches exactly one segment
        eventIdx++
        patternIdx++
      } else if (p === eventParts[eventIdx]) {
        // Exact match
        eventIdx++
        patternIdx++
      } else {
        return false
      }
    }

    // Both should be exhausted for a match
    return eventIdx === eventParts.length && patternIdx === patternParts.length
  }

  /**
   * Convert database row to Subscription object
   */
  private rowToSubscription(row: Record<string, unknown>): Subscription {
    return {
      id: row.id as string,
      workerId: row.worker_id as string,
      workerBinding: row.worker_binding as string | undefined,
      pattern: row.pattern as string,
      patternPrefix: row.pattern_prefix as string,
      rpcMethod: row.rpc_method as string,
      maxRetries: row.max_retries as number,
      timeoutMs: row.timeout_ms as number,
      active: row.active === 1,
      createdAt: row.created_at as number,
      updatedAt: row.updated_at as number,
    }
  }

  /**
   * Convert database row to Delivery object
   */
  private rowToDelivery(row: Record<string, unknown>): Delivery {
    return {
      id: row.id as string,
      subscriptionId: row.subscription_id as string,
      eventId: row.event_id as string,
      eventType: row.event_type as string,
      eventPayload: row.event_payload as string,
      status: row.status as 'pending' | 'delivered' | 'failed' | 'dead',
      attemptCount: row.attempt_count as number,
      nextAttemptAt: row.next_attempt_at as number | undefined,
      lastError: row.last_error as string | undefined,
      createdAt: row.created_at as number,
      deliveredAt: row.delivered_at as number | undefined,
    }
  }

  // ---------------------------------------------------------------------------
  // Event Fanout and Delivery
  // ---------------------------------------------------------------------------

  /**
   * Fan out an event to all matching subscriptions
   *
   * 1. Gets all active subscriptions
   * 2. Finds matching subscriptions using pattern matcher
   * 3. Creates delivery records for each match
   * 4. Attempts immediate delivery
   *
   * @param event - The event to fan out
   * @returns Object with matched count and delivery IDs
   */
  async fanout(event: {
    id: string
    type: string
    ts: string
    payload: unknown
  }): Promise<{ matched: number; deliveries: string[] }> {
    // 1. Get all active subscriptions
    const subscriptions = this.sql.exec(
      `SELECT * FROM subscriptions WHERE active = 1`
    ).toArray()

    // 2. Find matching subscriptions using pattern matcher
    const matches = findMatchingSubscriptions(
      event.type,
      subscriptions.map(s => ({
        ...s,
        id: s.id as string,
        pattern: s.pattern as string,
        patternPrefix: s.pattern_prefix as string,
      }))
    )

    // 3. Create delivery record for each match
    const deliveryIds: string[] = []
    const now = Date.now()

    for (const sub of matches) {
      const deliveryId = ulid()

      try {
        this.sql.exec(
          `INSERT INTO deliveries
           (id, subscription_id, event_id, event_type, event_payload, status, created_at)
           VALUES (?, ?, ?, ?, ?, 'pending', ?)`,
          deliveryId,
          sub.id,
          event.id,
          event.type,
          JSON.stringify(event.payload),
          now
        )
        deliveryIds.push(deliveryId)
      } catch (e) {
        // Likely duplicate - already delivered (UNIQUE constraint on subscription_id, event_id)
        console.log(`Delivery already exists for subscription ${sub.id}, event ${event.id}`)
      }
    }

    // 4. Attempt immediate delivery
    if (deliveryIds.length > 0) {
      await this.processDeliveries(deliveryIds)
    }

    return { matched: matches.length, deliveries: deliveryIds }
  }

  /**
   * Process a batch of deliveries
   */
  private async processDeliveries(deliveryIds: string[]): Promise<void> {
    for (const deliveryId of deliveryIds) {
      await this.deliverOne(deliveryId)
    }
  }

  /**
   * Deliver a single event to the subscriber
   *
   * Attempts RPC via service binding first, falls back to HTTP.
   * On failure, schedules retries with exponential backoff.
   * Moves to dead letter queue after max retries.
   */
  private async deliverOne(deliveryId: string): Promise<void> {
    // Get delivery and subscription
    const delivery = this.sql.exec(
      `SELECT d.*, s.worker_id, s.worker_binding, s.rpc_method, s.max_retries, s.timeout_ms
       FROM deliveries d
       JOIN subscriptions s ON d.subscription_id = s.id
       WHERE d.id = ?`,
      deliveryId
    ).one()

    if (!delivery || delivery.status === 'delivered' || delivery.status === 'dead') {
      return
    }

    const startTime = performance.now()
    const attemptNumber = (delivery.attempt_count as number) + 1

    try {
      let response: unknown

      // Try RPC via service binding first
      const workerBinding = delivery.worker_binding as string | null
      if (workerBinding && this.env[workerBinding]) {
        const worker = this.env[workerBinding] as { [method: string]: (payload: unknown) => Promise<unknown> }
        const rpcMethod = delivery.rpc_method as string

        if (typeof worker[rpcMethod] === 'function') {
          const timeoutMs = delivery.timeout_ms as number
          response = await Promise.race([
            worker[rpcMethod](JSON.parse(delivery.event_payload as string)),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('RPC timeout')), timeoutMs)
            ),
          ])
        } else {
          throw new Error(`RPC method ${rpcMethod} not found on worker`)
        }
      } else {
        // Fallback to HTTP
        const workerId = delivery.worker_id as string
        const rpcMethod = delivery.rpc_method as string
        const timeoutMs = delivery.timeout_ms as number

        const httpResponse = await fetch(
          `https://${workerId}.workers.dev/rpc/${rpcMethod}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: delivery.event_payload as string,
            signal: AbortSignal.timeout(timeoutMs),
          }
        )

        if (!httpResponse.ok) {
          throw new Error(`HTTP ${httpResponse.status}: ${await httpResponse.text()}`)
        }

        response = await httpResponse.json()
      }

      // Success!
      const durationMs = Math.round(performance.now() - startTime)

      this.sql.exec(
        `UPDATE deliveries SET status = 'delivered', delivered_at = ?, attempt_count = ? WHERE id = ?`,
        Date.now(),
        attemptNumber,
        deliveryId
      )

      // Log success
      this.logDeliveryAttempt(deliveryId, delivery.subscription_id as string, attemptNumber, 'success', durationMs, null, response)

    } catch (error) {
      const durationMs = Math.round(performance.now() - startTime)
      const errorMessage = error instanceof Error ? error.message : String(error)
      const maxRetries = delivery.max_retries as number

      if (attemptNumber < maxRetries) {
        // Schedule retry with exponential backoff
        const delay = this.calculateRetryDelay(attemptNumber)
        const nextAttemptAt = Date.now() + delay

        this.sql.exec(
          `UPDATE deliveries
           SET status = 'failed', attempt_count = ?, next_attempt_at = ?, last_error = ?
           WHERE id = ?`,
          attemptNumber,
          nextAttemptAt,
          errorMessage,
          deliveryId
        )

        // Schedule alarm for retry
        await this.scheduleRetryAlarm()
      } else {
        // Move to dead letter queue
        this.sql.exec(
          `UPDATE deliveries SET status = 'dead', attempt_count = ?, last_error = ? WHERE id = ?`,
          attemptNumber,
          errorMessage,
          deliveryId
        )

        this.sql.exec(
          `INSERT INTO dead_letters (id, delivery_id, subscription_id, event_id, event_payload, reason, last_error, created_at)
           VALUES (?, ?, ?, ?, ?, 'max_retries_exceeded', ?, ?)`,
          ulid(),
          deliveryId,
          delivery.subscription_id,
          delivery.event_id,
          delivery.event_payload,
          errorMessage,
          Date.now()
        )
      }

      // Log failure
      this.logDeliveryAttempt(deliveryId, delivery.subscription_id as string, attemptNumber, 'failed', durationMs, errorMessage, null)
    }
  }

  /**
   * Calculate retry delay with exponential backoff and jitter
   *
   * Base delay: 1 second
   * Max delay: 5 minutes
   * Formula: min(baseDelay * 2^(attempt-1), maxDelay) + random jitter (0-1s)
   */
  private calculateRetryDelay(attemptNumber: number): number {
    const baseDelay = 1000 // 1 second
    const maxDelay = 300000 // 5 minutes
    const exponential = Math.min(baseDelay * Math.pow(2, attemptNumber - 1), maxDelay)
    const jitter = Math.random() * 1000
    return Math.round(exponential + jitter)
  }

  /**
   * Schedule an alarm for the next pending retry
   *
   * Only sets alarm if:
   * - There are failed deliveries with next_attempt_at set
   * - No alarm is currently set, or the new time is sooner
   */
  private async scheduleRetryAlarm(): Promise<void> {
    const nextRetry = this.sql.exec(
      `SELECT MIN(next_attempt_at) as next_time FROM deliveries WHERE status = 'failed'`
    ).one()

    if (nextRetry?.next_time) {
      const currentAlarm = await this.ctx.storage.getAlarm()
      const nextTime = nextRetry.next_time as number

      // Only set alarm if no alarm or new time is sooner
      if (!currentAlarm || nextTime < currentAlarm) {
        await this.ctx.storage.setAlarm(nextTime)
      }
    }
  }

  /**
   * Alarm handler - processes failed deliveries ready for retry
   *
   * Called by Cloudflare when a scheduled alarm fires.
   * Processes up to 100 deliveries per alarm to avoid timeout.
   */
  async alarm(): Promise<void> {
    // Process all failed deliveries ready for retry
    const ready = this.sql.exec(
      `SELECT id FROM deliveries
       WHERE status = 'failed' AND next_attempt_at <= ?
       ORDER BY next_attempt_at ASC
       LIMIT 100`,
      Date.now()
    ).toArray()

    for (const row of ready) {
      await this.deliverOne(row.id as string)
    }

    // Schedule next alarm if more retries pending
    await this.scheduleRetryAlarm()
  }

  /**
   * Log a delivery attempt (success or failure)
   */
  private logDeliveryAttempt(
    deliveryId: string,
    subscriptionId: string,
    attemptNumber: number,
    status: 'success' | 'failed',
    durationMs: number,
    errorMessage: string | null,
    response: unknown
  ): void {
    this.sql.exec(
      `INSERT INTO delivery_log
       (id, delivery_id, subscription_id, attempt_number, status, duration_ms, error_message, worker_response, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ulid(),
      deliveryId,
      subscriptionId,
      attemptNumber,
      status,
      durationMs,
      errorMessage,
      response ? JSON.stringify(response) : null,
      Date.now()
    )
  }
}

// Export type for wrangler config
export type SubscriptionDOType = typeof SubscriptionDO
