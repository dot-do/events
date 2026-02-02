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
 * - Batched Delivery: Collect events over a time window and deliver as a batch
 */

import { DurableObject } from 'cloudflare:workers'
import { findMatchingSubscriptions, matchPattern, extractPatternPrefix } from './pattern-matcher.js'
import {
  getString,
  getNumber,
  getBoolean,
  getOptionalString,
  getOptionalNumber,
  getOptionalBoolean,
  type SqlRow,
} from './sql-mapper.js'
import { ulid } from './ulid.js'
import {
  DEFAULT_SUBSCRIPTION_MAX_RETRIES,
  DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
  SUBSCRIPTION_BATCH_LIMIT,
  SUBSCRIPTION_RETRY_BASE_DELAY_MS,
  SUBSCRIPTION_RETRY_MAX_DELAY_MS,
  DEFAULT_BATCH_DELIVERY_SIZE,
  DEFAULT_BATCH_DELIVERY_WINDOW_MS,
  MAX_BATCH_DELIVERY_SIZE,
  MAX_BATCH_DELIVERY_WINDOW_MS,
} from './config.js'
import {
  validateWorkerId,
  validateRpcMethod,
  buildSafeDeliveryUrl,
} from './worker-id-validation.js'

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
  /** Enable batched delivery (false = deliver immediately per event) */
  batchEnabled: boolean
  /** Maximum number of events to batch together */
  batchSize: number
  /** Maximum time window in ms to collect events before delivery */
  batchWindowMs: number
}

/** Configuration for batched delivery */
export interface BatchDeliveryConfig {
  /** Enable batched delivery */
  enabled: boolean
  /** Maximum number of events to batch together (1-1000, default 100) */
  batchSize?: number
  /** Maximum time window in ms to collect events (1-10000, default 1000) */
  batchWindowMs?: number
}

/** Result of a batched delivery attempt */
export interface BatchDeliveryResult {
  /** Total events in the batch */
  total: number
  /** Successfully delivered events */
  delivered: number
  /** Failed events */
  failed: number
  /** Duration in ms */
  durationMs: number
  /** Individual event results */
  results: Array<{
    deliveryId: string
    eventId: string
    success: boolean
    error?: string | undefined
  }>
}

/** Pending batch for a subscription */
export interface PendingBatch {
  id: string
  subscriptionId: string
  deliveryIds: string[]
  eventCount: number
  windowStart: number
  windowEnd?: number | undefined
  status: 'pending' | 'delivered' | 'failed' | 'dead'
  attemptCount: number
  nextAttemptAt?: number | undefined
  lastError?: string | undefined
  createdAt: number
  deliveredAt?: number | undefined
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
        batch_enabled INTEGER DEFAULT 0,
        batch_size INTEGER DEFAULT 100,
        batch_window_ms INTEGER DEFAULT 1000,
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

      CREATE INDEX IF NOT EXISTS idx_subscriptions_batch
        ON subscriptions(batch_enabled) WHERE batch_enabled = 1;

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
        batch_id TEXT,
        FOREIGN KEY (subscription_id) REFERENCES subscriptions(id),
        UNIQUE(subscription_id, event_id)
      );

      CREATE INDEX IF NOT EXISTS idx_deliveries_pending
        ON deliveries(status, next_attempt_at)
        WHERE status IN ('pending', 'failed');

      CREATE INDEX IF NOT EXISTS idx_deliveries_subscription
        ON deliveries(subscription_id);

      CREATE INDEX IF NOT EXISTS idx_deliveries_batch
        ON deliveries(batch_id) WHERE batch_id IS NOT NULL;

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

      -- Batch delivery tracking table
      CREATE TABLE IF NOT EXISTS batch_deliveries (
        id TEXT PRIMARY KEY,
        subscription_id TEXT NOT NULL,
        delivery_ids TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        event_count INTEGER NOT NULL,
        window_start INTEGER NOT NULL,
        window_end INTEGER,
        attempt_count INTEGER DEFAULT 0,
        next_attempt_at INTEGER,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        delivered_at INTEGER,
        FOREIGN KEY (subscription_id) REFERENCES subscriptions(id)
      );

      CREATE INDEX IF NOT EXISTS idx_batch_deliveries_pending
        ON batch_deliveries(status, next_attempt_at)
        WHERE status IN ('pending', 'failed');

      CREATE INDEX IF NOT EXISTS idx_batch_deliveries_subscription
        ON batch_deliveries(subscription_id);
    `)

    // Schema migration: add batch columns to existing subscriptions tables
    this.migrateSchema()
  }

  /**
   * Migrate existing schema to add batch delivery columns
   */
  private migrateSchema(): void {
    // Check if batch_enabled column exists
    const columns = this.sql.exec(`PRAGMA table_info(subscriptions)`).toArray()
    const hasBatchEnabled = columns.some(col => col.name === 'batch_enabled')

    if (!hasBatchEnabled) {
      // Add batch delivery columns to existing table
      this.sql.exec(`ALTER TABLE subscriptions ADD COLUMN batch_enabled INTEGER DEFAULT 0`)
      this.sql.exec(`ALTER TABLE subscriptions ADD COLUMN batch_size INTEGER DEFAULT 100`)
      this.sql.exec(`ALTER TABLE subscriptions ADD COLUMN batch_window_ms INTEGER DEFAULT 1000`)
    }

    // Check if batch_id column exists in deliveries
    const deliveryCols = this.sql.exec(`PRAGMA table_info(deliveries)`).toArray()
    const hasBatchId = deliveryCols.some(col => col.name === 'batch_id')

    if (!hasBatchId) {
      this.sql.exec(`ALTER TABLE deliveries ADD COLUMN batch_id TEXT`)
    }
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
    batchConfig?: BatchDeliveryConfig
  }): Promise<{ ok: true; subscriptionId: string } | { ok: false; error: string }> {
    // Validate workerId to prevent SSRF attacks
    const workerIdValidation = validateWorkerId(params.workerId)
    if (!workerIdValidation.valid) {
      return { ok: false, error: `Invalid workerId: ${workerIdValidation.error}` }
    }

    // Validate rpcMethod to prevent path traversal
    const rpcMethodValidation = validateRpcMethod(params.rpcMethod)
    if (!rpcMethodValidation.valid) {
      return { ok: false, error: `Invalid rpcMethod: ${rpcMethodValidation.error}` }
    }

    // Validate batch config if provided
    if (params.batchConfig?.enabled) {
      const batchSize = params.batchConfig.batchSize ?? DEFAULT_BATCH_DELIVERY_SIZE
      const batchWindowMs = params.batchConfig.batchWindowMs ?? DEFAULT_BATCH_DELIVERY_WINDOW_MS

      if (batchSize < 1 || batchSize > MAX_BATCH_DELIVERY_SIZE) {
        return { ok: false, error: `batchSize must be between 1 and ${MAX_BATCH_DELIVERY_SIZE}` }
      }
      if (batchWindowMs < 1 || batchWindowMs > MAX_BATCH_DELIVERY_WINDOW_MS) {
        return { ok: false, error: `batchWindowMs must be between 1 and ${MAX_BATCH_DELIVERY_WINDOW_MS}` }
      }
    }

    const patternPrefix = extractPatternPrefix(params.pattern)
    const id = ulid()
    const now = Date.now()

    const batchEnabled = params.batchConfig?.enabled ?? false
    const batchSize = params.batchConfig?.batchSize ?? DEFAULT_BATCH_DELIVERY_SIZE
    const batchWindowMs = params.batchConfig?.batchWindowMs ?? DEFAULT_BATCH_DELIVERY_WINDOW_MS

    try {
      this.sql.exec(
        `INSERT INTO subscriptions
         (id, worker_id, worker_binding, pattern, pattern_prefix, rpc_method, max_retries, timeout_ms, active, batch_enabled, batch_size, batch_window_ms, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
        id,
        params.workerId,
        params.workerBinding ?? null,
        params.pattern,
        patternPrefix,
        params.rpcMethod,
        params.maxRetries ?? DEFAULT_SUBSCRIPTION_MAX_RETRIES,
        params.timeoutMs ?? DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
        batchEnabled ? 1 : 0,
        batchSize,
        batchWindowMs,
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
  async deleteSubscription(subscriptionId: string): Promise<{ ok: boolean; deleted: { deliveries: number; logs: number; deadLetters: number; batchDeliveries: number } }> {
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

    const batchDeliveriesDeleted = this.sql.exec(
      `DELETE FROM batch_deliveries WHERE subscription_id = ?`,
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
        batchDeliveries: batchDeliveriesDeleted,
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
      batchConfig?: BatchDeliveryConfig
    }
  ): Promise<{ ok: boolean; error?: string }> {
    // Validate rpcMethod if being updated
    if (updates.rpcMethod !== undefined) {
      const rpcMethodValidation = validateRpcMethod(updates.rpcMethod)
      if (!rpcMethodValidation.valid) {
        return { ok: false, error: `Invalid rpcMethod: ${rpcMethodValidation.error}` }
      }
    }

    // Validate batch config if provided
    if (updates.batchConfig?.enabled) {
      const batchSize = updates.batchConfig.batchSize ?? DEFAULT_BATCH_DELIVERY_SIZE
      const batchWindowMs = updates.batchConfig.batchWindowMs ?? DEFAULT_BATCH_DELIVERY_WINDOW_MS

      if (batchSize < 1 || batchSize > MAX_BATCH_DELIVERY_SIZE) {
        return { ok: false, error: `batchSize must be between 1 and ${MAX_BATCH_DELIVERY_SIZE}` }
      }
      if (batchWindowMs < 1 || batchWindowMs > MAX_BATCH_DELIVERY_WINDOW_MS) {
        return { ok: false, error: `batchWindowMs must be between 1 and ${MAX_BATCH_DELIVERY_WINDOW_MS}` }
      }
    }

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
    if (updates.batchConfig !== undefined) {
      setClauses.push('batch_enabled = ?')
      params.push(updates.batchConfig.enabled ? 1 : 0)
      if (updates.batchConfig.batchSize !== undefined) {
        setClauses.push('batch_size = ?')
        params.push(updates.batchConfig.batchSize)
      }
      if (updates.batchConfig.batchWindowMs !== undefined) {
        setClauses.push('batch_window_ms = ?')
        params.push(updates.batchConfig.batchWindowMs)
      }
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
    batchEnabled?: boolean
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
    if (options?.batchEnabled !== undefined) {
      query += ' AND batch_enabled = ?'
      params.push(options.batchEnabled ? 1 : 0)
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
    const prefix = extractPatternPrefix(eventType)

    const rows = this.sql.exec(
      `SELECT * FROM subscriptions
       WHERE active = 1
       AND (pattern_prefix = ? OR pattern_prefix = '' OR ? LIKE pattern_prefix || '%')
       ORDER BY created_at`,
      prefix,
      eventType
    ).toArray()

    // Filter by full pattern match using the shared pattern matcher
    return rows
      .map(row => this.rowToSubscription(row))
      .filter(sub => matchPattern(sub.pattern, eventType))
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
    batchId?: string
  }): Promise<{ ok: true; deliveryId: string } | { ok: false; error: string }> {
    const id = ulid()
    const now = Date.now()

    try {
      this.sql.exec(
        `INSERT INTO deliveries
         (id, subscription_id, event_id, event_type, event_payload, status, attempt_count, next_attempt_at, created_at, batch_id)
         VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)`,
        id,
        params.subscriptionId,
        params.eventId,
        params.eventType,
        JSON.stringify(params.eventPayload),
        now,  // next_attempt_at = now (ready immediately)
        now,
        params.batchId ?? null
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
       AND batch_id IS NULL
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

    const delivery = this.rowToDelivery(deliveryRow as SqlRow)
    const maxRetries = getNumber(deliveryRow as SqlRow, 'max_retries')
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
    const backoffMs = Math.min(SUBSCRIPTION_RETRY_BASE_DELAY_MS * Math.pow(2, newAttemptCount), SUBSCRIPTION_RETRY_MAX_DELAY_MS)
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
  // Batched Delivery
  // ---------------------------------------------------------------------------

  /**
   * Add a delivery to a batch for a subscription
   * Creates a new batch if needed, or adds to existing batch
   */
  private async addToBatch(subscriptionId: string, deliveryId: string, subscription: Subscription): Promise<void> {
    const now = Date.now()

    // Find an open batch for this subscription
    const openBatch = this.sql.exec(
      `SELECT * FROM batch_deliveries
       WHERE subscription_id = ?
       AND status = 'pending'
       AND window_end IS NULL
       ORDER BY created_at DESC
       LIMIT 1`,
      subscriptionId
    ).one()

    if (openBatch) {
      const deliveryIds = JSON.parse(getString(openBatch as SqlRow, 'delivery_ids')) as string[]
      const eventCount = getNumber(openBatch as SqlRow, 'event_count')
      const windowStart = getNumber(openBatch as SqlRow, 'window_start')

      // Check if batch is full or window expired
      const windowExpired = (now - windowStart) >= subscription.batchWindowMs
      const batchFull = eventCount >= subscription.batchSize

      if (windowExpired || batchFull) {
        // Close current batch and schedule delivery
        this.sql.exec(
          `UPDATE batch_deliveries
           SET window_end = ?, next_attempt_at = ?
           WHERE id = ?`,
          now,
          now,
          openBatch.id
        )

        // Schedule alarm for batch delivery
        await this.scheduleBatchAlarm()

        // Create new batch with this delivery
        await this.createNewBatch(subscriptionId, deliveryId)
      } else {
        // Add to existing batch
        deliveryIds.push(deliveryId)
        this.sql.exec(
          `UPDATE batch_deliveries
           SET delivery_ids = ?, event_count = ?
           WHERE id = ?`,
          JSON.stringify(deliveryIds),
          eventCount + 1,
          openBatch.id
        )

        // Update delivery with batch_id
        this.sql.exec(
          `UPDATE deliveries SET batch_id = ? WHERE id = ?`,
          openBatch.id,
          deliveryId
        )
      }
    } else {
      // Create new batch
      await this.createNewBatch(subscriptionId, deliveryId)
    }
  }

  /**
   * Create a new batch for a subscription
   */
  private async createNewBatch(subscriptionId: string, deliveryId: string): Promise<void> {
    const batchId = ulid()
    const now = Date.now()

    this.sql.exec(
      `INSERT INTO batch_deliveries
       (id, subscription_id, delivery_ids, status, event_count, window_start, created_at)
       VALUES (?, ?, ?, 'pending', 1, ?, ?)`,
      batchId,
      subscriptionId,
      JSON.stringify([deliveryId]),
      now,
      now
    )

    // Update delivery with batch_id
    this.sql.exec(
      `UPDATE deliveries SET batch_id = ? WHERE id = ?`,
      batchId,
      deliveryId
    )

    // Get subscription to schedule window end alarm
    const sub = await this.getSubscription(subscriptionId)
    if (sub) {
      const windowEndTime = now + sub.batchWindowMs
      const currentAlarm = await this.ctx.storage.getAlarm()
      if (!currentAlarm || windowEndTime < currentAlarm) {
        await this.ctx.storage.setAlarm(windowEndTime)
      }
    }
  }

  /**
   * Schedule an alarm for batch delivery
   */
  private async scheduleBatchAlarm(): Promise<void> {
    const nextBatch = this.sql.exec(
      `SELECT MIN(next_attempt_at) as next_time FROM batch_deliveries
       WHERE status IN ('pending', 'failed')
       AND window_end IS NOT NULL`
    ).one()

    if (nextBatch?.next_time) {
      const currentAlarm = await this.ctx.storage.getAlarm()
      const nextTime = getNumber(nextBatch as SqlRow, 'next_time')

      // Only set alarm if no alarm or new time is sooner
      if (!currentAlarm || nextTime < currentAlarm) {
        await this.ctx.storage.setAlarm(nextTime)
      }
    }
  }

  /**
   * Deliver a batch of events to a subscriber
   */
  async deliverBatch(batchId: string): Promise<BatchDeliveryResult> {
    const startTime = performance.now()

    // Get batch and subscription info
    const batchRow = this.sql.exec(
      `SELECT b.*, s.worker_id, s.worker_binding, s.rpc_method, s.max_retries, s.timeout_ms
       FROM batch_deliveries b
       JOIN subscriptions s ON b.subscription_id = s.id
       WHERE b.id = ?`,
      batchId
    ).one()

    if (!batchRow || batchRow.status === 'delivered' || batchRow.status === 'dead') {
      return { total: 0, delivered: 0, failed: 0, durationMs: 0, results: [] }
    }

    const deliveryIds = JSON.parse(getString(batchRow as SqlRow, 'delivery_ids')) as string[]
    const subscriptionId = getString(batchRow as SqlRow, 'subscription_id')
    const attemptNumber = getNumber(batchRow as SqlRow, 'attempt_count') + 1

    // Get all deliveries in the batch
    const deliveries = this.sql.exec(
      `SELECT * FROM deliveries WHERE id IN (${deliveryIds.map(() => '?').join(',')})`,
      ...deliveryIds
    ).toArray()

    // Build batch payload
    const events = deliveries.map(d => ({
      deliveryId: getString(d as SqlRow, 'id'),
      eventId: getString(d as SqlRow, 'event_id'),
      eventType: getString(d as SqlRow, 'event_type'),
      payload: JSON.parse(getString(d as SqlRow, 'event_payload')),
    }))

    const results: BatchDeliveryResult['results'] = []
    let deliveredCount = 0
    let failedCount = 0

    try {
      let response: unknown

      // Try RPC via service binding first
      const workerBinding = getOptionalString(batchRow as SqlRow, 'worker_binding')
      if (workerBinding && this.env[workerBinding]) {
        const worker = this.env[workerBinding] as { [method: string]: (payload: unknown) => Promise<unknown> }
        const rpcMethod = getString(batchRow as SqlRow, 'rpc_method')

        if (typeof worker[rpcMethod] === 'function') {
          const timeoutMs = getNumber(batchRow as SqlRow, 'timeout_ms')
          response = await Promise.race([
            worker[rpcMethod]({ batch: true, events }),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('RPC timeout')), timeoutMs)
            ),
          ])
        } else {
          throw new Error(`RPC method ${rpcMethod} not found on worker`)
        }
      } else {
        // Fallback to HTTP
        const workerId = getString(batchRow as SqlRow, 'worker_id')
        const rpcMethod = getString(batchRow as SqlRow, 'rpc_method')
        const timeoutMs = getNumber(batchRow as SqlRow, 'timeout_ms')

        const deliveryUrl = buildSafeDeliveryUrl(workerId, rpcMethod)

        const httpResponse = await fetch(
          deliveryUrl,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ batch: true, events }),
            signal: AbortSignal.timeout(timeoutMs),
          }
        )

        if (!httpResponse.ok) {
          throw new Error(`HTTP ${httpResponse.status}: ${await httpResponse.text()}`)
        }

        response = await httpResponse.json()
      }

      // Handle partial success/failure from response
      const responseObj = response as { results?: Array<{ eventId: string; success: boolean; error?: string }> } | null
      if (responseObj?.results && Array.isArray(responseObj.results)) {
        // Response includes per-event results
        for (const eventResult of responseObj.results) {
          const delivery = events.find(e => e.eventId === eventResult.eventId)
          if (delivery) {
            results.push({
              deliveryId: delivery.deliveryId,
              eventId: eventResult.eventId,
              success: eventResult.success,
              error: eventResult.error,
            })
            if (eventResult.success) {
              deliveredCount++
              await this.markDelivered(delivery.deliveryId, 0, JSON.stringify(eventResult))
            } else {
              failedCount++
              await this.markFailed(delivery.deliveryId, eventResult.error ?? 'Batch delivery partial failure', 0)
            }
          }
        }
      } else {
        // All events delivered successfully
        deliveredCount = events.length
        for (const event of events) {
          results.push({
            deliveryId: event.deliveryId,
            eventId: event.eventId,
            success: true,
          })
          await this.markDelivered(event.deliveryId, 0, JSON.stringify(response))
        }
      }

      // Mark batch as delivered
      this.sql.exec(
        `UPDATE batch_deliveries
         SET status = 'delivered', delivered_at = ?, attempt_count = ?
         WHERE id = ?`,
        Date.now(),
        attemptNumber,
        batchId
      )

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      const maxRetries = getNumber(batchRow as SqlRow, 'max_retries')

      // All events in batch failed
      failedCount = events.length
      for (const event of events) {
        results.push({
          deliveryId: event.deliveryId,
          eventId: event.eventId,
          success: false,
          error: errorMessage,
        })
      }

      if (attemptNumber < maxRetries) {
        // Schedule retry with exponential backoff
        const delay = this.calculateRetryDelay(attemptNumber)
        const nextAttemptAt = Date.now() + delay

        this.sql.exec(
          `UPDATE batch_deliveries
           SET status = 'failed', attempt_count = ?, next_attempt_at = ?, last_error = ?
           WHERE id = ?`,
          attemptNumber,
          nextAttemptAt,
          errorMessage,
          batchId
        )

        await this.scheduleBatchAlarm()
      } else {
        // Move batch to dead status
        this.sql.exec(
          `UPDATE batch_deliveries
           SET status = 'dead', attempt_count = ?, last_error = ?
           WHERE id = ?`,
          attemptNumber,
          errorMessage,
          batchId
        )

        // Move all deliveries to dead letter queue
        for (const event of events) {
          await this.markFailed(event.deliveryId, errorMessage, 0)
        }
      }

      // Log batch attempt failure
      this.sql.exec(
        `INSERT INTO delivery_log
         (id, delivery_id, subscription_id, attempt_number, status, duration_ms, error_message, created_at)
         VALUES (?, ?, ?, ?, 'failed', ?, ?, ?)`,
        ulid(),
        batchId,
        subscriptionId,
        attemptNumber,
        Math.round(performance.now() - startTime),
        errorMessage,
        Date.now()
      )
    }

    const durationMs = Math.round(performance.now() - startTime)

    return {
      total: events.length,
      delivered: deliveredCount,
      failed: failedCount,
      durationMs,
      results,
    }
  }

  /**
   * Get pending batches for a subscription
   */
  async getPendingBatches(subscriptionId: string, limit = 100): Promise<PendingBatch[]> {
    const rows = this.sql.exec(
      `SELECT * FROM batch_deliveries
       WHERE subscription_id = ?
       AND status IN ('pending', 'failed')
       ORDER BY created_at ASC
       LIMIT ?`,
      subscriptionId,
      limit
    ).toArray()

    return rows.map(row => ({
      id: getString(row as SqlRow, 'id'),
      subscriptionId: getString(row as SqlRow, 'subscription_id'),
      deliveryIds: JSON.parse(getString(row as SqlRow, 'delivery_ids')),
      eventCount: getNumber(row as SqlRow, 'event_count'),
      windowStart: getNumber(row as SqlRow, 'window_start'),
      windowEnd: getOptionalNumber(row as SqlRow, 'window_end'),
      status: getString(row as SqlRow, 'status') as 'pending' | 'delivered' | 'failed' | 'dead',
      attemptCount: getNumber(row as SqlRow, 'attempt_count'),
      nextAttemptAt: getOptionalNumber(row as SqlRow, 'next_attempt_at'),
      lastError: getOptionalString(row as SqlRow, 'last_error'),
      createdAt: getNumber(row as SqlRow, 'created_at'),
      deliveredAt: getOptionalNumber(row as SqlRow, 'delivered_at'),
    }))
  }

  /**
   * Flush all open batches for immediate delivery
   */
  async flushBatches(): Promise<{ flushed: number }> {
    const now = Date.now()

    // Close all open batches
    const result = this.sql.exec(
      `UPDATE batch_deliveries
       SET window_end = ?, next_attempt_at = ?
       WHERE status = 'pending'
       AND window_end IS NULL`,
      now,
      now
    )

    if (result.rowsWritten > 0) {
      await this.scheduleBatchAlarm()
    }

    return { flushed: result.rowsWritten }
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

    const totalDelivered = deliveredRow ? getNumber(deliveredRow as SqlRow, 'count') : 0
    const totalAttempts = totalAttemptsRow ? getOptionalNumber(totalAttemptsRow as SqlRow, 'total') ?? 0 : 0

    return {
      subscription,
      stats: {
        pendingDeliveries: pendingRow ? getNumber(pendingRow as SqlRow, 'count') : 0,
        failedDeliveries: failedRow ? getNumber(failedRow as SqlRow, 'count') : 0,
        deadLetters: deadRow ? getNumber(deadRow as SqlRow, 'count') : 0,
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
      id: getString(row as SqlRow, 'id'),
      deliveryId: getString(row as SqlRow, 'delivery_id'),
      subscriptionId: getString(row as SqlRow, 'subscription_id'),
      eventId: getString(row as SqlRow, 'event_id'),
      eventPayload: getString(row as SqlRow, 'event_payload'),
      reason: getString(row as SqlRow, 'reason'),
      lastError: getOptionalString(row as SqlRow, 'last_error'),
      createdAt: getNumber(row as SqlRow, 'created_at'),
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
      subscriptionId: getString(row as SqlRow, 'subscription_id'),
      eventId: `${getString(row as SqlRow, 'event_id')}-retry-${Date.now()}`,
      eventType: getString(deliveryRow as SqlRow, 'event_type'),
      eventPayload: JSON.parse(getString(row as SqlRow, 'event_payload')),
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
      id: getString(row as SqlRow, 'id'),
      deliveryId: getString(row as SqlRow, 'delivery_id'),
      subscriptionId: getString(row as SqlRow, 'subscription_id'),
      attemptNumber: getNumber(row as SqlRow, 'attempt_number'),
      status: getString(row as SqlRow, 'status'),
      durationMs: getOptionalNumber(row as SqlRow, 'duration_ms'),
      errorMessage: getOptionalString(row as SqlRow, 'error_message'),
      workerResponse: getOptionalString(row as SqlRow, 'worker_response'),
      createdAt: getNumber(row as SqlRow, 'created_at'),
    }))
  }

  // ---------------------------------------------------------------------------
  // Helper Methods
  // ---------------------------------------------------------------------------

  /**
   * Convert database row to Subscription object
   */
  private rowToSubscription(row: SqlRow): Subscription {
    return {
      id: getString(row, 'id'),
      workerId: getString(row, 'worker_id'),
      workerBinding: getOptionalString(row, 'worker_binding'),
      pattern: getString(row, 'pattern'),
      patternPrefix: getString(row, 'pattern_prefix'),
      rpcMethod: getString(row, 'rpc_method'),
      maxRetries: getNumber(row, 'max_retries'),
      timeoutMs: getNumber(row, 'timeout_ms'),
      active: getBoolean(row, 'active'),
      createdAt: getNumber(row, 'created_at'),
      updatedAt: getNumber(row, 'updated_at'),
      batchEnabled: getOptionalBoolean(row, 'batch_enabled') ?? false,
      batchSize: getOptionalNumber(row, 'batch_size') ?? DEFAULT_BATCH_DELIVERY_SIZE,
      batchWindowMs: getOptionalNumber(row, 'batch_window_ms') ?? DEFAULT_BATCH_DELIVERY_WINDOW_MS,
    }
  }

  /**
   * Convert database row to Delivery object
   */
  private rowToDelivery(row: SqlRow): Delivery {
    return {
      id: getString(row, 'id'),
      subscriptionId: getString(row, 'subscription_id'),
      eventId: getString(row, 'event_id'),
      eventType: getString(row, 'event_type'),
      eventPayload: getString(row, 'event_payload'),
      status: getString(row, 'status') as 'pending' | 'delivered' | 'failed' | 'dead',
      attemptCount: getNumber(row, 'attempt_count'),
      nextAttemptAt: getOptionalNumber(row, 'next_attempt_at'),
      lastError: getOptionalString(row, 'last_error'),
      createdAt: getNumber(row, 'created_at'),
      deliveredAt: getOptionalNumber(row, 'delivered_at'),
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
   * 4. For non-batched: attempts immediate delivery
   * 5. For batched: adds to batch for time-window delivery
   *
   * @param event - The event to fan out
   * @returns Object with matched count and delivery IDs
   */
  async fanout(event: {
    id: string
    type: string
    ts: string
    payload: unknown
  }): Promise<{ matched: number; deliveries: string[]; batched: number }> {
    // 1. Get all active subscriptions
    const subscriptions = this.sql.exec(
      `SELECT * FROM subscriptions WHERE active = 1`
    ).toArray()

    // 2. Find matching subscriptions using pattern matcher
    const matches = findMatchingSubscriptions(
      event.type,
      subscriptions.map(s => ({
        ...s,
        id: getString(s as SqlRow, 'id'),
        pattern: getString(s as SqlRow, 'pattern'),
        patternPrefix: getString(s as SqlRow, 'pattern_prefix'),
      }))
    )

    // 3. Create delivery record for each match
    const deliveryIds: string[] = []
    const immediateDeliveryIds: string[] = []
    const now = Date.now()
    let batchedCount = 0

    for (const sub of matches) {
      const deliveryId = ulid()
      const subscription = this.rowToSubscription(sub as unknown as SqlRow)

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

        if (subscription.batchEnabled) {
          // Add to batch for time-window delivery
          await this.addToBatch(sub.id, deliveryId, subscription)
          batchedCount++
        } else {
          // Queue for immediate delivery
          immediateDeliveryIds.push(deliveryId)
        }
      } catch (_e) {
        // Likely duplicate - already delivered (UNIQUE constraint on subscription_id, event_id)
        console.log(`Delivery already exists for subscription ${sub.id}, event ${event.id}`)
      }
    }

    // 4. Attempt immediate delivery for non-batched subscriptions
    if (immediateDeliveryIds.length > 0) {
      await this.processDeliveries(immediateDeliveryIds)
    }

    return { matched: matches.length, deliveries: deliveryIds, batched: batchedCount }
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
    const attemptNumber = getNumber(delivery as SqlRow, 'attempt_count') + 1

    try {
      let response: unknown

      // Try RPC via service binding first
      const workerBinding = getOptionalString(delivery as SqlRow, 'worker_binding')
      if (workerBinding && this.env[workerBinding]) {
        const worker = this.env[workerBinding] as { [method: string]: (payload: unknown) => Promise<unknown> }
        const rpcMethod = getString(delivery as SqlRow, 'rpc_method')

        if (typeof worker[rpcMethod] === 'function') {
          const timeoutMs = getNumber(delivery as SqlRow, 'timeout_ms')
          response = await Promise.race([
            worker[rpcMethod](JSON.parse(getString(delivery as SqlRow, 'event_payload'))),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('RPC timeout')), timeoutMs)
            ),
          ])
        } else {
          throw new Error(`RPC method ${rpcMethod} not found on worker`)
        }
      } else {
        // Fallback to HTTP
        const workerId = getString(delivery as SqlRow, 'worker_id')
        const rpcMethod = getString(delivery as SqlRow, 'rpc_method')
        const timeoutMs = getNumber(delivery as SqlRow, 'timeout_ms')

        // Build URL with validation to prevent SSRF attacks
        const deliveryUrl = buildSafeDeliveryUrl(workerId, rpcMethod)

        const httpResponse = await fetch(
          deliveryUrl,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: getString(delivery as SqlRow, 'event_payload'),
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
      this.logDeliveryAttempt(deliveryId, getString(delivery as SqlRow, 'subscription_id'), attemptNumber, 'success', durationMs, null, response)

    } catch (error) {
      const durationMs = Math.round(performance.now() - startTime)
      const errorMessage = error instanceof Error ? error.message : String(error)
      const maxRetries = getNumber(delivery as SqlRow, 'max_retries')

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
      this.logDeliveryAttempt(deliveryId, getString(delivery as SqlRow, 'subscription_id'), attemptNumber, 'failed', durationMs, errorMessage, null)
    }
  }

  /**
   * Calculate retry delay with exponential backoff and jitter
   */
  private calculateRetryDelay(attemptNumber: number): number {
    const exponential = Math.min(SUBSCRIPTION_RETRY_BASE_DELAY_MS * Math.pow(2, attemptNumber - 1), SUBSCRIPTION_RETRY_MAX_DELAY_MS)
    const jitter = Math.random() * SUBSCRIPTION_RETRY_BASE_DELAY_MS
    return Math.round(exponential + jitter)
  }

  /**
   * Schedule an alarm for the next pending retry
   */
  private async scheduleRetryAlarm(): Promise<void> {
    const nextRetry = this.sql.exec(
      `SELECT MIN(next_attempt_at) as next_time FROM deliveries WHERE status = 'failed'`
    ).one()

    if (nextRetry?.next_time) {
      const currentAlarm = await this.ctx.storage.getAlarm()
      const nextTime = getNumber(nextRetry as SqlRow, 'next_time')

      if (!currentAlarm || nextTime < currentAlarm) {
        await this.ctx.storage.setAlarm(nextTime)
      }
    }
  }

  /**
   * Alarm handler - processes failed deliveries and batches ready for delivery
   */
  async alarm(): Promise<void> {
    const now = Date.now()

    // Close any batches whose windows have expired
    this.sql.exec(
      `UPDATE batch_deliveries
       SET window_end = ?, next_attempt_at = ?
       WHERE status = 'pending'
       AND window_end IS NULL
       AND window_start + (
         SELECT batch_window_ms FROM subscriptions WHERE id = batch_deliveries.subscription_id
       ) <= ?`,
      now,
      now,
      now
    )

    // Process batch deliveries ready for delivery
    const readyBatches = this.sql.exec(
      `SELECT id FROM batch_deliveries
       WHERE status IN ('pending', 'failed')
       AND window_end IS NOT NULL
       AND next_attempt_at <= ?
       ORDER BY next_attempt_at ASC
       LIMIT ?`,
      now,
      SUBSCRIPTION_BATCH_LIMIT
    ).toArray()

    for (const row of readyBatches) {
      await this.deliverBatch(getString(row as SqlRow, 'id'))
    }

    // Process individual failed deliveries ready for retry
    const ready = this.sql.exec(
      `SELECT id FROM deliveries
       WHERE status = 'failed' AND next_attempt_at <= ?
       AND batch_id IS NULL
       ORDER BY next_attempt_at ASC
       LIMIT ?`,
      now,
      SUBSCRIPTION_BATCH_LIMIT
    ).toArray()

    for (const row of ready) {
      await this.deliverOne(getString(row as SqlRow, 'id'))
    }

    // Schedule next alarm if more retries/batches pending
    await this.scheduleRetryAlarm()
    await this.scheduleBatchAlarm()

    // Check for open batches that need window-end alarms
    const openBatch = this.sql.exec(
      `SELECT MIN(b.window_start + s.batch_window_ms) as next_window
       FROM batch_deliveries b
       JOIN subscriptions s ON b.subscription_id = s.id
       WHERE b.status = 'pending'
       AND b.window_end IS NULL`
    ).one()

    if (openBatch?.next_window) {
      const nextWindow = getNumber(openBatch as SqlRow, 'next_window')
      const currentAlarm = await this.ctx.storage.getAlarm()
      if (!currentAlarm || nextWindow < currentAlarm) {
        await this.ctx.storage.setAlarm(nextWindow)
      }
    }
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

  // ---------------------------------------------------------------------------
  // Cleanup Methods
  // ---------------------------------------------------------------------------

  /**
   * Clean up old data to prevent unbounded growth
   */
  async cleanupOldData(cutoffTs: number): Promise<{
    deadLettersDeleted: number
    deliveryLogsDeleted: number
    deliveriesDeleted: number
    batchDeliveriesDeleted: number
  }> {
    // 1. Delete old dead letters
    const deadLettersResult = this.sql.exec(
      `DELETE FROM dead_letters WHERE created_at < ?`,
      cutoffTs
    )
    const deadLettersDeleted = deadLettersResult.rowsWritten

    // 2. Delete old delivery logs
    const logsResult = this.sql.exec(
      `DELETE FROM delivery_log
       WHERE created_at < ?
       AND delivery_id IN (
         SELECT id FROM deliveries
         WHERE status IN ('delivered', 'dead')
         AND (delivered_at IS NOT NULL AND delivered_at < ?)
       )`,
      cutoffTs,
      cutoffTs
    )
    const deliveryLogsDeleted = logsResult.rowsWritten

    // 3. Delete old deliveries
    const deliveriesResult = this.sql.exec(
      `DELETE FROM deliveries
       WHERE status IN ('delivered', 'dead')
       AND created_at < ?
       AND (delivered_at IS NOT NULL AND delivered_at < ?)`,
      cutoffTs,
      cutoffTs
    )
    const deliveriesDeleted = deliveriesResult.rowsWritten

    // 4. Delete old batch deliveries
    const batchResult = this.sql.exec(
      `DELETE FROM batch_deliveries
       WHERE status IN ('delivered', 'dead')
       AND created_at < ?
       AND (delivered_at IS NOT NULL AND delivered_at < ?)`,
      cutoffTs,
      cutoffTs
    )
    const batchDeliveriesDeleted = batchResult.rowsWritten

    return {
      deadLettersDeleted,
      deliveryLogsDeleted,
      deliveriesDeleted,
      batchDeliveriesDeleted,
    }
  }
}

// Export type for wrangler config
export type SubscriptionDOType = typeof SubscriptionDO
