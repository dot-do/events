/**
 * events-rl8: SubscriptionDO Tests
 *
 * Tests for subscribe/unsubscribe, pattern matching, fanout delivery,
 * and dead letter handling. Uses mocked SQLite storage.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock SQLite Storage using better-sqlite3-like approach
// ============================================================================

interface Row {
  [key: string]: unknown
}

/**
 * A simple in-memory SQL mock that tracks rows by table.
 * Rather than parsing full SQL, it intercepts specific known query patterns
 * used by SubscriptionDO.
 */
function createMockSqlStorage() {
  const tables: Record<string, Row[]> = {
    subscriptions: [],
    deliveries: [],
    delivery_log: [],
    dead_letters: [],
  }

  const sqlStorage = {
    exec: vi.fn((sql: string, ...params: unknown[]) => {
      const s = sql.trim().replace(/\s+/g, ' ')

      // CREATE TABLE / CREATE INDEX - no-op
      if (s.startsWith('CREATE')) {
        return { toArray: () => [], one: () => null, rowsWritten: 0 }
      }

      // ============================================================
      // INSERT patterns
      // ============================================================

      // INSERT INTO subscriptions
      // SQL: INSERT INTO subscriptions (id, worker_id, worker_binding, pattern, pattern_prefix, rpc_method,
      //        max_retries, timeout_ms, active, created_at, updated_at)
      //      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
      // Note: active = 1 is a SQL literal, not a param. So only 10 params.
      if (s.includes('INSERT INTO subscriptions')) {
        const row: Row = {
          id: params[0],
          worker_id: params[1],
          worker_binding: params[2],
          pattern: params[3],
          pattern_prefix: params[4],
          rpc_method: params[5],
          max_retries: params[6],
          timeout_ms: params[7],
          active: 1, // hardcoded in SQL as literal
          created_at: params[8],
          updated_at: params[9],
        }
        // Check UNIQUE(worker_id, pattern, rpc_method)
        const dup = tables.subscriptions.find(
          r => r.worker_id === row.worker_id && r.pattern === row.pattern && r.rpc_method === row.rpc_method
        )
        if (dup) throw new Error('UNIQUE constraint failed')
        tables.subscriptions.push(row)
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // INSERT INTO deliveries - two patterns:
      // 1. createDelivery: (id, sub_id, event_id, event_type, event_payload, status, attempt_count, next_attempt_at, created_at)
      //    with 'pending' and 0 as SQL literals, so 7 params: id, sub_id, event_id, type, payload, next_attempt_at, created_at
      // 2. fanout: (id, sub_id, event_id, event_type, event_payload, status='pending', created_at)
      //    with 'pending' as SQL literal, so 6 params: id, sub_id, event_id, type, payload, created_at
      if (s.includes('INSERT INTO deliveries')) {
        let row: Row

        if (s.includes("'pending', 0,")) {
          // createDelivery pattern: 7 params
          row = {
            id: params[0],
            subscription_id: params[1],
            event_id: params[2],
            event_type: params[3],
            event_payload: params[4],
            status: 'pending',
            attempt_count: 0,
            next_attempt_at: params[5],
            created_at: params[6],
            delivered_at: null,
            last_error: null,
          }
        } else if (s.includes("'pending'")) {
          // fanout pattern: 6 params
          row = {
            id: params[0],
            subscription_id: params[1],
            event_id: params[2],
            event_type: params[3],
            event_payload: params[4],
            status: 'pending',
            attempt_count: 0,
            next_attempt_at: null,
            created_at: params[5],
            delivered_at: null,
            last_error: null,
          }
        } else {
          // generic fallback: 9 params
          row = {
            id: params[0],
            subscription_id: params[1],
            event_id: params[2],
            event_type: params[3],
            event_payload: params[4],
            status: params[5] || 'pending',
            attempt_count: params[6] || 0,
            next_attempt_at: params[7] || null,
            created_at: params[8],
            delivered_at: null,
            last_error: null,
          }
        }

        // Check UNIQUE(subscription_id, event_id)
        const dup = tables.deliveries.find(
          r => r.subscription_id === row.subscription_id && r.event_id === row.event_id
        )
        if (dup) throw new Error('UNIQUE constraint failed')
        tables.deliveries.push(row)
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // INSERT INTO delivery_log
      // Two patterns:
      // 1. markFailed: (id, delivery_id, subscription_id, attempt_number, status='failed', duration_ms, error_message, created_at)
      //    with 'failed' literal, 7 params
      // 2. logDeliveryAttempt: (id, delivery_id, subscription_id, attempt_number, status, duration_ms, error_message, worker_response, created_at)
      //    9 params
      // 3. markDelivered: (id, delivery_id, subscription_id, attempt_number, status='delivered', duration_ms, worker_response, created_at)
      //    with 'delivered' literal, 7 params
      if (s.includes('INSERT INTO delivery_log')) {
        let row: Row

        if (s.includes("'failed'")) {
          row = {
            id: params[0],
            delivery_id: params[1],
            subscription_id: params[2],
            attempt_number: params[3],
            status: 'failed',
            duration_ms: params[4],
            error_message: params[5],
            worker_response: null,
            created_at: params[6],
          }
        } else if (s.includes("'delivered'")) {
          row = {
            id: params[0],
            delivery_id: params[1],
            subscription_id: params[2],
            attempt_number: params[3],
            status: 'delivered',
            duration_ms: params[4],
            error_message: null,
            worker_response: params[5],
            created_at: params[6],
          }
        } else {
          // logDeliveryAttempt: 9 params
          row = {
            id: params[0],
            delivery_id: params[1],
            subscription_id: params[2],
            attempt_number: params[3],
            status: params[4],
            duration_ms: params[5],
            error_message: params[6],
            worker_response: params[7],
            created_at: params[8],
          }
        }
        tables.delivery_log.push(row)
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // INSERT INTO dead_letters
      if (s.includes('INSERT INTO dead_letters')) {
        tables.dead_letters.push({
          id: params[0],
          delivery_id: params[1],
          subscription_id: params[2],
          event_id: params[3],
          event_payload: params[4],
          reason: params[5],
          last_error: params[6],
          created_at: params[7],
        })
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // ============================================================
      // UPDATE patterns
      // ============================================================

      // UPDATE subscriptions SET active = 0, updated_at = ? WHERE id = ?
      // OR: UPDATE subscriptions SET active = 1, updated_at = ? WHERE id = ?
      // The active value (0 or 1) is a SQL literal, not a param
      if (s.includes('UPDATE subscriptions SET active =') && s.includes('WHERE id =')) {
        const activeMatch = s.match(/active = (\d)/)
        const activeVal = activeMatch ? parseInt(activeMatch[1]) : 0
        const updatedAt = params[0]
        const id = params[1]

        for (const row of tables.subscriptions) {
          if (row.id === id) {
            row.active = activeVal
            row.updated_at = updatedAt
          }
        }
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // UPDATE subscriptions SET updated_at = ? (for updateSubscription)
      if (s.includes('UPDATE subscriptions SET') && !s.includes('active =')) {
        const id = params[params.length - 1]
        for (const row of tables.subscriptions) {
          if (row.id === id) {
            let pIdx = 0
            row.updated_at = params[pIdx++]
            if (s.includes('max_retries =')) row.max_retries = params[pIdx++]
            if (s.includes('timeout_ms =')) row.timeout_ms = params[pIdx++]
            if (s.includes('rpc_method =')) row.rpc_method = params[pIdx++]
          }
        }
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // UPDATE deliveries SET status = 'delivered'
      // Two patterns:
      // 1. markDelivered: SET status = 'delivered', delivered_at = ?, attempt_count = attempt_count + 1 WHERE id = ?
      //    params: [deliveredAt, id]
      // 2. deliverOne: SET status = 'delivered', delivered_at = ?, attempt_count = ? WHERE id = ?
      //    params: [deliveredAt, attemptCount, id]
      if (s.includes('UPDATE deliveries') && s.includes("status = 'delivered'")) {
        if (s.includes('attempt_count = attempt_count + 1')) {
          // markDelivered pattern
          const deliveredAt = params[0]
          const id = params[1]
          for (const row of tables.deliveries) {
            if (row.id === id) {
              row.status = 'delivered'
              row.delivered_at = deliveredAt
              row.attempt_count = (row.attempt_count as number) + 1
            }
          }
        } else {
          // deliverOne pattern
          const deliveredAt = params[0]
          const attemptCount = params[1]
          const id = params[2]
          for (const row of tables.deliveries) {
            if (row.id === id) {
              row.status = 'delivered'
              row.delivered_at = deliveredAt
              row.attempt_count = attemptCount
            }
          }
        }
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // UPDATE deliveries SET status = 'dead'
      if (s.includes('UPDATE deliveries') && s.includes("status = 'dead'")) {
        const attemptCount = params[0]
        const lastError = params[1]
        const id = params[2]
        for (const row of tables.deliveries) {
          if (row.id === id) {
            row.status = 'dead'
            row.attempt_count = attemptCount
            row.last_error = lastError
          }
        }
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // UPDATE deliveries SET status = 'failed'
      // Two patterns:
      // 1. markFailed: SET status = 'failed', attempt_count = ?, last_error = ?, next_attempt_at = ? WHERE id = ?
      //    params: [attemptCount, lastError, nextAttemptAt, id]
      // 2. deliverOne: SET status = 'failed', attempt_count = ?, next_attempt_at = ?, last_error = ? WHERE id = ?
      //    params: [attemptCount, nextAttemptAt, lastError, id]
      if (s.includes('UPDATE deliveries') && s.includes("status = 'failed'")) {
        const id = params[params.length - 1]
        const attemptCount = params[0]

        // Determine column order from SQL
        const setClause = s.match(/SET (.+?) WHERE/i)?.[1] || ''
        const colOrder = setClause.split(',').map(c => c.trim().split(/\s*=\s*/)[0])

        let lastError: unknown = null
        let nextAttemptAt: unknown = null

        // Find positions of last_error and next_attempt_at after attempt_count
        let pIdx = 1 // after attempt_count
        for (const col of colOrder) {
          if (col === 'last_error') { lastError = params[pIdx++]; continue }
          if (col === 'next_attempt_at') { nextAttemptAt = params[pIdx++]; continue }
          if (col === "status" || col === 'attempt_count') continue
        }

        for (const row of tables.deliveries) {
          if (row.id === id) {
            row.status = 'failed'
            row.attempt_count = attemptCount
            row.last_error = lastError
            row.next_attempt_at = nextAttemptAt
          }
        }
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // ============================================================
      // SELECT patterns
      // ============================================================

      // SELECT * FROM subscriptions WHERE id = ?
      if (s.startsWith('SELECT') && s.includes('FROM subscriptions WHERE id =')) {
        const id = params[0]
        const row = tables.subscriptions.find(r => r.id === id)
        return { toArray: () => row ? [row] : [], one: () => row || null }
      }

      // SELECT * FROM subscriptions WHERE active = 1
      if (s.includes('FROM subscriptions WHERE active = 1') && !s.includes('AND')) {
        const rows = tables.subscriptions.filter(r => r.active === 1)
        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT * FROM subscriptions WHERE active = 1 AND (pattern_prefix ...)
      if (s.includes('FROM subscriptions') && s.includes('active = 1') && s.includes('pattern_prefix')) {
        const rows = tables.subscriptions.filter(r => r.active === 1)
        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT * FROM subscriptions WHERE 1=1 (listSubscriptions)
      if (s.includes('FROM subscriptions WHERE 1=1')) {
        let rows = [...tables.subscriptions]
        let pIdx = 0

        if (s.includes('AND active = ?')) {
          const activeVal = params[pIdx++]
          rows = rows.filter(r => r.active === activeVal)
        }
        if (s.includes('AND worker_id = ?')) {
          const workerId = params[pIdx++]
          rows = rows.filter(r => r.worker_id === workerId)
        }
        if (s.includes('AND pattern_prefix LIKE ?')) {
          pIdx++ // skip LIKE param
        }

        // ORDER BY created_at DESC
        rows.sort((a, b) => (b.created_at as number) - (a.created_at as number))

        if (s.includes('LIMIT ?')) {
          const limit = params[pIdx++] as number
          rows = rows.slice(0, limit)
        }
        if (s.includes('OFFSET ?')) {
          const offset = params[pIdx++] as number
          rows = rows.slice(offset)
        }

        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT d.*, s.max_retries FROM deliveries d JOIN subscriptions s (markFailed)
      if (s.includes('FROM deliveries d') && s.includes('JOIN subscriptions s')) {
        const id = params[0]
        const delivery = tables.deliveries.find(r => r.id === id)
        if (!delivery) return { toArray: () => [], one: () => null }

        const subscription = tables.subscriptions.find(r => r.id === delivery.subscription_id)
        if (!subscription) return { toArray: () => [], one: () => null }

        const joined = { ...delivery, max_retries: subscription.max_retries, timeout_ms: subscription.timeout_ms, worker_id: subscription.worker_id, worker_binding: subscription.worker_binding, rpc_method: subscription.rpc_method }
        return { toArray: () => [joined], one: () => joined }
      }

      // SELECT subscription_id, attempt_count FROM deliveries WHERE id = ?
      if (s.includes('FROM deliveries WHERE id =') && s.includes('subscription_id')) {
        const id = params[0]
        const row = tables.deliveries.find(r => r.id === id)
        return { toArray: () => row ? [row] : [], one: () => row || null }
      }

      // SELECT * FROM deliveries WHERE status IN (...) (getPendingDeliveries)
      if (s.includes('FROM deliveries') && s.includes("status IN ('pending', 'failed')")) {
        const now = params[0] as number
        const limit = params[1] as number
        let rows = tables.deliveries.filter(r =>
          (r.status === 'pending' || r.status === 'failed') &&
          (r.next_attempt_at === null || (r.next_attempt_at as number) <= now)
        )
        rows.sort((a, b) => ((a.next_attempt_at as number) || 0) - ((b.next_attempt_at as number) || 0))
        rows = rows.slice(0, limit)
        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT id FROM deliveries WHERE status = 'failed' AND next_attempt_at <= ? (alarm)
      if (s.includes('FROM deliveries') && s.includes("status = 'failed'") && s.includes('next_attempt_at <=')) {
        const now = params[0] as number
        let rows = tables.deliveries.filter(r =>
          r.status === 'failed' && (r.next_attempt_at as number) <= now
        )
        rows.sort((a, b) => ((a.next_attempt_at as number) || 0) - ((b.next_attempt_at as number) || 0))
        rows = rows.slice(0, 100)
        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT * FROM delivery_log WHERE delivery_id = ?
      if (s.includes('FROM delivery_log WHERE delivery_id =')) {
        const id = params[0]
        const rows = tables.delivery_log.filter(r => r.delivery_id === id)
        rows.sort((a, b) => (a.created_at as number) - (b.created_at as number))
        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT * FROM dead_letters WHERE subscription_id = ?
      if (s.includes('FROM dead_letters WHERE subscription_id =')) {
        const subId = params[0]
        let rows = tables.dead_letters.filter(r => r.subscription_id === subId)
        rows.sort((a, b) => (b.created_at as number) - (a.created_at as number))
        if (s.includes('LIMIT')) {
          const limit = params[1] as number
          rows = rows.slice(0, limit)
        }
        return { toArray: () => rows, one: () => rows[0] || null }
      }

      // SELECT * FROM dead_letters WHERE id = ?
      if (s.includes('FROM dead_letters WHERE id =')) {
        const id = params[0]
        const row = tables.dead_letters.find(r => r.id === id)
        return { toArray: () => row ? [row] : [], one: () => row || null }
      }

      // SELECT event_type FROM deliveries WHERE id = ?
      if (s.includes('event_type FROM deliveries WHERE id =')) {
        const id = params[0]
        const row = tables.deliveries.find(r => r.id === id)
        return { toArray: () => row ? [row] : [], one: () => row || null }
      }

      // COUNT queries for stats
      if (s.includes('COUNT(*)') && s.includes('FROM deliveries')) {
        const subId = params[0]
        if (s.includes("status = 'pending'")) {
          const count = tables.deliveries.filter(r => r.subscription_id === subId && r.status === 'pending').length
          return { toArray: () => [{ count }], one: () => ({ count }) }
        }
        if (s.includes("status = 'failed'")) {
          const count = tables.deliveries.filter(r => r.subscription_id === subId && r.status === 'failed').length
          return { toArray: () => [{ count }], one: () => ({ count }) }
        }
        if (s.includes("status = 'delivered'")) {
          const count = tables.deliveries.filter(r => r.subscription_id === subId && r.status === 'delivered').length
          return { toArray: () => [{ count }], one: () => ({ count }) }
        }
      }

      if (s.includes('COUNT(*)') && s.includes('FROM dead_letters')) {
        const subId = params[0]
        const count = tables.dead_letters.filter(r => r.subscription_id === subId).length
        return { toArray: () => [{ count }], one: () => ({ count }) }
      }

      // SUM(attempt_count) for stats
      if (s.includes('SUM(attempt_count)') && s.includes('FROM deliveries')) {
        const subId = params[0]
        const total = tables.deliveries
          .filter(r => r.subscription_id === subId)
          .reduce((acc, r) => acc + ((r.attempt_count as number) || 0), 0)
        return { toArray: () => [{ total }], one: () => ({ total }) }
      }

      // MIN(next_attempt_at) for alarm scheduling
      if (s.includes('MIN(next_attempt_at)')) {
        const failed = tables.deliveries.filter(r => r.status === 'failed' && r.next_attempt_at != null)
        if (failed.length === 0) {
          return { toArray: () => [{ next_time: null }], one: () => ({ next_time: null }) }
        }
        const min = Math.min(...failed.map(r => r.next_attempt_at as number))
        return { toArray: () => [{ next_time: min }], one: () => ({ next_time: min }) }
      }

      // ============================================================
      // DELETE patterns
      // ============================================================

      if (s.includes('DELETE FROM dead_letters WHERE subscription_id =')) {
        const subId = params[0]
        const before = tables.dead_letters.length
        tables.dead_letters = tables.dead_letters.filter(r => r.subscription_id !== subId)
        return { toArray: () => [], one: () => null, rowsWritten: before - tables.dead_letters.length }
      }

      if (s.includes('DELETE FROM dead_letters WHERE id =')) {
        const id = params[0]
        const before = tables.dead_letters.length
        tables.dead_letters = tables.dead_letters.filter(r => r.id !== id)
        return { toArray: () => [], one: () => null, rowsWritten: before - tables.dead_letters.length }
      }

      if (s.includes('DELETE FROM delivery_log WHERE subscription_id =')) {
        const subId = params[0]
        const before = tables.delivery_log.length
        tables.delivery_log = tables.delivery_log.filter(r => r.subscription_id !== subId)
        return { toArray: () => [], one: () => null, rowsWritten: before - tables.delivery_log.length }
      }

      if (s.includes('DELETE FROM deliveries WHERE subscription_id =')) {
        const subId = params[0]
        const before = tables.deliveries.length
        tables.deliveries = tables.deliveries.filter(r => r.subscription_id !== subId)
        return { toArray: () => [], one: () => null, rowsWritten: before - tables.deliveries.length }
      }

      if (s.includes('DELETE FROM subscriptions WHERE id =')) {
        const id = params[0]
        tables.subscriptions = tables.subscriptions.filter(r => r.id !== id)
        return { toArray: () => [], one: () => null, rowsWritten: 1 }
      }

      // Default fallback
      return { toArray: () => [], one: () => null, rowsWritten: 0 }
    }),
    _tables: tables,
  }

  return sqlStorage
}

function createMockCtx() {
  const sqlStorage = createMockSqlStorage()

  return {
    id: {
      toString: () => 'subscription-do-id',
      equals: (other: any) => other.toString() === 'subscription-do-id',
    },
    storage: {
      sql: sqlStorage,
      get: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
      getAlarm: vi.fn(async () => null),
      setAlarm: vi.fn(),
    },
    waitUntil: vi.fn(),
    blockConcurrencyWhile: vi.fn(<T>(fn: () => Promise<T>) => fn()),
  }
}

// ============================================================================
// Mock cloudflare:workers before importing SubscriptionDO
// ============================================================================

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    ctx: any
    env: any
    constructor(ctx: any, env: any) {
      this.ctx = ctx
      this.env = env
    }
  },
}))

import { SubscriptionDO } from '../src/subscription'

// ============================================================================
// Tests
// ============================================================================

describe('SubscriptionDO', () => {
  let sub: SubscriptionDO
  let mockCtx: ReturnType<typeof createMockCtx>

  beforeEach(() => {
    mockCtx = createMockCtx()
    // Mock global fetch for delivery attempts (fanout calls deliverOne which calls fetch)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    ))
    sub = new SubscriptionDO(mockCtx as any, {})
  })

  // --------------------------------------------------------------------------
  // Subscribe / Unsubscribe
  // --------------------------------------------------------------------------

  describe('subscribe', () => {
    it('creates a new subscription successfully', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.github.*',
        rpcMethod: 'handleWebhook',
      })

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.subscriptionId).toBeDefined()
        expect(result.subscriptionId.length).toBe(26) // ULID length
      }
    })

    it('creates subscription with custom options', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        workerBinding: 'WEBHOOK_HANDLER',
        pattern: 'webhook.stripe.**',
        rpcMethod: 'processPayment',
        maxRetries: 10,
        timeoutMs: 60000,
      })

      expect(result.ok).toBe(true)
      if (result.ok) {
        const subscription = await sub.getSubscription(result.subscriptionId)
        expect(subscription).not.toBeNull()
        expect(subscription!.workerBinding).toBe('WEBHOOK_HANDLER')
        expect(subscription!.maxRetries).toBe(10)
        expect(subscription!.timeoutMs).toBe(60000)
      }
    })

    it('uses default maxRetries and timeoutMs', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })

      expect(result.ok).toBe(true)
      if (result.ok) {
        const subscription = await sub.getSubscription(result.subscriptionId)
        expect(subscription!.maxRetries).toBe(5)
        expect(subscription!.timeoutMs).toBe(30000)
      }
    })

    it('rejects duplicate subscription (same worker/pattern/method)', async () => {
      await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.github.*',
        rpcMethod: 'handleWebhook',
      })

      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.github.*',
        rpcMethod: 'handleWebhook',
      })

      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toContain('already exists')
      }
    })

    it('allows same pattern with different method', async () => {
      const r1 = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.github.*',
        rpcMethod: 'handleA',
      })
      const r2 = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.github.*',
        rpcMethod: 'handleB',
      })

      expect(r1.ok).toBe(true)
      expect(r2.ok).toBe(true)
    })

    it('allows same pattern with different worker', async () => {
      const r1 = await sub.subscribe({
        workerId: 'worker-a',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })
      const r2 = await sub.subscribe({
        workerId: 'worker-b',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })

      expect(r1.ok).toBe(true)
      expect(r2.ok).toBe(true)
    })
  })

  describe('unsubscribe', () => {
    it('deactivates a subscription (soft delete)', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })
      expect(result.ok).toBe(true)
      if (!result.ok) return

      await sub.unsubscribe(result.subscriptionId)

      const subscription = await sub.getSubscription(result.subscriptionId)
      expect(subscription).not.toBeNull()
      expect(subscription!.active).toBe(false)
    })

    it('returns ok even for non-existent subscription', async () => {
      const result = await sub.unsubscribe('nonexistent-id')
      expect(result.ok).toBe(true)
    })
  })

  describe('reactivate', () => {
    it('reactivates a deactivated subscription', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })
      if (!result.ok) return

      await sub.unsubscribe(result.subscriptionId)
      let subscription = await sub.getSubscription(result.subscriptionId)
      expect(subscription!.active).toBe(false)

      await sub.reactivate(result.subscriptionId)
      subscription = await sub.getSubscription(result.subscriptionId)
      expect(subscription!.active).toBe(true)
    })
  })

  describe('deleteSubscription', () => {
    it('permanently deletes subscription and related data', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })
      if (!result.ok) return

      const deleteResult = await sub.deleteSubscription(result.subscriptionId)
      expect(deleteResult.ok).toBe(true)

      const subscription = await sub.getSubscription(result.subscriptionId)
      expect(subscription).toBeNull()
    })
  })

  describe('updateSubscription', () => {
    it('updates maxRetries', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })
      if (!result.ok) return

      await sub.updateSubscription(result.subscriptionId, { maxRetries: 10 })

      const subscription = await sub.getSubscription(result.subscriptionId)
      expect(subscription!.maxRetries).toBe(10)
    })

    it('updates timeoutMs', async () => {
      const result = await sub.subscribe({
        workerId: 'my-worker',
        pattern: 'webhook.**',
        rpcMethod: 'handle',
      })
      if (!result.ok) return

      await sub.updateSubscription(result.subscriptionId, { timeoutMs: 60000 })

      const subscription = await sub.getSubscription(result.subscriptionId)
      expect(subscription!.timeoutMs).toBe(60000)
    })
  })

  describe('listSubscriptions', () => {
    it('lists all subscriptions', async () => {
      await sub.subscribe({ workerId: 'worker-1', pattern: 'a.*', rpcMethod: 'handle' })
      await sub.subscribe({ workerId: 'worker-2', pattern: 'b.*', rpcMethod: 'handle' })

      const subscriptions = await sub.listSubscriptions()
      expect(subscriptions).toHaveLength(2)
    })

    it('filters by active status', async () => {
      const r1 = await sub.subscribe({ workerId: 'worker-1', pattern: 'a.*', rpcMethod: 'handle' })
      await sub.subscribe({ workerId: 'worker-2', pattern: 'b.*', rpcMethod: 'handle' })

      if (r1.ok) {
        await sub.unsubscribe(r1.subscriptionId)
      }

      const active = await sub.listSubscriptions({ active: true })
      expect(active).toHaveLength(1)
      expect(active[0].workerId).toBe('worker-2')
    })

    it('filters by workerId', async () => {
      await sub.subscribe({ workerId: 'worker-1', pattern: 'a.*', rpcMethod: 'handle' })
      await sub.subscribe({ workerId: 'worker-1', pattern: 'b.*', rpcMethod: 'handle' })
      await sub.subscribe({ workerId: 'worker-2', pattern: 'c.*', rpcMethod: 'handle' })

      const results = await sub.listSubscriptions({ workerId: 'worker-1' })
      expect(results).toHaveLength(2)
      expect(results.every(s => s.workerId === 'worker-1')).toBe(true)
    })
  })

  // --------------------------------------------------------------------------
  // Pattern matching via findMatchingSubscriptions
  // --------------------------------------------------------------------------

  describe('findMatchingSubscriptions', () => {
    it('finds subscriptions matching exact event type', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.github.push', rpcMethod: 'handle' })
      await sub.subscribe({ workerId: 'w2', pattern: 'webhook.stripe.invoice', rpcMethod: 'handle' })

      const matches = await sub.findMatchingSubscriptions('webhook.github.push')
      expect(matches).toHaveLength(1)
      expect(matches[0].pattern).toBe('webhook.github.push')
    })

    it('finds subscriptions matching single wildcard', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.github.*', rpcMethod: 'handle' })

      const matches = await sub.findMatchingSubscriptions('webhook.github.push')
      expect(matches).toHaveLength(1)
    })

    it('single wildcard does not match multiple segments', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.github.*', rpcMethod: 'handle' })

      const matches = await sub.findMatchingSubscriptions('webhook.github.push.v1')
      expect(matches).toHaveLength(0)
    })

    it('finds subscriptions matching double wildcard', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.**', rpcMethod: 'handle' })

      const matches1 = await sub.findMatchingSubscriptions('webhook.github.push')
      expect(matches1.length).toBeGreaterThanOrEqual(1)

      const matches2 = await sub.findMatchingSubscriptions('webhook.stripe.invoice.paid')
      expect(matches2.length).toBeGreaterThanOrEqual(1)
    })

    it('double wildcard (**) matches everything', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })

      const matches = await sub.findMatchingSubscriptions('any.random.event')
      expect(matches).toHaveLength(1)
    })

    it('does not match inactive subscriptions', async () => {
      const r = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (r.ok) {
        await sub.unsubscribe(r.subscriptionId)
      }

      const matches = await sub.findMatchingSubscriptions('any.event')
      expect(matches).toHaveLength(0)
    })

    it('matches multiple subscriptions for one event', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.**', rpcMethod: 'handleAll' })
      await sub.subscribe({ workerId: 'w2', pattern: 'webhook.github.*', rpcMethod: 'handleGithub' })
      await sub.subscribe({ workerId: 'w3', pattern: '**', rpcMethod: 'handleEverything' })

      const matches = await sub.findMatchingSubscriptions('webhook.github.push')
      expect(matches.length).toBeGreaterThanOrEqual(3)
    })

    it('returns empty array for unmatched events', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.github.*', rpcMethod: 'handle' })

      const matches = await sub.findMatchingSubscriptions('email.sent')
      expect(matches).toHaveLength(0)
    })
  })

  // --------------------------------------------------------------------------
  // Delivery management
  // --------------------------------------------------------------------------

  describe('createDelivery', () => {
    it('creates a delivery record', async () => {
      const subResult = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (!subResult.ok) return

      const result = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'webhook.github.push',
        eventPayload: { action: 'push', repo: 'test' },
      })

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.deliveryId).toBeDefined()
      }
    })

    it('rejects duplicate delivery for same subscription/event', async () => {
      const subResult = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (!subResult.ok) return

      const r1 = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test',
        eventPayload: {},
      })

      const r2 = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test',
        eventPayload: {},
      })

      expect(r1.ok).toBe(true)
      expect(r2.ok).toBe(false)
      if (!r2.ok) {
        expect(r2.error).toContain('already exists')
      }
    })
  })

  describe('markDelivered', () => {
    it('marks a delivery as successful', async () => {
      const subResult = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (!subResult.ok) return

      const delResult = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test',
        eventPayload: {},
      })
      if (!delResult.ok) return

      await sub.markDelivered(delResult.deliveryId, 150, '{"ok":true}')

      // Check stats
      const status = await sub.getSubscriptionStatus(subResult.subscriptionId)
      expect(status.stats.totalDelivered).toBe(1)
    })
  })

  // --------------------------------------------------------------------------
  // Dead letter handling
  // --------------------------------------------------------------------------

  describe('markFailed and dead letters', () => {
    it('retries on failure when retries remain', async () => {
      const subResult = await sub.subscribe({
        workerId: 'w1',
        pattern: '**',
        rpcMethod: 'handle',
        maxRetries: 3,
      })
      if (!subResult.ok) return

      const delResult = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test',
        eventPayload: {},
      })
      if (!delResult.ok) return

      const result = await sub.markFailed(delResult.deliveryId, 'Connection timeout')

      expect(result.retrying).toBe(true)
      expect(result.deadLettered).toBe(false)
    })

    it('moves to dead letter after max retries', async () => {
      const subResult = await sub.subscribe({
        workerId: 'w1',
        pattern: '**',
        rpcMethod: 'handle',
        maxRetries: 1,
      })
      if (!subResult.ok) return

      const delResult = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test',
        eventPayload: { data: 'test' },
      })
      if (!delResult.ok) return

      const result = await sub.markFailed(delResult.deliveryId, 'Connection refused')

      expect(result.retrying).toBe(false)
      expect(result.deadLettered).toBe(true)

      // Check dead letters
      const deadLetters = await sub.getDeadLetters(subResult.subscriptionId)
      expect(deadLetters).toHaveLength(1)
      expect(deadLetters[0].reason).toBe('Max retries exceeded')
      expect(deadLetters[0].lastError).toBe('Connection refused')
    })

    it('returns non-retrying for non-existent delivery', async () => {
      const result = await sub.markFailed('nonexistent', 'Error')
      expect(result.retrying).toBe(false)
      expect(result.deadLettered).toBe(false)
    })
  })

  describe('getDeadLetters', () => {
    it('returns empty array when no dead letters', async () => {
      const subResult = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (!subResult.ok) return

      const deadLetters = await sub.getDeadLetters(subResult.subscriptionId)
      expect(deadLetters).toEqual([])
    })
  })

  describe('getDeliveryLogs', () => {
    it('returns delivery attempt logs', async () => {
      const subResult = await sub.subscribe({
        workerId: 'w1',
        pattern: '**',
        rpcMethod: 'handle',
        maxRetries: 5,
      })
      if (!subResult.ok) return

      const delResult = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test',
        eventPayload: {},
      })
      if (!delResult.ok) return

      // Mark as failed to create a log entry
      await sub.markFailed(delResult.deliveryId, 'Timeout', 5000)

      const logs = await sub.getDeliveryLogs(delResult.deliveryId)
      expect(logs).toHaveLength(1)
      expect(logs[0].status).toBe('failed')
      expect(logs[0].errorMessage).toBe('Timeout')
    })
  })

  // --------------------------------------------------------------------------
  // Subscription status
  // --------------------------------------------------------------------------

  describe('getSubscriptionStatus', () => {
    it('returns stats for a subscription', async () => {
      const subResult = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (!subResult.ok) return

      const status = await sub.getSubscriptionStatus(subResult.subscriptionId)
      expect(status.subscription).not.toBeNull()
      expect(status.stats.pendingDeliveries).toBe(0)
      expect(status.stats.failedDeliveries).toBe(0)
      expect(status.stats.deadLetters).toBe(0)
      expect(status.stats.totalDelivered).toBe(0)
    })

    it('returns null subscription for non-existent id', async () => {
      const status = await sub.getSubscriptionStatus('nonexistent')
      expect(status.subscription).toBeNull()
    })

    // Regression test for events-2dm: totalDelivered should use deliveredRow, not pendingRow
    it('correctly calculates totalDelivered from delivered deliveries', async () => {
      const subResult = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (!subResult.ok) return

      // Create and mark a delivery as delivered
      const delResult = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-001',
        eventType: 'test.event',
        eventPayload: { data: 'test' },
      })
      if (!delResult.ok) return

      await sub.markDelivered(delResult.deliveryId, 100, '{"ok":true}')

      // Create a second delivery and mark it as delivered too
      const delResult2 = await sub.createDelivery({
        subscriptionId: subResult.subscriptionId,
        eventId: 'evt-002',
        eventType: 'test.event2',
        eventPayload: { data: 'test2' },
      })
      if (!delResult2.ok) return

      await sub.markDelivered(delResult2.deliveryId, 150, '{"ok":true}')

      // Now check the stats - totalDelivered should be 2
      const status = await sub.getSubscriptionStatus(subResult.subscriptionId)
      expect(status.stats.totalDelivered).toBe(2)
      expect(status.stats.pendingDeliveries).toBe(0)
      expect(status.stats.failedDeliveries).toBe(0)
    })
  })

  // --------------------------------------------------------------------------
  // Fanout
  // --------------------------------------------------------------------------

  describe('fanout', () => {
    it('fans out event to matching subscriptions', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.github.*', rpcMethod: 'handle' })
      await sub.subscribe({ workerId: 'w2', pattern: 'webhook.stripe.*', rpcMethod: 'handle' })

      const result = await sub.fanout({
        id: 'evt-001',
        type: 'webhook.github.push',
        ts: new Date().toISOString(),
        payload: { action: 'push' },
      })

      // Should match the github subscription but not stripe
      expect(result.matched).toBeGreaterThanOrEqual(1)
      expect(result.deliveries.length).toBeGreaterThanOrEqual(1)
    })

    it('fans out to multiple matching subscriptions', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.**', rpcMethod: 'handleAll' })
      await sub.subscribe({ workerId: 'w2', pattern: 'webhook.github.*', rpcMethod: 'handleGithub' })
      await sub.subscribe({ workerId: 'w3', pattern: '**', rpcMethod: 'handleEverything' })

      const result = await sub.fanout({
        id: 'evt-001',
        type: 'webhook.github.push',
        ts: new Date().toISOString(),
        payload: { action: 'push' },
      })

      expect(result.matched).toBeGreaterThanOrEqual(3)
    })

    it('returns zero matches for unmatched event', async () => {
      await sub.subscribe({ workerId: 'w1', pattern: 'webhook.github.*', rpcMethod: 'handle' })

      const result = await sub.fanout({
        id: 'evt-001',
        type: 'email.sent',
        ts: new Date().toISOString(),
        payload: {},
      })

      expect(result.matched).toBe(0)
      expect(result.deliveries).toHaveLength(0)
    })

    it('skips inactive subscriptions during fanout', async () => {
      const r = await sub.subscribe({ workerId: 'w1', pattern: '**', rpcMethod: 'handle' })
      if (r.ok) {
        await sub.unsubscribe(r.subscriptionId)
      }

      const result = await sub.fanout({
        id: 'evt-001',
        type: 'test.event',
        ts: new Date().toISOString(),
        payload: {},
      })

      expect(result.matched).toBe(0)
    })
  })
})
