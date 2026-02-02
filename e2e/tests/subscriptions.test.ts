/**
 * Subscription Routes E2E Tests
 *
 * Tests /subscriptions/* endpoints for event subscription management.
 * Covers CRUD operations, pattern matching, and delivery flow.
 */

import { SELF } from 'cloudflare:test'
import { describe, it, expect, beforeEach } from 'vitest'

// ============================================================================
// Types
// ============================================================================

interface Subscription {
  id: string
  workerId: string
  workerBinding?: string
  pattern: string
  patternPrefix: string
  rpcMethod: string
  maxRetries: number
  timeoutMs: number
  active: boolean
  createdAt: number
  updatedAt: number
  batchEnabled: boolean
  batchSize: number
  batchWindowMs: number
  _shard?: string
}

interface SubscribeResponse {
  ok: boolean
  subscriptionId?: string
  error?: string
  shard?: string
  namespace?: string | null
  dynamicSharding?: boolean
}

interface ListResponse {
  subscriptions: Subscription[]
  shard?: string
  namespace?: string | null
}

interface GetResponse {
  subscription?: Subscription
  ok?: boolean
  error?: string
}

interface UpdateResponse {
  ok: boolean
  error?: string
}

interface DeleteResponse {
  ok: boolean
  deleted?: {
    deliveries: number
    logs: number
    deadLetters: number
    batchDeliveries: number
  }
  error?: string
}

interface MatchResponse {
  subscriptions: Subscription[]
  namespace?: string | null
}

interface StatusResponse {
  subscription: Subscription | null
  stats: {
    pendingDeliveries: number
    failedDeliveries: number
    deadLetters: number
    successRate: number
    totalDelivered: number
    totalAttempts: number
  }
}

interface UnsubscribeResponse {
  ok: boolean
  error?: string
}

interface ReactivateResponse {
  ok: boolean
  error?: string
}

interface DeadLettersResponse {
  deadLetters: Array<{
    id: string
    deliveryId: string
    subscriptionId: string
    eventId: string
    eventPayload: string
    reason: string
    lastError?: string
    createdAt: number
  }>
}

interface DeliveryLogsResponse {
  logs: Array<{
    id: string
    deliveryId: string
    subscriptionId: string
    attemptNumber: number
    status: string
    durationMs?: number
    errorMessage?: string
    workerResponse?: string
    createdAt: number
  }>
}

interface ErrorResponse {
  ok: false
  error: string
  path?: string
}

// ============================================================================
// Helper Functions
// ============================================================================

async function subscribe(params: {
  workerId: string
  workerBinding?: string
  pattern: string
  rpcMethod: string
  maxRetries?: number
  timeoutMs?: number
}): Promise<SubscribeResponse> {
  const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  return response.json() as Promise<SubscribeResponse>
}

async function listSubscriptions(params?: {
  active?: boolean
  workerId?: string
  patternPrefix?: string
  limit?: number
  offset?: number
  shard?: string
}): Promise<ListResponse> {
  const searchParams = new URLSearchParams()
  if (params?.active !== undefined) searchParams.set('active', String(params.active))
  if (params?.workerId) searchParams.set('workerId', params.workerId)
  if (params?.patternPrefix) searchParams.set('patternPrefix', params.patternPrefix)
  if (params?.limit !== undefined) searchParams.set('limit', String(params.limit))
  if (params?.offset !== undefined) searchParams.set('offset', String(params.offset))
  if (params?.shard) searchParams.set('shard', params.shard)

  const url = `http://localhost/subscriptions/list${searchParams.toString() ? `?${searchParams}` : ''}`
  const response = await SELF.fetch(url)
  return response.json() as Promise<ListResponse>
}

async function getSubscription(id: string): Promise<GetResponse> {
  const response = await SELF.fetch(`http://localhost/subscriptions/${id}`)
  return response.json() as Promise<GetResponse>
}

async function updateSubscription(
  id: string,
  updates: { maxRetries?: number; timeoutMs?: number; rpcMethod?: string }
): Promise<UpdateResponse> {
  const response = await SELF.fetch(`http://localhost/subscriptions/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(updates),
  })
  return response.json() as Promise<UpdateResponse>
}

async function deleteSubscription(id: string): Promise<DeleteResponse> {
  const response = await SELF.fetch(`http://localhost/subscriptions/${id}`, {
    method: 'DELETE',
  })
  return response.json() as Promise<DeleteResponse>
}

async function matchSubscriptions(eventType: string): Promise<MatchResponse> {
  const response = await SELF.fetch(`http://localhost/subscriptions/match?eventType=${encodeURIComponent(eventType)}`)
  return response.json() as Promise<MatchResponse>
}

async function getStatus(id: string): Promise<StatusResponse> {
  const response = await SELF.fetch(`http://localhost/subscriptions/status/${id}`)
  return response.json() as Promise<StatusResponse>
}

async function unsubscribe(subscriptionId: string, shard?: string): Promise<UnsubscribeResponse> {
  const response = await SELF.fetch('http://localhost/subscriptions/unsubscribe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ subscriptionId, shard }),
  })
  return response.json() as Promise<UnsubscribeResponse>
}

async function reactivate(subscriptionId: string, shard?: string): Promise<ReactivateResponse> {
  const response = await SELF.fetch('http://localhost/subscriptions/reactivate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ subscriptionId, shard }),
  })
  return response.json() as Promise<ReactivateResponse>
}

async function getDeadLetters(subscriptionId: string, limit?: number): Promise<DeadLettersResponse> {
  const url = limit !== undefined
    ? `http://localhost/subscriptions/${subscriptionId}/dead-letters?limit=${limit}`
    : `http://localhost/subscriptions/${subscriptionId}/dead-letters`
  const response = await SELF.fetch(url)
  return response.json() as Promise<DeadLettersResponse>
}

async function getDeliveryLogs(deliveryId: string): Promise<DeliveryLogsResponse> {
  const response = await SELF.fetch(`http://localhost/subscriptions/deliveries/${deliveryId}/logs`)
  return response.json() as Promise<DeliveryLogsResponse>
}

// ============================================================================
// 1. Create Subscription Tests - POST /subscriptions/subscribe
// ============================================================================

describe('POST /subscriptions/subscribe', () => {
  it('creates a subscription with required fields', async () => {
    const result = await subscribe({
      workerId: 'my-worker.example.com',
      pattern: 'collection.insert.*',
      rpcMethod: 'handleEvent',
    })

    expect(result.ok).toBe(true)
    expect(result.subscriptionId).toBeDefined()
    expect(result.subscriptionId).toMatch(/^[A-Z0-9]+$/)
    expect(result.shard).toBe('collection')
  })

  it('creates a subscription with all optional fields', async () => {
    const result = await subscribe({
      workerId: 'my-worker.example.com',
      workerBinding: 'MY_WORKER',
      pattern: 'webhook.github.*',
      rpcMethod: 'handleWebhook',
      maxRetries: 10,
      timeoutMs: 60000,
    })

    expect(result.ok).toBe(true)
    expect(result.subscriptionId).toBeDefined()
    expect(result.shard).toBe('webhook')
  })

  it('returns error for missing workerId', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        pattern: 'collection.insert.*',
        rpcMethod: 'handleEvent',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('workerId')
  })

  it('returns error for missing pattern', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        rpcMethod: 'handleEvent',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('pattern')
  })

  it('returns error for missing rpcMethod', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        pattern: 'collection.insert.*',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('rpcMethod')
  })

  it('returns error for invalid pattern with unsafe characters', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        pattern: 'collection<script>alert(1)</script>',
        rpcMethod: 'handleEvent',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('invalid')
  })

  it('returns error for pattern starting with dot', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        pattern: '.collection.insert',
        rpcMethod: 'handleEvent',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('dot')
  })

  it('returns error for pattern with consecutive dots', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        pattern: 'collection..insert',
        rpcMethod: 'handleEvent',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('consecutive dots')
  })

  it('returns error for invalid maxRetries type', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        pattern: 'collection.insert.*',
        rpcMethod: 'handleEvent',
        maxRetries: 'ten',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('maxRetries')
  })

  it('returns error for invalid timeoutMs type', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workerId: 'my-worker.example.com',
        pattern: 'collection.insert.*',
        rpcMethod: 'handleEvent',
        timeoutMs: '30000',
      }),
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('timeoutMs')
  })

  it('returns error for invalid JSON body', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: 'not-valid-json{{{',
    })

    expect(response.status).toBe(400)
    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('Invalid JSON')
  })

  it('routes wildcard patterns to default shard', async () => {
    const result = await subscribe({
      workerId: 'my-worker.example.com',
      pattern: '**',
      rpcMethod: 'handleAll',
    })

    expect(result.ok).toBe(true)
    expect(result.shard).toBe('default')
  })

  it('routes patterns with * prefix to default shard', async () => {
    const result = await subscribe({
      workerId: 'my-worker.example.com',
      pattern: '*.github.*',
      rpcMethod: 'handleAll',
    })

    expect(result.ok).toBe(true)
    expect(result.shard).toBe('default')
  })
})

// ============================================================================
// 2. List Subscriptions Tests - GET /subscriptions/list
// ============================================================================

describe('GET /subscriptions/list', () => {
  it('returns empty array when no subscriptions exist', async () => {
    const result = await listSubscriptions()
    expect(result.subscriptions).toBeInstanceOf(Array)
    expect(result.namespace).toBeNull()
  })

  it('returns subscriptions after creation', async () => {
    // Create a subscription
    const sub = await subscribe({
      workerId: 'list-test-worker.example.com',
      pattern: 'rpc.call.*',
      rpcMethod: 'handleRpc',
    })
    expect(sub.ok).toBe(true)

    // List subscriptions
    const result = await listSubscriptions()
    const found = result.subscriptions.find(s => s.id === sub.subscriptionId)
    expect(found).toBeDefined()
    expect(found?.workerId).toBe('list-test-worker.example.com')
    expect(found?.pattern).toBe('rpc.call.*')
  })

  it('filters by active status', async () => {
    // Create and then unsubscribe
    const sub = await subscribe({
      workerId: 'active-filter-test.example.com',
      pattern: 'do.create.*',
      rpcMethod: 'handleDO',
    })
    expect(sub.ok).toBe(true)

    await unsubscribe(sub.subscriptionId!)

    // List active only should not include deactivated subscription
    const activeResult = await listSubscriptions({ active: true })
    const activeFound = activeResult.subscriptions.find(s => s.id === sub.subscriptionId)
    expect(activeFound).toBeUndefined()

    // List inactive should include it
    const inactiveResult = await listSubscriptions({ active: false })
    const inactiveFound = inactiveResult.subscriptions.find(s => s.id === sub.subscriptionId)
    expect(inactiveFound).toBeDefined()
  })

  it('filters by workerId', async () => {
    const workerIdFilter = 'specific-worker-filter.example.com'

    await subscribe({
      workerId: workerIdFilter,
      pattern: 'ws.connect.*',
      rpcMethod: 'handleWS',
    })

    const result = await listSubscriptions({ workerId: workerIdFilter })
    expect(result.subscriptions.length).toBeGreaterThan(0)
    expect(result.subscriptions.every(s => s.workerId === workerIdFilter)).toBe(true)
  })

  it('filters by patternPrefix', async () => {
    await subscribe({
      workerId: 'prefix-filter-test.example.com',
      pattern: 'ws.message.text',
      rpcMethod: 'handleMessage',
    })

    const result = await listSubscriptions({ patternPrefix: 'ws' })
    expect(result.subscriptions.length).toBeGreaterThan(0)
    expect(result.subscriptions.every(s => s.patternPrefix.startsWith('ws'))).toBe(true)
  })

  it('applies limit', async () => {
    // Create multiple subscriptions
    await subscribe({
      workerId: 'limit-test-1.example.com',
      pattern: 'collection.delete.users',
      rpcMethod: 'handle1',
    })
    await subscribe({
      workerId: 'limit-test-2.example.com',
      pattern: 'collection.delete.posts',
      rpcMethod: 'handle2',
    })

    const result = await listSubscriptions({ limit: 1 })
    expect(result.subscriptions.length).toBeLessThanOrEqual(1)
  })

  it('applies offset', async () => {
    const result1 = await listSubscriptions({ limit: 10 })
    const result2 = await listSubscriptions({ limit: 10, offset: 1 })

    // If we have enough subscriptions, offset should give different results
    if (result1.subscriptions.length > 1) {
      expect(result2.subscriptions[0]?.id).not.toBe(result1.subscriptions[0]?.id)
    }
  })

  it('queries specific shard when provided', async () => {
    await subscribe({
      workerId: 'shard-test.example.com',
      pattern: 'webhook.stripe.*',
      rpcMethod: 'handleStripe',
    })

    const result = await listSubscriptions({ shard: 'webhook' })
    expect(result.shard).toBe('webhook')
    expect(result.subscriptions.every(s => s.pattern.startsWith('webhook'))).toBe(true)
  })
})

// ============================================================================
// 3. Get Subscription Tests - GET /subscriptions/:id
// ============================================================================

describe('GET /subscriptions/:id', () => {
  it('returns subscription by ID', async () => {
    const created = await subscribe({
      workerId: 'get-test.example.com',
      pattern: 'collection.update.*',
      rpcMethod: 'handleUpdate',
      maxRetries: 7,
      timeoutMs: 45000,
    })
    expect(created.ok).toBe(true)

    const result = await getSubscription(created.subscriptionId!)
    expect(result.subscription).toBeDefined()
    expect(result.subscription?.id).toBe(created.subscriptionId)
    expect(result.subscription?.workerId).toBe('get-test.example.com')
    expect(result.subscription?.pattern).toBe('collection.update.*')
    expect(result.subscription?.rpcMethod).toBe('handleUpdate')
    expect(result.subscription?.maxRetries).toBe(7)
    expect(result.subscription?.timeoutMs).toBe(45000)
    expect(result.subscription?.active).toBe(true)
  })

  it('returns 404 for non-existent subscription', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/NONEXISTENT123')
    expect(response.status).toBe(404)

    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('not found')
  })
})

// ============================================================================
// 4. Update Subscription Tests - PUT /subscriptions/:id
// ============================================================================

describe('PUT /subscriptions/:id', () => {
  it('updates maxRetries', async () => {
    const created = await subscribe({
      workerId: 'update-test.example.com',
      pattern: 'do.alarm.*',
      rpcMethod: 'handleAlarm',
    })
    expect(created.ok).toBe(true)

    const updateResult = await updateSubscription(created.subscriptionId!, { maxRetries: 15 })
    expect(updateResult.ok).toBe(true)

    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.subscription?.maxRetries).toBe(15)
  })

  it('updates timeoutMs', async () => {
    const created = await subscribe({
      workerId: 'timeout-update.example.com',
      pattern: 'do.hibernate.*',
      rpcMethod: 'handleHibernate',
    })
    expect(created.ok).toBe(true)

    const updateResult = await updateSubscription(created.subscriptionId!, { timeoutMs: 120000 })
    expect(updateResult.ok).toBe(true)

    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.subscription?.timeoutMs).toBe(120000)
  })

  it('updates rpcMethod', async () => {
    const created = await subscribe({
      workerId: 'method-update.example.com',
      pattern: 'ws.close.*',
      rpcMethod: 'handleClose',
    })
    expect(created.ok).toBe(true)

    const updateResult = await updateSubscription(created.subscriptionId!, { rpcMethod: 'handleDisconnect' })
    expect(updateResult.ok).toBe(true)

    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.subscription?.rpcMethod).toBe('handleDisconnect')
  })

  it('updates multiple fields at once', async () => {
    const created = await subscribe({
      workerId: 'multi-update.example.com',
      pattern: 'rpc.error.*',
      rpcMethod: 'handleError',
    })
    expect(created.ok).toBe(true)

    const updateResult = await updateSubscription(created.subscriptionId!, {
      maxRetries: 3,
      timeoutMs: 10000,
      rpcMethod: 'handleRpcError',
    })
    expect(updateResult.ok).toBe(true)

    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.subscription?.maxRetries).toBe(3)
    expect(getResult.subscription?.timeoutMs).toBe(10000)
    expect(getResult.subscription?.rpcMethod).toBe('handleRpcError')
  })

  it('returns 404 for non-existent subscription', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/NONEXISTENT456', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ maxRetries: 5 }),
    })
    expect(response.status).toBe(404)
  })

  it('returns error for invalid maxRetries type', async () => {
    const created = await subscribe({
      workerId: 'invalid-update.example.com',
      pattern: 'test.pattern.*',
      rpcMethod: 'handle',
    })

    const response = await SELF.fetch(`http://localhost/subscriptions/${created.subscriptionId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ maxRetries: 'invalid' }),
    })
    expect(response.status).toBe(400)
  })

  it('returns error for invalid JSON body', async () => {
    const created = await subscribe({
      workerId: 'json-error.example.com',
      pattern: 'test.json.*',
      rpcMethod: 'handle',
    })

    const response = await SELF.fetch(`http://localhost/subscriptions/${created.subscriptionId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: 'not-valid-json',
    })
    expect(response.status).toBe(400)
  })
})

// ============================================================================
// 5. Delete Subscription Tests - DELETE /subscriptions/:id
// ============================================================================

describe('DELETE /subscriptions/:id', () => {
  it('deletes subscription and returns deletion counts', async () => {
    const created = await subscribe({
      workerId: 'delete-test.example.com',
      pattern: 'collection.cleanup.*',
      rpcMethod: 'handleCleanup',
    })
    expect(created.ok).toBe(true)

    const deleteResult = await deleteSubscription(created.subscriptionId!)
    expect(deleteResult.ok).toBe(true)
    expect(deleteResult.deleted).toBeDefined()
    expect(typeof deleteResult.deleted?.deliveries).toBe('number')
    expect(typeof deleteResult.deleted?.logs).toBe('number')
    expect(typeof deleteResult.deleted?.deadLetters).toBe('number')

    // Verify it's deleted
    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.ok).toBe(false)
  })

  it('returns 404 for non-existent subscription', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/NONEXISTENT789', {
      method: 'DELETE',
    })
    expect(response.status).toBe(404)
  })
})

// ============================================================================
// 6. Pattern Matching Tests - GET /subscriptions/match
// ============================================================================

describe('GET /subscriptions/match', () => {
  it('returns subscriptions matching exact pattern', async () => {
    const created = await subscribe({
      workerId: 'exact-match.example.com',
      pattern: 'webhook.github.push',
      rpcMethod: 'handlePush',
    })
    expect(created.ok).toBe(true)

    const result = await matchSubscriptions('webhook.github.push')
    const found = result.subscriptions.find(s => s.id === created.subscriptionId)
    expect(found).toBeDefined()
  })

  it('returns subscriptions matching wildcard pattern', async () => {
    const created = await subscribe({
      workerId: 'wildcard-match.example.com',
      pattern: 'webhook.github.*',
      rpcMethod: 'handleGitHub',
    })
    expect(created.ok).toBe(true)

    const result = await matchSubscriptions('webhook.github.pull_request')
    const found = result.subscriptions.find(s => s.id === created.subscriptionId)
    expect(found).toBeDefined()
  })

  it('returns subscriptions matching double wildcard pattern', async () => {
    const created = await subscribe({
      workerId: 'double-wildcard.example.com',
      pattern: 'webhook.**',
      rpcMethod: 'handleAllWebhooks',
    })
    expect(created.ok).toBe(true)

    const result = await matchSubscriptions('webhook.github.issues.opened')
    const found = result.subscriptions.find(s => s.id === created.subscriptionId)
    expect(found).toBeDefined()
  })

  it('does not return non-matching subscriptions', async () => {
    const created = await subscribe({
      workerId: 'no-match.example.com',
      pattern: 'webhook.stripe.*',
      rpcMethod: 'handleStripe',
    })
    expect(created.ok).toBe(true)

    const result = await matchSubscriptions('webhook.github.push')
    const found = result.subscriptions.find(s => s.id === created.subscriptionId)
    expect(found).toBeUndefined()
  })

  it('returns error for missing eventType parameter', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/match')
    expect(response.status).toBe(400)

    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('eventType')
  })

  it('matches catch-all pattern from default shard', async () => {
    const created = await subscribe({
      workerId: 'catch-all.example.com',
      pattern: '**',
      rpcMethod: 'handleAll',
    })
    expect(created.ok).toBe(true)
    expect(created.shard).toBe('default')

    // Should match any event type
    const result = await matchSubscriptions('collection.insert.users')
    const found = result.subscriptions.find(s => s.id === created.subscriptionId)
    expect(found).toBeDefined()
  })

  it('returns multiple matching subscriptions', async () => {
    const sub1 = await subscribe({
      workerId: 'multi-match-1.example.com',
      pattern: 'rpc.*',
      rpcMethod: 'handle1',
    })
    const sub2 = await subscribe({
      workerId: 'multi-match-2.example.com',
      pattern: 'rpc.call',
      rpcMethod: 'handle2',
    })

    const result = await matchSubscriptions('rpc.call')
    const found1 = result.subscriptions.find(s => s.id === sub1.subscriptionId)
    const found2 = result.subscriptions.find(s => s.id === sub2.subscriptionId)

    expect(found1).toBeDefined()
    expect(found2).toBeDefined()
  })
})

// ============================================================================
// 7. Subscription Status Tests - GET /subscriptions/status/:id
// ============================================================================

describe('GET /subscriptions/status/:id', () => {
  it('returns subscription with stats', async () => {
    const created = await subscribe({
      workerId: 'status-test.example.com',
      pattern: 'collection.status.*',
      rpcMethod: 'handleStatus',
    })
    expect(created.ok).toBe(true)

    const result = await getStatus(created.subscriptionId!)
    expect(result.subscription).toBeDefined()
    expect(result.subscription?.id).toBe(created.subscriptionId)
    expect(result.stats).toBeDefined()
    expect(typeof result.stats.pendingDeliveries).toBe('number')
    expect(typeof result.stats.failedDeliveries).toBe('number')
    expect(typeof result.stats.deadLetters).toBe('number')
    expect(typeof result.stats.successRate).toBe('number')
    expect(typeof result.stats.totalDelivered).toBe('number')
    expect(typeof result.stats.totalAttempts).toBe('number')
  })

  it('returns null subscription for non-existent ID with zero stats', async () => {
    const result = await getStatus('NONEXISTENT999')
    expect(result.subscription).toBeNull()
    expect(result.stats.pendingDeliveries).toBe(0)
    expect(result.stats.failedDeliveries).toBe(0)
    expect(result.stats.deadLetters).toBe(0)
  })
})

// ============================================================================
// 8. Unsubscribe/Reactivate Tests
// ============================================================================

describe('POST /subscriptions/unsubscribe', () => {
  it('deactivates a subscription', async () => {
    const created = await subscribe({
      workerId: 'unsub-test.example.com',
      pattern: 'test.unsub.*',
      rpcMethod: 'handle',
    })
    expect(created.ok).toBe(true)

    const result = await unsubscribe(created.subscriptionId!)
    expect(result.ok).toBe(true)

    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.subscription?.active).toBe(false)
  })

  it('returns error for missing subscriptionId', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/unsubscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    })
    expect(response.status).toBe(400)

    const result = await response.json() as ErrorResponse
    expect(result.error).toContain('subscriptionId')
  })

  it('returns 404 for non-existent subscription', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/unsubscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subscriptionId: 'NONEXISTENT123' }),
    })
    expect(response.status).toBe(404)
  })
})

describe('POST /subscriptions/reactivate', () => {
  it('reactivates a deactivated subscription', async () => {
    const created = await subscribe({
      workerId: 'reactivate-test.example.com',
      pattern: 'test.reactivate.*',
      rpcMethod: 'handle',
    })
    expect(created.ok).toBe(true)

    await unsubscribe(created.subscriptionId!)

    const result = await reactivate(created.subscriptionId!)
    expect(result.ok).toBe(true)

    const getResult = await getSubscription(created.subscriptionId!)
    expect(getResult.subscription?.active).toBe(true)
  })

  it('returns error for missing subscriptionId', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/reactivate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    })
    expect(response.status).toBe(400)
  })

  it('returns 404 for non-existent subscription', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/reactivate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subscriptionId: 'NONEXISTENT456' }),
    })
    expect(response.status).toBe(404)
  })
})

// ============================================================================
// 9. Dead Letters Tests - GET /subscriptions/:id/dead-letters
// ============================================================================

describe('GET /subscriptions/:id/dead-letters', () => {
  it('returns empty array for subscription with no dead letters', async () => {
    const created = await subscribe({
      workerId: 'dead-letter-test.example.com',
      pattern: 'test.deadletter.*',
      rpcMethod: 'handle',
    })
    expect(created.ok).toBe(true)

    const result = await getDeadLetters(created.subscriptionId!)
    expect(result.deadLetters).toBeInstanceOf(Array)
    expect(result.deadLetters.length).toBe(0)
  })

  it('returns empty array for non-existent subscription', async () => {
    const result = await getDeadLetters('NONEXISTENT789')
    expect(result.deadLetters).toBeInstanceOf(Array)
    expect(result.deadLetters.length).toBe(0)
  })
})

// ============================================================================
// 10. Delivery Logs Tests - GET /subscriptions/deliveries/:id/logs
// ============================================================================

describe('GET /subscriptions/deliveries/:id/logs', () => {
  it('returns empty array for non-existent delivery', async () => {
    const result = await getDeliveryLogs('NONEXISTENT123')
    expect(result.logs).toBeInstanceOf(Array)
    expect(result.logs.length).toBe(0)
  })
})

// ============================================================================
// 11. Unknown Endpoint Tests
// ============================================================================

describe('Unknown subscription endpoints', () => {
  it('returns 404 for unknown subscription endpoint', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/unknown-endpoint')
    expect(response.status).toBe(404)

    const result = await response.json() as ErrorResponse
    expect(result.ok).toBe(false)
    expect(result.error).toContain('Unknown subscription endpoint')
  })
})

// ============================================================================
// 12. Sharding Tests - GET /subscriptions/shards
// ============================================================================

describe('GET /subscriptions/shards', () => {
  it('returns shard information', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/shards')
    expect(response.status).toBe(200)

    const result = await response.json() as Record<string, unknown>
    // Should have either dynamic sharding info or fallback shards
    expect(
      result.dynamicShardingEnabled !== undefined ||
      result.fallbackShards !== undefined
    ).toBe(true)
  })
})

describe('GET /subscriptions/shards/:prefix', () => {
  it('returns shards for a specific prefix', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/shards/collection')
    expect(response.status).toBe(200)

    const result = await response.json() as { prefix: string; shards: string[]; namespace: string | null }
    expect(result.prefix).toBe('collection')
    expect(result.shards).toBeInstanceOf(Array)
  })
})

// ============================================================================
// 13. CORS Headers Tests
// ============================================================================

describe('CORS headers', () => {
  it('returns Content-Type application/json header', async () => {
    const response = await SELF.fetch('http://localhost/subscriptions/list')
    expect(response.headers.get('Content-Type')).toBe('application/json')
  })
})
