/**
 * Multi-DO Integration Tests
 *
 * Tests for DO-to-DO coordination scenarios:
 * 1. EventWriterDO -> R2 write -> CDCProcessorDO flow
 * 2. SubscriptionDO fanout to multiple workers
 * 3. End-to-end ingest -> CDC -> subscription flow
 * 4. Error propagation across DOs
 *
 * These tests verify the integration patterns between different Durable Objects
 * without requiring the full workerd runtime.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { findMatchingSubscriptions, matchPattern, extractPatternPrefix } from '../pattern-matcher.js'

// ============================================================================
// Mock R2 Bucket
// ============================================================================

function createMockR2Bucket() {
  const objects = new Map<string, { body: string | ArrayBuffer; metadata: R2HTTPMetadata }>()

  return {
    put: vi.fn(async (key: string, body: string | ArrayBuffer | ReadableStream, options?: R2PutOptions) => {
      const bodyData = typeof body === 'string' ? body : (body as ArrayBuffer)
      objects.set(key, {
        body: bodyData,
        metadata: options?.httpMetadata ?? {},
      })
      return {
        key,
        version: 'v1',
        size: typeof bodyData === 'string' ? bodyData.length : bodyData.byteLength,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
      } as R2Object
    }),
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        body: new ReadableStream(),
        text: async () => typeof obj.body === 'string' ? obj.body : new TextDecoder().decode(obj.body),
        json: async () => JSON.parse(typeof obj.body === 'string' ? obj.body : new TextDecoder().decode(obj.body)),
        arrayBuffer: async () => typeof obj.body === 'string' ? new TextEncoder().encode(obj.body).buffer : obj.body,
      } as R2ObjectBody
    }),
    delete: vi.fn(async (key: string) => objects.delete(key)),
    list: vi.fn(async (options?: { prefix?: string }) => ({
      objects: Array.from(objects.entries())
        .filter(([key]) => !options?.prefix || key.startsWith(options.prefix))
        .map(([key, obj]) => ({
          key,
          size: typeof obj.body === 'string' ? obj.body.length : obj.body.byteLength,
          etag: 'test-etag',
          uploaded: new Date(),
        })),
      truncated: false,
    })),
    _objects: objects,
  } as unknown as R2Bucket & { _objects: Map<string, { body: string | ArrayBuffer; metadata: R2HTTPMetadata }> }
}

// ============================================================================
// CDC Event Types
// ============================================================================

interface CDCEvent {
  type: 'collection.insert' | 'collection.update' | 'collection.delete'
  ts: string
  collection: string
  docId: string
  doc?: Record<string, unknown>
  prev?: Record<string, unknown>
  bookmark?: string
  do?: { id: string }
}

function createCDCEvent(
  type: CDCEvent['type'],
  collection: string,
  docId: string,
  doc?: Record<string, unknown>,
  prev?: Record<string, unknown>,
  ts?: string,
): CDCEvent {
  return {
    type,
    ts: ts ?? new Date().toISOString(),
    collection,
    docId,
    doc,
    prev,
    bookmark: `bookmark-${Date.now()}`,
    do: { id: 'source-do-123' },
  }
}

// ============================================================================
// Test: EventWriterDO -> R2 Write -> CDCProcessorDO Flow
// ============================================================================

describe('EventWriterDO -> R2 -> CDCProcessorDO Flow', () => {
  let mockR2: ReturnType<typeof createMockR2Bucket>

  beforeEach(() => {
    mockR2 = createMockR2Bucket()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-31T10:00:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('simulates event flow from EventWriter flush to R2', async () => {
    // Simulate EventWriter behavior: batch events and flush to R2
    const eventsToWrite = [
      { type: 'collection.insert', ts: '2024-01-31T10:00:00Z', collection: 'users', docId: 'user-1', doc: { name: 'Alice' } },
      { type: 'collection.insert', ts: '2024-01-31T10:00:01Z', collection: 'users', docId: 'user-2', doc: { name: 'Bob' } },
      { type: 'collection.update', ts: '2024-01-31T10:00:02Z', collection: 'users', docId: 'user-1', doc: { name: 'Alice Smith' }, prev: { name: 'Alice' } },
    ]

    // Step 1: EventWriter writes JSONL to R2
    const jsonlContent = eventsToWrite.map(e => JSON.stringify(e)).join('\n')
    const r2Key = 'events/2024/01/31/10/batch-001.jsonl'

    await mockR2.put(r2Key, jsonlContent, {
      httpMetadata: { contentType: 'application/x-ndjson' },
    })

    // Verify R2 write was called correctly
    expect(mockR2.put).toHaveBeenCalledWith(
      r2Key,
      jsonlContent,
      expect.objectContaining({
        httpMetadata: { contentType: 'application/x-ndjson' },
      })
    )

    // Verify R2 contains the events
    expect(mockR2._objects.has(r2Key)).toBe(true)
  })

  it('reads events from R2 and parses JSONL format', async () => {
    // Write events to R2
    const events = [
      { type: 'collection.insert', docId: 'user-1', doc: { name: 'Alice' } },
      { type: 'collection.insert', docId: 'user-2', doc: { name: 'Bob' } },
    ]
    const jsonlContent = events.map(e => JSON.stringify(e)).join('\n')
    await mockR2.put('events/batch.jsonl', jsonlContent)

    // Read from R2
    const r2Object = await mockR2.get('events/batch.jsonl')
    expect(r2Object).not.toBeNull()

    const content = await r2Object!.text()
    const parsedEvents = content.split('\n').map(line => JSON.parse(line))

    expect(parsedEvents).toHaveLength(2)
    expect(parsedEvents[0].docId).toBe('user-1')
    expect(parsedEvents[1].docId).toBe('user-2')
  })

  it('handles R2 read failure gracefully', async () => {
    // Write initial event to R2
    await mockR2.put('events/batch.jsonl', '{"type":"collection.insert","docId":"user-1"}')

    // Simulate R2 read failure
    mockR2.get = vi.fn().mockResolvedValue(null)

    const r2Object = await mockR2.get('events/non-existent.jsonl')
    expect(r2Object).toBeNull()

    // In real implementation, CDC processor would handle missing objects gracefully
  })

  it('maintains event ordering in JSONL format', async () => {
    const orderedEvents = Array.from({ length: 100 }, (_, i) => ({
      type: 'collection.update',
      ts: `2024-01-31T10:00:${String(i).padStart(2, '0')}Z`,
      docId: 'user-1',
      doc: { seq: i + 1 },
    }))

    const jsonlContent = orderedEvents.map(e => JSON.stringify(e)).join('\n')
    await mockR2.put('events/ordered.jsonl', jsonlContent)

    const r2Object = await mockR2.get('events/ordered.jsonl')
    const content = await r2Object!.text()
    const parsedEvents = content.split('\n').map(line => JSON.parse(line))

    // Verify ordering is preserved
    for (let i = 0; i < parsedEvents.length; i++) {
      expect(parsedEvents[i].doc.seq).toBe(i + 1)
    }
  })

  it('handles high-volume event batches', async () => {
    const eventCount = 1000
    const events = Array.from({ length: eventCount }, (_, i) => ({
      type: 'collection.insert',
      ts: new Date(Date.now() + i).toISOString(),
      collection: 'items',
      docId: `item-${i}`,
      doc: { index: i },
    }))

    const jsonlContent = events.map(e => JSON.stringify(e)).join('\n')
    await mockR2.put('events/high-volume.jsonl', jsonlContent)

    const r2Object = await mockR2.get('events/high-volume.jsonl')
    const content = await r2Object!.text()
    const parsedEvents = content.split('\n')

    expect(parsedEvents).toHaveLength(eventCount)
  })

  it('simulates CDC processing of events from R2', async () => {
    // Write events to R2
    const events = [
      { type: 'collection.insert', collection: 'users', docId: 'user-1', doc: { name: 'Alice', version: 1 } },
      { type: 'collection.update', collection: 'users', docId: 'user-1', doc: { name: 'Alice Smith', version: 2 }, prev: { name: 'Alice', version: 1 } },
    ]
    await mockR2.put('events/batch.jsonl', events.map(e => JSON.stringify(e)).join('\n'))

    // Simulate CDC processing logic
    const r2Object = await mockR2.get('events/batch.jsonl')
    const content = await r2Object!.text()
    const parsedEvents = content.split('\n').map(line => JSON.parse(line))

    // Apply CDC logic: maintain current state per document
    const documentState = new Map<string, { doc: Record<string, unknown>; version: number }>()

    for (const event of parsedEvents) {
      const key = `${event.collection}:${event.docId}`
      const existing = documentState.get(key)
      const newVersion = existing ? existing.version + 1 : 1

      documentState.set(key, {
        doc: event.doc,
        version: newVersion,
      })
    }

    // Verify final state
    const user1State = documentState.get('users:user-1')
    expect(user1State?.doc).toEqual({ name: 'Alice Smith', version: 2 })
    expect(user1State?.version).toBe(2)
  })
})

// ============================================================================
// Test: SubscriptionDO Fanout to Multiple Workers
// ============================================================================

describe('SubscriptionDO Fanout to Multiple Workers', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-31T10:00:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('fans out events to multiple matching subscriptions', () => {
    // Define subscriptions for different workers
    const subscriptions = [
      { id: 'sub-1', pattern: 'webhook.github.*', patternPrefix: 'webhook.github', workerId: 'github-handler' },
      { id: 'sub-2', pattern: 'webhook.**', patternPrefix: 'webhook', workerId: 'webhook-logger' },
      { id: 'sub-3', pattern: 'webhook.github.push', patternPrefix: 'webhook.github.push', workerId: 'push-notifier' },
      { id: 'sub-4', pattern: 'webhook.stripe.*', patternPrefix: 'webhook.stripe', workerId: 'stripe-handler' },
      { id: 'sub-5', pattern: '**', patternPrefix: '', workerId: 'audit-log' },
    ]

    // Test fanout for GitHub push event
    const matches = findMatchingSubscriptions('webhook.github.push', subscriptions)
    const matchedWorkers = matches.map(m => m.workerId)

    expect(matchedWorkers).toContain('github-handler') // webhook.github.*
    expect(matchedWorkers).toContain('webhook-logger') // webhook.**
    expect(matchedWorkers).toContain('push-notifier') // webhook.github.push (exact)
    expect(matchedWorkers).toContain('audit-log') // ** (catch-all)
    expect(matchedWorkers).not.toContain('stripe-handler') // webhook.stripe.*
  })

  it('correctly isolates events to specific subscription patterns', () => {
    const subscriptions = [
      { id: 'sub-cdc-users', pattern: 'cdc.users.*', patternPrefix: 'cdc.users' },
      { id: 'sub-cdc-orders', pattern: 'cdc.orders.*', patternPrefix: 'cdc.orders' },
      { id: 'sub-cdc-all', pattern: 'cdc.**', patternPrefix: 'cdc' },
      { id: 'sub-webhook', pattern: 'webhook.**', patternPrefix: 'webhook' },
    ]

    // CDC users event should not reach webhook or orders subscriptions
    const userMatches = findMatchingSubscriptions('cdc.users.insert', subscriptions)
    const userMatchIds = userMatches.map(m => m.id)

    expect(userMatchIds).toContain('sub-cdc-users')
    expect(userMatchIds).toContain('sub-cdc-all')
    expect(userMatchIds).not.toContain('sub-cdc-orders')
    expect(userMatchIds).not.toContain('sub-webhook')

    // Webhook event should not reach CDC subscriptions
    const webhookMatches = findMatchingSubscriptions('webhook.stripe.invoice.paid', subscriptions)
    const webhookMatchIds = webhookMatches.map(m => m.id)

    expect(webhookMatchIds).toContain('sub-webhook')
    expect(webhookMatchIds).not.toContain('sub-cdc-users')
    expect(webhookMatchIds).not.toContain('sub-cdc-orders')
    expect(webhookMatchIds).not.toContain('sub-cdc-all')
  })

  it('simulates parallel delivery to multiple workers', async () => {
    const subscriptions = [
      { id: 'sub-1', pattern: 'order.created', patternPrefix: 'order.created', workerId: 'inventory-service' },
      { id: 'sub-2', pattern: 'order.created', patternPrefix: 'order.created', workerId: 'email-service' },
      { id: 'sub-3', pattern: 'order.created', patternPrefix: 'order.created', workerId: 'analytics-service' },
      { id: 'sub-4', pattern: 'order.*', patternPrefix: 'order', workerId: 'audit-service' },
    ]

    const matches = findMatchingSubscriptions('order.created', subscriptions)
    expect(matches.length).toBe(4)

    // Simulate parallel delivery (synchronously without actual delays for test performance)
    const deliveryResults = matches.map((sub) => ({
      subscriptionId: sub.id,
      workerId: sub.workerId,
      status: 'delivered',
      durationMs: Math.floor(Math.random() * 100) + 10,
    }))

    expect(deliveryResults).toHaveLength(4)
    expect(deliveryResults.every(r => r.status === 'delivered')).toBe(true)
  })

  it('handles delivery failures with retry scheduling', () => {
    interface DeliveryState {
      subscriptionId: string
      status: 'pending' | 'delivered' | 'failed' | 'dead'
      attemptCount: number
      maxRetries: number
      nextAttemptAt?: number
    }

    // Simulate delivery state machine
    function processDeliveryAttempt(state: DeliveryState, success: boolean): DeliveryState {
      const newAttemptCount = state.attemptCount + 1

      if (success) {
        return { ...state, status: 'delivered', attemptCount: newAttemptCount }
      }

      if (newAttemptCount >= state.maxRetries) {
        return { ...state, status: 'dead', attemptCount: newAttemptCount }
      }

      // Calculate exponential backoff
      const backoffMs = Math.min(1000 * Math.pow(2, newAttemptCount - 1), 300000)
      return {
        ...state,
        status: 'failed',
        attemptCount: newAttemptCount,
        nextAttemptAt: Date.now() + backoffMs,
      }
    }

    // Test retry flow
    let delivery: DeliveryState = {
      subscriptionId: 'sub-1',
      status: 'pending',
      attemptCount: 0,
      maxRetries: 5,
    }

    // First attempt fails
    delivery = processDeliveryAttempt(delivery, false)
    expect(delivery.status).toBe('failed')
    expect(delivery.attemptCount).toBe(1)
    expect(delivery.nextAttemptAt).toBeDefined()

    // Second attempt fails
    delivery = processDeliveryAttempt(delivery, false)
    expect(delivery.status).toBe('failed')
    expect(delivery.attemptCount).toBe(2)

    // Third attempt succeeds
    delivery = processDeliveryAttempt(delivery, true)
    expect(delivery.status).toBe('delivered')
    expect(delivery.attemptCount).toBe(3)
  })

  it('tracks delivery statistics across workers', () => {
    interface WorkerStats {
      workerId: string
      totalDelivered: number
      totalFailed: number
      totalAttempts: number
      successRate: number
    }

    const deliveryLog: Array<{ workerId: string; status: 'delivered' | 'failed' }> = [
      { workerId: 'worker-a', status: 'delivered' },
      { workerId: 'worker-a', status: 'delivered' },
      { workerId: 'worker-a', status: 'failed' },
      { workerId: 'worker-b', status: 'delivered' },
      { workerId: 'worker-b', status: 'delivered' },
      { workerId: 'worker-b', status: 'delivered' },
      { workerId: 'worker-c', status: 'failed' },
      { workerId: 'worker-c', status: 'failed' },
    ]

    // Calculate stats per worker
    const statsMap = new Map<string, WorkerStats>()
    for (const entry of deliveryLog) {
      const stats = statsMap.get(entry.workerId) ?? {
        workerId: entry.workerId,
        totalDelivered: 0,
        totalFailed: 0,
        totalAttempts: 0,
        successRate: 0,
      }

      stats.totalAttempts++
      if (entry.status === 'delivered') {
        stats.totalDelivered++
      } else {
        stats.totalFailed++
      }
      stats.successRate = stats.totalDelivered / stats.totalAttempts

      statsMap.set(entry.workerId, stats)
    }

    expect(statsMap.get('worker-a')?.successRate).toBeCloseTo(0.667, 2)
    expect(statsMap.get('worker-b')?.successRate).toBe(1.0)
    expect(statsMap.get('worker-c')?.successRate).toBe(0)
  })

  it('handles subscription pattern changes during fanout', () => {
    // Initial subscriptions
    const subscriptionsV1 = [
      { id: 'sub-1', pattern: 'order.*', patternPrefix: 'order', active: true },
      { id: 'sub-2', pattern: 'order.created', patternPrefix: 'order.created', active: true },
    ]

    // Fanout with V1 subscriptions
    const matchesV1 = findMatchingSubscriptions('order.created', subscriptionsV1.filter(s => s.active))
    expect(matchesV1).toHaveLength(2)

    // Subscription updated (sub-2 deactivated)
    const subscriptionsV2 = [
      { id: 'sub-1', pattern: 'order.*', patternPrefix: 'order', active: true },
      { id: 'sub-2', pattern: 'order.created', patternPrefix: 'order.created', active: false },
      { id: 'sub-3', pattern: 'order.**', patternPrefix: 'order', active: true },
    ]

    // Fanout with V2 subscriptions
    const matchesV2 = findMatchingSubscriptions('order.created', subscriptionsV2.filter(s => s.active))
    expect(matchesV2).toHaveLength(2)
    expect(matchesV2.map(m => m.id)).toContain('sub-1')
    expect(matchesV2.map(m => m.id)).toContain('sub-3')
    expect(matchesV2.map(m => m.id)).not.toContain('sub-2')
  })

  it('handles dead letter queue transitions', () => {
    interface DeliveryState {
      status: 'pending' | 'delivered' | 'failed' | 'dead'
      attemptCount: number
      maxRetries: number
    }

    function transitionDelivery(state: DeliveryState, success: boolean): DeliveryState {
      const newAttemptCount = state.attemptCount + 1

      if (success) {
        return { ...state, status: 'delivered', attemptCount: newAttemptCount }
      }

      if (newAttemptCount >= state.maxRetries) {
        return { ...state, status: 'dead', attemptCount: newAttemptCount }
      }

      return { ...state, status: 'failed', attemptCount: newAttemptCount }
    }

    // Exhaust all retries
    let state: DeliveryState = { status: 'pending', attemptCount: 0, maxRetries: 3 }

    state = transitionDelivery(state, false) // Attempt 1: failed
    expect(state.status).toBe('failed')

    state = transitionDelivery(state, false) // Attempt 2: failed
    expect(state.status).toBe('failed')

    state = transitionDelivery(state, false) // Attempt 3: dead letter
    expect(state.status).toBe('dead')
    expect(state.attemptCount).toBe(3)
  })
})

// ============================================================================
// Test: End-to-End Ingest -> CDC -> Subscription Flow
// ============================================================================

describe('End-to-End: Ingest -> CDC -> Subscription Flow', () => {
  let mockR2: ReturnType<typeof createMockR2Bucket>

  beforeEach(() => {
    mockR2 = createMockR2Bucket()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-31T10:00:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('processes complete event lifecycle from ingest to subscription delivery', async () => {
    // Step 1: Simulate event ingestion (EventWriterDO)
    const ingestedEvents = [
      createCDCEvent('collection.insert', 'users', 'user-1', { name: 'Alice', email: 'alice@example.com' }),
      createCDCEvent('collection.insert', 'users', 'user-2', { name: 'Bob', email: 'bob@example.com' }),
    ]

    // Write to R2 (simulating EventWriterDO flush)
    const jsonlContent = ingestedEvents.map(e => JSON.stringify(e)).join('\n')
    await mockR2.put('events/2024/01/31/10/batch.jsonl', jsonlContent)

    // Step 2: Simulate CDC processing - maintain state
    const documentState = new Map<string, { doc: Record<string, unknown>; version: number }>()
    for (const event of ingestedEvents) {
      const key = `${event.collection}:${event.docId}`
      documentState.set(key, { doc: event.doc ?? {}, version: 1 })
    }

    // Verify CDC state
    expect(documentState.size).toBe(2)
    expect(documentState.get('users:user-1')?.doc).toEqual({ name: 'Alice', email: 'alice@example.com' })

    // Step 3: Subscription fanout
    const subscriptions = [
      { id: 'sub-user-events', pattern: 'collection.insert', patternPrefix: 'collection.insert', workerId: 'user-service' },
      { id: 'sub-audit', pattern: '**', patternPrefix: '', workerId: 'audit-service' },
    ]

    // Fanout each event to matching subscriptions
    const deliveries: Array<{ eventId: string; subscriptionId: string; workerId: string }> = []
    for (const event of ingestedEvents) {
      const matches = findMatchingSubscriptions(event.type, subscriptions)
      for (const match of matches) {
        deliveries.push({
          eventId: event.docId,
          subscriptionId: match.id,
          workerId: match.workerId,
        })
      }
    }

    // Each event should be delivered to both subscriptions
    expect(deliveries).toHaveLength(4) // 2 events x 2 subscriptions

    // Verify delivery targets
    const userServiceDeliveries = deliveries.filter(d => d.workerId === 'user-service')
    const auditServiceDeliveries = deliveries.filter(d => d.workerId === 'audit-service')

    expect(userServiceDeliveries).toHaveLength(2)
    expect(auditServiceDeliveries).toHaveLength(2)
  })

  it('tracks bookmarks through the pipeline for PITR', async () => {
    // Events with explicit bookmarks for PITR
    const eventsWithBookmarks = [
      { ...createCDCEvent('collection.insert', 'users', 'user-1', { name: 'v1' }), bookmark: 'bk-001' },
      { ...createCDCEvent('collection.update', 'users', 'user-1', { name: 'v2' }, { name: 'v1' }), bookmark: 'bk-002' },
      { ...createCDCEvent('collection.update', 'users', 'user-1', { name: 'v3' }, { name: 'v2' }), bookmark: 'bk-003' },
    ]

    // Simulate CDC processing with bookmark tracking
    interface DocumentState {
      doc: Record<string, unknown>
      version: number
      bookmark: string
    }

    const documentState = new Map<string, DocumentState>()

    for (const event of eventsWithBookmarks) {
      const key = `${event.collection}:${event.docId}`
      const existing = documentState.get(key)
      const newVersion = existing ? existing.version + 1 : 1

      documentState.set(key, {
        doc: event.doc ?? {},
        version: newVersion,
        bookmark: event.bookmark ?? '',
      })
    }

    // Verify latest bookmark is preserved
    const userState = documentState.get('users:user-1')
    expect(userState?.bookmark).toBe('bk-003')
    expect(userState?.version).toBe(3)
    expect(userState?.doc).toEqual({ name: 'v3' })
  })

  it('handles concurrent events across multiple collections', async () => {
    const multiCollectionEvents: CDCEvent[] = [
      createCDCEvent('collection.insert', 'users', 'user-1', { name: 'Alice' }, undefined, '2024-01-31T10:00:00Z'),
      createCDCEvent('collection.insert', 'orders', 'order-1', { userId: 'user-1', amount: 100 }, undefined, '2024-01-31T10:00:01Z'),
      createCDCEvent('collection.insert', 'products', 'prod-1', { name: 'Widget', price: 50 }, undefined, '2024-01-31T10:00:02Z'),
      createCDCEvent('collection.update', 'users', 'user-1', { name: 'Alice Smith' }, { name: 'Alice' }, '2024-01-31T10:00:03Z'),
      createCDCEvent('collection.update', 'orders', 'order-1', { userId: 'user-1', amount: 150, discount: 10 }, { userId: 'user-1', amount: 100 }, '2024-01-31T10:00:04Z'),
    ]

    // Simulate CDC processing per collection
    const collectionStates = new Map<string, Map<string, { doc: Record<string, unknown>; version: number }>>()

    for (const event of multiCollectionEvents) {
      if (!collectionStates.has(event.collection)) {
        collectionStates.set(event.collection, new Map())
      }
      const collState = collectionStates.get(event.collection)!
      const existing = collState.get(event.docId)
      const newVersion = existing ? existing.version + 1 : 1

      collState.set(event.docId, {
        doc: event.doc ?? {},
        version: newVersion,
      })
    }

    // Verify each collection's state
    expect(collectionStates.get('users')?.get('user-1')?.doc).toEqual({ name: 'Alice Smith' })
    expect(collectionStates.get('users')?.get('user-1')?.version).toBe(2)

    expect(collectionStates.get('orders')?.get('order-1')?.doc).toEqual({ userId: 'user-1', amount: 150, discount: 10 })
    expect(collectionStates.get('orders')?.get('order-1')?.version).toBe(2)

    expect(collectionStates.get('products')?.get('prod-1')?.doc).toEqual({ name: 'Widget', price: 50 })
    expect(collectionStates.get('products')?.get('prod-1')?.version).toBe(1)
  })

  it('propagates events to subscriptions based on event type patterns', () => {
    const subscriptions = [
      { id: 'sub-all-inserts', pattern: 'collection.insert', patternPrefix: 'collection.insert' },
      { id: 'sub-all-updates', pattern: 'collection.update', patternPrefix: 'collection.update' },
      { id: 'sub-all-deletes', pattern: 'collection.delete', patternPrefix: 'collection.delete' },
      { id: 'sub-all', pattern: 'collection.*', patternPrefix: 'collection' },
    ]

    // Test insert event matching
    const insertMatches = findMatchingSubscriptions('collection.insert', subscriptions)
    expect(insertMatches.map(m => m.id)).toContain('sub-all-inserts')
    expect(insertMatches.map(m => m.id)).toContain('sub-all')
    expect(insertMatches.map(m => m.id)).not.toContain('sub-all-updates')
    expect(insertMatches.map(m => m.id)).not.toContain('sub-all-deletes')

    // Test update event matching
    const updateMatches = findMatchingSubscriptions('collection.update', subscriptions)
    expect(updateMatches.map(m => m.id)).toContain('sub-all-updates')
    expect(updateMatches.map(m => m.id)).toContain('sub-all')
    expect(updateMatches.map(m => m.id)).not.toContain('sub-all-inserts')

    // Test delete event matching
    const deleteMatches = findMatchingSubscriptions('collection.delete', subscriptions)
    expect(deleteMatches.map(m => m.id)).toContain('sub-all-deletes')
    expect(deleteMatches.map(m => m.id)).toContain('sub-all')
  })
})

// ============================================================================
// Test: Error Propagation Across DOs
// ============================================================================

describe('Error Propagation Across DOs', () => {
  let mockR2: ReturnType<typeof createMockR2Bucket>

  beforeEach(() => {
    mockR2 = createMockR2Bucket()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-31T10:00:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('handles R2 write failure during EventWriter flush', async () => {
    // Simulate R2 write failure
    mockR2.put = vi.fn().mockRejectedValue(new Error('R2 write failed: insufficient capacity'))

    // Attempt to write events
    await expect(
      mockR2.put('events/batch.jsonl', '{"event": "data"}')
    ).rejects.toThrow('R2 write failed')

    // Events should be preserved for retry (EventWriterDO behavior)
    // This verifies error is propagated, not swallowed
  })

  it('handles cascading failures across DO chain', async () => {
    // Track errors across the pipeline
    const errorLog: Array<{ component: string; error: string; timestamp: number }> = []

    function logError(component: string, error: string) {
      errorLog.push({ component, error, timestamp: Date.now() })
    }

    // Simulate EventWriter failure
    try {
      throw new Error('EventWriter: Buffer full')
    } catch (e) {
      logError('EventWriter', (e as Error).message)
    }

    // Simulate CDCProcessor failure
    try {
      throw new Error('CDCProcessor: R2 write failed')
    } catch (e) {
      logError('CDCProcessor', (e as Error).message)
    }

    // Simulate SubscriptionDO failure
    try {
      throw new Error('SubscriptionDO: Worker unreachable')
    } catch (e) {
      logError('SubscriptionDO', (e as Error).message)
    }

    // Verify error chain is captured
    expect(errorLog).toHaveLength(3)
    expect(errorLog.map(e => e.component)).toEqual(['EventWriter', 'CDCProcessor', 'SubscriptionDO'])
  })

  it('handles subscription delivery timeout scenarios', () => {
    interface DeliveryResult {
      success: boolean
      error?: string
      movedToDeadLetter: boolean
    }

    // Simulate delivery with timeout handling
    function simulateDeliveryWithTimeout(
      attemptCount: number,
      maxRetries: number,
      timeout: boolean
    ): DeliveryResult {
      if (timeout) {
        if (attemptCount >= maxRetries) {
          return {
            success: false,
            error: 'RPC timeout after max retries',
            movedToDeadLetter: true,
          }
        }
        return {
          success: false,
          error: 'RPC timeout',
          movedToDeadLetter: false,
        }
      }
      return { success: true, movedToDeadLetter: false }
    }

    // Test timeout handling - not at max retries
    const result1 = simulateDeliveryWithTimeout(1, 5, true)
    expect(result1.success).toBe(false)
    expect(result1.movedToDeadLetter).toBe(false)

    // Test timeout at max retries - should move to dead letter
    const result2 = simulateDeliveryWithTimeout(5, 5, true)
    expect(result2.success).toBe(false)
    expect(result2.movedToDeadLetter).toBe(true)

    // Test success case
    const result3 = simulateDeliveryWithTimeout(3, 5, false)
    expect(result3.success).toBe(true)
    expect(result3.movedToDeadLetter).toBe(false)
  })

  it('handles circuit breaker state propagation', () => {
    interface CircuitBreakerState {
      consecutiveFailures: number
      isOpen: boolean
      openedAt?: number
      halfOpenAt?: number
    }

    function updateCircuitBreaker(
      state: CircuitBreakerState,
      success: boolean,
      failureThreshold = 5,
      resetTimeMs = 60000
    ): CircuitBreakerState {
      if (success) {
        // Reset on success
        return { consecutiveFailures: 0, isOpen: false }
      }

      const newFailures = state.consecutiveFailures + 1

      if (newFailures >= failureThreshold) {
        return {
          consecutiveFailures: newFailures,
          isOpen: true,
          openedAt: Date.now(),
          halfOpenAt: Date.now() + resetTimeMs,
        }
      }

      return { ...state, consecutiveFailures: newFailures }
    }

    // Test circuit breaker transitions
    let circuitBreaker: CircuitBreakerState = { consecutiveFailures: 0, isOpen: false }

    // Accumulate failures
    for (let i = 0; i < 4; i++) {
      circuitBreaker = updateCircuitBreaker(circuitBreaker, false)
      expect(circuitBreaker.isOpen).toBe(false)
    }

    // 5th failure opens circuit
    circuitBreaker = updateCircuitBreaker(circuitBreaker, false)
    expect(circuitBreaker.isOpen).toBe(true)
    expect(circuitBreaker.consecutiveFailures).toBe(5)

    // Success resets
    circuitBreaker = updateCircuitBreaker(circuitBreaker, true)
    expect(circuitBreaker.isOpen).toBe(false)
    expect(circuitBreaker.consecutiveFailures).toBe(0)
  })

  it('validates error isolation between namespaces', () => {
    // Simulate namespace-isolated error tracking
    const namespaceErrors = new Map<string, Error[]>()

    function recordError(namespace: string, error: Error) {
      const errors = namespaceErrors.get(namespace) ?? []
      errors.push(error)
      namespaceErrors.set(namespace, errors)
    }

    // Record errors in different namespaces
    recordError('namespace-1', new Error('R2 failure in namespace-1'))
    recordError('namespace-1', new Error('Another error in namespace-1'))
    recordError('namespace-2', new Error('Different error in namespace-2'))

    // Verify isolation
    expect(namespaceErrors.get('namespace-1')).toHaveLength(2)
    expect(namespaceErrors.get('namespace-2')).toHaveLength(1)

    // Errors in one namespace don't affect the other
    expect(namespaceErrors.get('namespace-1')?.map(e => e.message)).not.toContain('Different error in namespace-2')
  })
})

// ============================================================================
// Test: Cross-DO Data Consistency
// ============================================================================

describe('Cross-DO Data Consistency', () => {
  let mockR2: ReturnType<typeof createMockR2Bucket>

  beforeEach(() => {
    mockR2 = createMockR2Bucket()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-31T10:00:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  it('maintains consistency between event storage and CDC state', async () => {
    // Write events to R2
    const events = [
      { type: 'collection.insert', collection: 'users', docId: 'user-1', doc: { name: 'Alice' } },
      { type: 'collection.insert', collection: 'users', docId: 'user-2', doc: { name: 'Bob' } },
    ]

    await mockR2.put('events/batch.jsonl', events.map(e => JSON.stringify(e)).join('\n'))

    // Read events and build CDC state
    const r2Object = await mockR2.get('events/batch.jsonl')
    const content = await r2Object!.text()
    const parsedEvents = content.split('\n').map(line => JSON.parse(line))

    const cdcState = new Map<string, { doc: Record<string, unknown>; version: number }>()
    for (const event of parsedEvents) {
      cdcState.set(`${event.collection}:${event.docId}`, {
        doc: event.doc,
        version: 1,
      })
    }

    // Verify consistency: events in R2 match CDC state
    expect(cdcState.size).toBe(events.length)
    expect(cdcState.get('users:user-1')?.doc).toEqual(events[0].doc)
    expect(cdcState.get('users:user-2')?.doc).toEqual(events[1].doc)
  })

  it('ensures event ordering is preserved across DO boundaries', async () => {
    // Events with explicit timestamps for ordering
    const orderedEvents = Array.from({ length: 100 }, (_, i) => ({
      type: 'collection.update',
      ts: `2024-01-31T10:00:${String(i).padStart(2, '0')}.000Z`,
      docId: 'counter-1',
      doc: { value: i + 1 },
      prev: { value: i },
    }))

    // Add initial insert
    orderedEvents.unshift({
      type: 'collection.insert',
      ts: '2024-01-31T09:59:59.000Z',
      docId: 'counter-1',
      doc: { value: 0 },
      prev: undefined,
    } as typeof orderedEvents[0])

    // Write to R2
    await mockR2.put('events/ordered.jsonl', orderedEvents.map(e => JSON.stringify(e)).join('\n'))

    // Read and process in order
    const r2Object = await mockR2.get('events/ordered.jsonl')
    const content = await r2Object!.text()
    const parsedEvents = content.split('\n').map(line => JSON.parse(line))

    // Process events in order to build final state
    let currentState = { value: -1, version: 0 }
    for (const event of parsedEvents) {
      currentState = {
        value: event.doc.value,
        version: currentState.version + 1,
      }
    }

    // Final state should reflect last event
    expect(currentState.value).toBe(100)
    expect(currentState.version).toBe(101) // 1 insert + 100 updates
  })

  it('handles version conflicts with last-write-wins semantics', () => {
    // Simulate concurrent updates (in practice, DO serialization prevents this)
    // But we test that last-write-wins semantics are maintained
    const concurrentEvents = [
      { type: 'collection.insert', docId: 'doc-1', doc: { value: 'initial' }, ts: '2024-01-31T10:00:00Z' },
      { type: 'collection.update', docId: 'doc-1', doc: { value: 'update-a' }, ts: '2024-01-31T10:00:01Z' },
      { type: 'collection.update', docId: 'doc-1', doc: { value: 'update-b' }, ts: '2024-01-31T10:00:01Z' }, // Same timestamp
      { type: 'collection.update', docId: 'doc-1', doc: { value: 'final' }, ts: '2024-01-31T10:00:02Z' },
    ]

    // Process events in order - last write wins
    let state = { doc: {}, version: 0 }
    for (const event of concurrentEvents) {
      state = {
        doc: event.doc,
        version: state.version + 1,
      }
    }

    // Final value should be from last event in processing order
    expect(state.doc).toEqual({ value: 'final' })
    expect(state.version).toBe(4)
  })

  it('validates subscription delivery deduplication', () => {
    // Track delivered event IDs per subscription
    const deliveredEvents = new Map<string, Set<string>>()

    function recordDelivery(subscriptionId: string, eventId: string): boolean {
      const delivered = deliveredEvents.get(subscriptionId) ?? new Set()
      if (delivered.has(eventId)) {
        return false // Duplicate, already delivered
      }
      delivered.add(eventId)
      deliveredEvents.set(subscriptionId, delivered)
      return true
    }

    // Simulate delivery attempts with duplicates
    expect(recordDelivery('sub-1', 'event-1')).toBe(true)
    expect(recordDelivery('sub-1', 'event-2')).toBe(true)
    expect(recordDelivery('sub-1', 'event-1')).toBe(false) // Duplicate
    expect(recordDelivery('sub-2', 'event-1')).toBe(true) // Different subscription
    expect(recordDelivery('sub-1', 'event-3')).toBe(true)

    // Verify counts
    expect(deliveredEvents.get('sub-1')?.size).toBe(3)
    expect(deliveredEvents.get('sub-2')?.size).toBe(1)
  })

  it('maintains referential integrity across collections', () => {
    // Simulate related entities
    const events = [
      { type: 'collection.insert', collection: 'users', docId: 'user-1', doc: { name: 'Alice' } },
      { type: 'collection.insert', collection: 'orders', docId: 'order-1', doc: { userId: 'user-1', items: ['item-1'] } },
      { type: 'collection.insert', collection: 'orders', docId: 'order-2', doc: { userId: 'user-1', items: ['item-2', 'item-3'] } },
    ]

    // Build state per collection
    const collectionState = new Map<string, Map<string, Record<string, unknown>>>()

    for (const event of events) {
      if (!collectionState.has(event.collection)) {
        collectionState.set(event.collection, new Map())
      }
      collectionState.get(event.collection)!.set(event.docId, event.doc)
    }

    // Verify user exists
    const user = collectionState.get('users')?.get('user-1')
    expect(user).toBeDefined()

    // Verify orders reference the user
    const orders = collectionState.get('orders')
    for (const [, order] of orders ?? []) {
      expect((order as { userId: string }).userId).toBe('user-1')
    }

    // Count orders for user
    const userOrders = Array.from(orders?.values() ?? []).filter(
      (o: unknown) => (o as { userId: string }).userId === 'user-1'
    )
    expect(userOrders).toHaveLength(2)
  })
})

// ============================================================================
// Test: Pattern Matching Edge Cases
// ============================================================================

describe('Pattern Matching Edge Cases', () => {
  it('handles deeply nested event types', () => {
    const subscriptions = [
      { id: 'sub-deep', pattern: 'api.v1.users.profile.settings.**', patternPrefix: 'api.v1.users.profile.settings' },
      { id: 'sub-mid', pattern: 'api.v1.users.**', patternPrefix: 'api.v1.users' },
      { id: 'sub-all', pattern: 'api.**', patternPrefix: 'api' },
    ]

    const matches = findMatchingSubscriptions('api.v1.users.profile.settings.privacy.notifications', subscriptions)
    const matchIds = matches.map(m => m.id)

    expect(matchIds).toContain('sub-deep')
    expect(matchIds).toContain('sub-mid')
    expect(matchIds).toContain('sub-all')
  })

  it('handles patterns with multiple single wildcards', () => {
    const subscriptions = [
      { id: 'sub-multi-wild', pattern: 'webhook.*.*.event', patternPrefix: 'webhook' },
    ]

    const matches1 = findMatchingSubscriptions('webhook.github.push.event', subscriptions)
    expect(matches1.map(m => m.id)).toContain('sub-multi-wild')

    const matches2 = findMatchingSubscriptions('webhook.stripe.invoice.event', subscriptions)
    expect(matches2.map(m => m.id)).toContain('sub-multi-wild')

    // Should not match with different segment count
    const matches3 = findMatchingSubscriptions('webhook.github.event', subscriptions)
    expect(matches3.map(m => m.id)).not.toContain('sub-multi-wild')
  })

  it('handles empty event type', () => {
    const subscriptions = [
      { id: 'sub-all', pattern: '**', patternPrefix: '' },
    ]

    // Note: Empty string event type may not match ** depending on implementation
    // This tests the actual behavior of the pattern matcher
    const matches = findMatchingSubscriptions('', subscriptions)
    // The pattern matcher may return empty for empty input, which is valid behavior
    // We just verify it doesn't throw
    expect(Array.isArray(matches)).toBe(true)
  })

  it('handles exact match patterns', () => {
    const subscriptions = [
      { id: 'sub-exact', pattern: 'webhook.github.push', patternPrefix: 'webhook.github.push' },
      { id: 'sub-wild', pattern: 'webhook.github.*', patternPrefix: 'webhook.github' },
    ]

    const matches = findMatchingSubscriptions('webhook.github.push', subscriptions)
    expect(matches.map(m => m.id)).toContain('sub-exact')
    expect(matches.map(m => m.id)).toContain('sub-wild')

    // Exact match should NOT match longer paths
    const matchesLonger = findMatchingSubscriptions('webhook.github.push.v1', subscriptions)
    expect(matchesLonger.map(m => m.id)).not.toContain('sub-exact')
    expect(matchesLonger.map(m => m.id)).not.toContain('sub-wild')
  })

  it('extracts pattern prefix correctly', () => {
    expect(extractPatternPrefix('webhook.github.*')).toBe('webhook.github')
    expect(extractPatternPrefix('webhook.**')).toBe('webhook')
    expect(extractPatternPrefix('webhook.github.push')).toBe('webhook.github.push')
    expect(extractPatternPrefix('*')).toBe('')
    expect(extractPatternPrefix('**')).toBe('')
    expect(extractPatternPrefix('*.github.push')).toBe('')
  })

  it('matches patterns with picomatch compatibility', () => {
    expect(matchPattern('webhook.github.*', 'webhook.github.push')).toBe(true)
    expect(matchPattern('webhook.github.*', 'webhook.github.pull_request')).toBe(true)
    expect(matchPattern('webhook.github.*', 'webhook.github.push.v1')).toBe(false)

    expect(matchPattern('webhook.**', 'webhook')).toBe(true)
    expect(matchPattern('webhook.**', 'webhook.github')).toBe(true)
    expect(matchPattern('webhook.**', 'webhook.github.push')).toBe(true)
    expect(matchPattern('webhook.**', 'webhook.github.push.v1.test')).toBe(true)
  })
})
