/**
 * Shared Event Test Fixtures
 *
 * Provides sample events and factory functions for testing:
 * - RpcCallEvent
 * - CollectionChangeEvent
 * - WebSocketEvent
 * - LifecycleEvent
 * - CustomEvent
 */

import type {
  RpcCallEvent,
  CollectionChangeEvent,
  WebSocketEvent,
  LifecycleEvent,
  CustomEvent,
  DurableEvent,
  BaseEvent,
} from '../../types.js'

// ============================================================================
// Default DO Identity
// ============================================================================

/** Default DO identity used in sample events */
export const defaultDoIdentity = {
  id: 'test-do-123',
  name: 'test-object',
  class: 'TestDurableObject',
  colo: 'SFO',
  worker: 'test-worker',
} as const

/** Default timestamp for sample events (ISO format) */
export const defaultTimestamp = '2024-01-15T12:00:00.000Z'

// ============================================================================
// Sample RPC Events
// ============================================================================

/** Sample successful RPC call event */
export const sampleRpcCallEvent: RpcCallEvent = {
  type: 'rpc.call',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  method: 'getUser',
  namespace: 'UserService',
  durationMs: 15,
  success: true,
}

/** Sample failed RPC call event */
export const sampleRpcCallEventFailed: RpcCallEvent = {
  type: 'rpc.call',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  method: 'createUser',
  namespace: 'UserService',
  durationMs: 250,
  success: false,
  error: 'User already exists',
}

/** Sample RPC call event without namespace */
export const sampleRpcCallEventNoNamespace: RpcCallEvent = {
  type: 'rpc.call',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  method: 'ping',
  durationMs: 1,
  success: true,
}

// ============================================================================
// Sample Collection Change Events (CDC)
// ============================================================================

/** Sample collection insert event */
export const sampleCollectionInsertEvent: CollectionChangeEvent = {
  type: 'collection.created',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  collection: 'users',
  docId: 'user-123',
  doc: { name: 'Alice', email: 'alice@example.com', active: true },
  bookmark: 'bookmark-abc123',
}

/** Sample collection update event */
export const sampleCollectionUpdateEvent: CollectionChangeEvent = {
  type: 'collection.updated',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  collection: 'users',
  docId: 'user-123',
  doc: { name: 'Alice Smith', email: 'alice@example.com', active: true },
  prev: { name: 'Alice', email: 'alice@example.com', active: true },
  bookmark: 'bookmark-def456',
}

/** Sample collection delete event */
export const sampleCollectionDeleteEvent: CollectionChangeEvent = {
  type: 'collection.deleted',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  collection: 'users',
  docId: 'user-123',
  prev: { name: 'Alice Smith', email: 'alice@example.com', active: true },
  bookmark: 'bookmark-ghi789',
}

/** Sample collection update event without previous value */
export const sampleCollectionUpdateEventNoPrev: CollectionChangeEvent = {
  type: 'collection.updated',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  collection: 'orders',
  docId: 'order-456',
  doc: { status: 'shipped', total: 99.99 },
  bookmark: 'bookmark-xyz',
}

// ============================================================================
// Sample WebSocket Events
// ============================================================================

/** Sample WebSocket connect event */
export const sampleWsConnectEvent: WebSocketEvent = {
  type: 'ws.connect',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  connectionCount: 1,
}

/** Sample WebSocket message event */
export const sampleWsMessageEvent: WebSocketEvent = {
  type: 'ws.message',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  connectionCount: 5,
}

/** Sample WebSocket close event */
export const sampleWsCloseEvent: WebSocketEvent = {
  type: 'ws.close',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  connectionCount: 4,
  code: 1000,
  reason: 'Normal closure',
}

/** Sample WebSocket error event */
export const sampleWsErrorEvent: WebSocketEvent = {
  type: 'ws.error',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  connectionCount: 3,
  code: 1006,
  reason: 'Connection lost unexpectedly',
}

// ============================================================================
// Sample Lifecycle Events
// ============================================================================

/** Sample DO create event */
export const sampleDoCreateEvent: LifecycleEvent = {
  type: 'do.create',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
}

/** Sample DO alarm event */
export const sampleDoAlarmEvent: LifecycleEvent = {
  type: 'do.alarm',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  reason: 'scheduled-task',
}

/** Sample DO hibernate event */
export const sampleDoHibernateEvent: LifecycleEvent = {
  type: 'do.hibernate',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  reason: 'idle-timeout',
}

/** Sample DO evict event */
export const sampleDoEvictEvent: LifecycleEvent = {
  type: 'do.evict',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  reason: 'memory-pressure',
}

// ============================================================================
// Sample Custom Events
// ============================================================================

/** Sample custom event */
export const sampleCustomEvent: CustomEvent = {
  type: 'custom.user.signup',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
  data: {
    userId: 'user-123',
    plan: 'premium',
    source: 'referral',
  },
}

/** Sample custom event with minimal data */
export const sampleCustomEventMinimal: CustomEvent = {
  type: 'custom.heartbeat',
  ts: defaultTimestamp,
  do: { ...defaultDoIdentity },
}

// ============================================================================
// Event Factory Functions
// ============================================================================

/**
 * Create an RPC call event with custom overrides
 */
export function createRpcCallEvent(overrides: Partial<RpcCallEvent> = {}): RpcCallEvent {
  return {
    type: 'rpc.call',
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    method: 'testMethod',
    durationMs: 10,
    success: true,
    ...overrides,
  }
}

/**
 * Create a collection insert event with custom overrides
 */
export function createCollectionInsertEvent(
  overrides: Partial<CollectionChangeEvent> = {}
): CollectionChangeEvent {
  return {
    type: 'collection.created',
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    collection: 'test_collection',
    docId: `doc-${Date.now()}`,
    doc: { name: 'Test Document' },
    bookmark: `bookmark-${Date.now()}`,
    ...overrides,
  }
}

/**
 * Create a collection update event with custom overrides
 */
export function createCollectionUpdateEvent(
  overrides: Partial<CollectionChangeEvent> = {}
): CollectionChangeEvent {
  return {
    type: 'collection.updated',
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    collection: 'test_collection',
    docId: `doc-${Date.now()}`,
    doc: { name: 'Updated Document' },
    prev: { name: 'Original Document' },
    bookmark: `bookmark-${Date.now()}`,
    ...overrides,
  }
}

/**
 * Create a collection delete event with custom overrides
 */
export function createCollectionDeleteEvent(
  overrides: Partial<CollectionChangeEvent> = {}
): CollectionChangeEvent {
  return {
    type: 'collection.deleted',
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    collection: 'test_collection',
    docId: `doc-${Date.now()}`,
    prev: { name: 'Deleted Document' },
    bookmark: `bookmark-${Date.now()}`,
    ...overrides,
  }
}

/**
 * Create a WebSocket event with custom overrides
 */
export function createWebSocketEvent(
  eventType: 'ws.connect' | 'ws.message' | 'ws.close' | 'ws.error',
  overrides: Partial<WebSocketEvent> = {}
): WebSocketEvent {
  return {
    type: eventType,
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    connectionCount: 1,
    ...overrides,
  }
}

/**
 * Create a lifecycle event with custom overrides
 */
export function createLifecycleEvent(
  eventType: 'do.create' | 'do.alarm' | 'do.hibernate' | 'do.evict',
  overrides: Partial<LifecycleEvent> = {}
): LifecycleEvent {
  return {
    type: eventType,
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    ...overrides,
  }
}

/**
 * Create a custom event with custom overrides
 */
export function createCustomEvent(
  customType: string,
  overrides: Partial<Omit<CustomEvent, 'type'>> = {}
): CustomEvent {
  const type = customType.startsWith('custom.') ? customType : `custom.${customType}`
  return {
    type: type as `custom.${string}`,
    ts: new Date().toISOString(),
    do: { ...defaultDoIdentity },
    ...overrides,
  }
}

// ============================================================================
// Event Batch Factory Functions
// ============================================================================

/**
 * Create a batch of mixed events for testing
 */
export function createEventBatch(count: number = 10): DurableEvent[] {
  const events: DurableEvent[] = []

  for (let i = 0; i < count; i++) {
    const type = i % 4
    switch (type) {
      case 0:
        events.push(createRpcCallEvent({ method: `method${i}`, durationMs: i * 10 }))
        break
      case 1:
        events.push(createCollectionInsertEvent({ docId: `doc-${i}` }))
        break
      case 2:
        events.push(createWebSocketEvent('ws.message', { connectionCount: i }))
        break
      case 3:
        events.push(createLifecycleEvent('do.alarm', { reason: `alarm-${i}` }))
        break
    }
  }

  return events
}

/**
 * Create a batch of CDC events for a single collection
 */
export function createCdcEventBatch(
  collection: string,
  operations: Array<{ op: 'created' | 'updated' | 'deleted'; docId: string; doc?: Record<string, unknown>; prev?: Record<string, unknown> }>
): CollectionChangeEvent[] {
  return operations.map((op, i) => {
    const base = {
      ts: new Date(Date.now() + i).toISOString(),
      do: { ...defaultDoIdentity },
      collection,
      docId: op.docId,
      bookmark: `bookmark-${i}`,
    }

    switch (op.op) {
      case 'created':
        return {
          ...base,
          type: 'collection.created' as const,
          doc: op.doc ?? { id: op.docId },
        }
      case 'updated':
        return {
          ...base,
          type: 'collection.updated' as const,
          doc: op.doc ?? { id: op.docId, updated: true },
          prev: op.prev ?? { id: op.docId },
        }
      case 'deleted':
        return {
          ...base,
          type: 'collection.deleted' as const,
          prev: op.prev ?? { id: op.docId },
        }
    }
  })
}

/**
 * Create a series of RPC events simulating a method call pattern
 */
export function createRpcEventSeries(
  methods: Array<{ method: string; success?: boolean; durationMs?: number; namespace?: string }>
): RpcCallEvent[] {
  return methods.map((m, i) =>
    createRpcCallEvent({
      method: m.method,
      namespace: m.namespace,
      success: m.success ?? true,
      durationMs: m.durationMs ?? 10 + i,
      ts: new Date(Date.now() + i).toISOString(),
    })
  )
}
