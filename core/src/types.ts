/**
 * Event Types for @dotdo/events
 */

/** Base event structure */
export interface BaseEvent {
  /** Event type (e.g., "rpc.call", "collection.write", "do.alarm") */
  type: string
  /** ISO timestamp */
  ts: string
  /** DO identity */
  do: {
    id: string
    name?: string | undefined
    class?: string | undefined
    colo?: string | undefined
    worker?: string | undefined
  }
}

/** RPC method call event */
export interface RpcCallEvent extends BaseEvent {
  type: 'rpc.call'
  method: string
  namespace?: string | undefined
  durationMs: number
  success: boolean
  error?: string | undefined
}

/** Collection change event (CDC) */
export interface CollectionChangeEvent extends BaseEvent {
  type: 'collection.insert' | 'collection.update' | 'collection.delete'
  collection: string
  docId: string
  /** For inserts/updates: the new document */
  doc?: Record<string, unknown> | undefined
  /** For updates: the previous document (if tracking enabled) */
  prev?: Record<string, unknown> | undefined
  /** SQLite bookmark for PITR (point-in-time recovery) */
  bookmark?: string | undefined
}

/** DO lifecycle event */
export interface LifecycleEvent extends BaseEvent {
  type: 'do.create' | 'do.alarm' | 'do.hibernate' | 'do.evict'
  reason?: string | undefined
}

/** WebSocket event */
export interface WebSocketEvent extends BaseEvent {
  type: 'ws.connect' | 'ws.message' | 'ws.close' | 'ws.error'
  connectionCount?: number | undefined
  code?: number | undefined
  reason?: string | undefined
}

/** Client-side analytics event (browser) */
export interface ClientEvent extends BaseEvent {
  type: 'page' | 'track' | 'identify'
  event?: string | undefined
  properties?: Record<string, unknown> | undefined
  traits?: Record<string, unknown> | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
}

/** Custom event with user-defined type */
export interface CustomEvent extends BaseEvent {
  type: `custom.${string}`
  /** Custom event data */
  data?: Record<string, unknown> | undefined
}

/** Union of all event types (discriminated by 'type' field) */
export type DurableEvent =
  | RpcCallEvent
  | CollectionChangeEvent
  | LifecycleEvent
  | WebSocketEvent
  | ClientEvent
  | CustomEvent

/** Fields that are auto-filled by the EventEmitter */
type AutoFilledFields = 'ts' | 'do'

/** Input type for emit() - each event type without the auto-filled fields */
export type EmitInput =
  | Omit<RpcCallEvent, AutoFilledFields>
  | Omit<CollectionChangeEvent, AutoFilledFields>
  | Omit<LifecycleEvent, AutoFilledFields>
  | Omit<WebSocketEvent, AutoFilledFields>
  | Omit<ClientEvent, AutoFilledFields>
  | Omit<CustomEvent, AutoFilledFields>

/**
 * Type guard for RpcCallEvent
 */
export function isRpcCallEvent(event: DurableEvent): event is RpcCallEvent {
  return event.type === 'rpc.call'
}

/**
 * Type guard for CollectionChangeEvent
 */
export function isCollectionChangeEvent(event: DurableEvent): event is CollectionChangeEvent {
  return event.type === 'collection.insert' || event.type === 'collection.update' || event.type === 'collection.delete'
}

/**
 * Type guard for LifecycleEvent
 */
export function isLifecycleEvent(event: DurableEvent): event is LifecycleEvent {
  return event.type === 'do.create' || event.type === 'do.alarm' || event.type === 'do.hibernate' || event.type === 'do.evict'
}

/**
 * Type guard for WebSocketEvent
 */
export function isWebSocketEvent(event: DurableEvent): event is WebSocketEvent {
  return event.type === 'ws.connect' || event.type === 'ws.message' || event.type === 'ws.close' || event.type === 'ws.error'
}

/**
 * Type guard for ClientEvent
 */
export function isClientEvent(event: DurableEvent): event is ClientEvent {
  return event.type === 'page' || event.type === 'track' || event.type === 'identify'
}

/**
 * Type guard for CustomEvent
 */
export function isCustomEvent(event: DurableEvent): event is CustomEvent {
  return event.type.startsWith('custom.')
}

/** Batch of events for ingestion */
export interface EventBatch {
  events: DurableEvent[]
}

/** Event emitter configuration */
export interface EventEmitterOptions {
  /** Endpoint to send events (default: events.do) */
  endpoint?: string | undefined
  /** Batch size before auto-flush (default: 100) */
  batchSize?: number | undefined
  /** Max time to hold events before flush in ms (default: 1000) */
  flushIntervalMs?: number | undefined
  /** Enable CDC for collections (default: false) */
  cdc?: boolean | undefined
  /** R2 bucket for lakehouse streaming (optional) */
  r2Bucket?: R2Bucket | undefined
  /** Include previous doc in CDC updates (more storage, enables diffs) */
  trackPrevious?: boolean | undefined
  /** API key for authentication (optional) */
  apiKey?: string | undefined
  /** Max retry queue size (default: 10000). When exceeded, oldest events are dropped. */
  maxRetryQueueSize?: number | undefined
  /** Max consecutive failures before circuit breaker opens (default: 10). Set to 0 to disable. */
  maxConsecutiveFailures?: number | undefined
  /** Circuit breaker reset timeout in ms (default: 300000 = 5 minutes) */
  circuitBreakerResetMs?: number | undefined
}

/** Error thrown when the retry buffer is full and backpressure is needed */
export class EventBufferFullError extends Error {
  constructor(
    message: string,
    public readonly droppedCount: number
  ) {
    super(message)
    this.name = 'EventBufferFullError'
  }
}

/** Error thrown when the circuit breaker is open */
export class CircuitBreakerOpenError extends Error {
  constructor(
    message: string,
    public readonly consecutiveFailures: number,
    public readonly resetAt: Date
  ) {
    super(message)
    this.name = 'CircuitBreakerOpenError'
  }
}
