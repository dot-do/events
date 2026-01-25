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
    name?: string
    class?: string
    colo?: string
    worker?: string
  }
}

/** RPC method call event */
export interface RpcCallEvent extends BaseEvent {
  type: 'rpc.call'
  method: string
  namespace?: string
  durationMs: number
  success: boolean
  error?: string
}

/** Collection change event (CDC) */
export interface CollectionChangeEvent extends BaseEvent {
  type: 'collection.insert' | 'collection.update' | 'collection.delete'
  collection: string
  docId: string
  /** For inserts/updates: the new document */
  doc?: Record<string, unknown>
  /** For updates: the previous document (if tracking enabled) */
  prev?: Record<string, unknown>
  /** SQLite bookmark for PITR (point-in-time recovery) */
  bookmark?: string
}

/** DO lifecycle event */
export interface LifecycleEvent extends BaseEvent {
  type: 'do.create' | 'do.alarm' | 'do.hibernate' | 'do.evict'
  reason?: string
}

/** WebSocket event */
export interface WebSocketEvent extends BaseEvent {
  type: 'ws.connect' | 'ws.message' | 'ws.close' | 'ws.error'
  connectionCount?: number
  code?: number
  reason?: string
}

/** Client-side analytics event (browser) */
export interface ClientEvent extends BaseEvent {
  type: 'page' | 'track' | 'identify'
  event?: string
  properties?: Record<string, unknown>
  traits?: Record<string, unknown>
  userId?: string
  anonymousId?: string
  sessionId?: string
}

export type DurableEvent =
  | RpcCallEvent
  | CollectionChangeEvent
  | LifecycleEvent
  | WebSocketEvent
  | ClientEvent
  | BaseEvent

/** Batch of events for ingestion */
export interface EventBatch {
  events: DurableEvent[]
}

/** Event emitter configuration */
export interface EventEmitterOptions {
  /** Endpoint to send events (default: events.do) */
  endpoint?: string
  /** Batch size before auto-flush (default: 100) */
  batchSize?: number
  /** Max time to hold events before flush in ms (default: 1000) */
  flushIntervalMs?: number
  /** Enable CDC for collections (default: false) */
  cdc?: boolean
  /** R2 bucket for lakehouse streaming (optional) */
  r2Bucket?: R2Bucket
  /** Include previous doc in CDC updates (more storage, enables diffs) */
  trackPrevious?: boolean
  /** API key for authentication (optional) */
  apiKey?: string
}
