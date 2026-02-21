/**
 * Event Types for @dotdo/events
 */

// ============================================================================
// New ClickHouse-aligned event type system
// ============================================================================

export * from './types/index.js'

// ============================================================================
// Branded Types for IDs
// ============================================================================

/** Unique symbol for branding types */
declare const __brand: unique symbol

/** Brand utility type for creating nominal types */
type Brand<T, B extends string> = T & { [__brand]: B }

/** Branded type for subscription identifiers */
export type SubscriptionId = Brand<string, 'SubscriptionId'>

/** Branded type for delivery identifiers */
export type DeliveryId = Brand<string, 'DeliveryId'>

/** Branded type for event identifiers */
export type EventId = Brand<string, 'EventId'>

/** Branded type for table identifiers */
export type TableId = Brand<string, 'TableId'>

/** Branded type for snapshot identifiers */
export type SnapshotId = Brand<string, 'SnapshotId'>

/** Branded type for schema identifiers */
export type SchemaId = Brand<string, 'SchemaId'>

// Factory functions for creating branded IDs

export function subscriptionId(id: string): SubscriptionId {
  return id as SubscriptionId
}

export function deliveryId(id: string): DeliveryId {
  return id as DeliveryId
}

export function eventId(id: string): EventId {
  return id as EventId
}

export function tableId(id: string): TableId {
  return id as TableId
}

export function snapshotId(id: string): SnapshotId {
  return id as SnapshotId
}

export function schemaId(id: string): SchemaId {
  return id as SchemaId
}

// ============================================================================
// Logger Interface
// ============================================================================

export interface EventLogger {
  debug(message: string, context?: Record<string, unknown>): void
  info(message: string, context?: Record<string, unknown>): void
  warn(message: string, context?: Record<string, unknown>): void
  error(message: string, context?: Record<string, unknown>): void
}

// ============================================================================
// Pipeline Interface
// ============================================================================

export interface PipelineLike {
  send(records: Record<string, unknown>[]): Promise<void>
}

// ============================================================================
// EventEmitter Configuration
// ============================================================================

export interface EventEmitterOptions {
  /** Pipeline binding — primary transport */
  pipeline: PipelineLike
  /** Batch size before auto-flush (default: 100) */
  batchSize?: number | undefined
  /** Flush interval in ms (default: 1000) */
  flushIntervalMs?: number | undefined
  /** Enable CDC event emission */
  cdc?: boolean | undefined
  /** Track previous document state for CDC */
  trackPrevious?: boolean | undefined
  /** Max events to persist in retry queue (default: 10000) */
  maxRetryQueueSize?: number | undefined
  /** Consecutive failures before circuit breaker opens (default: 10) */
  maxConsecutiveFailures?: number | undefined
  /** Time before circuit breaker resets in ms (default: 300000) */
  circuitBreakerResetMs?: number | undefined
  /** Logger */
  logger?: EventLogger | undefined
}

/** Resolved options with defaults applied */
export interface ResolvedEmitterOptions {
  batchSize: number
  flushIntervalMs: number
  cdc: boolean
  trackPrevious: boolean
  maxRetryQueueSize: number
  maxConsecutiveFailures: number
  circuitBreakerResetMs: number
}

// ============================================================================
// Error Classes
// ============================================================================

export class EventBufferFullError extends Error {
  constructor(
    message: string,
    public readonly droppedCount: number,
  ) {
    super(message)
    this.name = 'EventBufferFullError'
  }
}

export class CircuitBreakerOpenError extends Error {
  constructor(
    message: string,
    public readonly consecutiveFailures: number,
    public readonly resetAt: Date,
  ) {
    super(message)
    this.name = 'CircuitBreakerOpenError'
  }
}
