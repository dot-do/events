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
// EventEmitter Configuration
// ============================================================================

export interface EventEmitterOptions {
  endpoint?: string | undefined
  batchSize?: number | undefined
  flushIntervalMs?: number | undefined
  cdc?: boolean | undefined
  trackPrevious?: boolean | undefined
  apiKey?: string | undefined
  maxRetryQueueSize?: number | undefined
  maxConsecutiveFailures?: number | undefined
  circuitBreakerResetMs?: number | undefined
  fetchTimeoutMs?: number | undefined
  logger?: EventLogger | undefined
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
