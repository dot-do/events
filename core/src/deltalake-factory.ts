/**
 * DeltaTable Factory Functions
 *
 * Factory functions for creating event and CDC tables using @dotdo/deltalake.
 * These functions provide pre-configured DeltaTable instances for common use cases.
 *
 * Path Structure:
 * - Events: events/shard={shardId}/
 * - CDC: cdc/{namespace}/{collection}/
 */

import {
  DeltaTable,
  createCDCDeltaTable,
  type CDCDeltaTable,
  type StorageBackend,
  type WriteOptions,
} from '@dotdo/deltalake'

// =============================================================================
// TYPES
// =============================================================================

/**
 * Options for creating an events DeltaTable.
 */
export interface EventsTableOptions {
  /** Shard ID for partitioned writes (default: 0) */
  shardId?: number
  /** Base path for events (default: 'events') */
  basePath?: string
}

/**
 * Options for creating a CDC DeltaTable.
 */
export interface CDCTableOptions {
  /** Namespace for CDC data */
  namespace: string
  /** Collection/table name */
  collection: string
  /** Base path for CDC data (default: 'cdc') */
  basePath?: string
  /** Enable CDC tracking (default: true) */
  enableCDC?: boolean
}

/**
 * Event record type for DeltaTable writes.
 * Matches the EventRecord type from event-writer.ts.
 */
export type EventDeltaRecord = {
  ts: string
  type: string
  source?: string | null
  provider?: string | null
  eventType?: string | null
  verified?: boolean | null
  scriptName?: string | null
  outcome?: string | null
  method?: string | null
  url?: string | null
  statusCode?: number | null
  durationMs?: number | null
  payload?: string | null // JSON stringified
} & Record<string, unknown>

/**
 * CDC record type for DeltaTable writes.
 * Uses the deltalake CDC format.
 */
export type CDCDeltaRecord = {
  _id: string
  _seq: string // bigint as string for JSON serialization
  _op: 'c' | 'u' | 'd' | 'r'
  _before: string | null // JSON stringified
  _after: string | null // JSON stringified
  _ts: string // bigint as string (nanoseconds)
  _source: string // JSON stringified CDCSource
  _txn?: string
} & Record<string, unknown>

// =============================================================================
// EVENT TABLE FACTORY
// =============================================================================

/**
 * Create a DeltaTable for event storage.
 *
 * Events are partitioned by shard ID to avoid write conflicts when multiple
 * EventWriterDO shards write concurrently. Each shard writes to its own
 * partition: events/shard={shardId}/
 *
 * @param storage - Storage backend (use createR2Storage for R2)
 * @param options - Table configuration options
 * @returns DeltaTable configured for event writes
 *
 * @example
 * ```typescript
 * import { createR2Storage, createEventsTable } from '@dotdo/events'
 *
 * const storage = createR2Storage(env.EVENTS_BUCKET)
 * const events = createEventsTable(storage, { shardId: 2 })
 *
 * await events.write([{
 *   ts: new Date().toISOString(),
 *   type: 'webhook.github.push',
 *   source: 'github',
 *   payload: JSON.stringify({ repo: 'myrepo' }),
 * }])
 * ```
 */
export function createEventsTable(
  storage: StorageBackend,
  options: EventsTableOptions = {}
): DeltaTable<EventDeltaRecord> {
  const basePath = options.basePath ?? 'events'
  const shardId = options.shardId ?? 0

  // Each shard writes to its own partition path to avoid conflicts
  const tablePath = `${basePath}/shard=${shardId}`

  return new DeltaTable<EventDeltaRecord>(storage, tablePath)
}

/**
 * Write options for event writes with shard-based partitioning.
 */
export function getEventWriteOptions(): WriteOptions {
  return {
    partitionColumns: ['shard'],
  }
}

// =============================================================================
// CDC TABLE FACTORY
// =============================================================================

/**
 * Create a CDC-enabled DeltaTable for change data capture.
 *
 * CDC tables track all changes (inserts, updates, deletes) with full
 * before/after images. This enables:
 * - Point-in-time recovery
 * - Change streaming to downstream consumers
 * - Audit logging
 *
 * @param storage - Storage backend (use createR2Storage for R2)
 * @param options - Table configuration options
 * @returns CDCDeltaTable configured for CDC tracking
 *
 * @example
 * ```typescript
 * import { createR2Storage, createCDCTable } from '@dotdo/events'
 *
 * const storage = createR2Storage(env.EVENTS_BUCKET)
 * const users = createCDCTable(storage, {
 *   namespace: 'myapp',
 *   collection: 'users',
 * })
 *
 * // Enable CDC tracking
 * await users.setCDCEnabled(true)
 *
 * // Write data with CDC tracking
 * await users.write([{
 *   _id: 'user-1',
 *   _seq: '0',
 *   _op: 'c',
 *   _before: null,
 *   _after: JSON.stringify({ name: 'Alice' }),
 *   _ts: String(Date.now() * 1_000_000),
 *   _source: JSON.stringify({ system: 'deltalake', collection: 'users' }),
 * }])
 * ```
 */
export function createCDCTable(
  storage: StorageBackend,
  options: CDCTableOptions
): CDCDeltaTable<CDCDeltaRecord> {
  const basePath = options.basePath ?? 'cdc'
  const tablePath = `${basePath}/${options.namespace}/${options.collection}`

  const table = createCDCDeltaTable<CDCDeltaRecord>(storage, tablePath)

  // Enable CDC by default for CDC tables
  if (options.enableCDC !== false) {
    // Note: setCDCEnabled is async, but we don't await here to avoid
    // making the factory function async. Caller should await if needed.
    table.setCDCEnabled(true).catch(() => {
      // Ignore errors - CDC config will be set on first write
    })
  }

  return table
}

/**
 * Create a standard DeltaTable for CDC data (without CDCDeltaTable wrapper).
 *
 * Use this when you just need a simple DeltaTable for reading/writing CDC records
 * without the full CDC tracking features.
 *
 * @param storage - Storage backend
 * @param namespace - CDC namespace
 * @param collection - Collection name
 * @returns DeltaTable for CDC records
 */
export function createSimpleCDCTable(
  storage: StorageBackend,
  namespace: string,
  collection: string
): DeltaTable<CDCDeltaRecord> {
  const tablePath = `cdc/${namespace}/${collection}`
  return new DeltaTable<CDCDeltaRecord>(storage, tablePath)
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Get the path for an events DeltaTable given a shard ID.
 */
export function getEventsTablePath(shardId: number, basePath: string = 'events'): string {
  return `${basePath}/shard=${shardId}`
}

/**
 * Get the path for a CDC DeltaTable.
 */
export function getCDCTablePath(
  namespace: string,
  collection: string,
  basePath: string = 'cdc'
): string {
  return `${basePath}/${namespace}/${collection}`
}

/**
 * Parse shard ID from an events table path.
 */
export function parseShardFromPath(path: string): number | null {
  const match = path.match(/shard=(\d+)/)
  if (!match || !match[1]) return null
  return parseInt(match[1], 10)
}

/**
 * Parse namespace and collection from a CDC table path.
 */
export function parseCDCPathComponents(path: string): { namespace: string; collection: string } | null {
  // Expected format: cdc/{namespace}/{collection} or cdc/{namespace}/{collection}/...
  const match = path.match(/^cdc\/([^\/]+)\/([^\/]+)/)
  if (!match || !match[1] || !match[2]) return null
  return {
    namespace: match[1],
    collection: match[2],
  }
}
