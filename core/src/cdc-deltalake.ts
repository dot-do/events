/**
 * CDC Format Conversion Utilities for DeltaLake
 *
 * Converts between the events.do CDC format and the @dotdo/deltalake CDC format.
 *
 * events.do format:
 * - pk: primary key (document ID)
 * - op: operation ('insert' | 'update' | 'delete')
 * - data: current document state
 * - prev: previous document state (for updates/deletes)
 * - ts: ISO timestamp
 * - bookmark: SQLite PITR bookmark
 *
 * deltalake format:
 * - _id: entity ID
 * - _seq: sequence number (bigint)
 * - _op: operation ('c' | 'u' | 'd' | 'r')
 * - _before: previous state (null for create)
 * - _after: new state (null for delete)
 * - _ts: timestamp in nanoseconds (bigint)
 * - _source: source metadata
 */

import type { CDCRecord, CDCSource, CDCOperation } from '@dotdo/deltalake/cdc'

// =============================================================================
// TYPES
// =============================================================================

/**
 * events.do CDC operation types.
 */
export type EventsDoOperation = 'insert' | 'update' | 'delete'

/**
 * events.do CDC record format (from emitter.ts CollectionChangeEvent).
 */
export interface EventsDoCDCRecord {
  /** Primary key / document ID */
  pk: string
  /** Operation type */
  op: EventsDoOperation
  /** Current document state (for insert/update) */
  data?: Record<string, unknown>
  /** Previous document state (for update/delete if trackPrevious enabled) */
  prev?: Record<string, unknown>
  /** ISO timestamp */
  ts: string
  /** SQLite PITR bookmark */
  bookmark?: string
  /** Collection name */
  collection: string
  /** Durable Object identity */
  do?: {
    id: string
    name?: string
    colo?: string
    worker?: string
    class?: string
  }
}

/**
 * Sequence number generator state.
 */
interface SequenceState {
  /** Current sequence number */
  seq: bigint
  /** Last timestamp seen (for ordering) */
  lastTs: bigint
}

// =============================================================================
// OPERATION MAPPING
// =============================================================================

/**
 * Map events.do operation to deltalake CDC operation.
 */
export function mapOperationToDeltalake(op: EventsDoOperation): CDCOperation {
  switch (op) {
    case 'insert':
      return 'c' // create
    case 'update':
      return 'u' // update
    case 'delete':
      return 'd' // delete
    default:
      return 'c' // default to create
  }
}

/**
 * Map deltalake CDC operation to events.do operation.
 */
export function mapOperationFromDeltalake(op: CDCOperation): EventsDoOperation {
  switch (op) {
    case 'c':
      return 'insert'
    case 'u':
      return 'update'
    case 'd':
      return 'delete'
    case 'r': // snapshot/read operation maps to insert
      return 'insert'
    default:
      return 'insert'
  }
}

// =============================================================================
// CONVERSION FUNCTIONS
// =============================================================================

/**
 * Global sequence state per collection.
 * Maintains monotonically increasing sequence numbers.
 */
const sequenceStates = new Map<string, SequenceState>()

/**
 * Get or create sequence state for a collection.
 */
function getSequenceState(collection: string): SequenceState {
  let state = sequenceStates.get(collection)
  if (!state) {
    state = { seq: 0n, lastTs: 0n }
    sequenceStates.set(collection, state)
  }
  return state
}

/**
 * Generate next sequence number for a collection.
 * Ensures monotonically increasing sequences.
 */
function nextSequence(collection: string, tsNano: bigint): bigint {
  const state = getSequenceState(collection)

  // If timestamp is newer, use it as base
  if (tsNano > state.lastTs) {
    state.lastTs = tsNano
    state.seq = tsNano
  } else {
    // Same or older timestamp, increment sequence
    state.seq++
  }

  return state.seq
}

/**
 * Convert events.do CDC record to deltalake CDC record format.
 *
 * @param record - events.do CDC record
 * @returns deltalake CDCRecord
 *
 * @example
 * ```typescript
 * const eventsDoRecord: EventsDoCDCRecord = {
 *   pk: 'user-123',
 *   op: 'update',
 *   data: { name: 'Alice', score: 150 },
 *   prev: { name: 'Alice', score: 100 },
 *   ts: '2024-01-15T10:30:00.000Z',
 *   bookmark: 'bookmark-xyz',
 *   collection: 'users',
 * }
 *
 * const deltaRecord = convertToDeltalakeCDC(eventsDoRecord)
 * // deltaRecord._op === 'u'
 * // deltaRecord._before === { name: 'Alice', score: 100 }
 * // deltaRecord._after === { name: 'Alice', score: 150 }
 * ```
 */
export function convertToDeltalakeCDC(
  record: EventsDoCDCRecord
): CDCRecord<Record<string, unknown>> {
  // Convert ISO timestamp to nanoseconds
  const tsMs = new Date(record.ts).getTime()
  const tsNano = BigInt(tsMs) * 1_000_000n // milliseconds to nanoseconds

  // Generate sequence number
  const seq = nextSequence(record.collection, tsNano)

  // Build source metadata
  const source: CDCSource = {
    system: 'deltalake',
    collection: record.collection,
  }

  // Include DO identity in source if available
  if (record.do) {
    // Store DO metadata in the source
    // Note: CDCSource doesn't have all DO fields, but we preserve what we can
    if (record.do.colo) {
      source.serverId = record.do.colo
    }
  }

  // Map operation
  const op = mapOperationToDeltalake(record.op)

  // Determine before/after based on operation
  let before: Record<string, unknown> | null = null
  let after: Record<string, unknown> | null = null

  switch (record.op) {
    case 'insert':
      before = null
      after = record.data ?? {}
      break
    case 'update':
      before = record.prev ?? null
      after = record.data ?? {}
      break
    case 'delete':
      before = record.prev ?? record.data ?? {}
      after = null
      break
  }

  // Build CDC record
  const cdcRecord: CDCRecord<Record<string, unknown>> = {
    _id: record.pk,
    _seq: seq,
    _op: op,
    _before: before,
    _after: after,
    _ts: tsNano,
    _source: source,
  }

  // Include bookmark as transaction ID if available
  if (record.bookmark) {
    cdcRecord._txn = record.bookmark
  }

  return cdcRecord
}

/**
 * Convert deltalake CDC record back to events.do format.
 *
 * @param record - deltalake CDCRecord
 * @param collection - Collection name
 * @returns events.do CDC record
 */
export function convertFromDeltalakeCDC(
  record: CDCRecord<Record<string, unknown>>,
  collection: string
): EventsDoCDCRecord {
  // Convert nanoseconds to ISO timestamp
  const tsMs = Number(record._ts / 1_000_000n)
  const ts = new Date(tsMs).toISOString()

  // Map operation
  const op = mapOperationFromDeltalake(record._op)

  // Determine data/prev based on operation
  let data: Record<string, unknown> | undefined
  let prev: Record<string, unknown> | undefined

  switch (op) {
    case 'insert':
      data = record._after ?? undefined
      break
    case 'update':
      data = record._after ?? undefined
      prev = record._before ?? undefined
      break
    case 'delete':
      prev = record._before ?? undefined
      break
  }

  const eventsDoRecord: EventsDoCDCRecord = {
    pk: record._id,
    op,
    ts,
    collection,
  }

  if (data) eventsDoRecord.data = data
  if (prev) eventsDoRecord.prev = prev
  if (record._txn) eventsDoRecord.bookmark = record._txn

  return eventsDoRecord
}

/**
 * Convert a batch of events.do CDC records to deltalake format.
 *
 * @param records - Array of events.do CDC records
 * @returns Array of deltalake CDCRecords
 */
export function convertBatchToDeltalakeCDC(
  records: EventsDoCDCRecord[]
): CDCRecord<Record<string, unknown>>[] {
  return records.map(convertToDeltalakeCDC)
}

/**
 * Convert a batch of deltalake CDC records to events.do format.
 *
 * @param records - Array of deltalake CDCRecords
 * @param collection - Collection name for all records
 * @returns Array of events.do CDC records
 */
export function convertBatchFromDeltalakeCDC(
  records: CDCRecord<Record<string, unknown>>[],
  collection: string
): EventsDoCDCRecord[] {
  return records.map(r => convertFromDeltalakeCDC(r, collection))
}

// =============================================================================
// SERIALIZATION HELPERS
// =============================================================================

/**
 * Serialize a CDC record for Parquet storage.
 * Converts bigint fields to strings for JSON compatibility.
 */
export function serializeCDCRecord(
  record: CDCRecord<Record<string, unknown>>
): {
  _id: string
  _seq: string
  _op: CDCOperation
  _before: string | null
  _after: string | null
  _ts: string
  _source: string
  _txn?: string
} {
  return {
    _id: record._id,
    _seq: record._seq.toString(),
    _op: record._op,
    _before: record._before ? JSON.stringify(record._before) : null,
    _after: record._after ? JSON.stringify(record._after) : null,
    _ts: record._ts.toString(),
    _source: JSON.stringify(record._source),
    ...(record._txn ? { _txn: record._txn } : {}),
  }
}

/**
 * Deserialize a CDC record from Parquet storage.
 * Converts string fields back to bigint.
 */
export function deserializeCDCRecord(
  serialized: {
    _id: string
    _seq: string
    _op: string
    _before: string | null
    _after: string | null
    _ts: string
    _source: string
    _txn?: string
  }
): CDCRecord<Record<string, unknown>> {
  return {
    _id: serialized._id,
    _seq: BigInt(serialized._seq),
    _op: serialized._op as CDCOperation,
    _before: serialized._before ? JSON.parse(serialized._before) : null,
    _after: serialized._after ? JSON.parse(serialized._after) : null,
    _ts: BigInt(serialized._ts),
    _source: JSON.parse(serialized._source) as CDCSource,
    ...(serialized._txn ? { _txn: serialized._txn } : {}),
  }
}

// =============================================================================
// RESET UTILITIES (for testing)
// =============================================================================

/**
 * Reset sequence state for a collection (for testing).
 */
export function resetSequenceState(collection?: string): void {
  if (collection) {
    sequenceStates.delete(collection)
  } else {
    sequenceStates.clear()
  }
}

/**
 * Set sequence state for a collection (for testing or recovery).
 */
export function setSequenceState(collection: string, seq: bigint, lastTs: bigint): void {
  sequenceStates.set(collection, { seq, lastTs })
}
