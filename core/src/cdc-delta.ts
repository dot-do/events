/**
 * CDC Delta File Module
 *
 * Provides functions for creating, writing, and reading delta files for CDC.
 * Delta files store incremental changes in Parquet format with schema:
 * - pk: STRING (primary key)
 * - op: STRING (insert/update/delete)
 * - data: STRING (JSON-encoded VARIANT - new document state, null for delete)
 * - prev: STRING (JSON-encoded VARIANT - previous state, optional)
 * - ts: STRING (ISO 8601 timestamp)
 * - bookmark: STRING (SQLite bookmark for PITR)
 */

import { parquetReadObjects } from 'hyparquet'
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'
import { createAsyncBuffer } from './async-buffer.js'

// ============================================================================
// Types
// ============================================================================

/**
 * CDC Event from the emitter (matches CollectionChangeEvent)
 */
export interface CDCEvent {
  type: 'collection.insert' | 'collection.update' | 'collection.delete'
  collection: string
  docId: string
  doc?: Record<string, unknown>
  prev?: Record<string, unknown>
  bookmark?: string
  ts: string
  do: {
    id: string
    name?: string
    class?: string
    colo?: string
    worker?: string
  }
}

/**
 * Delta record for Parquet storage
 */
export interface DeltaRecord {
  /** Primary key (document ID) */
  pk: string
  /** Operation type: insert, update, or delete */
  op: 'insert' | 'update' | 'delete'
  /** New document state (JSON object), null for deletes */
  data: Record<string, unknown> | null
  /** Previous document state (JSON object), null if not tracked */
  prev: Record<string, unknown> | null
  /** ISO 8601 timestamp */
  ts: string
  /** SQLite bookmark for PITR (Point-in-Time Recovery) */
  bookmark: string | null
}

// ============================================================================
// Delta Record Creation
// ============================================================================

/**
 * Extracts operation type from CDC event type
 */
function extractOp(eventType: CDCEvent['type']): DeltaRecord['op'] {
  switch (eventType) {
    case 'collection.insert':
      return 'insert'
    case 'collection.update':
      return 'update'
    case 'collection.delete':
      return 'delete'
  }
}

/**
 * Creates a DeltaRecord from a CDC event
 *
 * @param event - CDC event from the emitter
 * @returns DeltaRecord ready for Parquet storage
 */
export function createDeltaRecord(event: CDCEvent): DeltaRecord {
  const op = extractOp(event.type)

  return {
    pk: event.docId,
    op,
    data: event.doc ?? null,
    prev: event.prev ?? null,
    ts: event.ts,
    bookmark: event.bookmark ?? null,
  }
}

// ============================================================================
// Parquet File Operations
// ============================================================================

/**
 * Writes delta records to a Parquet file buffer
 *
 * Uses STRING type for VARIANT columns (data/prev) by JSON-encoding the objects.
 * This allows efficient storage and querying with DuckDB's json_extract functions.
 *
 * @param records - Array of delta records to write
 * @returns Parquet file as ArrayBuffer
 */
export function writeDeltaFile(records: DeltaRecord[]): ArrayBuffer {
  // Handle empty records case
  if (records.length === 0) {
    return parquetWriteBuffer({
      columnData: [
        { name: 'pk', data: [], type: 'STRING' },
        { name: 'op', data: [], type: 'STRING' },
        { name: 'data', data: [], type: 'STRING' },
        { name: 'prev', data: [], type: 'STRING' },
        { name: 'ts', data: [], type: 'STRING' },
        { name: 'bookmark', data: [], type: 'STRING' },
      ],
      codec: 'UNCOMPRESSED',
      statistics: true,
    })
  }

  return parquetWriteBuffer({
    columnData: [
      {
        name: 'pk',
        data: records.map((r) => r.pk),
        type: 'STRING',
      },
      {
        name: 'op',
        data: records.map((r) => r.op),
        type: 'STRING',
      },
      {
        name: 'data',
        data: records.map((r) => (r.data === null ? '' : JSON.stringify(r.data))),
        type: 'STRING',
        nullable: true,
      },
      {
        name: 'prev',
        data: records.map((r) => (r.prev === null ? '' : JSON.stringify(r.prev))),
        type: 'STRING',
        nullable: true,
      },
      {
        name: 'ts',
        data: records.map((r) => r.ts),
        type: 'STRING',
      },
      {
        name: 'bookmark',
        data: records.map((r) => r.bookmark ?? ''),
        type: 'STRING',
        nullable: true,
      },
    ],
    codec: 'UNCOMPRESSED',
    statistics: true,
  })
}

/**
 * Reads delta records from a Parquet file buffer
 *
 * @param buffer - Parquet file as ArrayBuffer
 * @returns Array of delta records
 */
export async function readDeltaFile(buffer: ArrayBuffer): Promise<DeltaRecord[]> {
  const file = createAsyncBuffer(buffer)
  const rows = await parquetReadObjects({ file })

  if (rows.length === 0) {
    return []
  }

  return rows.map((row) => {
    const dataStr = row.data as string
    const prevStr = row.prev as string
    const bookmarkStr = row.bookmark as string

    return {
      pk: row.pk as string,
      op: row.op as DeltaRecord['op'],
      data: dataStr === '' ? null : (JSON.parse(dataStr) as Record<string, unknown>),
      prev: prevStr === '' ? null : (JSON.parse(prevStr) as Record<string, unknown>),
      ts: row.ts as string,
      bookmark: bookmarkStr === '' ? null : bookmarkStr,
    }
  })
}

// ============================================================================
// Delta Accumulation
// ============================================================================

/**
 * Accumulates deltas for the same primary key (last-write-wins)
 *
 * This is useful for compaction where multiple changes to the same record
 * should be collapsed to the final state.
 *
 * @param deltas - Array of delta records
 * @returns Map of pk -> final delta record
 */
export function accumulateDeltas(deltas: DeltaRecord[]): Map<string, DeltaRecord> {
  const accumulated = new Map<string, DeltaRecord>()

  for (const delta of deltas) {
    accumulated.set(delta.pk, delta)
  }

  return accumulated
}

// ============================================================================
// Path Generation
// ============================================================================

/**
 * Formats a timestamp for safe use in filenames
 *
 * Converts ISO 8601 format to filesystem-safe format by replacing colons
 * with dashes and dots with dashes.
 *
 * @param date - Date to format
 * @returns Filesystem-safe timestamp string
 */
function formatTimestampForFilename(date: Date): string {
  return date.toISOString().replace(/:/g, '-').replace(/\./g, '-')
}

/**
 * Generates the path for a delta file
 *
 * Path format: {ns}/{collection}/deltas/{seq}_{timestamp}.parquet
 *
 * @param ns - Namespace
 * @param collection - Collection name
 * @param seq - Sequence number (will be zero-padded to at least 3 digits)
 * @param timestamp - Optional timestamp (defaults to now)
 * @returns Delta file path
 */
export function generateDeltaPath(ns: string, collection: string, seq: number, timestamp?: Date): string {
  const ts = timestamp ?? new Date()
  const formattedTs = formatTimestampForFilename(ts)

  // Zero-pad sequence number to at least 3 digits
  const paddedSeq = seq.toString().padStart(3, '0')

  return `${ns}/${collection}/deltas/${paddedSeq}_${formattedTs}.parquet`
}
