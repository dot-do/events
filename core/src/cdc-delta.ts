/**
 * CDC Delta File Module
 *
 * Provides functions for creating, writing, and reading delta files for CDC.
 * Delta files store incremental changes in Parquet format with schema:
 * - pk: STRING (primary key)
 * - op: STRING (created/updated/deleted)
 * - data: STRING (JSON-encoded VARIANT - new document state, null for delete)
 * - prev: STRING (JSON-encoded VARIANT - previous state, optional)
 * - ts: STRING (ISO 8601 timestamp)
 * - bookmark: STRING (SQLite bookmark for PITR)
 */

import { parquetReadObjects } from 'hyparquet'
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'
import { createAsyncBuffer } from './async-buffer.js'
import type { CdcEvent } from './types/cdc.js'

// ============================================================================
// Types
// ============================================================================

/**
 * CDC Event from the emitter — alias for the ClickHouse-aligned CdcEvent type.
 * The event's `data` contains entity fields ({ type, id, ...rest }),
 * and `meta` may contain `prev` and `bookmark`.
 */
export type CDCEvent = CdcEvent

/** Operation type extracted from the event name (past tense — action=create, event=created) */
export type CDCOp = 'created' | 'updated' | 'deleted'

/**
 * Delta record for Parquet storage
 */
export interface DeltaRecord {
  pk: string
  op: CDCOp
  data: Record<string, unknown> | null
  prev: Record<string, unknown> | null
  ts: string
  bookmark: string | null
}

// ============================================================================
// Type Validators
// ============================================================================

function isRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function parseRecordJson(str: string, fieldName: string): Record<string, unknown> | null {
  if (str === '') {
    return null
  }
  const parsed: unknown = JSON.parse(str)
  if (!isRecordObject(parsed)) {
    throw new Error(`${fieldName} must be an object, got ${typeof parsed}`)
  }
  return parsed
}

// ============================================================================
// Delta Record Creation
// ============================================================================

/**
 * Extracts operation type from the CDC event name.
 * Expected format: `{collection}.{op}` (e.g. 'contacts.created', 'users.deleted')
 */
export function extractOp(eventName: string): CDCOp {
  const dot = eventName.lastIndexOf('.')
  const op = dot >= 0 ? eventName.slice(dot + 1) : eventName
  if (op === 'created' || op === 'updated' || op === 'deleted') {
    return op
  }
  throw new Error(`Unknown CDC operation in event name: ${eventName}`)
}

/**
 * Creates a DeltaRecord from a CDC event
 */
export function createDeltaRecord(event: CDCEvent): DeltaRecord {
  const op = extractOp(event.event)
  const { type: _type, id, ...rest } = event.data
  const prev = (event.meta as Record<string, unknown>).prev as Record<string, unknown> | undefined
  const bookmark = (event.meta as Record<string, unknown>).bookmark as string | undefined

  return {
    pk: id,
    op,
    data: Object.keys(rest).length > 0 ? rest : null,
    prev: prev ?? null,
    ts: event.ts,
    bookmark: bookmark ?? null,
  }
}

// ============================================================================
// Parquet File Operations
// ============================================================================

export function writeDeltaFile(records: DeltaRecord[]): ArrayBuffer {
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
      data: parseRecordJson(dataStr, 'data'),
      prev: parseRecordJson(prevStr, 'prev'),
      ts: row.ts as string,
      bookmark: bookmarkStr === '' ? null : bookmarkStr,
    }
  })
}

// ============================================================================
// Delta Accumulation
// ============================================================================

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

function formatTimestampForFilename(date: Date): string {
  return date.toISOString().replace(/:/g, '-').replace(/\./g, '-')
}

export function generateDeltaPath(ns: string, collection: string, seq: number, timestamp?: Date): string {
  const ts = timestamp ?? new Date()
  const formattedTs = formatTimestampForFilename(ts)
  const paddedSeq = seq.toString().padStart(3, '0')

  return `${ns}/${collection}/deltas/${paddedSeq}_${formattedTs}.parquet`
}
