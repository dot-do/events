/**
 * Parquet Compaction Module
 *
 * Provides functions for reading, merging, and writing Parquet files
 * for compaction operations in the events lakehouse.
 */

import { parquetReadObjects } from 'hyparquet'
import { parquetWriteBuffer } from 'hyparquet-writer'
import type { ColumnSource } from 'hyparquet-writer'
import { createAsyncBuffer } from './async-buffer.js'

/**
 * Reads a Parquet file and returns an array of row objects
 *
 * @param buffer - The Parquet file as an ArrayBuffer
 * @returns Array of record objects with column names as keys
 */
export async function readParquetRecords(buffer: ArrayBuffer): Promise<Record<string, unknown>[]> {
  const file = createAsyncBuffer(buffer)
  const records = await parquetReadObjects({ file })
  return records
}

/**
 * Reads and merges multiple Parquet files, sorting by timestamp
 *
 * @param files - Array of Parquet file buffers
 * @returns Merged and sorted array of records
 */
export async function mergeParquetRecords(files: ArrayBuffer[]): Promise<Record<string, unknown>[]> {
  if (files.length === 0) {
    return []
  }

  // Read all files in parallel
  const allRecordsArrays = await Promise.all(files.map((file) => readParquetRecords(file)))

  // Concatenate all records
  const allRecords = allRecordsArrays.flat()

  // Sort by ts field (handle both number and bigint)
  allRecords.sort((a, b) => {
    const tsA = BigInt(a.ts as bigint | number)
    const tsB = BigInt(b.ts as bigint | number)
    if (tsA < tsB) return -1
    if (tsA > tsB) return 1
    return 0
  })

  return allRecords
}

/**
 * Infers the column type from a sample value
 */
function inferColumnType(value: unknown): ColumnSource['type'] {
  if (value === null || value === undefined) {
    return 'STRING' // Default for null
  }
  if (typeof value === 'bigint') {
    return 'INT64'
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return 'INT32'
    }
    return 'DOUBLE'
  }
  if (typeof value === 'boolean') {
    return 'BOOLEAN'
  }
  return 'STRING'
}

/**
 * Builds column data from an array of records
 */
function buildColumnData(records: Record<string, unknown>[]): ColumnSource[] {
  if (records.length === 0) {
    return []
  }

  // Get all unique column names from all records
  const columnNames = new Set<string>()
  for (const record of records) {
    for (const key of Object.keys(record)) {
      columnNames.add(key)
    }
  }

  const columns: ColumnSource[] = []

  for (const name of columnNames) {
    // Find first non-null value to infer type
    let sampleValue: unknown = null
    for (const record of records) {
      if (record[name] !== null && record[name] !== undefined) {
        sampleValue = record[name]
        break
      }
    }

    const type = inferColumnType(sampleValue)

    // Extract column data
    let data: ColumnSource['data']

    if (type === 'INT64') {
      data = BigInt64Array.from(records.map((r) => (r[name] !== null && r[name] !== undefined ? BigInt(r[name] as bigint | number) : 0n)))
    } else if (type === 'INT32') {
      data = Int32Array.from(records.map((r) => (r[name] !== null && r[name] !== undefined ? Number(r[name]) : 0)))
    } else if (type === 'DOUBLE') {
      data = Float64Array.from(records.map((r) => (r[name] !== null && r[name] !== undefined ? Number(r[name]) : 0)))
    } else if (type === 'BOOLEAN') {
      data = records.map((r) => (r[name] !== null && r[name] !== undefined ? Boolean(r[name]) : false))
    } else {
      // STRING
      data = records.map((r) => (r[name] !== null && r[name] !== undefined ? String(r[name]) : ''))
    }

    columns.push({
      name,
      data,
      type,
      nullable: true,
    })
  }

  return columns
}

/**
 * Writes records to a new compacted Parquet file
 *
 * Uses UNCOMPRESSED codec since dictionary encoding provides compression.
 * Includes statistics for efficient query filtering.
 *
 * @param records - Array of record objects to write
 * @returns Parquet file as ArrayBuffer
 */
export function writeCompactedParquet(records: Record<string, unknown>[]): ArrayBuffer {
  const columnData = buildColumnData(records)

  // Handle empty records case - need at least a minimal schema
  if (columnData.length === 0) {
    return parquetWriteBuffer({
      columnData: [{ name: '_empty', data: [], type: 'STRING' }],
      codec: 'UNCOMPRESSED',
      statistics: true,
    })
  }

  return parquetWriteBuffer({
    columnData,
    codec: 'UNCOMPRESSED',
    statistics: true,
  })
}

/**
 * Lists files in an R2 bucket prefix that are eligible for compaction
 *
 * Excludes files that are already compacted (contain '_compact' in name).
 * Returns files sorted by key for deterministic ordering.
 *
 * @param bucket - R2 bucket instance
 * @param prefix - Key prefix to list (e.g., 'events/2024/01/15/12/')
 * @returns Array of R2Object metadata sorted by key
 */
export async function listFilesForCompaction(bucket: R2Bucket, prefix: string): Promise<R2Object[]> {
  const listed = await bucket.list({ prefix })

  // Filter out already compacted files
  const files = listed.objects.filter((obj) => !obj.key.includes('_compact'))

  // Sort by key for deterministic ordering
  files.sort((a, b) => a.key.localeCompare(b.key))

  return files
}
