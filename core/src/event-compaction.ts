/**
 * Event Stream Compaction Module
 *
 * Provides functions for compacting hourly Parquet event files into larger daily files.
 * This improves query performance by reducing the number of files to scan.
 *
 * Compaction process:
 * 1. List all Parquet files for completed days (not today)
 * 2. For each day with multiple files, merge into a single compacted file
 * 3. Delete source files after successful merge
 * 4. Target: 100MB or 1M events per file (whichever is reached first)
 *
 * File layout:
 * - Input:  {prefix}/{year}/{month}/{day}/{hour}/{ulid}.parquet
 * - Output: {prefix}/{year}/{month}/{day}/_compact_{ulid}.parquet
 */

import { readParquetRecords, mergeParquetRecords, writeCompactedParquet } from './compaction.js'

// ============================================================================
// Types
// ============================================================================

/**
 * Options for event stream compaction
 */
export interface EventCompactionOptions {
  /** Maximum bytes per compacted file (default: 100MB) */
  maxBytes?: number | undefined
  /** Maximum events per compacted file (default: 1M) */
  maxEvents?: number | undefined
  /** Prefixes to compact (default: ['events']) */
  prefixes?: string[] | undefined
  /** Days to look back (default: 7) */
  daysBack?: number | undefined
  /** Dry run mode - list files but don't compact (default: false) */
  dryRun?: boolean | undefined
}

/**
 * Result of compacting a single day
 */
export interface DayCompactionResult {
  /** Day path (e.g., "events/2024/01/15") */
  dayPath: string
  /** Whether compaction succeeded */
  success: boolean
  /** Number of source files merged */
  sourceFiles: number
  /** Paths of source files that were merged */
  sourceFilePaths: string[]
  /** Number of compacted output files */
  outputFiles: number
  /** Paths of output files */
  outputFilePaths: string[]
  /** Total records compacted */
  totalRecords: number
  /** Total bytes in output */
  totalBytes: number
  /** Any errors encountered */
  errors?: string[] | undefined
  /** Whether this was a dry run */
  dryRun?: boolean | undefined
}

/**
 * Result of a full compaction run
 */
export interface EventCompactionResult {
  /** Start time of compaction */
  startedAt: string
  /** End time of compaction */
  completedAt: string
  /** Duration in milliseconds */
  durationMs: number
  /** Days that were compacted */
  days: DayCompactionResult[]
  /** Total files processed */
  totalSourceFiles: number
  /** Total output files created */
  totalOutputFiles: number
  /** Total records processed */
  totalRecords: number
  /** Global errors */
  errors?: string[] | undefined
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_MAX_BYTES = 100 * 1024 * 1024 // 100MB
const DEFAULT_MAX_EVENTS = 1_000_000 // 1M events
const DEFAULT_PREFIXES = ['events']
const DEFAULT_DAYS_BACK = 7

// ULID generation
const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

function ulid(): string {
  const now = Date.now()
  let str = ''
  let ts = now
  for (let i = 9; i >= 0; i--) {
    str = ENCODING[ts % 32] + str
    ts = Math.floor(ts / 32)
  }
  for (let i = 0; i < 16; i++) {
    str += ENCODING[Math.floor(Math.random() * 32)]
  }
  return str
}

// ============================================================================
// Main Compaction Function
// ============================================================================

/**
 * Compacts event stream Parquet files for previous days
 *
 * This function:
 * 1. Lists files for each day in the lookback window (excluding today)
 * 2. For days with multiple files, merges them into larger compacted files
 * 3. Deletes source files after successful merge
 *
 * @param bucket - R2 bucket containing event files
 * @param options - Compaction options
 * @returns Compaction result with stats
 *
 * @example
 * ```typescript
 * const result = await compactEventStream(bucket, {
 *   prefixes: ['events', 'webhooks'],
 *   daysBack: 7,
 * })
 * console.log(`Compacted ${result.totalRecords} records across ${result.days.length} days`)
 * ```
 */
export async function compactEventStream(
  bucket: R2Bucket,
  options: EventCompactionOptions = {}
): Promise<EventCompactionResult> {
  const startTime = Date.now()
  const startedAt = new Date().toISOString()
  const errors: string[] = []
  const days: DayCompactionResult[] = []

  const maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES
  const maxEvents = options.maxEvents ?? DEFAULT_MAX_EVENTS
  const prefixes = options.prefixes ?? DEFAULT_PREFIXES
  const daysBack = options.daysBack ?? DEFAULT_DAYS_BACK
  const dryRun = options.dryRun ?? false

  // Get today's date to exclude
  const today = new Date()
  const todayPath = formatDatePath(today)

  // Generate list of days to check (excluding today)
  const daysToCheck: Date[] = []
  for (let i = 1; i <= daysBack; i++) {
    const date = new Date(today.getTime() - i * 24 * 60 * 60 * 1000)
    daysToCheck.push(date)
  }

  // Process each prefix
  for (const prefix of prefixes) {
    for (const date of daysToCheck) {
      const dayPath = `${prefix}/${formatDatePath(date)}`

      try {
        const result = await compactDay(bucket, dayPath, {
          maxBytes,
          maxEvents,
          dryRun,
        })

        if (result.sourceFiles > 1) {
          days.push(result)
        }
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err)
        errors.push(`Error compacting ${dayPath}: ${errorMsg}`)
      }
    }
  }

  const completedAt = new Date().toISOString()
  const durationMs = Date.now() - startTime

  return {
    startedAt,
    completedAt,
    durationMs,
    days,
    totalSourceFiles: days.reduce((sum, d) => sum + d.sourceFiles, 0),
    totalOutputFiles: days.reduce((sum, d) => sum + d.outputFiles, 0),
    totalRecords: days.reduce((sum, d) => sum + d.totalRecords, 0),
    errors: errors.length > 0 ? errors : undefined,
  }
}

/**
 * Compact a single day's files
 */
async function compactDay(
  bucket: R2Bucket,
  dayPath: string,
  options: {
    maxBytes: number
    maxEvents: number
    dryRun: boolean
  }
): Promise<DayCompactionResult> {
  const errors: string[] = []
  const sourceFilePaths: string[] = []
  const outputFilePaths: string[] = []
  let totalRecords = 0
  let totalBytes = 0

  // List all Parquet files for this day (across all hours)
  // Files are in: {dayPath}/{hour}/{ulid}.parquet
  const allFiles: R2Object[] = []

  // We need to list all hours (00-23)
  for (let hour = 0; hour < 24; hour++) {
    const hourPrefix = `${dayPath}/${String(hour).padStart(2, '0')}/`
    let cursor: string | undefined

    do {
      const listed = await bucket.list({
        prefix: hourPrefix,
        ...(cursor !== undefined ? { cursor } : {}),
        limit: 1000,
      })

      // Filter for .parquet files that are NOT already compacted
      const parquetFiles = listed.objects.filter(
        (obj) => obj.key.endsWith('.parquet') && !obj.key.includes('_compact')
      )
      allFiles.push(...parquetFiles)

      cursor = listed.truncated ? listed.cursor : undefined
    } while (cursor)
  }

  // Also check for files directly in the day folder (from previous compactions or other sources)
  let cursor: string | undefined
  do {
    const listed = await bucket.list({
      prefix: `${dayPath}/`,
      ...(cursor !== undefined ? { cursor } : {}),
      limit: 1000,
      delimiter: '/',
    })

    // Get files directly in the day folder (not in subdirectories)
    const directFiles = listed.objects.filter(
      (obj) =>
        obj.key.endsWith('.parquet') &&
        !obj.key.includes('_compact') &&
        // Ensure it's directly in dayPath, not in an hour subfolder
        obj.key.split('/').length === dayPath.split('/').length + 1
    )
    allFiles.push(...directFiles)

    cursor = listed.truncated ? listed.cursor : undefined
  } while (cursor)

  if (allFiles.length === 0) {
    return {
      dayPath,
      success: true,
      sourceFiles: 0,
      sourceFilePaths: [],
      outputFiles: 0,
      outputFilePaths: [],
      totalRecords: 0,
      totalBytes: 0,
    }
  }

  if (allFiles.length === 1) {
    const singleFile = allFiles[0]
    return {
      dayPath,
      success: true,
      sourceFiles: 1,
      sourceFilePaths: singleFile ? [singleFile.key] : [],
      outputFiles: 0, // No new file needed
      outputFilePaths: [],
      totalRecords: 0,
      totalBytes: 0,
    }
  }

  // Sort files by key for deterministic ordering
  allFiles.sort((a, b) => a.key.localeCompare(b.key))

  if (options.dryRun) {
    return {
      dayPath,
      success: true,
      sourceFiles: allFiles.length,
      sourceFilePaths: allFiles.map((f) => f.key),
      outputFiles: 0,
      outputFilePaths: [],
      totalRecords: 0,
      totalBytes: 0,
      dryRun: true,
    }
  }

  // Read all files
  const buffers: ArrayBuffer[] = []
  for (const file of allFiles) {
    try {
      const obj = await bucket.get(file.key)
      if (obj) {
        buffers.push(await obj.arrayBuffer())
        sourceFilePaths.push(file.key)
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err)
      errors.push(`Failed to read ${file.key}: ${errorMsg}`)
    }
  }

  if (buffers.length === 0) {
    return {
      dayPath,
      success: false,
      sourceFiles: allFiles.length,
      sourceFilePaths,
      outputFiles: 0,
      outputFilePaths: [],
      totalRecords: 0,
      totalBytes: 0,
      errors: ['No readable files found'],
    }
  }

  // Merge all records (sorted by timestamp)
  const mergedRecords = await mergeParquetRecords(buffers)

  if (mergedRecords.length === 0) {
    return {
      dayPath,
      success: false,
      sourceFiles: sourceFilePaths.length,
      sourceFilePaths,
      outputFiles: 0,
      outputFilePaths: [],
      totalRecords: 0,
      totalBytes: 0,
      errors: ['No records after merge'],
    }
  }

  // Split into chunks based on maxBytes/maxEvents thresholds
  const chunks = splitRecords(mergedRecords, options.maxBytes, options.maxEvents)

  // Write each chunk
  for (const chunk of chunks) {
    const compactedBuffer = writeCompactedParquet(chunk)
    const outputKey = `${dayPath}/_compact_${ulid()}.parquet`

    await bucket.put(outputKey, compactedBuffer, {
      customMetadata: {
        sourceFiles: String(sourceFilePaths.length),
        recordCount: String(chunk.length),
        compactedAt: new Date().toISOString(),
        compactionLevel: 'day',
      },
    })

    outputFilePaths.push(outputKey)
    totalRecords += chunk.length
    totalBytes += compactedBuffer.byteLength
  }

  // Delete source files after successful write
  for (const filePath of sourceFilePaths) {
    try {
      await bucket.delete(filePath)
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err)
      errors.push(`Failed to delete ${filePath}: ${errorMsg}`)
    }
  }

  return {
    dayPath,
    success: true,
    sourceFiles: sourceFilePaths.length,
    sourceFilePaths,
    outputFiles: outputFilePaths.length,
    outputFilePaths,
    totalRecords,
    totalBytes,
    errors: errors.length > 0 ? errors : undefined,
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Format a date as YYYY/MM/DD path
 */
function formatDatePath(date: Date): string {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0'),
  ].join('/')
}

/**
 * Split records into chunks based on estimated size limits
 *
 * Since we can't know exact Parquet size until we write, we estimate based on:
 * - Average 500 bytes per record (conservative estimate for events with payloads)
 * - This will be refined after the first chunk is written
 */
function splitRecords(
  records: Record<string, unknown>[],
  maxBytes: number,
  maxEvents: number
): Record<string, unknown>[][] {
  if (records.length === 0) {
    return []
  }

  // If total records fits in one chunk, return as-is
  // Estimate ~500 bytes per record for events with typical payloads
  const estimatedBytesPerRecord = 500
  const estimatedTotalBytes = records.length * estimatedBytesPerRecord

  if (records.length <= maxEvents && estimatedTotalBytes <= maxBytes) {
    return [records]
  }

  // Calculate optimal chunk size
  const recordsPerChunkByBytes = Math.floor(maxBytes / estimatedBytesPerRecord)
  const recordsPerChunk = Math.min(maxEvents, recordsPerChunkByBytes)

  const chunks: Record<string, unknown>[][] = []
  for (let i = 0; i < records.length; i += recordsPerChunk) {
    chunks.push(records.slice(i, i + recordsPerChunk))
  }

  return chunks
}
