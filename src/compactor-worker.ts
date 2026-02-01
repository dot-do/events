/**
 * events-compactor - Scheduled Compaction Worker
 *
 * Runs on a schedule to merge small Parquet files into larger compacted files.
 * This improves query performance by reducing the number of files to scan.
 *
 * Schedule: Every hour at :05 (5 * * * *)
 *
 * Endpoints:
 * - POST /compact?hour=YYYY/MM/DD/HH - Manual compaction trigger
 * - GET /status - Service status and endpoint documentation
 */

import {
  readParquetRecords,
  mergeParquetRecords,
  writeCompactedParquet,
  listFilesForCompaction,
} from '@dotdo/events'

// ============================================================================
// Types
// ============================================================================

import type { Env as FullEnv } from './env'

type Env = Pick<FullEnv, 'EVENTS_BUCKET' | 'ENVIRONMENT'>

interface CompactionResult {
  compacted: boolean
  reason?: string
  fileCount?: number
  sourceFiles?: number
  recordCount?: number
  outputKey?: string
  outputBytes?: number
}

interface DayCompactionResult {
  compacted: boolean
  hours: { hour: string; result: CompactionResult }[]
  finalCompact?: CompactionResult
}

// ============================================================================
// ULID - Time-ordered unique identifier (lexicographically sortable)
// ============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ' // Crockford Base32

function ulid(): string {
  const now = Date.now()
  let str = ''
  // Timestamp (48 bits = 10 chars)
  let ts = now
  for (let i = 9; i >= 0; i--) {
    str = ENCODING[ts % 32] + str
    ts = Math.floor(ts / 32)
  }
  // Randomness (80 bits = 16 chars)
  for (let i = 0; i < 16; i++) {
    str += ENCODING[Math.floor(Math.random() * 32)]
  }
  return str
}

// ============================================================================
// Worker Export
// ============================================================================

export default {
  /**
   * Scheduled handler - runs on cron (every hour at :05)
   */
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const now = new Date()
    console.log(`[COMPACT] Scheduled run at ${now.toISOString()}`)

    // Compact the PREVIOUS hour (not current - files may still be writing)
    const previousHour = new Date(now.getTime() - 60 * 60 * 1000)
    const hourResult = await compactHour(env, previousHour)
    console.log(`[COMPACT] Hour result:`, JSON.stringify(hourResult))

    // At midnight UTC (hour 0), also compact previous day into a single file
    if (now.getUTCHours() === 0) {
      const previousDay = new Date(now.getTime() - 24 * 60 * 60 * 1000)
      const dayResult = await compactDay(env, previousDay)
      console.log(`[COMPACT] Day result:`, JSON.stringify(dayResult))
    }
  },

  /**
   * HTTP handler for manual triggers and status
   */
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      })
    }

    // Manual compaction trigger
    if (url.pathname === '/compact' && request.method === 'POST') {
      const hour = url.searchParams.get('hour') // e.g., "2024/01/31/15"
      const day = url.searchParams.get('day') // e.g., "2024/01/31"

      if (day) {
        // Compact entire day
        const [year, month, dayNum] = day.split('/').map(Number)
        const date = new Date(Date.UTC(year!, (month ?? 1) - 1, dayNum))
        const result = await compactDay(env, date)
        return Response.json(result, { headers: corsHeaders() })
      }

      if (hour) {
        const result = await compactHourPath(env, hour)
        return Response.json(result, { headers: corsHeaders() })
      }

      return Response.json(
        { error: 'hour or day param required (hour=YYYY/MM/DD/HH or day=YYYY/MM/DD)' },
        { status: 400, headers: corsHeaders() }
      )
    }

    // Status endpoint
    if (url.pathname === '/status') {
      return Response.json(
        {
          service: 'events-compactor',
          environment: env.ENVIRONMENT,
          endpoints: {
            'POST /compact?hour=YYYY/MM/DD/HH': 'Compact specific hour',
            'POST /compact?day=YYYY/MM/DD': 'Compact entire day',
            'GET /status': 'This status page',
          },
          schedule: 'Every hour at :05 (5 * * * *)',
          notes: {
            hourly: 'Compacts previous hour (not current) to avoid race conditions',
            daily: 'At midnight UTC, also compacts all hourly files into daily compact',
          },
        },
        { headers: corsHeaders() }
      )
    }

    // Root endpoint
    if (url.pathname === '/') {
      return Response.json(
        {
          service: 'events-compactor',
          version: '1.0.0',
        },
        { headers: corsHeaders() }
      )
    }

    return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders() })
  },
}

// ============================================================================
// CORS Headers
// ============================================================================

function corsHeaders(): HeadersInit {
  return {
    'Access-Control-Allow-Origin': '*',
    'Content-Type': 'application/json',
  }
}

// ============================================================================
// Compaction Functions
// ============================================================================

/**
 * Compact files for a specific hour
 */
async function compactHour(env: Env, date: Date): Promise<CompactionResult> {
  const hourPath = [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0'),
    String(date.getUTCHours()).padStart(2, '0'),
  ].join('/')

  return compactHourPath(env, hourPath)
}

/**
 * Compact files for a specific hour path (e.g., "2024/01/31/15")
 */
async function compactHourPath(env: Env, hourPath: string): Promise<CompactionResult> {
  const prefix = `events/${hourPath}/`

  // List files to compact (exclude _compact files)
  const files = await listFilesForCompaction(env.EVENTS_BUCKET, prefix)

  if (files.length === 0) {
    return { compacted: false, reason: 'no files found', fileCount: 0 }
  }

  if (files.length === 1) {
    return { compacted: false, reason: 'only one file, skipping', fileCount: 1 }
  }

  console.log(`[COMPACT] Starting compaction of ${files.length} files in ${prefix}`)

  // Read all Parquet files
  const buffers: ArrayBuffer[] = []
  for (const file of files) {
    const obj = await env.EVENTS_BUCKET.get(file.key)
    if (obj) {
      buffers.push(await obj.arrayBuffer())
    }
  }

  if (buffers.length === 0) {
    return { compacted: false, reason: 'no readable files', fileCount: files.length }
  }

  // Merge records from all files (sorted by timestamp)
  const mergedRecords = await mergeParquetRecords(buffers)

  if (mergedRecords.length === 0) {
    return { compacted: false, reason: 'no records after merge', fileCount: files.length }
  }

  // Write compacted Parquet file
  const compactedBuffer = writeCompactedParquet(mergedRecords)

  // Use ULID for uniqueness in case of re-compaction
  const compactKey = `${prefix}_compact_${ulid()}.parquet`

  await env.EVENTS_BUCKET.put(compactKey, compactedBuffer, {
    customMetadata: {
      sourceFiles: String(files.length),
      recordCount: String(mergedRecords.length),
      compactedAt: new Date().toISOString(),
      compactionLevel: 'hour',
    },
  })

  // Delete original files after successful compaction
  for (const file of files) {
    await env.EVENTS_BUCKET.delete(file.key)
  }

  console.log(`[COMPACT] Completed: ${files.length} files -> ${compactKey} (${mergedRecords.length} records)`)

  return {
    compacted: true,
    sourceFiles: files.length,
    recordCount: mergedRecords.length,
    outputKey: compactKey,
    outputBytes: compactedBuffer.byteLength,
  }
}

/**
 * Compact all hourly _compact files for a day into a single daily compact file
 */
async function compactDay(env: Env, date: Date): Promise<DayCompactionResult> {
  const dayPath = [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0'),
  ].join('/')

  const results: { hour: string; result: CompactionResult }[] = []

  // First, ensure all hours are compacted
  for (let hour = 0; hour < 24; hour++) {
    const hourPath = `${dayPath}/${String(hour).padStart(2, '0')}`
    const result = await compactHourPath(env, hourPath)
    results.push({ hour: hourPath, result })
  }

  // Now compact all hourly _compact files into a single daily file
  const dayPrefix = `events/${dayPath}/`

  // List all _compact files from all hours
  const allFiles: R2Object[] = []
  for (let hour = 0; hour < 24; hour++) {
    const hourPrefix = `${dayPrefix}${String(hour).padStart(2, '0')}/`
    const listed = await env.EVENTS_BUCKET.list({ prefix: hourPrefix })

    // Only get _compact files
    const compactFiles = listed.objects.filter((obj) => obj.key.includes('_compact'))
    allFiles.push(...compactFiles)
  }

  if (allFiles.length <= 1) {
    return {
      compacted: false,
      hours: results,
    }
  }

  console.log(`[COMPACT] Compacting ${allFiles.length} hourly files into daily compact for ${dayPath}`)

  // Read all hourly compact files
  const buffers: ArrayBuffer[] = []
  for (const file of allFiles) {
    const obj = await env.EVENTS_BUCKET.get(file.key)
    if (obj) {
      buffers.push(await obj.arrayBuffer())
    }
  }

  if (buffers.length === 0) {
    return {
      compacted: false,
      hours: results,
    }
  }

  // Merge all records
  const mergedRecords = await mergeParquetRecords(buffers)

  if (mergedRecords.length === 0) {
    return {
      compacted: false,
      hours: results,
    }
  }

  // Write daily compact file
  const compactedBuffer = writeCompactedParquet(mergedRecords)
  const dailyCompactKey = `${dayPrefix}_compact_${ulid()}.parquet`

  await env.EVENTS_BUCKET.put(dailyCompactKey, compactedBuffer, {
    customMetadata: {
      sourceFiles: String(allFiles.length),
      recordCount: String(mergedRecords.length),
      compactedAt: new Date().toISOString(),
      compactionLevel: 'day',
      date: dayPath,
    },
  })

  // Delete hourly compact files
  for (const file of allFiles) {
    await env.EVENTS_BUCKET.delete(file.key)
  }

  console.log(`[COMPACT] Daily compaction complete: ${allFiles.length} files -> ${dailyCompactKey} (${mergedRecords.length} records)`)

  return {
    compacted: true,
    hours: results,
    finalCompact: {
      compacted: true,
      sourceFiles: allFiles.length,
      recordCount: mergedRecords.length,
      outputKey: dailyCompactKey,
      outputBytes: compactedBuffer.byteLength,
    },
  }
}
