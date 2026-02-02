/**
 * Events query and recent events route handlers
 */

import type { DurableEvent } from '@dotdo/events'
import type { Env } from '../env'
import { readParquetRecords } from '../../core/src/compaction'
import { matchPattern } from '../../core/src/pattern-matcher'
import { authCorsHeaders } from '../utils'
import { logger, logError } from '../logger'

const log = logger.child({ component: 'EventsRoutes' })

// ============================================================================
// Recent Events (Debug)
// ============================================================================

export async function handleRecent(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url)
  const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '100'), 1000)

  // List recent files from current hour
  const now = new Date()
  const hourPath = [
    now.getUTCFullYear(),
    String(now.getUTCMonth() + 1).padStart(2, '0'),
    String(now.getUTCDate()).padStart(2, '0'),
    String(now.getUTCHours()).padStart(2, '0'),
  ].join('/')

  const prefix = `events/${hourPath}/`
  const listed = await env.EVENTS_BUCKET.list({
    prefix,
    limit: 100,
  })

  console.log(`[recent] Listing prefix: ${prefix}, found ${listed.objects.length} objects`)

  const jsonlFiles = listed.objects.filter(o => o.key.endsWith('.jsonl'))
  const parquetFiles = listed.objects.filter(o => o.key.endsWith('.parquet'))

  // Read both JSONL and Parquet files (sorted by ULID = time order, newest first)
  const allFiles = listed.objects
    .filter(o => o.key.endsWith('.jsonl') || o.key.endsWith('.parquet'))
    .sort((a, b) => b.key.localeCompare(a.key))

  const events: DurableEvent[] = []

  for (const obj of allFiles.slice(0, 20)) {
    const data = await env.EVENTS_BUCKET.get(obj.key)
    if (!data) continue

    if (obj.key.endsWith('.jsonl')) {
      const text = await data.text()
      const lines = text.split('\n').filter(Boolean)
      for (const line of lines) {
        try {
          events.push(JSON.parse(line))
        } catch {
          // Skip malformed
        }
      }
    } else if (obj.key.endsWith('.parquet')) {
      try {
        const buffer = await data.arrayBuffer()
        const records = await readParquetRecords(buffer)
        for (const record of records) {
          // Convert BigInt timestamps to ISO strings
          const event = convertBigInts(record) as DurableEvent
          events.push(event)
        }
      } catch (err) {
        logError(log, 'Error reading parquet file', err, { key: obj.key })
      }
    }

    if (events.length >= limit) break
  }

  return Response.json({
    events: events.slice(0, limit),
    count: events.length,
    bucket: hourPath,
    prefix,
    filesFound: listed.objects.length,
    jsonlFiles: jsonlFiles.length,
    parquetFiles: parquetFiles.length,
    recentFiles: listed.objects.slice(-10).map(o => o.key),
  }, { headers: authCorsHeaders(request, env) })
}

// ============================================================================
// Events Query (read parquet files with hyparquet)
// ============================================================================

interface EventsQueryParams {
  type?: string       // Filter by event type (glob pattern)
  before?: string     // Cursor: get events before this timestamp (for pagination)
  after?: string      // Cursor: get events after this timestamp
  from?: string       // Start timestamp (ISO) - absolute time range
  to?: string         // End timestamp (ISO) - absolute time range
  limit?: number      // Max events to return (default 100, max 1000)
  provider?: string   // Filter by webhook provider
  format?: string     // parquet (default) or jsonl (legacy fallback)
  source?: string     // events (webhooks/ingest), tail (worker traces), or all
}

interface EventsMeta {
  query: {
    source: string
    startTime: string
    endTime: string
    durationMs: number
  }
  scan: {
    bucketsScanned: number
    bucketsTotal: number
    filesListed: number
    filesRead: number
    bytesRead: number
    eventsScanned: number
    eventsReturned: number
  }
  timing: {
    listMs: number
    readMs: number
    filterMs: number
  }
}

interface PaginationLinks {
  self: string
  first: string
  next?: string
  prev?: string
}

export async function handleEventsQuery(request: Request, env: Env): Promise<Response> {
  const queryStart = performance.now()
  const url = new URL(request.url)

  const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '20'), 1000)
  const params: EventsQueryParams = {
    type: url.searchParams.get('type') ?? undefined,
    before: url.searchParams.get('before') ?? undefined,
    after: url.searchParams.get('after') ?? undefined,
    from: url.searchParams.get('from') ?? undefined,
    to: url.searchParams.get('to') ?? undefined,
    limit,
    provider: url.searchParams.get('provider') ?? undefined,
    format: url.searchParams.get('format') ?? 'parquet',
    source: url.searchParams.get('source') ?? 'all', // events, tail, or all
  }

  // Initialize timing trackers
  let listMs = 0
  let readMs = 0
  let filterMs = 0

  // Initialize scan stats
  let bucketsScanned = 0
  let filesListed = 0
  let filesRead = 0
  let bytesRead = 0
  let eventsScanned = 0

  // Determine time boundaries
  // Priority: cursor (before/after) > absolute range (from/to) > default (last 24h)
  const now = new Date()
  let endTime: Date
  let startTime: Date

  if (params.before) {
    // Paginating backwards from a cursor
    endTime = new Date(params.before)
    startTime = params.from ? new Date(params.from) : new Date(endTime.getTime() - 24 * 60 * 60 * 1000)
  } else if (params.after) {
    // Paginating forwards from a cursor
    startTime = new Date(params.after)
    endTime = params.to ? new Date(params.to) : now
  } else {
    // Default or explicit range
    endTime = params.to ? new Date(params.to) : now
    startTime = params.from ? new Date(params.from) : new Date(endTime.getTime() - 24 * 60 * 60 * 1000)
  }

  // Determine which prefixes to scan based on source
  const sourcePrefixes: string[] = []
  if (params.source === 'all' || params.source === 'events') {
    sourcePrefixes.push('events')
  }
  if (params.source === 'all' || params.source === 'tail') {
    sourcePrefixes.push('tail')
  }

  // Generate hour buckets in reverse chronological order (newest first)
  const buckets: string[] = []
  const current = new Date(endTime)
  current.setUTCMinutes(0, 0, 0) // Round to hour

  while (current >= startTime) {
    const datePath = [
      current.getUTCFullYear(),
      String(current.getUTCMonth() + 1).padStart(2, '0'),
      String(current.getUTCDate()).padStart(2, '0'),
      String(current.getUTCHours()).padStart(2, '0'),
    ].join('/')

    // Add bucket for each source prefix
    for (const prefix of sourcePrefixes) {
      buckets.push(`${prefix}/${datePath}/`)
    }
    current.setUTCHours(current.getUTCHours() - 1)
  }

  const bucketsTotal = buckets.length

  // Collect events (newest first)
  const allEvents: Record<string, unknown>[] = []

  // Process buckets until we have enough events (with some buffer for filtering)
  const targetCount = limit + 1 // +1 to know if there are more

  for (const prefix of buckets) {
    if (allEvents.length >= targetCount) break
    bucketsScanned++

    const listStart = performance.now()
    // Paginate through all R2 list results to avoid silently dropping files
    const allObjects: R2Object[] = []
    let listed = await env.EVENTS_BUCKET.list({ prefix, limit: 200 })
    allObjects.push(...listed.objects)
    while (listed.truncated) {
      listed = await env.EVENTS_BUCKET.list({ prefix, limit: 200, cursor: listed.cursor })
      allObjects.push(...listed.objects)
    }
    listMs += performance.now() - listStart
    filesListed += allObjects.length

    // Read both Parquet and JSONL files (JSONL is legacy fallback for historical data)
    // Sort by ULID in filename for time order (newest first)
    const targetFiles = allObjects
      .filter(o => o.key.endsWith('.parquet') || o.key.endsWith('.jsonl'))
      .sort((a, b) => b.key.localeCompare(a.key)) // ULID sort = time order desc

    // Read files in parallel batches for speed
    const BATCH_SIZE = 10
    const filesToRead = targetFiles.slice(0, Math.min(targetFiles.length, targetCount - allEvents.length + 10))

    for (let i = 0; i < filesToRead.length && allEvents.length < targetCount; i += BATCH_SIZE) {
      const batch = filesToRead.slice(i, i + BATCH_SIZE)

      const readStart = performance.now()
      const results = await Promise.all(
        batch.map(async (file) => {
          try {
            const obj = await env.EVENTS_BUCKET.get(file.key)
            if (!obj) return { file, events: [] as Record<string, unknown>[] }

            filesRead++
            bytesRead += file.size

            if (file.key.endsWith('.parquet')) {
              // Primary path: Parquet files (all new data)
              const buffer = await obj.arrayBuffer()
              const records = await readParquetRecords(buffer)
              const events: Record<string, unknown>[] = []

              for (const record of records) {
                eventsScanned++
                if (passesFilters(record, params)) {
                  events.push(record)
                }
              }
              return { file, events }
            } else {
              // Legacy fallback: JSONL files (historical data from before Parquet migration)
              const text = await obj.text()
              const lines = text.split('\n').filter(Boolean)
              const events: Record<string, unknown>[] = []

              for (const line of lines) {
                eventsScanned++
                try {
                  const record = JSON.parse(line) as Record<string, unknown>
                  if (passesFilters(record, params)) {
                    events.push(record)
                  }
                } catch {
                  // Skip malformed
                }
              }
              return { file, events }
            }
          } catch (err) {
            logError(log, 'Error reading events file', err, { key: file.key })
            return { file, events: [] as Record<string, unknown>[] }
          }
        })
      )
      readMs += performance.now() - readStart

      // Collect events from batch (maintaining file order = time order)
      for (const result of results) {
        allEvents.push(...result.events)
      }
    }
  }

  // Sort all events by timestamp descending
  // Handle both string (ISO) and BigInt (ms) timestamps
  allEvents.sort((a, b) => {
    const tsA = getTimestampMs(a.ts)
    const tsB = getTimestampMs(b.ts)
    return tsB - tsA
  })

  // Check if there are more results
  const hasMore = allEvents.length > limit
  const events = allEvents.slice(0, limit)

  // Get cursors for pagination (convert to ISO string for cursor values)
  const firstEvent = events[0]
  const lastEvent = events[events.length - 1]
  const oldestTs = lastEvent ? getTimestampIso(lastEvent.ts) : undefined
  const newestTs = firstEvent ? getTimestampIso(firstEvent.ts) : undefined

  // Build pagination links
  const baseUrl = `${url.origin}${url.pathname}`
  const buildUrl = (overrides: Record<string, string | undefined>) => {
    const u = new URL(baseUrl)
    // Preserve existing params
    if (params.type) u.searchParams.set('type', params.type)
    if (params.provider) u.searchParams.set('provider', params.provider)
    if (params.format && params.format !== 'parquet') u.searchParams.set('format', params.format)
    if (params.source && params.source !== 'events') u.searchParams.set('source', params.source)
    u.searchParams.set('limit', String(limit))
    // Apply overrides
    for (const [k, v] of Object.entries(overrides)) {
      if (v) u.searchParams.set(k, v)
      else u.searchParams.delete(k)
    }
    return u.toString()
  }

  const links: PaginationLinks = {
    self: url.toString(),
    first: buildUrl({ before: undefined, after: undefined, from: undefined, to: undefined }),
  }

  if (hasMore && oldestTs) {
    links.next = buildUrl({ before: oldestTs, after: undefined })
  }

  if (params.before && newestTs) {
    links.prev = buildUrl({ after: newestTs, before: undefined })
  }

  // Build meta object
  const queryEnd = performance.now()
  const meta: EventsMeta = {
    query: {
      source: params.source || 'events',
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      durationMs: Math.round(queryEnd - queryStart),
    },
    scan: {
      bucketsScanned,
      bucketsTotal,
      filesListed,
      filesRead,
      bytesRead,
      eventsScanned,
      eventsReturned: events.length,
    },
    timing: {
      listMs: Math.round(listMs),
      readMs: Math.round(readMs),
      filterMs: Math.round(filterMs),
    },
  }

  // Convert BigInts to numbers for JSON serialization
  const serializable = events.map(e => convertBigInts(e))

  return Response.json({
    links,
    meta,
    data: serializable,
  }, { headers: authCorsHeaders(request, env) })
}

// ============================================================================
// Utility functions
// ============================================================================

/**
 * Check if an event passes all filter criteria
 */
function passesFilters(record: Record<string, unknown>, params: EventsQueryParams): boolean {
  // Type filter (glob pattern) - uses shared pattern matcher
  if (params.type && typeof record.type === 'string') {
    if (!matchPattern(params.type, record.type)) return false
  }

  // Provider filter
  if (params.provider) {
    const webhookProvider = (record as { webhook?: { provider?: string } }).webhook?.provider
    // Also check top-level provider field (new Parquet schema)
    const topLevelProvider = record.provider as string | undefined
    if (webhookProvider !== params.provider && topLevelProvider !== params.provider) return false
  }

  // Get timestamp in ms for comparison (handles both string ISO and BigInt ms)
  const recordMs = getTimestampMs(record.ts)
  if (recordMs === 0) return true // Can't filter on invalid timestamp

  // Time range filters (for absolute from/to, not cursor-based)
  if (params.from) {
    const fromMs = new Date(params.from).getTime()
    if (recordMs < fromMs) return false
  }
  if (params.to) {
    const toMs = new Date(params.to).getTime()
    if (recordMs > toMs) return false
  }

  // Cursor filters
  if (params.before) {
    const beforeMs = new Date(params.before).getTime()
    if (recordMs >= beforeMs) return false
  }
  if (params.after) {
    const afterMs = new Date(params.after).getTime()
    if (recordMs <= afterMs) return false
  }

  return true
}

/**
 * Convert various timestamp formats to milliseconds
 * Handles: string (ISO), BigInt (ms), number (ms)
 */
function getTimestampMs(ts: unknown): number {
  if (typeof ts === 'bigint') {
    return Number(ts)
  }
  if (typeof ts === 'number') {
    return ts
  }
  if (typeof ts === 'string') {
    return new Date(ts).getTime()
  }
  return 0
}

/**
 * Convert various timestamp formats to ISO string
 * Handles: string (ISO), BigInt (ms), number (ms)
 */
function getTimestampIso(ts: unknown): string | undefined {
  if (typeof ts === 'string') {
    return ts
  }
  if (typeof ts === 'bigint') {
    return new Date(Number(ts)).toISOString()
  }
  if (typeof ts === 'number') {
    return new Date(ts).toISOString()
  }
  return undefined
}

/**
 * Recursively convert BigInts to numbers for JSON serialization
 * Also converts 'ts' timestamps to ISO strings for readability
 */
export function convertBigInts(obj: unknown): unknown {
  if (typeof obj === 'bigint') {
    return Number(obj)
  }
  // Handle Date objects
  if (obj instanceof Date) {
    return obj.toISOString()
  }
  if (Array.isArray(obj)) {
    return obj.map(convertBigInts)
  }
  if (obj !== null && typeof obj === 'object') {
    const result: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(obj)) {
      // Convert timestamp fields to ISO strings (handles BigInt and Date)
      if (key === 'ts') {
        if (typeof value === 'bigint') {
          result[key] = new Date(Number(value)).toISOString()
        } else if (value instanceof Date) {
          result[key] = value.toISOString()
        } else if (typeof value === 'number') {
          result[key] = new Date(value).toISOString()
        } else {
          result[key] = value
        }
      } else {
        result[key] = convertBigInts(value)
      }
    }
    return result
  }
  return obj
}

// Note: Pattern matching now uses the shared matchPattern() from pattern-matcher.ts
// This provides consistent glob-style matching with * (single segment) and ** (multiple segments)
