/**
 * Lakehouse Query Helpers for DuckDB
 */

/** Maximum allowed string length for query parameters to prevent DoS */
const MAX_STRING_LENGTH = 1000

/** Valid columns that can be used in orderBy clause */
export type OrderByColumn = 'ts' | 'type' | 'id' | 'do_id' | 'collection' | 'colo'

/** Valid sort directions */
export type OrderDirection = 'ASC' | 'DESC'

/** Valid orderBy values - column name optionally followed by direction */
export type OrderBy = OrderByColumn | `${OrderByColumn} ${OrderDirection}`

/** Set of valid orderBy column names for runtime validation */
const VALID_ORDER_COLUMNS: Set<string> = new Set(['ts', 'type', 'id', 'do_id', 'collection', 'colo'])

/** Set of valid orderBy directions for runtime validation */
const VALID_ORDER_DIRECTIONS: Set<string> = new Set(['ASC', 'DESC'])

/**
 * Validate an orderBy value at runtime
 * @throws Error if the orderBy value is invalid
 */
function validateOrderBy(orderBy: string): void {
  const parts = orderBy.split(' ')
  if (parts.length < 1 || parts.length > 2) {
    throw new Error(`Invalid orderBy format: "${orderBy}". Expected "column" or "column direction"`)
  }

  const column = parts[0] as string
  if (!VALID_ORDER_COLUMNS.has(column)) {
    throw new Error(`Invalid orderBy column: "${column}". Valid columns: ${Array.from(VALID_ORDER_COLUMNS).join(', ')}`)
  }

  if (parts.length === 2) {
    const direction = parts[1] as string
    if (!VALID_ORDER_DIRECTIONS.has(direction)) {
      throw new Error(`Invalid orderBy direction: "${direction}". Valid directions: ASC, DESC`)
    }
  }
}

/**
 * Escape single quotes in SQL strings to prevent SQL injection
 */
function escapeSql(value: string): string {
  if (value.length > MAX_STRING_LENGTH) {
    throw new Error(`Input exceeds maximum length of ${MAX_STRING_LENGTH} characters`)
  }
  return value.replace(/'/g, "''")
}

/**
 * Validate and escape a string value for SQL
 */
function validateAndEscape(value: string, fieldName: string): string {
  if (typeof value !== 'string') {
    throw new Error(`${fieldName} must be a string`)
  }
  return escapeSql(value)
}

export interface QueryOptions {
  /** R2 bucket name */
  bucket: string
  /** Date range filter */
  dateRange?: { start: Date; end: Date }
  /** Filter by DO ID */
  doId?: string
  /** Filter by DO class */
  doClass?: string
  /** Filter by colo */
  colo?: string
  /** Filter by event types */
  eventTypes?: string[]
  /** Filter by collection name (for CDC events) */
  collection?: string
  /** Limit results */
  limit?: number
  /** Order by (default: ts DESC) */
  orderBy?: OrderBy
}

/**
 * Build a DuckDB query for events stored in R2
 *
 * @example
 * ```typescript
 * const sql = buildQuery({
 *   bucket: 'my-events',
 *   dateRange: { start: new Date('2024-01-01'), end: new Date('2024-01-31') },
 *   eventTypes: ['rpc.call'],
 *   doClass: 'ChatRoom',
 * })
 * // SELECT * FROM read_json_auto('r2://my-events/events/2024/01/**\/*.jsonl')
 * // WHERE type IN ('rpc.call') AND "do".class = 'ChatRoom'
 * ```
 */
export function buildQuery(options: QueryOptions): string {
  const { bucket, dateRange, doId, doClass, colo, eventTypes, collection, limit, orderBy } = options

  // Build path pattern based on date range
  let pathPattern = `r2://${bucket}/events/`
  if (dateRange) {
    const start = dateRange.start
    const end = dateRange.end

    if (start.getFullYear() === end.getFullYear()) {
      pathPattern += `${start.getFullYear()}/`
      if (start.getMonth() === end.getMonth()) {
        pathPattern += `${String(start.getMonth() + 1).padStart(2, '0')}/`
        if (start.getDate() === end.getDate()) {
          pathPattern += `${String(start.getDate()).padStart(2, '0')}/`
        } else {
          pathPattern += '**/'
        }
      } else {
        pathPattern += '**/'
      }
    } else {
      pathPattern += '**/'
    }
  } else {
    pathPattern += '**/'
  }
  pathPattern += '*.jsonl'

  // Build WHERE clauses
  const conditions: string[] = []

  if (doId) {
    conditions.push(`"do".id = '${validateAndEscape(doId, 'doId')}'`)
  }

  if (doClass) {
    conditions.push(`"do".class = '${validateAndEscape(doClass, 'doClass')}'`)
  }

  if (colo) {
    conditions.push(`"do".colo = '${validateAndEscape(colo, 'colo')}'`)
  }

  if (eventTypes && eventTypes.length > 0) {
    conditions.push(`type IN (${eventTypes.map(t => `'${validateAndEscape(t, 'eventType')}'`).join(', ')})`)
  }

  if (collection) {
    conditions.push(`collection = '${validateAndEscape(collection, 'collection')}'`)
  }

  if (dateRange) {
    conditions.push(`ts >= '${dateRange.start.toISOString()}'`)
    conditions.push(`ts <= '${dateRange.end.toISOString()}'`)
  }

  // Build query
  let query = `SELECT * FROM read_json_auto('${pathPattern}')`

  if (conditions.length > 0) {
    query += `\nWHERE ${conditions.join('\n  AND ')}`
  }

  // Validate orderBy at runtime to prevent SQL injection
  const orderByValue = orderBy ?? 'ts DESC'
  validateOrderBy(orderByValue)
  query += `\nORDER BY ${orderByValue}`

  if (limit) {
    query += `\nLIMIT ${Math.floor(limit)}`
  }

  return query
}

/**
 * Build a query to reconstruct document history (time travel)
 */
export function buildHistoryQuery(options: {
  bucket: string
  collection: string
  docId: string
  dateRange?: { start: Date; end: Date }
}): string {
  const base = buildQuery({
    bucket: options.bucket,
    dateRange: options.dateRange,
    eventTypes: ['collection.created', 'collection.updated', 'collection.deleted'],
    collection: options.collection,
    orderBy: 'ts ASC',
  })

  return base.replace(
    'WHERE',
    `WHERE docId = '${validateAndEscape(options.docId, 'docId')}'\n  AND`
  )
}

/**
 * Build a query for RPC latency analytics
 */
export function buildLatencyQuery(options: {
  bucket: string
  dateRange?: { start: Date; end: Date }
  doClass?: string
  method?: string
}): string {
  const conditions: string[] = [`type = 'rpc.call'`]

  if (options.doClass) {
    conditions.push(`"do".class = '${validateAndEscape(options.doClass, 'doClass')}'`)
  }

  if (options.method) {
    conditions.push(`method = '${validateAndEscape(options.method, 'method')}'`)
  }

  let pathPattern = `r2://${options.bucket}/events/`
  if (options.dateRange) {
    pathPattern += `${options.dateRange.start.getFullYear()}/`
  }
  pathPattern += '**/*.jsonl'

  return `SELECT
  "do".class,
  method,
  COUNT(*) as call_count,
  AVG(durationMs) as avg_duration_ms,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY durationMs) as p50_ms,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY durationMs) as p95_ms,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY durationMs) as p99_ms,
  MAX(durationMs) as max_duration_ms,
  SUM(CASE WHEN success THEN 0 ELSE 1 END) as error_count
FROM read_json_auto('${pathPattern}')
WHERE ${conditions.join('\n  AND ')}
GROUP BY "do".class, method
ORDER BY call_count DESC`
}

/**
 * Build a query to find events between two PITR bookmarks
 */
export function buildPITRRangeQuery(options: {
  bucket: string
  startBookmark: string
  endBookmark: string
  collection?: string
}): string {
  const conditions = [
    `bookmark > '${validateAndEscape(options.startBookmark, 'startBookmark')}'`,
    `bookmark <= '${validateAndEscape(options.endBookmark, 'endBookmark')}'`,
  ]

  if (options.collection) {
    conditions.push(`collection = '${validateAndEscape(options.collection, 'collection')}'`)
  }

  return `SELECT *
FROM read_json_auto('r2://${options.bucket}/events/**/*.jsonl')
WHERE ${conditions.join('\n  AND ')}
ORDER BY ts ASC`
}
