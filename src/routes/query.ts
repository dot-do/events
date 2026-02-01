/**
 * Query builder route handler - POST /query
 */

import type { Env } from '../env'
import { authCorsHeaders } from '../utils'

// ============================================================================
// Query Builder
// ============================================================================

interface QueryRequest {
  dateRange?: { start: string; end: string }
  doId?: string
  doClass?: string
  eventTypes?: string[]
  collection?: string
  colo?: string
  limit?: number
}

const MAX_QUERY_LIMIT = 10000
const MAX_EVENT_TYPES = 100
const MAX_STRING_PARAM_LENGTH = 256

/**
 * Escape single quotes in SQL strings to prevent SQL injection
 */
function escapeSql(value: string): string {
  return value.replace(/'/g, "''")
}

function validateQueryRequest(query: unknown): query is QueryRequest {
  if (typeof query !== 'object' || query === null) return false
  const q = query as Record<string, unknown>

  // Validate dateRange if present
  if (q.dateRange !== undefined) {
    if (typeof q.dateRange !== 'object' || q.dateRange === null) return false
    const dr = q.dateRange as Record<string, unknown>
    if (typeof dr.start !== 'string' || isNaN(Date.parse(dr.start))) return false
    if (typeof dr.end !== 'string' || isNaN(Date.parse(dr.end))) return false
    // Ensure start <= end
    if (Date.parse(dr.start) > Date.parse(dr.end)) return false
  }

  // Validate string parameters
  if (q.doId !== undefined && (typeof q.doId !== 'string' || q.doId.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.doClass !== undefined && (typeof q.doClass !== 'string' || q.doClass.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.collection !== undefined && (typeof q.collection !== 'string' || q.collection.length > MAX_STRING_PARAM_LENGTH)) return false
  if (q.colo !== undefined && (typeof q.colo !== 'string' || q.colo.length > MAX_STRING_PARAM_LENGTH)) return false

  // Validate eventTypes if present
  if (q.eventTypes !== undefined) {
    if (!Array.isArray(q.eventTypes)) return false
    if (q.eventTypes.length > MAX_EVENT_TYPES) return false
    for (const t of q.eventTypes) {
      if (typeof t !== 'string' || t.length > MAX_STRING_PARAM_LENGTH) return false
    }
  }

  // Validate limit if present
  if (q.limit !== undefined) {
    if (typeof q.limit !== 'number' || q.limit < 1 || q.limit > MAX_QUERY_LIMIT || !Number.isInteger(q.limit)) return false
  }

  return true
}

export async function handleQuery(request: Request, env: Env): Promise<Response> {
  let queryBody: unknown
  try {
    queryBody = await request.json()
  } catch {
    return Response.json({ error: 'Invalid JSON' }, { status: 400, headers: authCorsHeaders(request, env) })
  }

  if (!validateQueryRequest(queryBody)) {
    return Response.json(
      { error: 'Invalid query parameters. Check dateRange (valid timestamps, start <= end), limit (1-10000), and string lengths (max 256 chars)' },
      { status: 400, headers: authCorsHeaders(request, env) }
    )
  }

  const query = queryBody as QueryRequest

  const conditions: string[] = []
  let pathPattern = 'events/'

  // Optimize path based on date range
  // Path structure: events/YYYY/MM/DD/HH/uuid.parquet (4 directory levels after events/)
  // Use UTC methods since events are stored in UTC time buckets
  if (query.dateRange) {
    const start = new Date(query.dateRange.start)
    const end = new Date(query.dateRange.end)

    if (start.getUTCFullYear() === end.getUTCFullYear()) {
      // Same year - add year, need wildcards for month/day/hour (3 levels)
      pathPattern += `${start.getUTCFullYear()}/`
      if (start.getUTCMonth() === end.getUTCMonth()) {
        // Same month - add month, need wildcards for day/hour (2 levels)
        pathPattern += `${String(start.getUTCMonth() + 1).padStart(2, '0')}/`
        if (start.getUTCDate() === end.getUTCDate()) {
          // Same day - add day, need wildcard for hour (1 level)
          pathPattern += `${String(start.getUTCDate()).padStart(2, '0')}/*/`
        } else {
          // Different days - need wildcards for day/hour (2 levels)
          pathPattern += '*/*/';
        }
      } else {
        // Different months - need wildcards for month/day/hour (3 levels)
        pathPattern += '*/*/*/'
      }
    } else {
      // Different years - need wildcards for all 4 levels
      pathPattern += '*/*/*/*/'
    }
  } else {
    // No date range - use recursive glob
    pathPattern += '**/'
  }
  pathPattern += '*.parquet'

  // Build conditions (all string values are escaped to prevent SQL injection)
  if (query.doId) {
    conditions.push(`"do".id = '${escapeSql(query.doId)}'`)
  }
  if (query.doClass) {
    conditions.push(`"do".class = '${escapeSql(query.doClass)}'`)
  }
  if (query.colo) {
    conditions.push(`"do".colo = '${escapeSql(query.colo)}'`)
  }
  if (query.eventTypes?.length) {
    conditions.push(`type IN (${query.eventTypes.map(t => `'${escapeSql(t)}'`).join(', ')})`)
  }
  if (query.collection) {
    conditions.push(`collection = '${escapeSql(query.collection)}'`)
  }
  if (query.dateRange) {
    conditions.push(`ts >= '${escapeSql(query.dateRange.start)}'`)
    conditions.push(`ts <= '${escapeSql(query.dateRange.end)}'`)
  }

  // Primary query uses Parquet; fall back to JSONL for historical data
  const jsonlPattern = pathPattern.replace(/\.parquet$/, '.jsonl')
  let sql = `SELECT *
FROM read_parquet('${pathPattern}',
  filename=true,
  hive_partitioning=false
)
UNION ALL
SELECT *
FROM read_json_auto('${jsonlPattern}',
  filename=true,
  hive_partitioning=false
)`

  if (conditions.length > 0) {
    sql += `\nWHERE ${conditions.join('\n  AND ')}`
  }

  sql += `\nORDER BY ts DESC`

  if (query.limit) {
    sql += `\nLIMIT ${query.limit}`
  }

  return Response.json({ sql }, { headers: authCorsHeaders(request, env) })
}
