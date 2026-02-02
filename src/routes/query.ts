/**
 * Query builder route handler - POST /query
 *
 * Supports multi-tenant isolation via namespace parameter.
 * When a namespace is provided, queries are automatically scoped to that namespace's data.
 */

import type { Env } from '../env'
import { authCorsHeaders } from '../utils'
import type { TenantContext } from '../middleware/tenant'
import { PayloadTooLargeError } from '../../core/src/errors'
import { MAX_QUERY_BODY_SIZE } from '../middleware/ingest/types'
import { readBodyWithLimit } from '../middleware/ingest/validate'

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
  /** Namespace to query (for multi-tenant isolation) */
  namespace?: string
}

const MAX_QUERY_LIMIT = 10000
const MAX_EVENT_TYPES = 100
const MAX_STRING_PARAM_LENGTH = 256

/**
 * Regex whitelist for path patterns - only allows safe characters:
 * alphanumeric, slashes, asterisks, dashes, underscores, and dots.
 * Also rejects SQL comment syntax (--) to prevent injection.
 */
const SAFE_PATH_PATTERN = /^[a-zA-Z0-9\/_*.-]+$/

/**
 * Validate that a path pattern contains only safe characters.
 * Prevents SQL injection via path pattern interpolation.
 */
function validatePathPattern(pattern: string): boolean {
  // First check character whitelist
  if (!SAFE_PATH_PATTERN.test(pattern)) {
    return false
  }
  // Reject SQL comment syntax (double dashes)
  if (pattern.includes('--')) {
    return false
  }
  return true
}

/**
 * Escape single quotes in SQL strings to prevent SQL injection
 */
function escapeSql(value: string): string {
  return value.replace(/'/g, "''")
}

function validateQueryRequest(query: unknown): query is QueryRequest {
  if (typeof query !== 'object' || query === null || Array.isArray(query)) return false
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
  if (q.namespace !== undefined && (typeof q.namespace !== 'string' || q.namespace.length > MAX_STRING_PARAM_LENGTH)) return false

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

/**
 * Handle query requests with optional tenant context for namespace isolation
 *
 * @param request - The HTTP request
 * @param env - Environment bindings
 * @param tenant - Optional tenant context (if provided, queries are scoped to this tenant's namespace)
 */
export async function handleQuery(request: Request, env: Env, tenant?: TenantContext): Promise<Response> {
  // Get max body size from env or use default (64KB for query requests)
  const maxSize = env.MAX_QUERY_BODY_SIZE
    ? parseInt(env.MAX_QUERY_BODY_SIZE, 10) || MAX_QUERY_BODY_SIZE
    : MAX_QUERY_BODY_SIZE

  // Check Content-Length header first (fast rejection)
  const contentLength = request.headers.get('content-length')
  if (contentLength) {
    const length = parseInt(contentLength, 10)
    if (!isNaN(length) && length > maxSize) {
      return Response.json(
        {
          error: `Request body too large: ${length} bytes exceeds ${maxSize} byte limit`,
          code: 'PAYLOAD_TOO_LARGE',
          details: { maxSize, contentLength: length }
        },
        { status: 413, headers: authCorsHeaders(request, env) }
      )
    }
  }

  // Read body with size limit and parse JSON
  let queryBody: unknown
  try {
    const bodyText = await readBodyWithLimit(request, maxSize)
    queryBody = JSON.parse(bodyText)
  } catch (error) {
    if (error instanceof PayloadTooLargeError) {
      return Response.json(
        {
          error: error.message,
          code: 'PAYLOAD_TOO_LARGE',
          details: error.details
        },
        { status: 413, headers: authCorsHeaders(request, env) }
      )
    }
    return Response.json({ error: 'Invalid JSON' }, { status: 400, headers: authCorsHeaders(request, env) })
  }

  if (!validateQueryRequest(queryBody)) {
    return Response.json(
      { error: 'Invalid query parameters. Check dateRange (valid timestamps, start <= end), limit (1-10000), and string lengths (max 256 chars)' },
      { status: 400, headers: authCorsHeaders(request, env) }
    )
  }

  const query = queryBody as QueryRequest

  // Determine namespace for query isolation
  // Priority: 1) Request body namespace, 2) Tenant context namespace, 3) No isolation (admin only)
  let namespace: string | undefined
  if (query.namespace) {
    // Explicit namespace in request - validate access
    if (tenant && !tenant.isAdmin && tenant.namespace !== query.namespace) {
      return Response.json(
        { error: 'Access denied: cannot query namespace you do not have access to' },
        { status: 403, headers: authCorsHeaders(request, env) }
      )
    }
    namespace = query.namespace
  } else if (tenant && !tenant.isAdmin) {
    // Non-admin tenant must be scoped to their namespace
    namespace = tenant.namespace
  }
  // Admin without explicit namespace can query all data

  const conditions: string[] = []

  // Build namespace-isolated path pattern
  // Events are stored under: ns/<namespace>/events/... for tenant isolation
  let pathPattern = namespace ? `ns/${escapeSql(namespace)}/events/` : 'events/'

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

  // Add namespace isolation condition to payload._namespace field
  // This is a defense-in-depth measure - path isolation is the primary mechanism
  if (namespace) {
    conditions.push(`payload._namespace = '${escapeSql(namespace)}'`)
  }

  // Validate pathPattern to prevent SQL injection
  // Even though pathPattern is built from Date methods, validate as defense-in-depth
  if (!validatePathPattern(pathPattern)) {
    return Response.json(
      { error: 'Invalid path pattern generated' },
      { status: 500, headers: authCorsHeaders(request, env) }
    )
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

  return Response.json({
    sql,
    namespace: namespace || null,
    isolated: !!namespace,
  }, { headers: authCorsHeaders(request, env) })
}
