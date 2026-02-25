/**
 * EventsService — Unified read + write RPC entrypoint for events.do.
 *
 * Write: env.EVENTS.ingest(events)
 *   Opens a WebSocket to a ClickHouseBufferDO shard (round-robin).
 *   Copied from the original BufferService — same shard distribution logic.
 *
 * Read:
 *   env.EVENTS.search(filters, scope?)   — paginated event search
 *   env.EVENTS.facets(options, scope?)    — grouped aggregation on a dimension
 *   env.EVENTS.count(options, scope?)     — count with optional groupBy
 *   env.EVENTS.sql(query, params)         — raw SQL escape hatch
 *
 * ClickHouse reads hit the HTTP API directly (parameterized, retry on 5xx).
 */
import { WorkerEntrypoint } from 'cloudflare:workers'
import type { Env } from './env'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface EventFilters {
  /** Tenant / namespace */
  ns?: string
  /** Event name pattern (exact or LIKE with %) */
  event?: string
  /** Entity type ($type) */
  type?: string
  /** Source identifier */
  source?: string
  /** ISO-8601 or relative ("1h", "7d", "30m", "2w") */
  since?: string
  /** ISO-8601 upper bound */
  until?: string
  /** Pagination offset (default 0) */
  cursor?: number
  /** Page size (default 50, max 1000) */
  limit?: number
  /** Sort direction (default "desc") */
  sort?: 'asc' | 'desc'
}

export interface EventFacetsOptions {
  /** Dimension to group by */
  dimension: 'type' | 'event' | 'source' | 'ns'
  /** Optional filters to narrow the facet query */
  filters?: Omit<EventFilters, 'cursor' | 'limit' | 'sort'>
  /** Max groups returned (default 100) */
  limit?: number
}

export interface EventCountOptions {
  /** Optional filters to narrow the count */
  filters?: Omit<EventFilters, 'cursor' | 'limit' | 'sort'>
  /** Optional groupBy dimension */
  groupBy?: 'type' | 'event' | 'source' | 'ns'
}

export interface EventSearchResult {
  data: Record<string, unknown>[]
  total: number
  limit: number
  offset: number
  hasMore: boolean
}

export interface EventFacetResult {
  facets: Array<{ value: string; count: number }>
  total: number
}

export interface EventCountResult {
  count: number
  groups?: Array<{ value: string; count: number }>
}

export interface SqlResult {
  data: Record<string, unknown>[]
  rows: number
  elapsed: number
}

// ---------------------------------------------------------------------------
// ClickHouse Helpers
// ---------------------------------------------------------------------------

const CH_DATABASE = 'platform'
const CH_TABLE = 'events'
const CH_COLUMNS = 'id, ns, ts, type, event, source, url, data'
const DEFAULT_LIMIT = 50
const MAX_LIMIT = 1000
const DEFAULT_WINDOW_DAYS = 7
const CH_TIMEOUT_MS = 10_000
const CH_MAX_RETRIES = 2

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

/** Convert relative duration ("1h", "7d", "30m", "2w") to DateTime64-compatible timestamp. */
function parseSince(since: string): string {
  const match = since.match(/^(\d+)([mhdw])$/)
  if (!match) return since.replace(/Z$/, '') // strip trailing Z for DateTime64(3)
  const [, amount, unit] = match
  const ms = { m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 }[unit!]!
  return new Date(Date.now() - Number(amount) * ms).toISOString().replace(/Z$/, '')
}

interface WhereResult {
  clause: string
  params: Record<string, string | number>
}

function buildWhereClause(
  filters: {
    ns?: string | undefined
    event?: string | undefined
    type?: string | undefined
    source?: string | undefined
    since?: string | undefined
    until?: string | undefined
  },
  scope?: string | string[] | undefined,
): WhereResult {
  const parts: string[] = []
  const params: Record<string, string | number> = {}

  // Scope — namespace-level LIKE filtering
  if (scope) {
    const scopes = Array.isArray(scope) ? scope : [scope]
    if (scopes.length === 1) {
      parts.push('ns LIKE {scope0:String}')
      params.scope0 = `%${scopes[0]}%`
    } else {
      const conditions = scopes.map((s, i) => {
        params[`scope${i}`] = `%${s}%`
        return `ns LIKE {scope${i}:String}`
      })
      parts.push(`(${conditions.join(' OR ')})`)
    }
  }

  if (filters.ns) {
    parts.push('ns = {ns:String}')
    params.ns = filters.ns
  }
  if (filters.event) {
    if (filters.event.includes('%')) {
      parts.push('event LIKE {event:String}')
    } else {
      parts.push('event = {event:String}')
    }
    params.event = filters.event
  }
  if (filters.type) {
    parts.push('type = {type:String}')
    params.type = filters.type
  }
  if (filters.source) {
    parts.push('source = {source:String}')
    params.source = filters.source
  }

  // Time window — default to last 7 days when no time filter is specified
  if (filters.since) {
    parts.push('ts >= {since:DateTime64(3)}')
    params.since = parseSince(filters.since)
  } else if (!filters.until) {
    parts.push('ts >= {since:DateTime64(3)}')
    params.since = new Date(Date.now() - DEFAULT_WINDOW_DAYS * 86_400_000).toISOString().replace(/Z$/, '')
  }

  if (filters.until) {
    parts.push('ts <= {until:DateTime64(3)}')
    params.until = filters.until
  }

  return {
    clause: parts.length > 0 ? `WHERE ${parts.join(' AND ')}` : '',
    params,
  }
}

// ---------------------------------------------------------------------------
// Buffer shard state (module-level — safe across requests, no I/O objects)
// ---------------------------------------------------------------------------

const MIN_SHARDS = 8
const MAX_SHARDS = 64
const CACHE_READ_INTERVAL_MS = 60_000
const MAX_SHARD_ATTEMPTS = 8

let shardCount = MIN_SHARDS
let shardCountLastRead = 0
let nextShard = Math.floor(Math.random() * MIN_SHARDS)

// ---------------------------------------------------------------------------
// Read-only guard for sql()
// ---------------------------------------------------------------------------

const FORBIDDEN_SQL = /^\s*(INSERT|ALTER|DROP|TRUNCATE|CREATE|ATTACH|DETACH|RENAME|OPTIMIZE)\b/i

// ---------------------------------------------------------------------------
// EventsService
// ---------------------------------------------------------------------------

export class EventsService extends WorkerEntrypoint<Env> {
  // -------------------------------------------------------------------------
  // Private: ClickHouse HTTP query
  // -------------------------------------------------------------------------

  private async chQuery(query: string, params: Record<string, string | number> = {}): Promise<{ data: Record<string, unknown>[]; meta: Array<{ name: string; type: string }>; elapsed: number; networkMs: number }> {
    const rawUrl = this.env.CLICKHOUSE_URL
    const password = this.env.CLICKHOUSE_PASSWORD
    if (!rawUrl || !password) throw new Error('CLICKHOUSE_URL / CLICKHOUSE_PASSWORD not configured')

    // Build URL with config + params in query string, SQL in POST body
    const trimmed = rawUrl.trim()
    const chUrl = new URL(trimmed.startsWith('http') ? trimmed : `https://${trimmed}`)
    chUrl.searchParams.set('default_format', 'JSONCompact')
    chUrl.searchParams.set('database', CH_DATABASE)
    for (const [k, v] of Object.entries(params)) {
      chUrl.searchParams.set(`param_${k}`, String(v))
    }

    let lastError: Error | undefined
    for (let attempt = 0; attempt <= CH_MAX_RETRIES; attempt++) {
      const controller = new AbortController()
      const timer = setTimeout(() => controller.abort(), CH_TIMEOUT_MS)

      try {
        const fetchStart = performance.now()
        const resp = await fetch(chUrl.toString(), {
          method: 'POST',
          headers: {
            'Content-Type': 'text/plain',
            'X-ClickHouse-User': 'default',
            'X-ClickHouse-Key': password,
          },
          body: query,
          signal: controller.signal,
        })

        clearTimeout(timer)

        if (resp.ok) {
          const json = (await resp.json()) as {
            meta: Array<{ name: string; type: string }>
            data: unknown[][]
            rows: number
            elapsed: number
          }
          const networkMs = Math.round(performance.now() - fetchStart)
          // Convert columnar JSONCompact → array of objects
          const names = json.meta.map((m) => m.name)
          const data = json.data.map((row) => Object.fromEntries(names.map((n, i) => [n, row[i]])))
          const chMs = Math.round(json.elapsed * 1000)
          console.log(`[ch] ${data.length} rows, ${networkMs}ms network, ${chMs}ms ch-exec${attempt > 0 ? `, attempt ${attempt + 1}` : ''}`)
          return { data, meta: json.meta, elapsed: json.elapsed, networkMs }
        }

        // Retry on 5xx
        if (resp.status >= 500 && attempt < CH_MAX_RETRIES) {
          lastError = new Error(`ClickHouse ${resp.status}: ${await resp.text()}`)
          continue
        }

        throw new Error(`ClickHouse ${resp.status}: ${await resp.text()}`)
      } catch (err) {
        clearTimeout(timer)
        if (err instanceof DOMException && err.name === 'AbortError') {
          lastError = new Error('ClickHouse query timed out')
          if (attempt < CH_MAX_RETRIES) continue
        }
        throw lastError ?? err
      }
    }

    throw lastError ?? new Error('ClickHouse query failed')
  }

  // -------------------------------------------------------------------------
  // Write: ingest (from BufferService)
  // -------------------------------------------------------------------------

  async ingest(events: Array<Record<string, unknown>>): Promise<void> {
    if (!events || events.length === 0) return

    await this.maybeRefreshShardCount()

    const ns = this.env.CH_BUFFER as DurableObjectNamespace | undefined
    if (!ns) throw new Error('CH_BUFFER binding not available')

    for (let attempt = 0; attempt < MAX_SHARD_ATTEMPTS; attempt++) {
      const shard = nextShard % shardCount
      nextShard++

      const stub = ns.get(ns.idFromName(`shard-${shard}`))
      const resp = await stub.fetch('https://buffer/ws', {
        headers: { Upgrade: 'websocket' },
      })

      if (resp.status === 429) {
        await this.scaleUp()
        continue
      }

      if (resp.webSocket) {
        resp.webSocket.accept()
        resp.webSocket.send(JSON.stringify(events))
        resp.webSocket.close(1000, 'done')
        return
      }

      throw new Error(`Unexpected response from Buffer DO shard-${shard}: ${resp.status}`)
    }

    throw new Error(`All ${shardCount} CH buffer shards overloaded after ${MAX_SHARD_ATTEMPTS} attempts`)
  }

  // -------------------------------------------------------------------------
  // Read: search
  // -------------------------------------------------------------------------

  async search(filters: EventFilters = {}, scope?: string | string[]): Promise<EventSearchResult> {
    const limit = clamp(filters.limit ?? DEFAULT_LIMIT, 1, MAX_LIMIT)
    const offset = filters.cursor ?? 0
    const { clause, params } = buildWhereClause(filters, scope)

    // Fetch limit+1 to determine hasMore without a separate count query
    const fetchLimit = limit + 1
    const dataQuery = `SELECT ${CH_COLUMNS} FROM ${CH_TABLE} ${clause} ORDER BY id DESC LIMIT {fetchLimit:UInt32} OFFSET {offset:UInt32}`

    const allParams = { ...params, fetchLimit, offset }

    const t0 = performance.now()
    const dataResult = await this.chQuery(dataQuery, allParams)
    const queryMs = Math.round(performance.now() - t0)
    console.log(`[events] search: ${dataResult.data.length} rows, ${queryMs}ms (ch: ${Math.round(dataResult.elapsed * 1000)}ms)`)

    const hasMore = dataResult.data.length > limit
    const data = hasMore ? dataResult.data.slice(0, limit) : dataResult.data

    return {
      data,
      total: hasMore ? offset + limit + 1 : offset + data.length, // estimate — exact count not available without extra query
      limit,
      offset,
      hasMore,
    }
  }

  // -------------------------------------------------------------------------
  // Read: facets
  // -------------------------------------------------------------------------

  async facets(options: EventFacetsOptions, scope?: string | string[]): Promise<EventFacetResult> {
    const { dimension, filters, limit: maxBuckets } = options
    const bucketLimit = clamp(maxBuckets ?? 100, 1, 1000)

    // Validate dimension to prevent injection
    const allowedDimensions = ['type', 'event', 'source', 'ns'] as const
    if (!allowedDimensions.includes(dimension)) {
      throw new Error(`Invalid dimension: ${dimension}`)
    }

    const { clause, params } = buildWhereClause(filters ?? {}, scope)
    const query = `SELECT ${dimension} AS value, count() AS count FROM ${CH_TABLE} ${clause} GROUP BY ${dimension} ORDER BY count DESC LIMIT {bucketLimit:UInt32}`

    const t0 = performance.now()
    const result = await this.chQuery(query, { ...params, bucketLimit })
    const queryMs = Math.round(performance.now() - t0)

    const facets = result.data.map((row) => ({
      value: String(row.value ?? ''),
      count: Number(row.count ?? 0),
    }))
    const total = facets.reduce((sum, f) => sum + f.count, 0)
    console.log(`[events] facets(${dimension}): ${facets.length} buckets, ${queryMs}ms (ch: ${Math.round(result.elapsed * 1000)}ms)`)

    return { facets, total }
  }

  // -------------------------------------------------------------------------
  // Read: count
  // -------------------------------------------------------------------------

  async count(options: EventCountOptions = {}, scope?: string | string[]): Promise<EventCountResult> {
    const { groupBy, filters } = options
    const { clause, params } = buildWhereClause(filters ?? {}, scope)

    if (groupBy) {
      const allowedDimensions = ['type', 'event', 'source', 'ns'] as const
      if (!allowedDimensions.includes(groupBy)) {
        throw new Error(`Invalid groupBy: ${groupBy}`)
      }

      const query = `SELECT ${groupBy} AS value, count() AS count FROM ${CH_TABLE} ${clause} GROUP BY ${groupBy} ORDER BY count DESC`
      const result = await this.chQuery(query, params)

      const groups = result.data.map((row) => ({
        value: String(row.value ?? ''),
        count: Number(row.count ?? 0),
      }))
      const count = groups.reduce((sum, g) => sum + g.count, 0)

      return { count, groups }
    }

    const query = `SELECT count() AS count FROM ${CH_TABLE} ${clause}`
    const result = await this.chQuery(query, params)
    return { count: Number(result.data[0]?.count ?? 0) }
  }

  // -------------------------------------------------------------------------
  // Read: sql (raw escape hatch)
  // -------------------------------------------------------------------------

  async sql(query: string, params: Record<string, string | number> = {}): Promise<SqlResult> {
    if (FORBIDDEN_SQL.test(query)) throw new Error('Write operations not allowed via sql()')

    const start = Date.now()
    const result = await this.chQuery(query, params)
    return {
      data: result.data,
      rows: result.data.length,
      elapsed: Date.now() - start,
    }
  }

  // -------------------------------------------------------------------------
  // Private: shard management (from BufferService)
  // -------------------------------------------------------------------------

  private async maybeRefreshShardCount(): Promise<void> {
    if (Date.now() - shardCountLastRead < CACHE_READ_INTERVAL_MS) return
    shardCountLastRead = Date.now()

    try {
      const cached = await caches.default.match('https://ch-buffer/shard-count')
      if (cached) {
        const count = parseInt(await cached.text(), 10)
        if (count >= MIN_SHARDS && count <= MAX_SHARDS) shardCount = count
      }
    } catch {
      // Cache miss — keep current count
    }
  }

  private async scaleUp(): Promise<void> {
    const newCount = Math.min(shardCount + 4, MAX_SHARDS)
    if (newCount === shardCount) return

    shardCount = newCount
    try {
      await caches.default.put(
        'https://ch-buffer/shard-count',
        new Response(String(shardCount), {
          headers: { 'Cache-Control': 'max-age=60' },
        }),
      )
    } catch {
      // Non-fatal
    }
  }
}
