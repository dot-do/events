/**
 * EventsService — Unified read + write RPC entrypoint for events.do.
 *
 * Write: env.EVENTS.ingest(events)
 *   Opens a WebSocket to a ClickHouseBufferDO shard (round-robin).
 *   Copied from the original BufferService — same shard distribution logic.
 *
 * Read:
 *   env.EVENTS.search(filters)   — paginated event search
 *   env.EVENTS.facets(options)    — grouped aggregation on a dimension
 *   env.EVENTS.count(options)     — count with optional groupBy
 *   env.EVENTS.sql(query, params) — raw SQL escape hatch
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
  /** ISO-8601 or relative ("1h", "7d", "30m") */
  since?: string
  /** ISO-8601 upper bound */
  until?: string
  /** Pagination cursor (offset) */
  cursor?: number
  /** Page size (default 50, max 1000) */
  limit?: number
  /** Sort direction (default "desc") */
  sort?: 'asc' | 'desc'
}

export interface EventFacetsOptions {
  /** Dimension to group by */
  dimension: 'type' | 'event' | 'source' | 'ns'
  ns?: string
  since?: string
  until?: string
  /** Max groups returned (default 100) */
  limit?: number
}

export interface EventCountOptions {
  ns?: string
  event?: string
  type?: string
  source?: string
  since?: string
  until?: string
  /** Optional groupBy dimension */
  groupBy?: 'type' | 'event' | 'source' | 'ns'
}

export interface EventSearchResult {
  items: Record<string, unknown>[]
  total: number
  cursor: number | null
}

export interface EventFacetResult {
  dimension: string
  buckets: Array<{ key: string; count: number }>
}

export interface EventCountResult {
  total: number
  groups?: Array<{ key: string; count: number }>
}

export interface SqlResult {
  rows: Record<string, unknown>[]
  meta: Array<{ name: string; type: string }>
  elapsed: number
}

// ---------------------------------------------------------------------------
// ClickHouse Helpers
// ---------------------------------------------------------------------------

const CH_DATABASE = 'platform'
const CH_TABLE = 'events'
const DEFAULT_LIMIT = 50
const MAX_LIMIT = 1000
const CH_TIMEOUT_MS = 10_000
const CH_MAX_RETRIES = 2

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

/** Convert relative duration ("1h", "7d", "30m") to ISO-8601 timestamp. */
function parseSince(since: string): string {
  const match = since.match(/^(\d+)([mhd])$/)
  if (!match) return since // assume ISO-8601 already
  const [, amount, unit] = match
  const ms = { m: 60_000, h: 3_600_000, d: 86_400_000 }[unit!]!
  return new Date(Date.now() - Number(amount) * ms).toISOString()
}

interface WhereResult {
  clause: string
  params: Record<string, string | number>
}

function buildWhereClause(filters: {
  ns?: string | undefined
  event?: string | undefined
  type?: string | undefined
  source?: string | undefined
  since?: string | undefined
  until?: string | undefined
}): WhereResult {
  const parts: string[] = []
  const params: Record<string, string | number> = {}

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
  if (filters.since) {
    parts.push('timestamp >= {since:DateTime64(3)}')
    params.since = parseSince(filters.since)
  }
  if (filters.until) {
    parts.push('timestamp <= {until:DateTime64(3)}')
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
// EventsService
// ---------------------------------------------------------------------------

export class EventsService extends WorkerEntrypoint<Env> {
  // -------------------------------------------------------------------------
  // Private: ClickHouse HTTP query
  // -------------------------------------------------------------------------

  private async chQuery(query: string, params: Record<string, string | number> = {}): Promise<{ data: Record<string, unknown>[]; meta: Array<{ name: string; type: string }>; elapsed: number }> {
    const url = this.env.CLICKHOUSE_URL
    const password = this.env.CLICKHOUSE_PASSWORD
    if (!url || !password) throw new Error('CLICKHOUSE_URL / CLICKHOUSE_PASSWORD not configured')

    const body = new URLSearchParams({
      query,
      default_format: 'JSONCompact',
      database: CH_DATABASE,
      ...Object.fromEntries(Object.entries(params).map(([k, v]) => [`param_${k}`, String(v)])),
    })

    let lastError: Error | undefined
    for (let attempt = 0; attempt <= CH_MAX_RETRIES; attempt++) {
      const controller = new AbortController()
      const timer = setTimeout(() => controller.abort(), CH_TIMEOUT_MS)

      try {
        const resp = await fetch(url, {
          method: 'POST',
          headers: {
            'X-ClickHouse-User': 'default',
            'X-ClickHouse-Key': password,
          },
          body: body.toString(),
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
          // Convert columnar JSONCompact → array of objects
          const names = json.meta.map((m) => m.name)
          const data = json.data.map((row) => Object.fromEntries(names.map((n, i) => [n, row[i]])))
          return { data, meta: json.meta, elapsed: json.elapsed }
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

  async search(filters: EventFilters = {}): Promise<EventSearchResult> {
    const limit = clamp(filters.limit ?? DEFAULT_LIMIT, 1, MAX_LIMIT)
    const cursor = filters.cursor ?? 0
    const sort = filters.sort ?? 'desc'
    const { clause, params } = buildWhereClause(filters)

    const countQuery = `SELECT count() AS total FROM ${CH_TABLE} ${clause}`
    const dataQuery = `SELECT * FROM ${CH_TABLE} ${clause} ORDER BY timestamp ${sort} LIMIT {limit:UInt32} OFFSET {offset:UInt32}`

    const allParams = { ...params, limit, offset: cursor }

    const [countResult, dataResult] = await Promise.all([this.chQuery(countQuery, allParams), this.chQuery(dataQuery, allParams)])

    const total = Number(countResult.data[0]?.total ?? 0)
    const nextCursor = cursor + limit < total ? cursor + limit : null

    return {
      items: dataResult.data,
      total,
      cursor: nextCursor,
    }
  }

  // -------------------------------------------------------------------------
  // Read: facets
  // -------------------------------------------------------------------------

  async facets(options: EventFacetsOptions): Promise<EventFacetResult> {
    const { dimension, ns, since, until, limit: maxBuckets } = options
    const bucketLimit = clamp(maxBuckets ?? 100, 1, 1000)

    // Validate dimension to prevent injection
    const allowedDimensions = ['type', 'event', 'source', 'ns'] as const
    if (!allowedDimensions.includes(dimension)) {
      throw new Error(`Invalid dimension: ${dimension}`)
    }

    const { clause, params } = buildWhereClause({ ns, since, until })
    const query = `SELECT ${dimension} AS key, count() AS count FROM ${CH_TABLE} ${clause} GROUP BY ${dimension} ORDER BY count DESC LIMIT {bucketLimit:UInt32}`

    const result = await this.chQuery(query, { ...params, bucketLimit })

    return {
      dimension,
      buckets: result.data.map((row) => ({
        key: String(row.key ?? ''),
        count: Number(row.count ?? 0),
      })),
    }
  }

  // -------------------------------------------------------------------------
  // Read: count
  // -------------------------------------------------------------------------

  async count(options: EventCountOptions = {}): Promise<EventCountResult> {
    const { groupBy, ...filters } = options
    const { clause, params } = buildWhereClause(filters)

    if (groupBy) {
      const allowedDimensions = ['type', 'event', 'source', 'ns'] as const
      if (!allowedDimensions.includes(groupBy)) {
        throw new Error(`Invalid groupBy: ${groupBy}`)
      }

      const query = `SELECT ${groupBy} AS key, count() AS count FROM ${CH_TABLE} ${clause} GROUP BY ${groupBy} ORDER BY count DESC`
      const result = await this.chQuery(query, params)

      const groups = result.data.map((row) => ({
        key: String(row.key ?? ''),
        count: Number(row.count ?? 0),
      }))
      const total = groups.reduce((sum, g) => sum + g.count, 0)

      return { total, groups }
    }

    const query = `SELECT count() AS total FROM ${CH_TABLE} ${clause}`
    const result = await this.chQuery(query, params)
    return { total: Number(result.data[0]?.total ?? 0) }
  }

  // -------------------------------------------------------------------------
  // Read: sql (raw escape hatch)
  // -------------------------------------------------------------------------

  async sql(query: string, params: Record<string, string | number> = {}): Promise<SqlResult> {
    const start = Date.now()
    const result = await this.chQuery(query, params)
    return {
      rows: result.data,
      meta: result.meta,
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
