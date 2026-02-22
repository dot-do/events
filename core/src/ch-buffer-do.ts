/**
 * ClickHouseBufferDO — Batched event buffer for real-time ClickHouse ingestion.
 *
 * Hibernatable WebSocket DO that accumulates events in memory and flushes to
 * ClickHouse in batches (~100ms intervals). Callers open a WebSocket, send
 * JSON arrays of events, and close. The DO batches and inserts via HTTP.
 *
 * Overload protection: returns 429 when buffer exceeds limits (bytes, count,
 * or connection count). Shard-0 has lower limits to trigger scale-out faster.
 *
 * Durability: Events in the buffer are NOT persisted to DO storage.
 * The Pipeline -> R2 -> S3Queue path is the durable log of record.
 * This DO is purely for real-time ClickHouse queryability.
 * If ClickHouse is down, events are logged and dropped.
 */
import { DurableObject } from 'cloudflare:workers'

/** Flush interval — accumulate events for this long before sending to ClickHouse. */
export const FLUSH_INTERVAL_MS = 100

/** Shard-0 limits — lower thresholds for faster scale-out. */
export const FIRST_SHARD_LIMITS = { bytes: 10 * 1024 * 1024, count: 10_000, conns: 200 } as const

/** Default limits for all other shards. */
export const DEFAULT_LIMITS = { bytes: 50 * 1024 * 1024, count: 50_000, conns: 1000 } as const

/** ClickHouse fetch timeout to prevent hung connections. */
const CH_FETCH_TIMEOUT_MS = 10_000

export interface BufferEnv {
  CH_BUFFER: DurableObjectNamespace
  CLICKHOUSE_URL?: string
  CLICKHOUSE_PASSWORD?: string
}

/**
 * Maps a raw event record to the ClickHouse `default.events` NDJSON row shape.
 * `data` and `meta` are serialized to JSON strings.
 */
export function toNdjsonRow(r: Record<string, unknown>): string {
  return JSON.stringify({
    id: r.id ?? crypto.randomUUID(),
    ns: r.ns ?? '',
    ts: r.ts ?? new Date().toISOString(),
    type: r.type ?? '',
    event: r.event ?? '',
    url: r.url ?? '',
    source: r.source ?? '',
    actor: r.actor ?? '',
    data: typeof r.data === 'string' ? r.data : JSON.stringify(r.data ?? {}),
    meta: typeof r.meta === 'string' ? r.meta : JSON.stringify(r.meta ?? {}),
  })
}

export class ClickHouseBufferDO extends DurableObject<BufferEnv> {
  buffer: Array<Record<string, unknown>> = []
  bufferBytes = 0
  lastFlushTime = 0
  limits: { bytes: number; count: number; conns: number }

  constructor(ctx: DurableObjectState, env: BufferEnv) {
    super(ctx, env)
    this.limits = this.detectLimits(ctx, env)
  }

  /** Detect whether this is shard-0 (lower limits) or a default shard. */
  private detectLimits(
    ctx: DurableObjectState,
    env: BufferEnv,
  ): { bytes: number; count: number; conns: number } {
    try {
      if (env.CH_BUFFER && ctx.id.toString() === env.CH_BUFFER.idFromName('shard-0').toString()) {
        return { ...FIRST_SHARD_LIMITS }
      }
    } catch {
      // env.CH_BUFFER may not exist in test environments
    }
    return { ...DEFAULT_LIMITS }
  }

  // -- WebSocket upgrade (only entry point) ----------------------------------

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('WebSocket required', { status: 426 })
    }

    // Reject new connections when overloaded
    if (
      this.bufferBytes >= this.limits.bytes ||
      this.buffer.length >= this.limits.count ||
      this.ctx.getWebSockets().length >= this.limits.conns
    ) {
      return new Response('Buffer overloaded', { status: 429 })
    }

    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    this.ctx.acceptWebSocket(server)
    return new Response(null, { status: 101, webSocket: client })
  }

  // -- Hibernatable WebSocket handlers ---------------------------------------

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const data = typeof message === 'string' ? message : new TextDecoder().decode(message)

    try {
      const records = JSON.parse(data) as Array<Record<string, unknown>>
      this.buffer.push(...records)
      this.bufferBytes += data.length
    } catch {
      return
    }

    // Only flush if enough time has passed
    const now = Date.now()
    if (now - this.lastFlushTime >= FLUSH_INTERVAL_MS) {
      this.lastFlushTime = now
      await this.flush()
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void> {
    // Drain remaining buffer when the last connection closes
    if (this.buffer.length > 0 && this.ctx.getWebSockets().length === 0) {
      await this.flush()
    }
  }

  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('[ch-buffer] WebSocket error:', error)
  }

  /** No-op: safely handles any legacy in-flight alarms. */
  async alarm(): Promise<void> {}

  // -- Flush logic -----------------------------------------------------------

  private async flush(): Promise<void> {
    if (this.buffer.length === 0) return

    const batch = this.buffer.splice(0)
    this.bufferBytes = 0

    const chUrl = this.env.CLICKHOUSE_URL
    const chPass = this.env.CLICKHOUSE_PASSWORD
    if (!chUrl || !chPass) {
      console.error('[ch-buffer] CLICKHOUSE_URL or CLICKHOUSE_PASSWORD not configured, dropping', batch.length, 'events')
      return
    }

    try {
      const ndjson = batch.map(toNdjsonRow).join('\n')

      const url = new URL(chUrl.trim())
      url.searchParams.set('query', 'INSERT INTO default.events FORMAT JSONEachRow')

      const resp = await fetch(url.toString(), {
        method: 'POST',
        headers: {
          'X-ClickHouse-User': 'default',
          'X-ClickHouse-Key': chPass.trim(),
        },
        body: ndjson,
        signal: AbortSignal.timeout(CH_FETCH_TIMEOUT_MS),
      })

      // Always consume response body to release TCP connection
      const text = await resp.text()

      if (!resp.ok) {
        console.error(`[ch-buffer] ClickHouse insert failed (${batch.length} records):`, resp.status, text.slice(0, 200))
      }
    } catch (err) {
      console.error('[ch-buffer] ClickHouse insert error, dropping', batch.length, 'events:', err)
    }
  }
}
