/**
 * ClickHouseBufferDO — Batched event buffer for real-time ClickHouse ingestion.
 *
 * Hibernatable WebSocket DO that accumulates events in memory and flushes to
 * ClickHouse in batches (~50ms intervals). BufferService distributes events
 * across 8-64 shards via round-robin — each shard handles 1/N of the load.
 *
 * Overload protection: returns 429 when buffer exceeds limits so BufferService
 * can scale out to more shards.
 *
 * Durability: Events in the buffer are NOT persisted to DO storage.
 * The Pipeline -> R2 -> S3Queue path is the durable log of record.
 * This DO is purely for real-time ClickHouse queryability.
 * If ClickHouse is down, events are logged and dropped.
 */
import { DurableObject } from 'cloudflare:workers'

const ULID_ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
const ULID_RE = /^[0-9A-HJKMNP-TV-Z]{26}$/

function ulid(): string {
  let str = ''
  let ts = Date.now()
  for (let i = 9; i >= 0; i--) {
    str = ULID_ENCODING[ts % 32] + str
    ts = Math.floor(ts / 32)
  }
  const random = new Uint8Array(16)
  crypto.getRandomValues(random)
  for (let i = 0; i < 16; i++) {
    str += ULID_ENCODING[random[i] % 32]
  }
  return str
}

/** Flush interval — accumulate events for this long before sending to ClickHouse. */
export const FLUSH_INTERVAL_MS = 50

/** Safety alarm delay — fallback flush for events buffered without a time-based flush. */
export const ALARM_SAFETY_MS = 150

/** All shards have the same limits — BufferService handles distribution. */
export const SHARD_LIMITS = { bytes: 50 * 1024 * 1024, count: 50_000, conns: 1000 } as const

/** ClickHouse fetch timeout to prevent hung connections. */
const CH_FETCH_TIMEOUT_MS = 10_000

export interface BufferEnv {
  CH_BUFFER: DurableObjectNamespace
  CLICKHOUSE_URL?: string
  CLICKHOUSE_PASSWORD?: string
}

/**
 * Maps a raw event record to the ClickHouse `platform.events` NDJSON row shape.
 * `actor`, `data`, and `meta` are native JSON columns — passed as raw objects
 * so the outer JSON.stringify produces proper nested JSON for JSONEachRow format.
 */
export function toNdjsonRow(r: Record<string, unknown>): string {
  // Normalize actor: if string (legacy), wrap as { id }; if missing, empty object
  const rawActor = r.actor
  const actor = typeof rawActor === 'string' ? (rawActor ? { id: rawActor } : {}) : rawActor ?? {}

  return JSON.stringify({
    id: typeof r.id === 'string' && ULID_RE.test(r.id) ? r.id : ulid(),
    ns: r.ns ?? '',
    ts: r.ts ?? new Date().toISOString(),
    type: r.type ?? '',
    event: r.event ?? '',
    url: r.url ?? '',
    source: r.source ?? '',
    actor,
    data: typeof r.data === 'string' ? JSON.parse(r.data) : (r.data ?? {}),
    meta: typeof r.meta === 'string' ? JSON.parse(r.meta) : (r.meta ?? {}),
  })
}

export class ClickHouseBufferDO extends DurableObject<BufferEnv> {
  buffer: Array<Record<string, unknown>> = []
  bufferBytes = 0
  lastFlushTime = 0

  // -- WebSocket upgrade (only entry point) ----------------------------------

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('WebSocket required', { status: 426 })
    }

    // Reject new connections when overloaded
    if (
      this.bufferBytes >= SHARD_LIMITS.bytes ||
      this.buffer.length >= SHARD_LIMITS.count ||
      this.ctx.getWebSockets().length >= SHARD_LIMITS.conns
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
    } else {
      // Safety net: set an alarm to flush stranded events if no more messages arrive.
      // Without this, events buffered between flushes could sit indefinitely
      // if the connection goes idle (hibernated) with no close event.
      this.ctx.storage.setAlarm(Date.now() + ALARM_SAFETY_MS)
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

  /** Safety-net flush: drain any stranded events left in the buffer. */
  async alarm(): Promise<void> {
    if (this.buffer.length > 0) {
      await this.flush()
    }
  }

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
      url.searchParams.set('query', 'INSERT INTO platform.events FORMAT JSONEachRow')

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
