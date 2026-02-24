/**
 * BufferService — Workers RPC entrypoint for ClickHouse Buffer DO.
 *
 * Callers: env.EVENTS.ingest(events) — no WS management needed.
 *
 * Opens a fresh WebSocket per ingest() call (no cross-request caching —
 * Workers prohibit reusing I/O objects across requests). Distributes
 * across 8+ shards via round-robin. On 429, scales up via Cache API.
 *
 * The ClickHouseBufferDO is hibernatable — the WS stays open on the DO side
 * even after we close our end, so batching still works across callers.
 */
import { WorkerEntrypoint } from 'cloudflare:workers'
import type { Env } from './env'

/** Minimum shards to distribute across — avoids single-shard bottleneck. */
const MIN_SHARDS = 8

/** Maximum shards before we stop scaling. */
const MAX_SHARDS = 64

/** How often to re-read shard count from Cache API. */
const CACHE_READ_INTERVAL_MS = 60_000

/** Max attempts to find a healthy shard before throwing. */
const MAX_SHARD_ATTEMPTS = 8

// Module-level state — safe across requests (no I/O objects)
let shardCount = MIN_SHARDS
let shardCountLastRead = 0
let nextShard = Math.floor(Math.random() * MIN_SHARDS)

export class BufferService extends WorkerEntrypoint<Env> {
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

  /** Read shard count from Cache API (per-colo coordination). */
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

  /** Scale up shard count and persist to Cache API. */
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
