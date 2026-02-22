/**
 * BufferService — Workers RPC entrypoint for ClickHouse Buffer DO.
 *
 * Callers: env.EVENTS.ingest(events) — no WS management needed.
 * Internally caches a hibernatable WS to the Buffer DO at module level.
 * Handles shard escalation via 429 + Cache API.
 */
import { WorkerEntrypoint } from 'cloudflare:workers'
import type { Env } from './env'

// Module-level cached WS — persists across RPC calls on same isolate
let cachedWs: WebSocket | null = null
let cachedShardCount = 1
let shardCountLastRead = 0

export class BufferService extends WorkerEntrypoint<Env> {
  async ingest(events: Array<Record<string, unknown>>): Promise<void> {
    if (!events || events.length === 0) return
    const ws = await this.getOrOpenWs()
    try {
      ws.send(JSON.stringify(events))
    } catch {
      cachedWs = null
      const ws2 = await this.getOrOpenWs()
      ws2.send(JSON.stringify(events))
    }
  }

  private async getOrOpenWs(): Promise<WebSocket> {
    if (cachedWs && cachedWs.readyState === WebSocket.OPEN) return cachedWs

    // Read shard count from Cache API (per-colo, 60s TTL) — re-read every 60s
    if (Date.now() - shardCountLastRead > 60_000) {
      shardCountLastRead = Date.now()
      try {
        const cached = await caches.default.match('https://ch-buffer/shard-count')
        if (cached) {
          const count = parseInt(await cached.text(), 10)
          if (count > 0 && count <= 64) cachedShardCount = count
        }
      } catch {
        // Cache miss — use default
      }
    }

    // Try shards, escalating on 429
    for (let attempt = 0; attempt < 3; attempt++) {
      const shard = Math.floor(Math.random() * cachedShardCount)
      const ns = this.env.CH_BUFFER as DurableObjectNamespace | undefined
      if (!ns) throw new Error('CH_BUFFER binding not available')

      const stub = ns.get(ns.idFromName(`shard-${shard}`))
      const resp = await stub.fetch('https://buffer/ws', {
        headers: { Upgrade: 'websocket' },
      })

      if (resp.status === 429) {
        const newCount = Math.min(cachedShardCount + 1, 64)
        if (newCount !== cachedShardCount) {
          cachedShardCount = newCount
          try {
            await caches.default.put(
              'https://ch-buffer/shard-count',
              new Response(String(cachedShardCount), {
                headers: { 'Cache-Control': 'max-age=60' },
              }),
            )
          } catch {
            // Cache write failed — non-fatal
          }
        }
        continue
      }

      if (resp.webSocket) {
        cachedWs = resp.webSocket
        cachedWs.accept()
        cachedWs.addEventListener('close', () => { cachedWs = null })
        cachedWs.addEventListener('error', () => { cachedWs = null })
        return cachedWs
      }

      throw new Error(`Unexpected response from Buffer DO: ${resp.status}`)
    }

    throw new Error('All CH buffer shards overloaded')
  }
}
