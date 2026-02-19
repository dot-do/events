/**
 * Tail Worker - Capture worker traces and write to Parquet
 *
 * Architecture:
 * - Uses shared EventWriterDO via Workers RPC (type-safe direct calls)
 * - This worker is NOT tailed (prevents infinite loops)
 * - Full logging/observability in the DO
 * - Sharded writers for high throughput (from lakehouse pattern)
 *
 * Loop prevention:
 * - events.do worker IS tailed -> traces come here
 * - This worker is NOT tailed -> chain stops
 * - DO can log freely, its logs are part of this worker's execution
 */

import { ingestWithOverflow } from './event-writer-do'
import type { EventRecord } from './event-writer'
import type { Env as FullEnv } from './env'
import { logger, sanitize } from './logger'

const log = logger.child({ component: 'tail' })

type Env = Pick<FullEnv, 'EVENTS_BUCKET' | 'EVENT_WRITER' | 'TAIL_AUTH_SECRET'> & {
  EVENTS_PIPELINE?: Pipeline<Record<string, unknown>>
}

// ============================================================================
// Auth helper
// ============================================================================
function checkAuth(request: Request, env: Env): Response | null {
  const secret = env.TAIL_AUTH_SECRET
  if (!secret) {
    // If no secret is configured, deny all requests as a safe default
    return Response.json({ error: 'Server misconfigured: no auth secret' }, { status: 500 })
  }
  const authHeader = request.headers.get('Authorization')
  if (!authHeader || authHeader !== `Bearer ${secret}`) {
    return Response.json({ error: 'Unauthorized' }, { status: 401 })
  }
  return null
}

// ============================================================================
// Exports
// ============================================================================
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    // All HTTP endpoints require authentication
    const authError = checkAuth(request, env)
    if (authError) return authError

    // Status endpoint
    if (url.pathname === '/status') {
      return Response.json({
        service: 'tail',
        architecture: 'shared-do-immediate',
      })
    }

    // List recent files (now under 'events/' prefix, not 'tail/')
    if (url.pathname === '/stats') {
      const listed = await env.EVENTS_BUCKET.list({
        prefix: 'events/',
        limit: 100,
      })

      const sorted = listed.objects.sort((a, b) =>
        (b.uploaded?.getTime() ?? 0) - (a.uploaded?.getTime() ?? 0)
      )

      const files = await Promise.all(
        sorted.slice(0, 20).map(async (obj) => {
          const head = await env.EVENTS_BUCKET.head(obj.key)
          return {
            key: obj.key,
            size: obj.size,
            uploaded: obj.uploaded?.toISOString(),
            ...head?.customMetadata,
          } as { key: string; size: number; uploaded?: string; events?: string; cpuMs?: string }
        })
      )

      // Summary
      const totalEvents = files.reduce((s, f) => s + parseInt(f.events ?? '0'), 0)
      const totalBytes = files.reduce((s, f) => s + (f.size ?? 0), 0)
      const avgCpuMs = files.length > 0
        ? files.reduce((s, f) => s + parseFloat(f.cpuMs ?? '0'), 0) / files.length
        : 0

      return Response.json({
        summary: {
          files: files.length,
          totalEvents,
          totalBytes,
          avgEventsPerFile: files.length > 0 ? Math.round(totalEvents / files.length) : 0,
          avgBytesPerEvent: totalEvents > 0 ? Math.round(totalBytes / totalEvents) : 0,
          avgCpuMs: avgCpuMs.toFixed(2),
        },
        files,
      })
    }

    return Response.json({
      service: 'tail',
      architecture: 'shared-do-immediate',
      endpoints: ['/status', '/stats'],
    })
  },

  async tail(events: TraceItem[], env: Env): Promise<void> {
    log.info('tail() called', { eventCount: events.length })

    if (events.length === 0) return

    // Parse trace events into records
    const records: EventRecord[] = []
    for (const trace of events) {
      // Skip self to prevent any potential loops (belt + suspenders)
      if (trace.scriptName === 'tail') continue

      // Determine event type and extract request info
      let eventType = 'unknown'
      let method: string | undefined
      let url: string | undefined
      let statusCode: number | undefined

      // Check execution model for DO events
      const execModel = (trace as { executionModel?: string }).executionModel
      const entrypoint = (trace as { entrypoint?: string }).entrypoint

      if (trace.event) {
        if ('request' in trace.event) {
          eventType = 'fetch'
          const req = trace.event.request as { method?: string; url?: string }
          method = req.method
          url = req.url

          if ('response' in trace.event) {
            const res = trace.event.response as { status?: number }
            statusCode = res.status
          }
        } else if ('rpcMethod' in trace.event) {
          // Durable Object RPC call
          const rpcMethod = (trace.event as { rpcMethod?: string }).rpcMethod
          eventType = entrypoint ? `do.${entrypoint}.${rpcMethod}` : `rpc.${rpcMethod}`
          method = rpcMethod
        } else if ('scheduledTime' in trace.event) {
          eventType = 'scheduled'
        } else if ('queue' in trace.event) {
          eventType = 'queue'
        } else if ('alarm' in trace.event || (trace.event as { type?: string }).type === 'alarm') {
          eventType = entrypoint ? `do.${entrypoint}.alarm` : 'alarm'
        } else if ('type' in trace.event) {
          eventType = String(trace.event.type)
        }
      } else if (execModel === 'durableObject' && entrypoint) {
        // DO event without specific event info
        eventType = `do.${entrypoint}`
      }

      // Calculate duration from logs or event
      let durationMs: number | undefined
      if (trace.eventTimestamp && trace.logs.length > 0) {
        const lastLog = trace.logs[trace.logs.length - 1]
        if (lastLog?.timestamp) {
          durationMs = lastLog.timestamp - trace.eventTimestamp
        }
      }

      // Safely serialize entire TraceItem (TraceLog/TraceException aren't serializable for RPC)
      let safePayload: unknown
      try {
        // JSON round-trip to strip non-serializable values
        // Then sanitize to remove any sensitive data from logs/traces
        const serialized = JSON.parse(JSON.stringify(trace))
        safePayload = sanitize.payload(serialized)
      } catch (e) {
        log.warn('Serialization error', { error: sanitize.errorMessage(String(e)) })
        safePayload = { event: { type: eventType }, scriptName: trace.scriptName }
      }

      // Create event record
      const record: EventRecord = {
        ts: new Date(trace.eventTimestamp ?? Date.now()).toISOString(),
        type: `tail.${trace.scriptName ?? 'unknown'}.${eventType}`,
        source: 'tail',
        scriptName: trace.scriptName ?? 'unknown',
        outcome: trace.outcome,
        eventType,
        method,
        url,
        statusCode,
        durationMs,
        payload: safePayload,
      }

      records.push(record)
    }

    if (records.length === 0) return

    // Send to EventWriterDO (Parquet on R2) and Pipeline (→ ClickHouse) in parallel
    const doIngest = ingestWithOverflow(env, records, 'tail').then((result) => {
      if (!result.ok) log.error('Failed DO ingest', { shard: result.shard })
      else log.info('DO ingested', { shard: result.shard, buffered: result.buffered })
    })

    const pipelineIngest = env.EVENTS_PIPELINE
      ? env.EVENTS_PIPELINE.send(records as unknown as Record<string, unknown>[]).catch((err) => {
          log.error('Pipeline send failed', { error: String(err) })
        })
      : Promise.resolve()

    await Promise.all([doIngest, pipelineIngest])
  },
}
