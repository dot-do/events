/**
 * events.do - Event Ingestion Worker
 *
 * Routes:
 * - events.do (primary)
 * - events.workers.do (alias)
 * - apis.do/events, apis.do/e (client-side with cookies)
 *
 * Endpoints:
 * - POST /ingest - Receive batched events
 * - GET /health - Health check
 * - POST /query - Generate DuckDB query
 * - GET /recent - List recent events (debug)
 */

import type { DurableEvent, EventBatch } from '@dotdo/events'

interface Env {
  EVENTS_BUCKET: R2Bucket
  EVENTS_QUEUE?: Queue<EventBatch>
  AUTH_TOKEN?: string
  ENVIRONMENT: string
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    // CORS for client-side analytics
    if (request.method === 'OPTIONS') {
      return handleCORS()
    }

    // Health check
    if (url.pathname === '/health' || url.pathname === '/') {
      return Response.json({
        status: 'ok',
        service: 'events.do',
        ts: new Date().toISOString(),
        env: env.ENVIRONMENT,
      })
    }

    // Ingest endpoint
    if ((url.pathname === '/ingest' || url.pathname === '/e') && request.method === 'POST') {
      return handleIngest(request, env, ctx)
    }

    // Query builder endpoint
    if (url.pathname === '/query' && request.method === 'POST') {
      return handleQuery(request, env)
    }

    // Recent events (debug)
    if (url.pathname === '/recent' && request.method === 'GET') {
      return handleRecent(request, env)
    }

    return new Response('Not found', { status: 404 })
  },
}

// ============================================================================
// CORS
// ============================================================================

function handleCORS(): Response {
  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400',
    },
  })
}

function corsHeaders(): HeadersInit {
  return {
    'Access-Control-Allow-Origin': '*',
  }
}

// ============================================================================
// Ingest Handler
// ============================================================================

async function handleIngest(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  // Optional auth
  if (env.AUTH_TOKEN) {
    const auth = request.headers.get('Authorization')
    if (auth !== `Bearer ${env.AUTH_TOKEN}`) {
      return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders() })
    }
  }

  let batch: EventBatch
  try {
    batch = await request.json()
  } catch {
    return Response.json({ error: 'Invalid JSON' }, { status: 400, headers: corsHeaders() })
  }

  if (!batch.events || !Array.isArray(batch.events)) {
    return Response.json({ error: 'Missing events array' }, { status: 400, headers: corsHeaders() })
  }

  // Group events by hour bucket for efficient R2 organization
  const buckets = groupByTimeBucket(batch.events)

  // Write to R2 (in background for fast response)
  ctx.waitUntil((async () => {
    const writes = Object.entries(buckets).map(async ([bucket, events]) => {
      const key = `events/${bucket}/${crypto.randomUUID()}.jsonl`
      const body = events.map(e => JSON.stringify(e)).join('\n')

      await env.EVENTS_BUCKET.put(key, body, {
        httpMetadata: { contentType: 'application/x-ndjson' },
        customMetadata: {
          eventCount: String(events.length),
          firstTs: events[0].ts,
          lastTs: events[events.length - 1].ts,
        },
      })
    })

    await Promise.all(writes)

    // Optionally send to Queue for real-time consumers
    if (env.EVENTS_QUEUE) {
      await env.EVENTS_QUEUE.send(batch)
    }
  })())

  return Response.json({
    ok: true,
    received: batch.events.length,
  }, { headers: corsHeaders() })
}

/**
 * Group events by time bucket (YYYY/MM/DD/HH)
 */
function groupByTimeBucket(events: DurableEvent[]): Record<string, DurableEvent[]> {
  const buckets: Record<string, DurableEvent[]> = {}

  for (const event of events) {
    const date = new Date(event.ts)
    const bucket = [
      date.getUTCFullYear(),
      String(date.getUTCMonth() + 1).padStart(2, '0'),
      String(date.getUTCDate()).padStart(2, '0'),
      String(date.getUTCHours()).padStart(2, '0'),
    ].join('/')

    if (!buckets[bucket]) {
      buckets[bucket] = []
    }
    buckets[bucket].push(event)
  }

  return buckets
}

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

async function handleQuery(request: Request, env: Env): Promise<Response> {
  const query: QueryRequest = await request.json()

  const conditions: string[] = []
  let pathPattern = 'events/'

  // Optimize path based on date range
  if (query.dateRange) {
    const start = new Date(query.dateRange.start)
    const end = new Date(query.dateRange.end)

    if (start.getFullYear() === end.getFullYear()) {
      pathPattern += `${start.getFullYear()}/`
      if (start.getMonth() === end.getMonth()) {
        pathPattern += `${String(start.getMonth() + 1).padStart(2, '0')}/`
        if (start.getDate() === end.getDate()) {
          pathPattern += `${String(start.getDate()).padStart(2, '0')}/`
        } else {
          pathPattern += '*/'
        }
      } else {
        pathPattern += '*/'
      }
    } else {
      pathPattern += '*/'
    }
  } else {
    pathPattern += '**/'
  }
  pathPattern += '*.jsonl'

  // Build conditions
  if (query.doId) {
    conditions.push(`"do".id = '${query.doId}'`)
  }
  if (query.doClass) {
    conditions.push(`"do".class = '${query.doClass}'`)
  }
  if (query.colo) {
    conditions.push(`"do".colo = '${query.colo}'`)
  }
  if (query.eventTypes?.length) {
    conditions.push(`type IN (${query.eventTypes.map(t => `'${t}'`).join(', ')})`)
  }
  if (query.collection) {
    conditions.push(`collection = '${query.collection}'`)
  }
  if (query.dateRange) {
    conditions.push(`ts >= '${query.dateRange.start}'`)
    conditions.push(`ts <= '${query.dateRange.end}'`)
  }

  let sql = `SELECT *
FROM read_json_auto('${pathPattern}',
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

  return Response.json({ sql }, { headers: corsHeaders() })
}

// ============================================================================
// Recent Events (Debug)
// ============================================================================

async function handleRecent(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url)
  const limit = parseInt(url.searchParams.get('limit') ?? '100')

  // List recent files from current hour
  const now = new Date()
  const hourPath = [
    now.getUTCFullYear(),
    String(now.getUTCMonth() + 1).padStart(2, '0'),
    String(now.getUTCDate()).padStart(2, '0'),
    String(now.getUTCHours()).padStart(2, '0'),
  ].join('/')

  const listed = await env.EVENTS_BUCKET.list({
    prefix: `events/${hourPath}/`,
    limit: 10,
  })

  const events: DurableEvent[] = []

  for (const obj of listed.objects) {
    const data = await env.EVENTS_BUCKET.get(obj.key)
    if (data) {
      const text = await data.text()
      const lines = text.split('\n').filter(Boolean)
      for (const line of lines) {
        try {
          events.push(JSON.parse(line))
        } catch {
          // Skip malformed
        }
      }
    }
    if (events.length >= limit) break
  }

  return Response.json({
    events: events.slice(0, limit),
    count: events.length,
    bucket: hourPath,
  }, { headers: corsHeaders() })
}
