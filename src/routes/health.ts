/**
 * Health check, pipeline check, and benchmark route handlers
 */

import type { Env } from '../env'
import { authCorsHeaders } from '../utils'

export function handleHealth(
  request: Request,
  env: Env,
  url: URL,
  isWebhooksDomain: boolean,
  startTime: number,
): Response | null {
  if (url.pathname !== '/health' && url.pathname !== '/') return null

  const serviceName = isWebhooksDomain ? 'webhooks.do' : 'events.do'
  const cpuTime = performance.now() - startTime
  console.log(`[CPU:${cpuTime.toFixed(2)}ms] GET /health`)
  return Response.json({
    status: 'ok',
    service: serviceName,
    ts: new Date().toISOString(),
    env: env.ENVIRONMENT,
    cpuTimeMs: cpuTime,
    providers: isWebhooksDomain ? ['github', 'stripe', 'workos', 'slack', 'linear', 'svix'] : undefined,
  })
}

export async function handlePipelineCheck(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url)
  const prefix = url.searchParams.get('prefix') ?? 'test-wiktionary/'
  const limit = parseInt(url.searchParams.get('limit') ?? '20')

  const listed = await env.PIPELINE_BUCKET.list({
    prefix,
    limit,
  })

  const files = listed.objects.map((obj) => ({
    key: obj.key,
    size: obj.size,
    uploaded: obj.uploaded?.toISOString(),
    etag: obj.etag,
  }))

  // Also check root level
  const rootList = await env.PIPELINE_BUCKET.list({ limit: 10 })
  const rootPrefixes = [...new Set(rootList.objects.map((o) => o.key.split('/')[0]))]

  return Response.json({
    bucket: 'platform-events',
    prefix,
    fileCount: listed.objects.length,
    totalSize: listed.objects.reduce((sum, o) => sum + o.size, 0),
    files,
    rootPrefixes,
    truncated: listed.truncated,
  }, { headers: authCorsHeaders(request, env) })
}

export async function handleBenchmark(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url)
  const prefix = url.searchParams.get('prefix') ?? 'benchmark/'

  const listed = await env.BENCHMARK_BUCKET.list({
    prefix,
    limit: 100,
  })

  // Group by codec and collect stats
  const stats: Record<string, { files: number; totalBytes: number; totalCpuMs: number; events: number }> = {}

  const files = await Promise.all(
    listed.objects.map(async (obj) => {
      const head = await env.BENCHMARK_BUCKET.head(obj.key)
      const meta = head?.customMetadata ?? {}

      const codec = meta.codec ?? 'unknown'
      if (!stats[codec]) {
        stats[codec] = { files: 0, totalBytes: 0, totalCpuMs: 0, events: 0 }
      }
      stats[codec].files++
      stats[codec].totalBytes += obj.size
      stats[codec].totalCpuMs += parseFloat(meta.cpuMs ?? '0')
      stats[codec].events += parseInt(meta.events ?? '0')

      return {
        key: obj.key,
        size: obj.size,
        codec: meta.codec,
        cpuMs: meta.cpuMs,
        events: meta.events,
        uploaded: obj.uploaded?.toISOString(),
      }
    })
  )

  // Calculate averages
  const summary = Object.entries(stats).map(([codec, s]) => ({
    codec,
    files: s.files,
    totalEvents: s.events,
    totalBytes: s.totalBytes,
    totalCpuMs: s.totalCpuMs.toFixed(2),
    avgCpuMsPerFile: (s.totalCpuMs / s.files).toFixed(2),
    avgBytesPerEvent: s.events > 0 ? Math.round(s.totalBytes / s.events) : 0,
  }))

  return Response.json({
    bucket: 'events-parquet-test',
    prefix,
    fileCount: files.length,
    summary,
    files: files.slice(0, 20), // First 20 files
  }, { headers: authCorsHeaders(request, env) })
}
