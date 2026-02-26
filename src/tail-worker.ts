/**
 * Tail Worker — Pipeline-only (events.do canonical)
 *
 * Captures ALL trace types from tailed workers and forwards to Pipeline.
 * Pipeline → R2 → S3Queue → ClickHouse (~10-90s latency, durable).
 *
 * Sends MINIMAL records: { id, source, data }. No ts — derived from ULID.
 * All normalization (ns, type, event, url, ray, actor) is derived downstream
 * in the ClickHouse S3Queue MV (streams.ingest). This means improving
 * normalization only requires recreating the MV — no worker redeployment.
 *
 * `data` contains the ENTIRE TraceItem — logs, exceptions, diagnostics,
 * cpu/wall time, DO IDs, request headers, cf object, everything.
 * The MV extracts: hostname from data.event.request.url → ns,
 * cf-ray from data.event.request.headers → ray,
 * data.event.request.cf → actor geo, and classifies type/event from event shape.
 *
 * Only two things MUST stay at source:
 *   1. maskTrace() — redact sensitive data BEFORE it hits R2 (durable log)
 *   2. Self-filter — prevent infinite loops (tail worker traces itself)
 *
 * This worker is NOT tailed (prevents infinite loops).
 */

import { ulid } from '../core/src/ulid'

// =============================================================================
// Pricing — flat per-request pricing in USD microcents (1 USD = 100,000,000)
// =============================================================================

/** Per-request price: $0.00001 = 1,000 microcents. Matches PRICING.request in apps/api/src/lib/pricing.ts */
const REQUEST_PRICE_MICROCENTS = 1_000

// =============================================================================
// Sensitive data masking — prevent API keys from leaking into R2/ClickHouse
// =============================================================================

const SENSITIVE_HEADERS = new Set(['authorization', 'x-api-key', 'x-auth-token', 'cookie'])
const SENSITIVE_PARAMS = /[?&](apikey|api_key|token)=[^&]*/gi

function maskTrace(trace: TraceItem): unknown {
  const data = JSON.parse(JSON.stringify(trace))
  const evt = data.event as Record<string, unknown> | null
  if (evt && typeof evt === 'object') {
    const req = (evt as { request?: { url?: string; headers?: Record<string, string> } }).request
    if (req) {
      if (req.headers && typeof req.headers === 'object') {
        for (const key of Object.keys(req.headers)) {
          if (SENSITIVE_HEADERS.has(key.toLowerCase())) {
            req.headers[key] = '[REDACTED]'
          }
        }
      }
      if (typeof req.url === 'string') {
        req.url = req.url.replace(SENSITIVE_PARAMS, (match) => {
          const eqIdx = match.indexOf('=')
          return match.slice(0, eqIdx + 1) + '[REDACTED]'
        })
      }
    }
  }
  return data
}

// =============================================================================
// Tail handler — Pipeline-only (no Buffer DO to avoid feedback loop)
// =============================================================================

interface TailEnv {
  EVENTS_PIPELINE: { send(events: unknown[]): Promise<void> }
}

export default {
  async tail(events: TraceItem[], env: TailEnv): Promise<void> {
    if (events.length === 0) return

    const records: Record<string, unknown>[] = []

    for (const trace of events) {
      // Skip tail/otel workers to prevent infinite loops
      if (trace.scriptName === 'tail' || trace.scriptName === 'otel') continue

      const eventTime = trace.eventTimestamp ?? Date.now()
      const masked = maskTrace(trace) as Record<string, unknown>
      // Stamp flat per-request pricing into event data (emission-time pricing)
      masked.cost = 0
      masked.price = REQUEST_PRICE_MICROCENTS
      records.push({
        id: ulid(eventTime),
        source: trace.scriptName || 'tail',
        data: masked,
      })
    }

    if (records.length === 0) return

    await env.EVENTS_PIPELINE.send(records)
  },
}
