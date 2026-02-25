/**
 * Shared event emission — canonical shape construction + pipeline send.
 *
 * Application code uses this instead of constructing event records directly.
 * Only infrastructure producers (tail worker, otel snippet, ingest endpoint,
 * CDC events) should construct events manually.
 *
 * Usage:
 *   import { emit } from '@dotdo/events'
 *
 *   await emit(env.EVENTS_PIPELINE, {
 *     ns: 'api.headless.ly',
 *     type: 'source_call',
 *     event: 'source_call',
 *     source: 'proxy',
 *     data: { provider, model, tokens, latencyMs },
 *   })
 */

import { ulid } from './ulid.js'

export interface EmitEvent {
  ns: string
  type: string
  event: string
  source: string
  data: Record<string, unknown>
  ray?: string
  domain?: string
  url?: string
  actor?: Record<string, unknown>
  meta?: Record<string, unknown>
  /** Optional timestamp for ULID — defaults to Date.now() */
  timestamp?: number
}

export interface Pipeline {
  send(events: unknown[]): Promise<void>
}

/**
 * Emit a single event to the pipeline with canonical shape.
 *
 * Handles ULID generation, canonical shape construction, and error catching.
 * Errors are caught and logged — event emission should never break the caller.
 */
export async function emit(pipeline: Pipeline, event: EmitEvent): Promise<void> {
  try {
    await pipeline.send([{
      id: ulid(event.timestamp),
      ray: event.ray ?? '',
      ns: event.ns,
      domain: event.domain ?? '',
      type: event.type,
      event: event.event,
      url: event.url ?? '',
      source: event.source,
      actor: event.actor ?? {},
      data: event.data,
      meta: event.meta ?? {},
    }])
  } catch (err) {
    console.error('[emit] Pipeline send failed:', err)
  }
}

/**
 * Emit multiple events to the pipeline in a single batch.
 */
export async function emitBatch(pipeline: Pipeline, events: EmitEvent[]): Promise<void> {
  if (events.length === 0) return
  try {
    await pipeline.send(events.map((event) => ({
      id: ulid(event.timestamp),
      ray: event.ray ?? '',
      ns: event.ns,
      domain: event.domain ?? '',
      type: event.type,
      event: event.event,
      url: event.url ?? '',
      source: event.source,
      actor: event.actor ?? {},
      data: event.data,
      meta: event.meta ?? {},
    })))
  } catch (err) {
    console.error('[emitBatch] Pipeline send failed:', err)
  }
}

// =============================================================================
// Unified emission — Pipeline (durable) + BufferDO (real-time ClickHouse)
// =============================================================================

export interface EmitEnv {
  EVENTS_PIPELINE?: Pipeline
  EVENTS?: { ingest(events: Record<string, unknown>[]): Promise<void> }
}

export function formatRecord(event: EmitEvent): Record<string, unknown> {
  return {
    id: ulid(event.timestamp),
    ray: event.ray ?? '',
    ns: event.ns,
    domain: event.domain ?? '',
    type: event.type,
    event: event.event,
    url: event.url ?? '',
    source: event.source,
    actor: event.actor ?? {},
    data: event.data,
    meta: event.meta ?? {},
  }
}

/**
 * Unified event emission — Pipeline (durable) + BufferDO (real-time CH).
 * Either binding can be absent. Errors caught and logged.
 * BufferDO writes are non-blocking when ctx is provided.
 */
export async function emitEvents(
  env: EmitEnv,
  events: EmitEvent | EmitEvent[],
  ctx?: { waitUntil(p: Promise<unknown>): void },
): Promise<void> {
  const batch = Array.isArray(events) ? events : [events]
  if (batch.length === 0) return
  const records = batch.map(formatRecord)
  await _sendRecords(env, records, ctx)
}

/**
 * Emit pre-formatted records (for CDC and other producers that build their own shape).
 */
export async function emitRawRecords(
  env: EmitEnv,
  records: Record<string, unknown>[],
  ctx?: { waitUntil(p: Promise<unknown>): void },
): Promise<void> {
  if (records.length === 0) return
  await _sendRecords(env, records, ctx)
}

async function _sendRecords(
  env: EmitEnv,
  records: Record<string, unknown>[],
  ctx?: { waitUntil(p: Promise<unknown>): void },
): Promise<void> {
  if (env.EVENTS_PIPELINE) {
    try { await env.EVENTS_PIPELINE.send(records) }
    catch (err) { console.error('[emit] Pipeline send failed:', err) }
  }
  if (env.EVENTS) {
    const p = env.EVENTS.ingest(records).catch((err: unknown) =>
      console.error('[emit] BufferDO ingest failed:', err))
    ctx ? ctx.waitUntil(p) : await p
  }
}
