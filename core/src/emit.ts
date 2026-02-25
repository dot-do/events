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
