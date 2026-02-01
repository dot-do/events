/**
 * Event Writer - Unified Parquet writer for events
 *
 * Used by both:
 * - Webhook handler (webhooks/ingest events)
 * - Tail worker (worker traces)
 *
 * Schema design:
 * - Typed columns for filtering: ts, type, source, etc. (with statistics)
 * - VARIANT column for payload (efficient semi-structured storage)
 */

import { parquetWriteBuffer, createVariantColumn } from 'hyparquet-writer'
import { ulid } from '../core/src/ulid'

export { ulid }

// ============================================================================
// Types
// ============================================================================

export interface EventRecord {
  // Core fields (typed columns with statistics)
  ts: string              // ISO timestamp
  type: string            // Event type (e.g., "webhook.github.push")
  source?: string         // Event source (e.g., "github", "stripe", "tail")

  // Optional typed fields
  provider?: string       // Webhook provider
  eventType?: string      // Original event type from provider
  verified?: boolean      // Signature verification status
  scriptName?: string     // Worker name (for tail events)
  outcome?: string        // ok, exception, etc. (for tail events)
  method?: string         // HTTP method
  url?: string            // Request URL
  statusCode?: number     // HTTP status
  durationMs?: number     // Request duration

  // Full payload (stored as VARIANT)
  payload?: unknown
}

export interface WriteResult {
  key: string
  events: number
  bytes: number
  cpuMs: number
}

// ============================================================================
// Event Writer
// ============================================================================

/**
 * Write events to R2 as Parquet with typed columns + VARIANT payload
 */
export async function writeEvents(
  bucket: R2Bucket,
  prefix: string,
  events: EventRecord[]
): Promise<WriteResult> {
  if (events.length === 0) {
    throw new Error('No events to write')
  }

  const startCpu = performance.now()

  // Build VARIANT column for payload (binary semi-structured storage)
  const payloadVariant = createVariantColumn('payload', events.map(e => e.payload ?? null))

  // Explicit schema: typed columns + VARIANT group
  const numTypedColumns = 12
  const schema = [
    { name: 'root', num_children: numTypedColumns + 1 }, // +1 for payload VARIANT group
    // Primary index columns
    { name: 'ts', type: 'INT64' as const, converted_type: 'TIMESTAMP_MILLIS' as const, repetition_type: 'REQUIRED' as const },
    { name: 'type', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'REQUIRED' as const },
    { name: 'source', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    // Webhook-specific columns
    { name: 'provider', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'event_type', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'verified', type: 'BOOLEAN' as const, repetition_type: 'OPTIONAL' as const },
    // Tail worker columns
    { name: 'script_name', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'outcome', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'method', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'url', type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'status_code', type: 'INT32' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'duration_ms', type: 'DOUBLE' as const, repetition_type: 'OPTIONAL' as const },
    // VARIANT group for payload
    ...payloadVariant.schema,
  ]

  // Column data (no type - schema is explicit)
  const columnData = [
    { name: 'ts', data: events.map(e => new Date(e.ts)) },
    { name: 'type', data: events.map(e => e.type) },
    { name: 'source', data: events.map(e => e.source ?? null) },
    { name: 'provider', data: events.map(e => e.provider ?? null) },
    { name: 'event_type', data: events.map(e => e.eventType ?? null) },
    { name: 'verified', data: events.map(e => e.verified ?? null) },
    { name: 'script_name', data: events.map(e => e.scriptName ?? null) },
    { name: 'outcome', data: events.map(e => e.outcome ?? null) },
    { name: 'method', data: events.map(e => e.method ?? null) },
    { name: 'url', data: events.map(e => e.url ?? null) },
    { name: 'status_code', data: events.map(e => e.statusCode ?? null) },
    { name: 'duration_ms', data: events.map(e => e.durationMs ?? null) },
    { name: 'payload', data: payloadVariant.data },
  ]

  // Write Parquet with SNAPPY compression
  const buffer = parquetWriteBuffer({
    schema,
    columnData,
    statistics: true,
    codec: 'SNAPPY',
  })

  const cpuMs = performance.now() - startCpu

  // Generate time-partitioned path
  const now = new Date()
  const datePath = [
    now.getUTCFullYear(),
    String(now.getUTCMonth() + 1).padStart(2, '0'),
    String(now.getUTCDate()).padStart(2, '0'),
    String(now.getUTCHours()).padStart(2, '0'),
  ].join('/')

  const key = `${prefix}/${datePath}/${ulid()}.parquet`

  // Get timestamp range from events
  const timestamps = events.map(e => new Date(e.ts).getTime())
  const minTs = Math.min(...timestamps)
  const maxTs = Math.max(...timestamps)

  // Get unique types for metadata
  const types = [...new Set(events.map(e => e.type))]

  await bucket.put(key, buffer, {
    customMetadata: {
      events: String(events.length),
      bytes: String(buffer.byteLength),
      bytesPerEvent: String(Math.round(buffer.byteLength / events.length)),
      cpuMs: cpuMs.toFixed(2),
      minTs: String(minTs),
      maxTs: String(maxTs),
      types: types.slice(0, 10).join(','), // First 10 types
      format: 'parquet-v1',
    },
  })

  console.log(`[WRITE] ${key} events=${events.length} bytes=${buffer.byteLength} cpuMs=${cpuMs.toFixed(2)}`)

  return { key, events: events.length, bytes: buffer.byteLength, cpuMs }
}

// ============================================================================
// Event Buffer - For batching events before writing
// ============================================================================

export class EventBuffer {
  private buffer: EventRecord[] = []
  private lastFlushTime = Date.now()
  private flushPending = false
  private flushPromise: Promise<void> | null = null

  constructor(
    private bucket: R2Bucket,
    private prefix: string,
    private options: {
      countThreshold?: number
      timeThresholdMs?: number
      maxBufferSize?: number
    } = {}
  ) {}

  get countThreshold() { return this.options.countThreshold ?? 50 }
  get timeThresholdMs() { return this.options.timeThresholdMs ?? 5000 }
  get maxBufferSize() { return this.options.maxBufferSize ?? 10000 }
  get length() { return this.buffer.length }

  /**
   * Add events to the buffer
   */
  add(events: EventRecord | EventRecord[]): void {
    const toAdd = Array.isArray(events) ? events : [events]
    this.buffer.push(...toAdd)
  }

  /**
   * Check if buffer should be flushed
   */
  shouldFlush(): boolean {
    return (
      this.buffer.length >= this.countThreshold ||
      this.buffer.length >= this.maxBufferSize ||
      (Date.now() - this.lastFlushTime) >= this.timeThresholdMs
    )
  }

  /**
   * Get buffer stats
   */
  stats() {
    return {
      buffered: this.buffer.length,
      lastFlushTime: new Date(this.lastFlushTime).toISOString(),
      timeSinceFlush: Date.now() - this.lastFlushTime,
      flushPending: this.flushPending,
    }
  }

  /**
   * Flush buffer to R2
   */
  async flush(): Promise<WriteResult | null> {
    if (this.buffer.length === 0) {
      this.flushPending = false
      return null
    }

    const events = this.buffer
    this.buffer = []
    this.lastFlushTime = Date.now()
    this.flushPending = false

    return writeEvents(this.bucket, this.prefix, events)
  }

  /**
   * Schedule a deferred flush using ctx.waitUntil
   */
  scheduleFlush(ctx: ExecutionContext): void {
    if (this.flushPending) return
    this.flushPending = true

    this.flushPromise = new Promise<void>(resolve => {
      setTimeout(async () => {
        await this.flush()
        resolve()
      }, this.timeThresholdMs)
    })

    ctx.waitUntil(this.flushPromise)
  }

  /**
   * Flush if needed, otherwise schedule deferred flush
   */
  async maybeFlush(ctx: ExecutionContext): Promise<WriteResult | null> {
    if (this.shouldFlush()) {
      const result = await this.flush()
      ctx.waitUntil(Promise.resolve()) // Keep isolate alive briefly
      return result
    }
    this.scheduleFlush(ctx)
    return null
  }
}

// ============================================================================
// Module-level buffers for different event sources
// ============================================================================

// These are created lazily when needed
const buffers = new Map<string, EventBuffer>()

export function getEventBuffer(
  bucket: R2Bucket,
  prefix: string,
  options?: {
    countThreshold?: number
    timeThresholdMs?: number
    maxBufferSize?: number
  }
): EventBuffer {
  const key = prefix
  let buffer = buffers.get(key)
  if (!buffer) {
    buffer = new EventBuffer(bucket, prefix, options)
    buffers.set(key, buffer)
  }
  return buffer
}
