/**
 * CDCProcessorDO - Durable Object for CDC Event Processing
 *
 * Processes CDC events (created/updated/deleted), maintains state in SQLite,
 * and flushes deltas to R2 as Parquet files with debounced flushing.
 */

import { DurableObject } from 'cloudflare:workers'
import { writeDeltaFile } from './cdc-delta.js'
import type { CDCEvent, DeltaRecord } from './cdc-delta.js'
import {
  getString,
  getNumber,
  getBoolean,
  getOptionalString,
  getOptionalNumber,
  getJson,
  getOptionalJson,
  type SqlRow,
} from './sql-mapper.js'

// Re-export CDCEvent and type guard
export type { CDCEvent } from './cdc-delta.js'
export { isCdcEvent } from './types.js'

/**
 * Type guard for collection change events (CDC: insert/update/delete).
 */
export function isCollectionChangeEvent(event: { type?: string }): boolean {
  return typeof event.type === 'string' && event.type.startsWith('collection.')
}

// ============================================================================
// Types
// ============================================================================

/**
 * Environment bindings for CDCProcessorDO
 */
export interface Env {
  EVENTS_BUCKET: R2Bucket
}

/**
 * Current state of a document in the processor
 */
export interface DocumentState {
  docId: string
  doc: Record<string, unknown>
  version: number
  lastUpdated: string
  bookmark?: string | undefined
  deleted: boolean
}

/**
 * Delta file reference in manifest
 */
export interface DeltaRef {
  sequence: number
  path: string
  createdAt: string
  eventCount: number
  minTs: string
  maxTs: string
}

/**
 * Collection manifest with schema and delta references (processor-specific)
 */
export interface ProcessorManifest {
  collection: string
  schema?: Record<string, string> | undefined
  deltaSequence: number
  deltas: DeltaRef[]
  lastFlushAt: string | null
  lastSnapshotAt: string | null
  stats: {
    totalDocs: number
    totalEvents: number
    insertCount: number
    updateCount: number
    deleteCount: number
  }
}

/**
 * Processor state returned by getState()
 */
export interface ProcessorState {
  collection: string
  documents: DocumentState[]
  pendingCount: number
  lastEventTs: string | null
}

/**
 * Configuration options for CDCProcessorDO
 */
export interface CDCProcessorOptions {
  /** Number of pending events before auto-flush (default: Infinity - no auto-flush) */
  flushThreshold?: number | undefined
  /** Debounce time in ms before flush (default: 5000) */
  debounceMs?: number | undefined
}

/**
 * Internal pending delta record
 */
interface PendingDelta {
  id: number
  collection: string
  docId: string
  op: 'created' | 'updated' | 'deleted'
  data: Record<string, unknown> | null
  prev: Record<string, unknown> | null
  ts: string
  bookmark: string | null
}

/**
 * Internal manifest state
 */
interface InternalManifest {
  collection: string
  schema: Record<string, string> | null
  deltaSequence: number
  deltas: DeltaRef[]
  lastFlushAt: string | null
  lastSnapshotAt: string | null
  stats: {
    totalDocs: number
    totalEvents: number
    insertCount: number
    updateCount: number
    deleteCount: number
  }
}

// ============================================================================
// CDCProcessorDO Implementation
// ============================================================================

export class CDCProcessorDO extends DurableObject<Env> {
  private initialized = false
  private options: Required<CDCProcessorOptions>

  constructor(ctx: DurableObjectState, env: Env, options: CDCProcessorOptions = {}) {
    super(ctx, env)
    this.options = {
      flushThreshold: options.flushThreshold ?? Infinity,
      debounceMs: options.debounceMs ?? 5000,
    }
  }

  /**
   * Initialize SQLite tables if not already done
   */
  private ensureInitialized(): void {
    if (this.initialized) return

    // Create cdc_state table for current document state
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS cdc_state (
        collection TEXT NOT NULL,
        doc_id TEXT NOT NULL,
        data TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        deleted INTEGER NOT NULL DEFAULT 0,
        bookmark TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (collection, doc_id)
      )
    `)

    // Create pending_deltas table for unflushed changes
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS pending_deltas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        doc_id TEXT NOT NULL,
        op TEXT NOT NULL,
        data TEXT,
        prev TEXT,
        ts TEXT NOT NULL,
        bookmark TEXT
      )
    `)

    // Create manifests table for collection metadata
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS manifests (
        collection TEXT PRIMARY KEY,
        schema TEXT,
        delta_sequence INTEGER NOT NULL DEFAULT 0,
        deltas TEXT NOT NULL DEFAULT '[]',
        last_flush_at TEXT,
        last_snapshot_at TEXT,
        stats TEXT NOT NULL DEFAULT '{}'
      )
    `)

    this.initialized = true
  }

  /**
   * Get the namespace (DO name or 'default')
   */
  private getNamespace(): string {
    return this.ctx.id.name ?? 'default'
  }

  /**
   * Generates the path for a delta file
   * Format: {ns}/{collection}/deltas/{seq}_{timestamp}.parquet
   * Timestamp format: YYYY-MM-DDTHH-MM (without seconds)
   */
  private generateDeltaPath(ns: string, collection: string, seq: number, timestamp?: Date): string {
    const ts = timestamp ?? new Date()
    // Format: YYYY-MM-DDTHH-MM (matches test expectations)
    const year = ts.getUTCFullYear()
    const month = String(ts.getUTCMonth() + 1).padStart(2, '0')
    const day = String(ts.getUTCDate()).padStart(2, '0')
    const hours = String(ts.getUTCHours()).padStart(2, '0')
    const minutes = String(ts.getUTCMinutes()).padStart(2, '0')
    const formattedTs = `${year}-${month}-${day}T${hours}-${minutes}`

    // Zero-pad sequence number to at least 3 digits
    const paddedSeq = seq.toString().padStart(3, '0')

    return `${ns}/${collection}/deltas/${paddedSeq}_${formattedTs}.parquet`
  }

  /**
   * Get a document's state from SQLite
   */
  private getDocumentState(collection: string, docId: string): DocumentState | null {
    const rows = [...this.ctx.storage.sql.exec(
      `SELECT doc_id, data, version, updated_at, bookmark, deleted FROM cdc_state WHERE collection = ? AND doc_id = ?`,
      collection,
      docId,
    )]
    if (rows.length === 0) return null
    const row = rows[0] as SqlRow
    return {
      docId: getString(row, 'doc_id'),
      doc: getJson<Record<string, unknown>>(row, 'data'),
      version: getNumber(row, 'version'),
      lastUpdated: getString(row, 'updated_at'),
      bookmark: getOptionalString(row, 'bookmark') ?? undefined,
      deleted: getBoolean(row, 'deleted'),
    }
  }

  /**
   * Upsert a document's state into SQLite
   */
  private upsertDocumentState(collection: string, state: DocumentState): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO cdc_state (collection, doc_id, data, version, deleted, bookmark, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(collection, doc_id) DO UPDATE SET
         data = excluded.data,
         version = excluded.version,
         deleted = excluded.deleted,
         bookmark = excluded.bookmark,
         updated_at = excluded.updated_at`,
      collection,
      state.docId,
      JSON.stringify(state.doc),
      state.version,
      state.deleted ? 1 : 0,
      state.bookmark ?? null,
      state.lastUpdated,
      state.lastUpdated,
    )
  }

  /**
   * Get all documents for a collection from SQLite
   */
  private getCollectionDocuments(collection: string): DocumentState[] {
    const rows = [...this.ctx.storage.sql.exec(
      `SELECT doc_id, data, version, updated_at, bookmark, deleted FROM cdc_state WHERE collection = ?`,
      collection,
    )]
    return rows.map((row) => ({
      docId: getString(row as SqlRow, 'doc_id'),
      doc: getJson<Record<string, unknown>>(row as SqlRow, 'data'),
      version: getNumber(row as SqlRow, 'version'),
      lastUpdated: getString(row as SqlRow, 'updated_at'),
      bookmark: getOptionalString(row as SqlRow, 'bookmark') ?? undefined,
      deleted: getBoolean(row as SqlRow, 'deleted'),
    }))
  }

  /**
   * Process a batch of CDC events
   */
  async process(events: CDCEvent[]): Promise<{ processed: number; pending: number }> {
    this.ensureInitialized()

    if (events.length === 0) {
      return { processed: 0, pending: 0 }
    }

    for (const event of events) {
      await this.processEvent(event)
    }

    // Check if we need to auto-flush based on threshold
    const pendingCount = this.getPendingDeltaCount()
    if (pendingCount >= this.options.flushThreshold) {
      await this.flushInternal()
    } else if (pendingCount > 0) {
      // Schedule debounced flush
      await this.scheduleFlush()
    }

    return {
      processed: events.length,
      pending: this.getPendingDeltaCount(),
    }
  }

  /**
   * Process a single CDC event.
   * In the new Event format: data contains { type, id, ...fields }, meta may contain prev/bookmark.
   * The `type` field in data is the singular noun (e.g. 'contact', 'deal').
   */
  private async processEvent(event: CDCEvent): Promise<void> {
    const { type: _noun, id: docId, ...docFields } = event.data
    const noun = event.data.type
    const ts = event.ts
    const meta = event.meta as Record<string, unknown>
    const prev = meta.prev as Record<string, unknown> | undefined
    const bookmark = meta.bookmark as string | undefined
    const op = this.extractOp(event.event)

    // Get existing document state from SQLite (column named 'collection' for backward compat)
    const existing = this.getDocumentState(noun, docId)

    let newData: Record<string, unknown>
    let prevData: Record<string, unknown> | null = null
    let newVersion = 1
    let deleted = false

    if (op === 'created') {
      newData = docFields
      newVersion = existing ? existing.version + 1 : 1
      deleted = false
    } else if (op === 'updated') {
      newData = docFields
      prevData = existing ? existing.doc : (prev ?? null)
      newVersion = existing ? existing.version + 1 : 1
      deleted = false
    } else {
      // delete
      prevData = existing ? existing.doc : (prev ?? null)
      newData = prevData ?? {}
      newVersion = existing ? existing.version + 1 : 1
      deleted = true
    }

    // Update state in SQLite
    const docState: DocumentState = {
      docId,
      doc: newData,
      version: newVersion,
      lastUpdated: ts,
      bookmark: bookmark ?? undefined,
      deleted,
    }
    this.upsertDocumentState(noun, docState)

    // Add to pending_deltas in SQLite (column named 'collection' for backward compat)
    this.ctx.storage.sql.exec(
      `INSERT INTO pending_deltas (collection, doc_id, op, data, prev, ts, bookmark)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      noun,
      docId,
      op,
      JSON.stringify(newData),
      prevData ? JSON.stringify(prevData) : null,
      ts,
      bookmark ?? null,
    )

    // Ensure manifest exists for this noun
    this.ensureManifest(noun)
  }

  /**
   * Extract operation type from the event name.
   * Format: `{noun}.{op}` (e.g. 'contact.created')
   */
  private extractOp(eventName: string): 'created' | 'updated' | 'deleted' {
    const dot = eventName.lastIndexOf('.')
    const op = dot >= 0 ? eventName.slice(dot + 1) : eventName
    if (op === 'created' || op === 'updated' || op === 'deleted') {
      return op
    }
    throw new Error(`Unknown CDC operation in event name: ${eventName}`)
  }

  /**
   * Ensure a manifest exists for a collection
   */
  private ensureManifest(collection: string): void {
    const rows = [...this.ctx.storage.sql.exec(
      `SELECT collection FROM manifests WHERE collection = ?`,
      collection,
    )]
    if (rows.length === 0) {
      const defaultStats = JSON.stringify({
        totalDocs: 0,
        totalEvents: 0,
        insertCount: 0,
        updateCount: 0,
        deleteCount: 0,
      })
      this.ctx.storage.sql.exec(
        `INSERT INTO manifests (collection, schema, delta_sequence, deltas, last_flush_at, last_snapshot_at, stats)
         VALUES (?, NULL, 0, '[]', NULL, NULL, ?)`,
        collection,
        defaultStats,
      )
    }
  }

  /**
   * Load a manifest from SQLite
   */
  private loadManifest(collection: string): InternalManifest | null {
    const rows = [...this.ctx.storage.sql.exec(
      `SELECT collection, schema, delta_sequence, deltas, last_flush_at, last_snapshot_at, stats
       FROM manifests WHERE collection = ?`,
      collection,
    )]
    if (rows.length === 0) return null
    const row = rows[0] as SqlRow
    return {
      collection: getString(row, 'collection'),
      schema: getOptionalJson<Record<string, string>>(row, 'schema'),
      deltaSequence: getNumber(row, 'delta_sequence'),
      deltas: getJson<DeltaRef[]>(row, 'deltas'),
      lastFlushAt: getOptionalString(row, 'last_flush_at'),
      lastSnapshotAt: getOptionalString(row, 'last_snapshot_at'),
      stats: getJson<InternalManifest['stats']>(row, 'stats'),
    }
  }

  /**
   * Save a manifest to SQLite
   */
  private saveManifest(manifest: InternalManifest): void {
    this.ctx.storage.sql.exec(
      `UPDATE manifests SET schema = ?, delta_sequence = ?, deltas = ?, last_flush_at = ?, last_snapshot_at = ?, stats = ?
       WHERE collection = ?`,
      manifest.schema ? JSON.stringify(manifest.schema) : null,
      manifest.deltaSequence,
      JSON.stringify(manifest.deltas),
      manifest.lastFlushAt,
      manifest.lastSnapshotAt,
      JSON.stringify(manifest.stats),
      manifest.collection,
    )
  }

  /**
   * Get total count of pending deltas
   */
  private getPendingDeltaCount(): number {
    const rows = [...this.ctx.storage.sql.exec(`SELECT COUNT(*) as cnt FROM pending_deltas`)]
    return getNumber(rows[0] as SqlRow, 'cnt')
  }

  /**
   * Get all pending deltas from SQLite
   */
  private loadPendingDeltas(): PendingDelta[] {
    const rows = [...this.ctx.storage.sql.exec(
      `SELECT id, collection, doc_id, op, data, prev, ts, bookmark FROM pending_deltas ORDER BY id`,
    )]
    return rows.map((row) => ({
      id: getNumber(row as SqlRow, 'id'),
      collection: getString(row as SqlRow, 'collection'),
      docId: getString(row as SqlRow, 'doc_id'),
      op: getString(row as SqlRow, 'op') as 'created' | 'updated' | 'deleted',
      data: getOptionalJson<Record<string, unknown>>(row as SqlRow, 'data'),
      prev: getOptionalJson<Record<string, unknown>>(row as SqlRow, 'prev'),
      ts: getString(row as SqlRow, 'ts'),
      bookmark: getOptionalString(row as SqlRow, 'bookmark'),
    }))
  }

  /**
   * Clear all pending deltas from SQLite
   */
  private clearPendingDeltas(): void {
    this.ctx.storage.sql.exec(`DELETE FROM pending_deltas`)
  }

  /**
   * Schedule a debounced flush
   */
  private async scheduleFlush(): Promise<void> {
    const alarmTime = Date.now() + this.options.debounceMs
    await this.ctx.storage.setAlarm(alarmTime)
  }

  /**
   * Flush pending deltas to R2
   */
  async flush(force?: boolean): Promise<{ flushed: boolean; deltaPath?: string | undefined; eventCount?: number | undefined }> {
    this.ensureInitialized()

    // If force, cancel any pending alarm
    if (force) {
      await this.ctx.storage.deleteAlarm()
    }

    return this.flushInternal()
  }

  /**
   * Internal flush implementation
   */
  private async flushInternal(): Promise<{ flushed: boolean; deltaPath?: string | undefined; eventCount?: number | undefined }> {
    const pendingDeltas = this.loadPendingDeltas()
    if (pendingDeltas.length === 0) {
      return { flushed: false }
    }

    // Group deltas by collection
    const deltasByCollection = new Map<string, PendingDelta[]>()
    for (const delta of pendingDeltas) {
      const existing = deltasByCollection.get(delta.collection) ?? []
      existing.push(delta)
      deltasByCollection.set(delta.collection, existing)
    }

    let lastDeltaPath: string | undefined
    let totalEventCount = 0

    // Flush each collection separately
    for (const [collection, collectionDeltas] of deltasByCollection) {
      const result = await this.flushCollection(collection, collectionDeltas)
      if (result.deltaPath) {
        lastDeltaPath = result.deltaPath
        totalEventCount += result.eventCount
      }
    }

    // Clear pending deltas from SQLite
    this.clearPendingDeltas()

    return {
      flushed: true,
      deltaPath: lastDeltaPath,
      eventCount: totalEventCount,
    }
  }

  /**
   * Flush deltas for a single collection
   */
  private async flushCollection(
    collection: string,
    deltas: PendingDelta[]
  ): Promise<{ deltaPath: string; eventCount: number }> {
    // Get or create manifest
    this.ensureManifest(collection)
    const manifest = this.loadManifest(collection)!

    const newSequence = manifest.deltaSequence + 1

    // Convert to DeltaRecords
    const deltaRecords: DeltaRecord[] = deltas.map((d) => ({
      pk: d.docId,
      op: d.op,
      data: d.data,
      prev: d.prev,
      ts: d.ts,
      bookmark: d.bookmark,
    }))

    // Write delta file to R2
    const ns = this.getNamespace()
    const deltaPath = this.generateDeltaPath(ns, collection, newSequence, new Date())
    const parquetBuffer = writeDeltaFile(deltaRecords)

    await this.env.EVENTS_BUCKET.put(deltaPath, parquetBuffer, {
      httpMetadata: {
        contentType: 'application/vnd.apache.parquet',
      },
    })

    // Update manifest
    const timestamps = deltas.map((d) => d.ts).sort()

    const newDeltaRef: DeltaRef = {
      sequence: newSequence,
      path: deltaPath,
      createdAt: new Date().toISOString(),
      eventCount: deltas.length,
      minTs: timestamps[0] ?? '',
      maxTs: timestamps[timestamps.length - 1] ?? '',
    }
    manifest.deltas.push(newDeltaRef)
    manifest.deltaSequence = newSequence
    manifest.lastFlushAt = new Date().toISOString()

    // Update stats
    for (const delta of deltas) {
      manifest.stats.totalEvents++
      if (delta.op === 'created') manifest.stats.insertCount++
      else if (delta.op === 'updated') manifest.stats.updateCount++
      else if (delta.op === 'deleted') manifest.stats.deleteCount++
    }

    // Calculate totalDocs (non-deleted documents) from SQLite
    const countRows = [...this.ctx.storage.sql.exec(
      `SELECT COUNT(*) as cnt FROM cdc_state WHERE collection = ? AND deleted = 0`,
      collection,
    )]
    manifest.stats.totalDocs = getNumber(countRows[0] as SqlRow, 'cnt')

    // Infer schema from first document with data
    for (const delta of deltas) {
      if (delta.data) {
        manifest.schema = this.inferSchema(delta.data)
        break
      }
    }

    // Persist manifest to SQLite
    this.saveManifest(manifest)

    return { deltaPath, eventCount: deltas.length }
  }

  /**
   * Infer schema from a document
   */
  private inferSchema(doc: Record<string, unknown>): Record<string, string> {
    const schema: Record<string, string> = {}
    for (const [key, value] of Object.entries(doc)) {
      if (value === null) {
        schema[key] = 'null'
      } else if (Array.isArray(value)) {
        schema[key] = 'array'
      } else {
        schema[key] = typeof value
      }
    }
    return schema
  }

  /**
   * Get the current state of a collection
   */
  async getState(collection: string): Promise<ProcessorState> {
    this.ensureInitialized()

    const documents = this.getCollectionDocuments(collection)

    // Get pending count for this collection from SQLite
    const countRows = [...this.ctx.storage.sql.exec(
      `SELECT COUNT(*) as cnt FROM pending_deltas WHERE collection = ?`,
      collection,
    )]
    const pendingCount = getNumber(countRows[0] as SqlRow, 'cnt')

    // Get last event timestamp from pending deltas or documents
    let lastEventTs: string | null = null
    const tsRows = [...this.ctx.storage.sql.exec(
      `SELECT MAX(ts) as max_ts FROM pending_deltas WHERE collection = ?`,
      collection,
    )]
    const maxPendingTs = getOptionalString(tsRows[0] as SqlRow, 'max_ts')
    if (maxPendingTs) {
      lastEventTs = maxPendingTs
    } else if (documents.length > 0) {
      const timestamps = documents.map((d) => d.lastUpdated).sort()
      lastEventTs = timestamps[timestamps.length - 1] ?? null
    }

    return {
      collection,
      documents,
      pendingCount,
      lastEventTs,
    }
  }

  /**
   * Get the manifest for a collection
   */
  async getManifest(collection: string): Promise<ProcessorManifest | null> {
    this.ensureInitialized()

    const manifest = this.loadManifest(collection)

    if (!manifest || manifest.deltaSequence === 0) {
      return null
    }

    return {
      collection: manifest.collection,
      schema: manifest.schema ?? undefined,
      deltaSequence: manifest.deltaSequence,
      deltas: manifest.deltas,
      lastFlushAt: manifest.lastFlushAt,
      lastSnapshotAt: manifest.lastSnapshotAt,
      stats: manifest.stats,
    }
  }

  /**
   * Get a specific document by collection and ID
   */
  async getDocument(collection: string, docId: string): Promise<DocumentState | null> {
    this.ensureInitialized()
    return this.getDocumentState(collection, docId)
  }

  /**
   * Alarm handler - triggers flush
   */
  async alarm(): Promise<void> {
    await this.flushInternal()
  }
}
