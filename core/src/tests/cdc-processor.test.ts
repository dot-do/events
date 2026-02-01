/**
 * CDCProcessorDO Tests
 *
 * TDD Red Phase: Tests for CDC (Change Data Capture) data file processing
 *
 * The CDCProcessorDO maintains current-state Parquet files from CDC events.
 * Storage layout:
 * ```
 * {ns}/{collection}/
 *   data.parquet           # Current consolidated state (sorted by PK)
 *   deltas/
 *     001_2024-01-31T10-00.parquet
 *     002_2024-01-31T10-05.parquet
 *   snapshots/
 *     2024-01-31.parquet   # Daily snapshot
 *     2024-01.parquet      # Monthly snapshot
 *   manifest.json          # Schema, stats, delta refs
 * ```
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import type { CollectionChangeEvent } from '../types.js'

// ============================================================================
// Types
// ============================================================================

/**
 * CDC event for processing (subset of CollectionChangeEvent with required fields)
 */
interface CDCEvent {
  type: 'collection.insert' | 'collection.update' | 'collection.delete'
  ts: string
  collection: string
  docId: string
  doc?: Record<string, unknown>
  prev?: Record<string, unknown>
  bookmark?: string
  do: {
    id: string
    name?: string
    class?: string
  }
}

/**
 * Current state of a document in the processor
 */
interface DocumentState {
  docId: string
  doc: Record<string, unknown>
  version: number
  lastUpdated: string
  bookmark?: string
  deleted: boolean
}

/**
 * Delta file reference in manifest
 */
interface DeltaRef {
  sequence: number
  path: string
  createdAt: string
  eventCount: number
  minTs: string
  maxTs: string
}

/**
 * Collection manifest with schema and delta references
 */
interface CollectionManifest {
  collection: string
  schema?: Record<string, string>
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
interface ProcessorState {
  collection: string
  documents: DocumentState[]
  pendingCount: number
  lastEventTs: string | null
}

/**
 * CDCProcessorDO interface (to be implemented)
 */
interface CDCProcessorDO {
  process(events: CDCEvent[]): Promise<{ processed: number; pending: number }>
  flush(force?: boolean): Promise<{ flushed: boolean; deltaPath?: string; eventCount?: number }>
  getState(collection: string): Promise<ProcessorState>
  getManifest(collection: string): Promise<CollectionManifest | null>
  getDocument(collection: string, docId: string): Promise<DocumentState | null>
}

// ============================================================================
// Mock Factories
// ============================================================================

/**
 * Creates a mock DurableObjectId
 */
function createMockDurableObjectId(id = 'cdc-processor-test', name?: string): DurableObjectId {
  return {
    toString: () => id,
    equals: (other: DurableObjectId) => other.toString() === id,
    name,
  } as DurableObjectId
}

/**
 * Creates a mock SqlStorage with in-memory tables
 */
function createMockSqlStorage() {
  const tables = new Map<string, Map<string, Record<string, unknown>>>()
  const execFn = vi.fn((query: string, ...params: unknown[]) => {
    // Track queries for assertions
    return {
      toArray: () => [],
      changes: 0,
    }
  })

  return {
    exec: execFn,
    _tables: tables,
    _queries: [] as string[],
  }
}

/**
 * Creates a mock DurableObjectStorage with SQLite support
 */
function createMockStorage() {
  const storage = new Map<string, unknown>()
  let alarm: number | null = null
  const sql = createMockSqlStorage()

  return {
    get: vi.fn(async <T>(key: string): Promise<T | undefined> => {
      return storage.get(key) as T | undefined
    }),
    put: vi.fn(async (key: string, value: unknown) => {
      storage.set(key, value)
    }),
    delete: vi.fn(async (key: string | string[]) => {
      if (Array.isArray(key)) {
        let deleted = 0
        for (const k of key) {
          if (storage.delete(k)) deleted++
        }
        return deleted
      }
      return storage.delete(key)
    }),
    list: vi.fn(async () => storage),
    getAlarm: vi.fn(async () => alarm),
    setAlarm: vi.fn(async (time: number | Date) => {
      alarm = typeof time === 'number' ? time : time.getTime()
    }),
    deleteAlarm: vi.fn(async () => {
      alarm = null
    }),
    sql,
    // Expose internal storage for test assertions
    _storage: storage,
    _getAlarm: () => alarm,
    getCurrentBookmark: vi.fn(() => 'bookmark-test-123'),
  }
}

/**
 * Creates a mock DurableObjectState (ctx)
 */
function createMockCtx(options: { id?: string; name?: string } = {}) {
  const storage = createMockStorage()

  return {
    id: createMockDurableObjectId(options.id ?? 'cdc-processor-test', options.name),
    storage,
    waitUntil: vi.fn((promise: Promise<unknown>) => promise),
    blockConcurrencyWhile: vi.fn(<T>(fn: () => Promise<T>) => fn()),
  } as unknown as DurableObjectState & { storage: ReturnType<typeof createMockStorage> }
}

/**
 * Creates a mock R2Bucket for delta/snapshot storage
 */
function createMockR2Bucket() {
  const objects = new Map<string, { body: string; metadata: R2HTTPMetadata; customMetadata: Record<string, string> }>()

  return {
    put: vi.fn(async (key: string, body: string | ArrayBuffer | ReadableStream, options?: R2PutOptions) => {
      const bodyStr = typeof body === 'string' ? body : body instanceof ArrayBuffer ? new TextDecoder().decode(body) : ''
      objects.set(key, {
        body: bodyStr,
        metadata: options?.httpMetadata ?? {},
        customMetadata: options?.customMetadata ?? {},
      })
      return {
        key,
        version: 'v1',
        size: bodyStr.length,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        httpMetadata: options?.httpMetadata ?? {},
        customMetadata: options?.customMetadata ?? {},
        checksums: { md5: new ArrayBuffer(16) },
      } as R2Object
    }),
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        version: 'v1',
        size: obj.body.length,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        httpMetadata: obj.metadata,
        customMetadata: obj.customMetadata,
        checksums: { md5: new ArrayBuffer(16) },
        body: new ReadableStream(),
        text: async () => obj.body,
        json: async () => JSON.parse(obj.body),
        arrayBuffer: async () => new TextEncoder().encode(obj.body).buffer,
        blob: async () => new Blob([obj.body]),
      } as R2ObjectBody
    }),
    delete: vi.fn(async (key: string) => objects.delete(key)),
    list: vi.fn(async (options?: { prefix?: string }) => ({
      objects: Array.from(objects.entries())
        .filter(([key]) => !options?.prefix || key.startsWith(options.prefix))
        .map(([key, obj]) => ({
          key,
          size: obj.body.length,
          etag: 'test-etag',
          uploaded: new Date(),
        })),
      truncated: false,
    })),
    head: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        version: 'v1',
        size: obj.body.length,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        httpMetadata: obj.metadata,
        customMetadata: obj.customMetadata,
        checksums: { md5: new ArrayBuffer(16) },
      } as R2Object
    }),
    // Expose internal storage for test assertions
    _objects: objects,
  } as unknown as R2Bucket & { _objects: Map<string, { body: string; metadata: R2HTTPMetadata; customMetadata: Record<string, string> }> }
}

/**
 * Creates a sample CDC insert event
 */
function createInsertEvent(collection: string, docId: string, doc: Record<string, unknown>, ts?: string): CDCEvent {
  return {
    type: 'collection.insert',
    ts: ts ?? new Date().toISOString(),
    collection,
    docId,
    doc,
    bookmark: `bookmark-${Date.now()}`,
    do: { id: 'source-do-123' },
  }
}

/**
 * Creates a sample CDC update event
 */
function createUpdateEvent(
  collection: string,
  docId: string,
  doc: Record<string, unknown>,
  prev: Record<string, unknown>,
  ts?: string
): CDCEvent {
  return {
    type: 'collection.update',
    ts: ts ?? new Date().toISOString(),
    collection,
    docId,
    doc,
    prev,
    bookmark: `bookmark-${Date.now()}`,
    do: { id: 'source-do-123' },
  }
}

/**
 * Creates a sample CDC delete event
 */
function createDeleteEvent(
  collection: string,
  docId: string,
  prev: Record<string, unknown>,
  ts?: string
): CDCEvent {
  return {
    type: 'collection.delete',
    ts: ts ?? new Date().toISOString(),
    collection,
    docId,
    prev,
    bookmark: `bookmark-${Date.now()}`,
    do: { id: 'source-do-123' },
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('CDCProcessorDO', () => {
  let mockCtx: ReturnType<typeof createMockCtx>
  let mockR2: ReturnType<typeof createMockR2Bucket>
  let mockEnv: { EVENTS_BUCKET: R2Bucket }

  beforeEach(() => {
    mockCtx = createMockCtx({ id: 'cdc-processor-test', name: 'default' })
    mockR2 = createMockR2Bucket()
    mockEnv = { EVENTS_BUCKET: mockR2 as unknown as R2Bucket }

    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-31T10:00:00.000Z'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  // ==========================================================================
  // Initialization Tests
  // ==========================================================================

  describe('Initialization', () => {
    it('creates SQLite tables on first use (cdc_state, pending_deltas, manifests)', async () => {
      // Import and instantiate the CDCProcessorDO
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      // Process a single event to trigger initialization
      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Verify SQLite tables were created
      const execCalls = mockCtx.storage.sql.exec.mock.calls
      const createTableQueries = execCalls.filter((call: unknown[]) =>
        (call[0] as string).includes('CREATE TABLE')
      )

      // Should have created cdc_state table
      expect(createTableQueries.some((call: unknown[]) =>
        (call[0] as string).includes('cdc_state')
      )).toBe(true)

      // Should have created pending_deltas table
      expect(createTableQueries.some((call: unknown[]) =>
        (call[0] as string).includes('pending_deltas')
      )).toBe(true)

      // Should have created manifests table
      expect(createTableQueries.some((call: unknown[]) =>
        (call[0] as string).includes('manifests')
      )).toBe(true)
    })

    it('returns empty state for new collection', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const state = await processor.getState('non_existent_collection')

      expect(state).toEqual({
        collection: 'non_existent_collection',
        documents: [],
        pendingCount: 0,
        lastEventTs: null,
      })
    })

    it('returns null manifest for new collection', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const manifest = await processor.getManifest('non_existent_collection')

      expect(manifest).toBeNull()
    })
  })

  // ==========================================================================
  // CDC Event Processing Tests
  // ==========================================================================

  describe('CDC Event Processing', () => {
    describe('Insert events', () => {
      it('processes insert event - adds to pending state', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        const event = createInsertEvent('users', 'user-1', {
          name: 'Alice',
          email: 'alice@example.com',
        })

        const result = await processor.process([event])

        expect(result.processed).toBe(1)
        expect(result.pending).toBeGreaterThan(0)

        // Verify document is in state
        const state = await processor.getState('users')
        expect(state.documents).toHaveLength(1)
        expect(state.documents[0].docId).toBe('user-1')
        expect(state.documents[0].doc).toEqual({
          name: 'Alice',
          email: 'alice@example.com',
        })
        expect(state.documents[0].deleted).toBe(false)
      })

      it('tracks correct version for inserted document', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
        ])

        const state = await processor.getState('users')
        expect(state.documents[0].version).toBe(1)
      })

      it('stores bookmark with inserted document', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        const event = createInsertEvent('users', 'user-1', { name: 'Alice' })
        event.bookmark = 'bookmark-abc-123'

        await processor.process([event])

        const state = await processor.getState('users')
        expect(state.documents[0].bookmark).toBe('bookmark-abc-123')
      })
    })

    describe('Update events', () => {
      it('processes update event - modifies existing record', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        // First insert
        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice', active: true }),
        ])

        // Then update
        await processor.process([
          createUpdateEvent(
            'users',
            'user-1',
            { name: 'Alice Smith', active: false },
            { name: 'Alice', active: true }
          ),
        ])

        const state = await processor.getState('users')
        expect(state.documents).toHaveLength(1)
        expect(state.documents[0].doc).toEqual({
          name: 'Alice Smith',
          active: false,
        })
      })

      it('increments version on update', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
        ])
        await processor.process([
          createUpdateEvent('users', 'user-1', { name: 'Bob' }, { name: 'Alice' }),
        ])

        const state = await processor.getState('users')
        expect(state.documents[0].version).toBe(2)
      })

      it('updates lastUpdated timestamp on update', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        const insertTs = '2024-01-31T09:00:00.000Z'
        const updateTs = '2024-01-31T10:00:00.000Z'

        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }, insertTs),
        ])
        await processor.process([
          createUpdateEvent('users', 'user-1', { name: 'Bob' }, { name: 'Alice' }, updateTs),
        ])

        const state = await processor.getState('users')
        expect(state.documents[0].lastUpdated).toBe(updateTs)
      })

      it('handles update event for document not yet seen (upsert behavior)', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        // Update without prior insert (should upsert)
        await processor.process([
          createUpdateEvent(
            'users',
            'user-1',
            { name: 'Alice' },
            { name: 'Unknown' }
          ),
        ])

        const state = await processor.getState('users')
        expect(state.documents).toHaveLength(1)
        expect(state.documents[0].doc).toEqual({ name: 'Alice' })
      })
    })

    describe('Delete events', () => {
      it('processes delete event - marks record as deleted', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        // Insert then delete
        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
        ])
        await processor.process([
          createDeleteEvent('users', 'user-1', { name: 'Alice' }),
        ])

        const state = await processor.getState('users')
        expect(state.documents).toHaveLength(1)
        expect(state.documents[0].deleted).toBe(true)
      })

      it('preserves document data in deleted state for audit', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
        ])
        await processor.process([
          createDeleteEvent('users', 'user-1', { name: 'Alice' }),
        ])

        const doc = await processor.getDocument('users', 'user-1')
        expect(doc).not.toBeNull()
        expect(doc?.deleted).toBe(true)
        // Previous state should be preserved
        expect(doc?.doc).toEqual({ name: 'Alice' })
      })

      it('increments version on delete', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
        ])
        await processor.process([
          createDeleteEvent('users', 'user-1', { name: 'Alice' }),
        ])

        const doc = await processor.getDocument('users', 'user-1')
        expect(doc?.version).toBe(2)
      })

      it('handles delete event for document not yet seen (tombstone)', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        // Delete without prior insert (should create tombstone)
        await processor.process([
          createDeleteEvent('users', 'user-1', { name: 'Unknown' }),
        ])

        const doc = await processor.getDocument('users', 'user-1')
        expect(doc).not.toBeNull()
        expect(doc?.deleted).toBe(true)
      })
    })

    describe('Batch processing', () => {
      it('handles batch of mixed operations', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        const events: CDCEvent[] = [
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
          createInsertEvent('users', 'user-2', { name: 'Bob' }),
          createUpdateEvent('users', 'user-1', { name: 'Alice Smith' }, { name: 'Alice' }),
          createDeleteEvent('users', 'user-2', { name: 'Bob' }),
          createInsertEvent('orders', 'order-1', { product: 'Widget', qty: 5 }),
        ]

        const result = await processor.process(events)

        expect(result.processed).toBe(5)

        // Check users collection
        const usersState = await processor.getState('users')
        expect(usersState.documents).toHaveLength(2)

        const user1 = usersState.documents.find(d => d.docId === 'user-1')
        expect(user1?.doc).toEqual({ name: 'Alice Smith' })
        expect(user1?.deleted).toBe(false)

        const user2 = usersState.documents.find(d => d.docId === 'user-2')
        expect(user2?.deleted).toBe(true)

        // Check orders collection
        const ordersState = await processor.getState('orders')
        expect(ordersState.documents).toHaveLength(1)
        expect(ordersState.documents[0].doc).toEqual({ product: 'Widget', qty: 5 })
      })

      it('processes events in order within batch', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        // Multiple updates to same document in one batch
        const events: CDCEvent[] = [
          createInsertEvent('users', 'user-1', { name: 'A', version: 1 }, '2024-01-31T10:00:00Z'),
          createUpdateEvent('users', 'user-1', { name: 'B', version: 2 }, { name: 'A', version: 1 }, '2024-01-31T10:00:01Z'),
          createUpdateEvent('users', 'user-1', { name: 'C', version: 3 }, { name: 'B', version: 2 }, '2024-01-31T10:00:02Z'),
        ]

        await processor.process(events)

        const state = await processor.getState('users')
        expect(state.documents[0].doc).toEqual({ name: 'C', version: 3 })
        expect(state.documents[0].version).toBe(3)
      })

      it('maintains correct timestamps and bookmarks for each event', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        const ts1 = '2024-01-31T10:00:00.000Z'
        const ts2 = '2024-01-31T10:00:05.000Z'
        const bookmark1 = 'bookmark-001'
        const bookmark2 = 'bookmark-002'

        const event1 = createInsertEvent('users', 'user-1', { name: 'Alice' }, ts1)
        event1.bookmark = bookmark1

        const event2 = createUpdateEvent('users', 'user-1', { name: 'Bob' }, { name: 'Alice' }, ts2)
        event2.bookmark = bookmark2

        await processor.process([event1, event2])

        const doc = await processor.getDocument('users', 'user-1')
        expect(doc?.lastUpdated).toBe(ts2)
        expect(doc?.bookmark).toBe(bookmark2)
      })
    })

    describe('Multi-collection support', () => {
      it('processes events for multiple collections independently', async () => {
        const { CDCProcessorDO } = await import('../cdc-processor.js')
        const processor = new CDCProcessorDO(mockCtx, mockEnv)

        await processor.process([
          createInsertEvent('users', 'user-1', { name: 'Alice' }),
          createInsertEvent('orders', 'order-1', { amount: 100 }),
          createInsertEvent('products', 'prod-1', { sku: 'ABC' }),
        ])

        const usersState = await processor.getState('users')
        const ordersState = await processor.getState('orders')
        const productsState = await processor.getState('products')

        expect(usersState.documents).toHaveLength(1)
        expect(ordersState.documents).toHaveLength(1)
        expect(productsState.documents).toHaveLength(1)
      })
    })
  })

  // ==========================================================================
  // Delta Flush Tests
  // ==========================================================================

  describe('Delta Flush', () => {
    it('flushes pending changes to delta file when threshold reached', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { flushThreshold: 3 })

      // Add events up to threshold
      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
        createInsertEvent('users', 'user-3', { name: 'Charlie' }),
      ])

      // Check that R2 put was called
      expect(mockR2.put).toHaveBeenCalled()

      // Verify delta file path format
      const putCalls = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls
      const deltaPath = putCalls[0]?.[0] as string
      expect(deltaPath).toMatch(/^default\/users\/deltas\/\d{3}_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}\.parquet$/)
    })

    it('generates correct delta file path with sequence number', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { flushThreshold: 1 })

      // First flush
      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Second flush
      await processor.process([createInsertEvent('users', 'user-2', { name: 'Bob' })])

      const putCalls = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls
      const path1 = putCalls[0]?.[0] as string
      const path2 = putCalls[1]?.[0] as string

      // First delta should be 001
      expect(path1).toContain('/001_')
      // Second delta should be 002
      expect(path2).toContain('/002_')
    })

    it('clears pending state after flush', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { flushThreshold: 2 })

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
      ])

      const state = await processor.getState('users')
      expect(state.pendingCount).toBe(0)
    })

    it('updates manifest with delta reference', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { flushThreshold: 2 })

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }, '2024-01-31T10:00:00Z'),
        createInsertEvent('users', 'user-2', { name: 'Bob' }, '2024-01-31T10:00:05Z'),
      ])

      const manifest = await processor.getManifest('users')

      expect(manifest).not.toBeNull()
      expect(manifest?.deltas).toHaveLength(1)
      expect(manifest?.deltas[0].sequence).toBe(1)
      expect(manifest?.deltas[0].eventCount).toBe(2)
      expect(manifest?.deltas[0].minTs).toBe('2024-01-31T10:00:00Z')
      expect(manifest?.deltas[0].maxTs).toBe('2024-01-31T10:00:05Z')
    })

    it('updates manifest stats after flush', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { flushThreshold: 5 })

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
        createUpdateEvent('users', 'user-1', { name: 'Alice Smith' }, { name: 'Alice' }),
        createDeleteEvent('users', 'user-2', { name: 'Bob' }),
        createInsertEvent('users', 'user-3', { name: 'Charlie' }),
      ])

      const manifest = await processor.getManifest('users')

      expect(manifest?.stats.totalEvents).toBe(5)
      expect(manifest?.stats.insertCount).toBe(3)
      expect(manifest?.stats.updateCount).toBe(1)
      expect(manifest?.stats.deleteCount).toBe(1)
      expect(manifest?.stats.totalDocs).toBe(2) // user-1 and user-3 (user-2 deleted)
    })

    it('flush returns correct result with delta path and event count', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
      ])

      const result = await processor.flush()

      expect(result.flushed).toBe(true)
      expect(result.deltaPath).toBeDefined()
      expect(result.deltaPath).toMatch(/\.parquet$/)
      expect(result.eventCount).toBe(2)
    })

    it('flush returns flushed: false when no pending events', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const result = await processor.flush()

      expect(result.flushed).toBe(false)
      expect(result.deltaPath).toBeUndefined()
      expect(result.eventCount).toBeUndefined()
    })

    it('writes delta file in Parquet format', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { flushThreshold: 1 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      const putCalls = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls
      const [path, body, options] = putCalls[0]

      // Should be a Parquet file
      expect(path).toMatch(/\.parquet$/)
      // Body should be ArrayBuffer (Parquet binary)
      expect(body).toBeInstanceOf(ArrayBuffer)
      // Content-Type should indicate Parquet
      expect(options?.httpMetadata?.contentType).toBe('application/vnd.apache.parquet')
    })
  })

  // ==========================================================================
  // Debounce Tests
  // ==========================================================================

  describe('Debounce', () => {
    it('debounces rapid writes within window (5-10 second default)', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { debounceMs: 5000 })

      // Rapid writes
      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])
      await processor.process([createInsertEvent('users', 'user-2', { name: 'Bob' })])
      await processor.process([createInsertEvent('users', 'user-3', { name: 'Charlie' })])

      // Should not have flushed yet (debouncing)
      expect(mockR2.put).not.toHaveBeenCalled()

      // Verify pending count
      const state = await processor.getState('users')
      expect(state.pendingCount).toBe(3)
    })

    it('schedules alarm for flush after debounce window', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { debounceMs: 5000 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Alarm should be scheduled
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()

      const alarmTime = mockCtx.storage._getAlarm()
      expect(alarmTime).toBeDefined()
      expect(alarmTime! - Date.now()).toBeGreaterThanOrEqual(5000)
    })

    it('flushes on alarm trigger', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { debounceMs: 5000 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Advance time past debounce window
      vi.advanceTimersByTime(6000)

      // Trigger alarm
      await processor.alarm()

      // Should have flushed
      expect(mockR2.put).toHaveBeenCalled()

      // Pending should be cleared
      const state = await processor.getState('users')
      expect(state.pendingCount).toBe(0)
    })

    it('resets debounce timer on new writes', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { debounceMs: 5000 })

      // First write
      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Advance 3 seconds (within window)
      vi.advanceTimersByTime(3000)

      // Second write should reset timer
      await processor.process([createInsertEvent('users', 'user-2', { name: 'Bob' })])

      // Advance 3 more seconds (6 total from first, 3 from second)
      vi.advanceTimersByTime(3000)

      // Should not have flushed yet (timer was reset)
      expect(mockR2.put).not.toHaveBeenCalled()

      // Advance 3 more seconds (now past debounce from second write)
      vi.advanceTimersByTime(3000)
      await processor.alarm()

      // Now should have flushed
      expect(mockR2.put).toHaveBeenCalled()
    })

    it('flushes immediately when force flag set', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { debounceMs: 10000 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Force flush (bypasses debounce)
      const result = await processor.flush(true)

      expect(result.flushed).toBe(true)
      expect(mockR2.put).toHaveBeenCalled()
    })

    it('cancels pending alarm on force flush', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv, { debounceMs: 10000 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      // Verify alarm was set
      expect(mockCtx.storage.setAlarm).toHaveBeenCalled()

      // Force flush
      await processor.flush(true)

      // Alarm should be cancelled
      expect(mockCtx.storage.deleteAlarm).toHaveBeenCalled()
    })
  })

  // ==========================================================================
  // State Query Tests
  // ==========================================================================

  describe('State Queries', () => {
    it('returns current state of collection', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice', role: 'admin' }),
        createInsertEvent('users', 'user-2', { name: 'Bob', role: 'user' }),
      ])

      const state = await processor.getState('users')

      expect(state.collection).toBe('users')
      expect(state.documents).toHaveLength(2)
      expect(state.lastEventTs).not.toBeNull()
    })

    it('filters documents by primary key', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
        createInsertEvent('users', 'user-3', { name: 'Charlie' }),
      ])

      const doc = await processor.getDocument('users', 'user-2')

      expect(doc).not.toBeNull()
      expect(doc?.docId).toBe('user-2')
      expect(doc?.doc).toEqual({ name: 'Bob' })
    })

    it('returns null for non-existent document', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
      ])

      const doc = await processor.getDocument('users', 'non-existent')

      expect(doc).toBeNull()
    })

    it('returns correct document versions', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'v1' }),
        createUpdateEvent('users', 'user-1', { name: 'v2' }, { name: 'v1' }),
        createUpdateEvent('users', 'user-1', { name: 'v3' }, { name: 'v2' }),
      ])

      const doc = await processor.getDocument('users', 'user-1')

      expect(doc?.version).toBe(3)
      expect(doc?.doc).toEqual({ name: 'v3' })
    })

    it('excludes deleted documents from active state by default', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
        createDeleteEvent('users', 'user-1', { name: 'Alice' }),
      ])

      const state = await processor.getState('users')

      // Only non-deleted documents in default state
      const activeDocs = state.documents.filter(d => !d.deleted)
      expect(activeDocs).toHaveLength(1)
      expect(activeDocs[0].docId).toBe('user-2')
    })

    it('returns lastEventTs from most recent processed event', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const ts1 = '2024-01-31T09:00:00.000Z'
      const ts2 = '2024-01-31T10:00:00.000Z'
      const ts3 = '2024-01-31T11:00:00.000Z'

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }, ts1),
        createInsertEvent('users', 'user-2', { name: 'Bob' }, ts2),
        createInsertEvent('users', 'user-3', { name: 'Charlie' }, ts3),
      ])

      const state = await processor.getState('users')
      expect(state.lastEventTs).toBe(ts3)
    })
  })

  // ==========================================================================
  // Manifest Tests
  // ==========================================================================

  describe('Manifest Management', () => {
    it('creates manifest on first event for collection', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
      ])
      await processor.flush(true)

      const manifest = await processor.getManifest('users')

      expect(manifest).not.toBeNull()
      expect(manifest?.collection).toBe('users')
      expect(manifest?.deltaSequence).toBeGreaterThan(0)
    })

    it('tracks schema from document fields', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', {
          name: 'Alice',
          age: 30,
          active: true,
          metadata: { created: '2024-01-01' },
        }),
      ])
      await processor.flush(true)

      const manifest = await processor.getManifest('users')

      expect(manifest?.schema).toBeDefined()
      expect(manifest?.schema?.name).toBe('string')
      expect(manifest?.schema?.age).toBe('number')
      expect(manifest?.schema?.active).toBe('boolean')
      expect(manifest?.schema?.metadata).toBe('object')
    })

    it('updates lastFlushAt after successful flush', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
      ])
      await processor.flush(true)

      const manifest = await processor.getManifest('users')

      expect(manifest?.lastFlushAt).toBe('2024-01-31T10:00:00.000Z')
    })

    it('maintains delta history in manifest', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      // First batch
      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
      ])
      await processor.flush(true)

      // Advance time
      vi.advanceTimersByTime(60000)

      // Second batch
      await processor.process([
        createInsertEvent('users', 'user-2', { name: 'Bob' }),
      ])
      await processor.flush(true)

      const manifest = await processor.getManifest('users')

      expect(manifest?.deltas).toHaveLength(2)
      expect(manifest?.deltas[0].sequence).toBe(1)
      expect(manifest?.deltas[1].sequence).toBe(2)
    })
  })

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('Edge Cases', () => {
    it('handles empty event batch', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const result = await processor.process([])

      expect(result.processed).toBe(0)
      expect(result.pending).toBe(0)
    })

    it('handles document with nested objects', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const nestedDoc = {
        name: 'Alice',
        address: {
          street: '123 Main St',
          city: 'Springfield',
          location: {
            lat: 40.7128,
            lng: -74.006,
          },
        },
        tags: ['admin', 'active'],
      }

      await processor.process([
        createInsertEvent('users', 'user-1', nestedDoc),
      ])

      const doc = await processor.getDocument('users', 'user-1')
      expect(doc?.doc).toEqual(nestedDoc)
    })

    it('handles special characters in collection name', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('user-profiles_v2', 'user-1', { name: 'Alice' }),
      ])

      const state = await processor.getState('user-profiles_v2')
      expect(state.documents).toHaveLength(1)
    })

    it('handles special characters in document ID', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const specialId = 'doc:with/special@chars#and$symbols'

      await processor.process([
        createInsertEvent('users', specialId, { name: 'Alice' }),
      ])

      const doc = await processor.getDocument('users', specialId)
      expect(doc?.docId).toBe(specialId)
    })

    it('handles rapid insert-delete cycles', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'A' }),
        createDeleteEvent('users', 'user-1', { name: 'A' }),
        createInsertEvent('users', 'user-1', { name: 'B' }),
        createDeleteEvent('users', 'user-1', { name: 'B' }),
        createInsertEvent('users', 'user-1', { name: 'C' }),
      ])

      const doc = await processor.getDocument('users', 'user-1')
      expect(doc?.doc).toEqual({ name: 'C' })
      expect(doc?.deleted).toBe(false)
      expect(doc?.version).toBe(5)
    })

    it('handles very large document', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      const largeDoc: Record<string, unknown> = {}
      for (let i = 0; i < 1000; i++) {
        largeDoc[`field_${i}`] = `value_${i}_${'x'.repeat(100)}`
      }

      await processor.process([
        createInsertEvent('large_collection', 'doc-1', largeDoc),
      ])

      const doc = await processor.getDocument('large_collection', 'doc-1')
      expect(Object.keys(doc?.doc ?? {}).length).toBe(1000)
    })

    it('handles concurrent events for different collections', async () => {
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(mockCtx, mockEnv)

      // Interleaved events from multiple collections
      await processor.process([
        createInsertEvent('users', 'user-1', { name: 'Alice' }),
        createInsertEvent('orders', 'order-1', { amount: 100 }),
        createUpdateEvent('users', 'user-1', { name: 'Alice Smith' }, { name: 'Alice' }),
        createInsertEvent('products', 'prod-1', { sku: 'ABC' }),
        createDeleteEvent('orders', 'order-1', { amount: 100 }),
      ])

      const usersState = await processor.getState('users')
      const ordersState = await processor.getState('orders')
      const productsState = await processor.getState('products')

      expect(usersState.documents.find(d => d.docId === 'user-1')?.doc).toEqual({ name: 'Alice Smith' })
      expect(ordersState.documents.find(d => d.docId === 'order-1')?.deleted).toBe(true)
      expect(productsState.documents.find(d => d.docId === 'prod-1')?.doc).toEqual({ sku: 'ABC' })
    })
  })

  // ==========================================================================
  // Namespace Isolation Tests
  // ==========================================================================

  describe('Namespace Isolation', () => {
    it('uses DO name as namespace in file paths', async () => {
      const customCtx = createMockCtx({ id: 'cdc-test', name: 'my-app' })
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(customCtx, mockEnv, { flushThreshold: 1 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      const putCalls = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls
      const deltaPath = putCalls[0]?.[0] as string

      expect(deltaPath).toMatch(/^my-app\/users\/deltas\//)
    })

    it('uses default namespace when DO name not set', async () => {
      const noNameCtx = createMockCtx({ id: 'cdc-test' }) // No name
      const { CDCProcessorDO } = await import('../cdc-processor.js')
      const processor = new CDCProcessorDO(noNameCtx, mockEnv, { flushThreshold: 1 })

      await processor.process([createInsertEvent('users', 'user-1', { name: 'Alice' })])

      const putCalls = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls
      const deltaPath = putCalls[0]?.[0] as string

      expect(deltaPath).toMatch(/^default\/users\/deltas\//)
    })
  })
})
