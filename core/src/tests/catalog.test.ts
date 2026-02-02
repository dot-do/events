/**
 * CatalogDO Tests
 *
 * Comprehensive tests for the Iceberg-style catalog Durable Object.
 * Tests cover:
 * - Namespace CRUD operations
 * - Table creation and management
 * - Snapshot operations (commit, list, get)
 * - Schema evolution
 * - buildQuery() SQL generation
 * - Time-travel queries (getSnapshotAtTime, getSnapshotByBookmark)
 * - Error handling and edge cases
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { CatalogDO, type SchemaField, type DataFile, type Snapshot, type TableMetadata } from '../catalog.js'
import { createMockDurableObjectId } from './mocks.js'

// ============================================================================
// Mock SqlStorage
// ============================================================================

interface MockRow {
  [key: string]: unknown
}

/**
 * Creates a mock SqlStorage with in-memory SQLite-like behavior.
 * Uses simple in-memory maps to simulate tables.
 */
function createMockSqlStorage() {
  const namespaces = new Map<string, { name: string; properties: string; created_at: string }>()
  const tables = new Map<string, {
    id: string
    namespace: string
    name: string
    location: string
    metadata: string
    created_at: string
    updated_at: string
  }>()
  const snapshots = new Map<string, {
    id: string
    table_id: string
    parent_id: string | null
    timestamp_ms: number
    operation: string
    manifest_list: string
    summary: string
    cdc_bookmark: string | null
    created_at: string
  }>()
  const dataFiles = new Map<string, {
    path: string
    table_id: string
    snapshot_id: string
    format: string
    record_count: number
    file_size_bytes: number
    column_stats: string | null
    partition_values: string | null
    created_at: string
  }>()
  const schemaHistory = new Map<string, {
    id: string
    table_id: string
    version: number
    fields: string
    from_version: number | null
    changes: string | null
    trigger: string | null
    created_at: string
  }>()

  const exec = vi.fn((sql: string, ...params: unknown[]) => {
    const sqlLower = sql.toLowerCase().trim()

    // CREATE TABLE statements - no-op since we use in-memory maps
    if (sqlLower.startsWith('create table') || sqlLower.startsWith('create index')) {
      return {
        toArray: () => [],
        one: () => null,
      }
    }

    // INSERT INTO namespaces
    if (sqlLower.includes('insert into namespaces')) {
      const name = params[0] as string
      const properties = params[1] as string
      namespaces.set(name, {
        name,
        properties,
        created_at: new Date().toISOString(),
      })
      return { toArray: () => [], one: () => null }
    }

    // SELECT name FROM namespaces
    if (sqlLower.includes('select name from namespaces')) {
      const rows = Array.from(namespaces.values()).map((ns) => ({ name: ns.name }))
      rows.sort((a, b) => a.name.localeCompare(b.name))
      return { toArray: () => rows, one: () => rows[0] || null }
    }

    // SELECT COUNT(*) FROM tables WHERE namespace = ?
    if (sqlLower.includes('select count(*)') && sqlLower.includes('from tables') && sqlLower.includes('namespace')) {
      const namespace = params[0] as string
      const count = Array.from(tables.values()).filter((t) => t.namespace === namespace).length
      return { toArray: () => [{ count }], one: () => ({ count }) }
    }

    // DELETE FROM namespaces
    if (sqlLower.includes('delete from namespaces')) {
      const name = params[0] as string
      namespaces.delete(name)
      return { toArray: () => [], one: () => null }
    }

    // INSERT INTO tables
    if (sqlLower.includes('insert into tables')) {
      const id = params[0] as string
      const namespace = params[1] as string
      const name = params[2] as string
      const location = params[3] as string
      const metadata = params[4] as string
      const now = new Date().toISOString()
      tables.set(id, { id, namespace, name, location, metadata, created_at: now, updated_at: now })
      return { toArray: () => [], one: () => null }
    }

    // SELECT metadata FROM tables WHERE namespace = ? AND name = ?
    if (sqlLower.includes('select metadata from tables') && sqlLower.includes('namespace') && sqlLower.includes('name')) {
      const namespace = params[0] as string
      const name = params[1] as string
      const table = Array.from(tables.values()).find((t) => t.namespace === namespace && t.name === name)
      return { toArray: () => (table ? [{ metadata: table.metadata }] : []), one: () => (table ? { metadata: table.metadata } : null) }
    }

    // SELECT name FROM tables WHERE namespace = ?
    if (sqlLower.includes('select name from tables') && sqlLower.includes('namespace')) {
      const namespace = params[0] as string
      const rows = Array.from(tables.values())
        .filter((t) => t.namespace === namespace)
        .map((t) => ({ name: t.name }))
        .sort((a, b) => a.name.localeCompare(b.name))
      return { toArray: () => rows, one: () => rows[0] || null }
    }

    // DELETE FROM data_files WHERE table_id = ?
    if (sqlLower.includes('delete from data_files') && sqlLower.includes('table_id')) {
      const tableId = params[0] as string
      for (const [key, file] of dataFiles.entries()) {
        if (file.table_id === tableId) dataFiles.delete(key)
      }
      return { toArray: () => [], one: () => null }
    }

    // DELETE FROM snapshots WHERE table_id = ?
    if (sqlLower.includes('delete from snapshots') && sqlLower.includes('table_id')) {
      const tableId = params[0] as string
      for (const [key, snap] of snapshots.entries()) {
        if (snap.table_id === tableId) snapshots.delete(key)
      }
      return { toArray: () => [], one: () => null }
    }

    // DELETE FROM tables WHERE id = ?
    if (sqlLower.includes('delete from tables') && sqlLower.includes('id')) {
      const id = params[0] as string
      tables.delete(id)
      return { toArray: () => [], one: () => null }
    }

    // INSERT INTO snapshots
    if (sqlLower.includes('insert into snapshots')) {
      const id = params[0] as string
      const table_id = params[1] as string
      const parent_id = params[2] as string | null
      const timestamp_ms = params[3] as number
      const operation = params[4] as string
      const manifest_list = params[5] as string
      const summary = params[6] as string
      const cdc_bookmark = params[7] as string | null
      const now = new Date().toISOString()
      snapshots.set(id, { id, table_id, parent_id, timestamp_ms, operation, manifest_list, summary, cdc_bookmark, created_at: now })
      return { toArray: () => [], one: () => null }
    }

    // INSERT INTO data_files
    if (sqlLower.includes('insert into data_files')) {
      const path = params[0] as string
      const table_id = params[1] as string
      const snapshot_id = params[2] as string
      const format = params[3] as string
      const record_count = params[4] as number
      const file_size_bytes = params[5] as number
      const column_stats = params[6] as string | null
      const partition_values = params[7] as string | null
      const now = new Date().toISOString()
      dataFiles.set(path, { path, table_id, snapshot_id, format, record_count, file_size_bytes, column_stats, partition_values, created_at: now })
      return { toArray: () => [], one: () => null }
    }

    // UPDATE tables SET metadata = ?, updated_at = ? WHERE id = ?
    if (sqlLower.includes('update tables set metadata')) {
      const metadata = params[0] as string
      const updated_at = params[1] as string
      const id = params[2] as string
      const table = tables.get(id)
      if (table) {
        table.metadata = metadata
        table.updated_at = updated_at
      }
      return { toArray: () => [], one: () => null }
    }

    // SELECT * FROM snapshots WHERE table_id = ? AND cdc_bookmark = ?
    if (sqlLower.includes('select * from snapshots') && sqlLower.includes('cdc_bookmark')) {
      const tableId = params[0] as string
      const cdcBookmark = params[1] as string
      const snap = Array.from(snapshots.values()).find((s) => s.table_id === tableId && s.cdc_bookmark === cdcBookmark)
      if (snap) {
        return {
          toArray: () => [snap],
          one: () => snap,
        }
      }
      return { toArray: () => [], one: () => null }
    }

    // SELECT * FROM snapshots WHERE table_id = ? AND timestamp_ms <= ? ORDER BY timestamp_ms DESC LIMIT 1
    if (sqlLower.includes('select * from snapshots') && sqlLower.includes('timestamp_ms')) {
      const tableId = params[0] as string
      const timestampMs = params[1] as number
      const matching = Array.from(snapshots.values())
        .filter((s) => s.table_id === tableId && s.timestamp_ms <= timestampMs)
        .sort((a, b) => b.timestamp_ms - a.timestamp_ms)
      return {
        toArray: () => matching.slice(0, 1),
        one: () => matching[0] || null,
      }
    }

    // SELECT * FROM data_files WHERE table_id = ? AND snapshot_id = ?
    if (sqlLower.includes('select * from data_files')) {
      const tableId = params[0] as string
      const snapshotId = params[1] as string
      const files = Array.from(dataFiles.values()).filter(
        (f) => f.table_id === tableId && f.snapshot_id === snapshotId
      )
      return { toArray: () => files, one: () => files[0] || null }
    }

    // SELECT * FROM schema_history WHERE table_id = ? ORDER BY version ASC
    if (sqlLower.includes('select * from schema_history') && sqlLower.includes('order by version')) {
      const tableId = params[0] as string
      const rows = Array.from(schemaHistory.values())
        .filter((s) => s.table_id === tableId)
        .sort((a, b) => a.version - b.version)
      return { toArray: () => rows, one: () => rows[0] || null }
    }

    // INSERT INTO schema_history
    if (sqlLower.includes('insert into schema_history')) {
      const id = params[0] as string
      const table_id = params[1] as string
      const version = params[2] as number
      const fields = params[3] as string
      const from_version = params[4] as number | null
      const changes = params[5] as string | null
      const trigger = params[6] as string | null
      const now = new Date().toISOString()
      schemaHistory.set(id, { id, table_id, version, fields, from_version, changes, trigger, created_at: now })
      return { toArray: () => [], one: () => null }
    }

    // SELECT id FROM schema_history WHERE table_id = ? AND version = 0
    if (sqlLower.includes('select id from schema_history') && sqlLower.includes('version = 0')) {
      const tableId = params[0] as string
      const row = Array.from(schemaHistory.values()).find((s) => s.table_id === tableId && s.version === 0)
      return { toArray: () => (row ? [{ id: row.id }] : []), one: () => (row ? { id: row.id } : null) }
    }

    // Default fallback
    return { toArray: () => [], one: () => null }
  })

  return {
    exec,
    _namespaces: namespaces,
    _tables: tables,
    _snapshots: snapshots,
    _dataFiles: dataFiles,
    _schemaHistory: schemaHistory,
  }
}

/**
 * Creates a mock DurableObjectState with SQL storage
 */
function createMockCtxWithSql(options: { id?: string; name?: string } = {}) {
  const sql = createMockSqlStorage()

  return {
    id: createMockDurableObjectId(options.id ?? 'catalog-do-id', options.name),
    storage: {
      sql,
    },
    waitUntil: vi.fn((promise: Promise<unknown>) => promise),
    blockConcurrencyWhile: vi.fn(<T>(fn: () => Promise<T>) => fn()),
  } as unknown as DurableObjectState & {
    storage: {
      sql: ReturnType<typeof createMockSqlStorage>
    }
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('CatalogDO', () => {
  let ctx: ReturnType<typeof createMockCtxWithSql>
  let catalog: CatalogDO

  beforeEach(() => {
    // Mock crypto.randomUUID for deterministic IDs
    let uuidCounter = 0
    vi.spyOn(crypto, 'randomUUID').mockImplementation(() => `uuid-${++uuidCounter}`)

    ctx = createMockCtxWithSql()
    catalog = new CatalogDO(ctx as unknown as DurableObjectState, {})
  })

  // --------------------------------------------------------------------------
  // Namespace Operations
  // --------------------------------------------------------------------------

  describe('Namespace CRUD Operations', () => {
    describe('createNamespace', () => {
      it('creates a namespace with default properties', async () => {
        await catalog.createNamespace('test-namespace')

        expect(ctx.storage.sql.exec).toHaveBeenCalledWith(
          expect.stringContaining('INSERT INTO namespaces'),
          'test-namespace',
          '{}'
        )
        expect(ctx.storage.sql._namespaces.has('test-namespace')).toBe(true)
      })

      it('creates a namespace with custom properties', async () => {
        await catalog.createNamespace('my-namespace', { owner: 'alice', tier: 'premium' })

        expect(ctx.storage.sql.exec).toHaveBeenCalledWith(
          expect.stringContaining('INSERT INTO namespaces'),
          'my-namespace',
          JSON.stringify({ owner: 'alice', tier: 'premium' })
        )
      })

      it('allows multiple namespaces', async () => {
        await catalog.createNamespace('ns1')
        await catalog.createNamespace('ns2')
        await catalog.createNamespace('ns3')

        expect(ctx.storage.sql._namespaces.size).toBe(3)
      })
    })

    describe('listNamespaces', () => {
      it('returns empty array when no namespaces exist', async () => {
        const namespaces = await catalog.listNamespaces()
        expect(namespaces).toEqual([])
      })

      it('returns all namespaces sorted by name', async () => {
        await catalog.createNamespace('zebra')
        await catalog.createNamespace('alpha')
        await catalog.createNamespace('beta')

        const namespaces = await catalog.listNamespaces()
        expect(namespaces).toEqual(['alpha', 'beta', 'zebra'])
      })
    })

    describe('dropNamespace', () => {
      it('deletes an empty namespace', async () => {
        await catalog.createNamespace('to-delete')
        expect(ctx.storage.sql._namespaces.has('to-delete')).toBe(true)

        await catalog.dropNamespace('to-delete')
        expect(ctx.storage.sql._namespaces.has('to-delete')).toBe(false)
      })

      it('throws error when namespace has tables', async () => {
        await catalog.createNamespace('with-tables')
        await catalog.createTable('with-tables', 'events', [{ name: 'id', type: 'string' }])

        await expect(catalog.dropNamespace('with-tables')).rejects.toThrow('not empty')
      })

      it('succeeds silently when namespace does not exist', async () => {
        // Should not throw
        await catalog.dropNamespace('nonexistent')
      })
    })
  })

  // --------------------------------------------------------------------------
  // Table Operations
  // --------------------------------------------------------------------------

  describe('Table Creation and Management', () => {
    beforeEach(async () => {
      await catalog.createNamespace('analytics')
    })

    describe('createTable', () => {
      it('creates a table with basic schema', async () => {
        const schema: SchemaField[] = [
          { name: 'id', type: 'string' },
          { name: 'timestamp', type: 'timestamp' },
          { name: 'value', type: 'double' },
        ]

        const table = await catalog.createTable('analytics', 'events', schema)

        expect(table.tableId).toBe('uuid-1')
        expect(table.name).toBe('events')
        expect(table.namespace).toBe('analytics')
        expect(table.location).toBe('analytics/events')
        expect(table.schemas[0]?.fields).toEqual(schema)
        expect(table.currentSchemaId).toBe(0)
        expect(table.snapshots).toEqual([])
      })

      it('creates a table with custom location', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]

        const table = await catalog.createTable('analytics', 'users', schema, {
          location: 'custom/path/users',
        })

        expect(table.location).toBe('custom/path/users')
      })

      it('creates a table with partition spec', async () => {
        const schema: SchemaField[] = [
          { name: 'event_date', type: 'timestamp' },
          { name: 'data', type: 'json' },
        ]

        const table = await catalog.createTable('analytics', 'partitioned', schema, {
          partitionSpec: [
            { sourceId: 0, name: 'event_day', transform: 'day' },
          ],
        })

        expect(table.partitionSpecs[0]?.fields).toHaveLength(1)
        expect(table.partitionSpecs[0]?.fields[0]?.transform).toBe('day')
      })

      it('creates a table with custom properties', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]

        const table = await catalog.createTable('analytics', 'with-props', schema, {
          properties: { 'write.format.default': 'parquet', 'commit.retry.num-retries': '5' },
        })

        expect(table.properties['write.format.default']).toBe('parquet')
        expect(table.properties['commit.retry.num-retries']).toBe('5')
      })
    })

    describe('loadTable', () => {
      it('returns null for non-existent table', async () => {
        const table = await catalog.loadTable('analytics', 'nonexistent')
        expect(table).toBeNull()
      })

      it('loads an existing table', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('analytics', 'test-table', schema)

        const table = await catalog.loadTable('analytics', 'test-table')

        expect(table).not.toBeNull()
        expect(table?.name).toBe('test-table')
        expect(table?.namespace).toBe('analytics')
      })
    })

    describe('listTables', () => {
      it('returns empty array for namespace with no tables', async () => {
        const tables = await catalog.listTables('analytics')
        expect(tables).toEqual([])
      })

      it('returns all tables in namespace sorted by name', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('analytics', 'zebra', schema)
        await catalog.createTable('analytics', 'alpha', schema)
        await catalog.createTable('analytics', 'beta', schema)

        const tables = await catalog.listTables('analytics')
        expect(tables).toEqual(['alpha', 'beta', 'zebra'])
      })

      it('only returns tables from the specified namespace', async () => {
        await catalog.createNamespace('other')
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]

        await catalog.createTable('analytics', 'events', schema)
        await catalog.createTable('other', 'users', schema)

        const analyticsTables = await catalog.listTables('analytics')
        expect(analyticsTables).toEqual(['events'])

        const otherTables = await catalog.listTables('other')
        expect(otherTables).toEqual(['users'])
      })
    })

    describe('dropTable', () => {
      it('deletes a table and its snapshots/files', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('analytics', 'to-delete', schema)

        // Verify table exists
        const before = await catalog.listTables('analytics')
        expect(before).toContain('to-delete')

        await catalog.dropTable('analytics', 'to-delete')

        // Verify table is gone
        const after = await catalog.listTables('analytics')
        expect(after).not.toContain('to-delete')
      })

      it('succeeds silently when table does not exist', async () => {
        // Should not throw
        await catalog.dropTable('analytics', 'nonexistent')
      })
    })
  })

  // --------------------------------------------------------------------------
  // Snapshot Operations
  // --------------------------------------------------------------------------

  describe('Snapshot Operations', () => {
    let tableSchema: SchemaField[]

    beforeEach(async () => {
      await catalog.createNamespace('lakehouse')
      tableSchema = [
        { name: 'id', type: 'string' },
        { name: 'ts', type: 'timestamp' },
        { name: 'value', type: 'double' },
      ]
      await catalog.createTable('lakehouse', 'metrics', tableSchema)
    })

    describe('commitSnapshot', () => {
      it('commits a snapshot with append operation', async () => {
        const files: DataFile[] = [
          {
            path: 'lakehouse/metrics/data/part-001.parquet',
            format: 'parquet',
            recordCount: 1000,
            fileSizeBytes: 50000,
            createdAt: new Date().toISOString(),
          },
        ]

        const snapshot = await catalog.commitSnapshot('lakehouse', 'metrics', files)

        expect(snapshot.snapshotId).toBe('uuid-2')
        expect(snapshot.operation).toBe('append')
        expect(snapshot.summary.addedRecords).toBe(1000)
        expect(snapshot.summary.addedFiles).toBe(1)
        expect(snapshot.summary.totalRecords).toBe(1000)
        expect(snapshot.summary.totalFiles).toBe(1)
        expect(snapshot.manifestList).toEqual(['lakehouse/metrics/data/part-001.parquet'])
      })

      it('commits multiple files in a single snapshot', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
          { path: 'data/part-002.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
          { path: 'data/part-003.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
        ]

        const snapshot = await catalog.commitSnapshot('lakehouse', 'metrics', files)

        expect(snapshot.summary.addedRecords).toBe(1500)
        expect(snapshot.summary.addedFiles).toBe(3)
        expect(snapshot.manifestList).toHaveLength(3)
      })

      it('sets parent snapshot ID for subsequent commits', async () => {
        const files1: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
        ]
        const files2: DataFile[] = [
          { path: 'data/part-002.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
        ]

        const snapshot1 = await catalog.commitSnapshot('lakehouse', 'metrics', files1)
        const snapshot2 = await catalog.commitSnapshot('lakehouse', 'metrics', files2)

        expect(snapshot1.parentSnapshotId).toBeUndefined()
        expect(snapshot2.parentSnapshotId).toBe(snapshot1.snapshotId)
      })

      it('tracks cumulative totals across snapshots', async () => {
        const files1: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
        ]
        const files2: DataFile[] = [
          { path: 'data/part-002.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
        ]

        await catalog.commitSnapshot('lakehouse', 'metrics', files1)
        const snapshot2 = await catalog.commitSnapshot('lakehouse', 'metrics', files2)

        expect(snapshot2.summary.totalRecords).toBe(1500)
        expect(snapshot2.summary.totalFiles).toBe(2)
      })

      it('includes CDC bookmark when provided', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
        ]

        const snapshot = await catalog.commitSnapshot('lakehouse', 'metrics', files, {
          cdcBookmark: 'bookmark-abc123',
        })

        expect(snapshot.cdcBookmark).toBe('bookmark-abc123')
      })

      it('supports overwrite operation', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
        ]

        const snapshot = await catalog.commitSnapshot('lakehouse', 'metrics', files, {
          operation: 'overwrite',
        })

        expect(snapshot.operation).toBe('overwrite')
      })

      it('throws error for non-existent table', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
        ]

        await expect(catalog.commitSnapshot('lakehouse', 'nonexistent', files)).rejects.toThrow('not found')
      })
    })

    describe('listFiles', () => {
      it('returns empty array when no snapshots exist', async () => {
        const files = await catalog.listFiles('lakehouse', 'metrics')
        expect(files).toEqual([])
      })

      it('returns files from current snapshot', async () => {
        const dataFiles: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
          { path: 'data/part-002.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
        ]

        await catalog.commitSnapshot('lakehouse', 'metrics', dataFiles)

        const files = await catalog.listFiles('lakehouse', 'metrics')
        expect(files).toHaveLength(2)
        expect(files[0]?.path).toBe('data/part-001.parquet')
        expect(files[0]?.format).toBe('parquet')
        expect(files[0]?.recordCount).toBe(1000)
      })

      it('returns files from specific snapshot ID', async () => {
        const files1: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 1000, fileSizeBytes: 50000, createdAt: new Date().toISOString() },
        ]
        const files2: DataFile[] = [
          { path: 'data/part-002.parquet', format: 'parquet', recordCount: 500, fileSizeBytes: 25000, createdAt: new Date().toISOString() },
        ]

        const snapshot1 = await catalog.commitSnapshot('lakehouse', 'metrics', files1)
        await catalog.commitSnapshot('lakehouse', 'metrics', files2)

        const filesFromSnapshot1 = await catalog.listFiles('lakehouse', 'metrics', snapshot1.snapshotId)
        expect(filesFromSnapshot1).toHaveLength(1)
        expect(filesFromSnapshot1[0]?.path).toBe('data/part-001.parquet')
      })

      it('returns empty array for non-existent table', async () => {
        const files = await catalog.listFiles('lakehouse', 'nonexistent')
        expect(files).toEqual([])
      })
    })
  })

  // --------------------------------------------------------------------------
  // Time-Travel Queries
  // --------------------------------------------------------------------------

  describe('Time-Travel Queries', () => {
    let baseTime: number

    beforeEach(async () => {
      await catalog.createNamespace('timetravel')
      await catalog.createTable('timetravel', 'events', [{ name: 'id', type: 'string' }])
      baseTime = Date.now()
    })

    describe('getSnapshotByBookmark', () => {
      it('returns null when table does not exist', async () => {
        const snapshot = await catalog.getSnapshotByBookmark('timetravel', 'nonexistent', 'bookmark-123')
        expect(snapshot).toBeNull()
      })

      it('returns null when bookmark does not exist', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]
        await catalog.commitSnapshot('timetravel', 'events', files, { cdcBookmark: 'other-bookmark' })

        const snapshot = await catalog.getSnapshotByBookmark('timetravel', 'events', 'nonexistent-bookmark')
        expect(snapshot).toBeNull()
      })

      it('finds snapshot by CDC bookmark', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]
        await catalog.commitSnapshot('timetravel', 'events', files, { cdcBookmark: 'bookmark-abc123' })

        const snapshot = await catalog.getSnapshotByBookmark('timetravel', 'events', 'bookmark-abc123')

        expect(snapshot).not.toBeNull()
        expect(snapshot?.cdcBookmark).toBe('bookmark-abc123')
        expect(snapshot?.summary.addedRecords).toBe(100)
      })

      it('finds correct snapshot among multiple with different bookmarks', async () => {
        const files1: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]
        const files2: DataFile[] = [
          { path: 'data/part-002.parquet', format: 'parquet', recordCount: 200, fileSizeBytes: 10000, createdAt: new Date().toISOString() },
        ]

        await catalog.commitSnapshot('timetravel', 'events', files1, { cdcBookmark: 'bookmark-1' })
        await catalog.commitSnapshot('timetravel', 'events', files2, { cdcBookmark: 'bookmark-2' })

        const snapshot1 = await catalog.getSnapshotByBookmark('timetravel', 'events', 'bookmark-1')
        const snapshot2 = await catalog.getSnapshotByBookmark('timetravel', 'events', 'bookmark-2')

        expect(snapshot1?.summary.addedRecords).toBe(100)
        expect(snapshot2?.summary.addedRecords).toBe(200)
      })
    })

    describe('getSnapshotAtTime', () => {
      it('returns null when table does not exist', async () => {
        const snapshot = await catalog.getSnapshotAtTime('timetravel', 'nonexistent', Date.now())
        expect(snapshot).toBeNull()
      })

      it('returns null when no snapshots exist before the given time', async () => {
        const pastTime = Date.now() - 1000000
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]
        await catalog.commitSnapshot('timetravel', 'events', files)

        const snapshot = await catalog.getSnapshotAtTime('timetravel', 'events', pastTime)
        expect(snapshot).toBeNull()
      })

      it('returns the most recent snapshot at or before the given time', async () => {
        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]
        const committed = await catalog.commitSnapshot('timetravel', 'events', files)

        const snapshot = await catalog.getSnapshotAtTime('timetravel', 'events', Date.now() + 1000)

        expect(snapshot).not.toBeNull()
        expect(snapshot?.snapshotId).toBe(committed.snapshotId)
      })
    })
  })

  // --------------------------------------------------------------------------
  // Query Builder
  // --------------------------------------------------------------------------

  describe('buildQuery() SQL Generation', () => {
    beforeEach(async () => {
      await catalog.createNamespace('query')
      await catalog.createTable('query', 'events', [
        { name: 'id', type: 'string' },
        { name: 'timestamp', type: 'timestamp' },
        { name: 'value', type: 'double' },
      ])
    })

    it('returns empty string when no files exist', async () => {
      const sql = await catalog.buildQuery('query', 'events')
      expect(sql).toBe('')
    })

    it('generates basic SELECT * query for parquet files', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events')

      expect(sql).toBe("SELECT * FROM read_parquet(['data/part-001.parquet'])")
    })

    it('generates SELECT with specific columns', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events', {
        columns: ['id', 'value'],
      })

      expect(sql).toBe("SELECT id, value FROM read_parquet(['data/part-001.parquet'])")
    })

    it('generates SELECT with WHERE clause', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events', {
        where: "value > 100 AND id IS NOT NULL",
      })

      expect(sql).toBe("SELECT * FROM read_parquet(['data/part-001.parquet']) WHERE value > 100 AND id IS NOT NULL")
    })

    it('generates SELECT with LIMIT', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events', { limit: 10 })

      expect(sql).toBe("SELECT * FROM read_parquet(['data/part-001.parquet']) LIMIT 10")
    })

    it('generates query with all options combined', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events', {
        columns: ['id', 'value'],
        where: 'value > 50',
        limit: 100,
      })

      expect(sql).toBe("SELECT id, value FROM read_parquet(['data/part-001.parquet']) WHERE value > 50 LIMIT 100")
    })

    it('generates read_json_auto for JSONL files', async () => {
      const files: DataFile[] = [
        { path: 'data/events.jsonl', format: 'jsonl', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events')

      expect(sql).toBe("SELECT * FROM read_json_auto(['data/events.jsonl'])")
    })

    it('handles multiple files in the query', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        { path: 'data/part-002.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      const sql = await catalog.buildQuery('query', 'events')

      expect(sql).toBe("SELECT * FROM read_parquet(['data/part-001.parquet', 'data/part-002.parquet'])")
    })

    it('rejects invalid column names', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      await expect(
        catalog.buildQuery('query', 'events', { columns: ['valid_column', 'DROP TABLE users; --'] })
      ).rejects.toThrow('Invalid column name')
    })

    it('rejects invalid where clauses', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      await expect(
        catalog.buildQuery('query', 'events', { where: 'value > 1; DROP TABLE users;' })
      ).rejects.toThrow('Invalid where clause')
    })

    it('rejects invalid limit values', async () => {
      const files: DataFile[] = [
        { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files)

      await expect(
        catalog.buildQuery('query', 'events', { limit: -1 })
      ).rejects.toThrow('Invalid limit')

      await expect(
        catalog.buildQuery('query', 'events', { limit: 1.5 })
      ).rejects.toThrow('Invalid limit')
    })

    it('rejects invalid file paths (SQL injection)', async () => {
      // Manually add a file with an unsafe path to test the sanitization
      const files: DataFile[] = [
        { path: "data/part-001.parquet'; DROP TABLE users; --", format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
      ]
      await catalog.commitSnapshot('query', 'events', files, { skipSchemaEvolution: true })

      await expect(catalog.buildQuery('query', 'events')).rejects.toThrow('Invalid file path')
    })
  })

  // --------------------------------------------------------------------------
  // Schema Evolution
  // --------------------------------------------------------------------------

  describe('Schema Evolution', () => {
    beforeEach(async () => {
      await catalog.createNamespace('evolve')
    })

    describe('evolveSchema', () => {
      it('adds new fields to schema', async () => {
        const initialSchema: SchemaField[] = [
          { name: 'id', type: 'string' },
          { name: 'name', type: 'string' },
        ]
        await catalog.createTable('evolve', 'users', initialSchema)

        const newFields: SchemaField[] = [
          { name: 'email', type: 'string' },
          { name: 'age', type: 'int32' },
        ]

        const evolved = await catalog.evolveSchema('evolve', 'users', newFields)

        expect(evolved).not.toBeNull()
        expect(evolved?.version).toBe(1)
        expect(evolved?.fields).toHaveLength(4) // 2 original + 2 new
        expect(evolved?.fields.map((f) => f.name)).toContain('email')
        expect(evolved?.fields.map((f) => f.name)).toContain('age')
      })

      it('makes new fields nullable by default', async () => {
        const initialSchema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'users', initialSchema)

        const newFields: SchemaField[] = [{ name: 'email', type: 'string', nullable: false }]

        const evolved = await catalog.evolveSchema('evolve', 'users', newFields)

        const emailField = evolved?.fields.find((f) => f.name === 'email')
        expect(emailField?.nullable).toBe(true) // Forced to nullable for backward compat
      })

      it('returns null when no evolution is needed', async () => {
        const schema: SchemaField[] = [
          { name: 'id', type: 'string' },
          { name: 'name', type: 'string' },
        ]
        await catalog.createTable('evolve', 'users', schema)

        // Try to evolve with existing fields
        const evolved = await catalog.evolveSchema('evolve', 'users', [{ name: 'id', type: 'string' }])

        expect(evolved).toBeNull()
      })

      it('records schema change history', async () => {
        const initialSchema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'users', initialSchema)

        const newFields: SchemaField[] = [{ name: 'email', type: 'string' }]
        const evolved = await catalog.evolveSchema('evolve', 'users', newFields, 'manual')

        expect(evolved?.change).toBeDefined()
        expect(evolved?.change?.trigger).toBe('manual')
        expect(evolved?.change?.changes).toHaveLength(1)
        expect(evolved?.change?.changes[0]?.changeType).toBe('added')
        expect(evolved?.change?.changes[0]?.field).toBe('email')
      })

      it('throws error for non-existent table', async () => {
        await expect(
          catalog.evolveSchema('evolve', 'nonexistent', [{ name: 'id', type: 'string' }])
        ).rejects.toThrow('not found')
      })
    })

    describe('getSchemaHistory', () => {
      it('returns empty array for non-existent table', async () => {
        const history = await catalog.getSchemaHistory('evolve', 'nonexistent')
        expect(history).toEqual([])
      })

      it('returns initial schema from table metadata when no history recorded', async () => {
        const schema: SchemaField[] = [
          { name: 'id', type: 'string' },
          { name: 'name', type: 'string' },
        ]
        await catalog.createTable('evolve', 'users', schema)

        const history = await catalog.getSchemaHistory('evolve', 'users')

        expect(history).toHaveLength(1)
        expect(history[0]?.version).toBe(0)
        expect(history[0]?.fields).toEqual(schema)
      })

      it('returns full schema history after evolutions', async () => {
        const initialSchema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'users', initialSchema)
        await catalog.recordInitialSchema('evolve', 'users')

        await catalog.evolveSchema('evolve', 'users', [{ name: 'email', type: 'string' }])
        await catalog.evolveSchema('evolve', 'users', [{ name: 'phone', type: 'string' }])

        const history = await catalog.getSchemaHistory('evolve', 'users')

        expect(history).toHaveLength(3)
        expect(history[0]?.version).toBe(0)
        expect(history[1]?.version).toBe(1)
        expect(history[2]?.version).toBe(2)
      })
    })

    describe('checkSchemaEvolution', () => {
      it('evolves schema when new columns are detected in file stats', async () => {
        const initialSchema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'metrics', initialSchema)

        const files: DataFile[] = [
          {
            path: 'data/part-001.parquet',
            format: 'parquet',
            recordCount: 100,
            fileSizeBytes: 5000,
            createdAt: new Date().toISOString(),
            columnStats: {
              id: { minValue: 'a', maxValue: 'z' },
              value: { minValue: 0, maxValue: 100 },
              timestamp: { minValue: '2024-01-01T00:00:00Z', maxValue: '2024-01-31T23:59:59Z' },
            },
          },
        ]

        const evolved = await catalog.checkSchemaEvolution('evolve', 'metrics', files)

        expect(evolved).not.toBeNull()
        expect(evolved?.fields.map((f) => f.name)).toContain('value')
        expect(evolved?.fields.map((f) => f.name)).toContain('timestamp')
      })

      it('returns null when files have no column stats', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'metrics', schema)

        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]

        const evolved = await catalog.checkSchemaEvolution('evolve', 'metrics', files)
        expect(evolved).toBeNull()
      })

      it('infers types from column stats', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'typed', schema)

        const files: DataFile[] = [
          {
            path: 'data/part-001.parquet',
            format: 'parquet',
            recordCount: 100,
            fileSizeBytes: 5000,
            createdAt: new Date().toISOString(),
            columnStats: {
              count: { minValue: 1, maxValue: 100 },
              price: { minValue: 1.99, maxValue: 99.99 },
              date: { minValue: '2024-01-01', maxValue: '2024-12-31' },
              name: { minValue: 'alice', maxValue: 'zoe' },
            },
          },
        ]

        const evolved = await catalog.checkSchemaEvolution('evolve', 'typed', files)

        const countField = evolved?.fields.find((f) => f.name === 'count')
        const priceField = evolved?.fields.find((f) => f.name === 'price')
        const dateField = evolved?.fields.find((f) => f.name === 'date')
        const nameField = evolved?.fields.find((f) => f.name === 'name')

        expect(countField?.type).toBe('int64')
        expect(priceField?.type).toBe('double')
        expect(dateField?.type).toBe('timestamp')
        expect(nameField?.type).toBe('string')
      })
    })

    describe('recordInitialSchema', () => {
      it('records initial schema in history table', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'users', schema)

        await catalog.recordInitialSchema('evolve', 'users')

        // Check that schema history was recorded
        expect(ctx.storage.sql._schemaHistory.size).toBe(1)
      })

      it('does not duplicate initial schema if already recorded', async () => {
        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        await catalog.createTable('evolve', 'users', schema)

        await catalog.recordInitialSchema('evolve', 'users')
        await catalog.recordInitialSchema('evolve', 'users')

        // Should still be only one entry
        const entries = Array.from(ctx.storage.sql._schemaHistory.values()).filter(
          (s) => s.version === 0
        )
        expect(entries).toHaveLength(1)
      })

      it('does nothing for non-existent table', async () => {
        // Should not throw
        await catalog.recordInitialSchema('evolve', 'nonexistent')
        expect(ctx.storage.sql._schemaHistory.size).toBe(0)
      })
    })
  })

  // --------------------------------------------------------------------------
  // Error Handling and Edge Cases
  // --------------------------------------------------------------------------

  describe('Error Handling and Edge Cases', () => {
    describe('Table uniqueness', () => {
      it('handles tables with same name in different namespaces', async () => {
        await catalog.createNamespace('ns1')
        await catalog.createNamespace('ns2')

        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]

        const table1 = await catalog.createTable('ns1', 'events', schema)
        const table2 = await catalog.createTable('ns2', 'events', schema)

        expect(table1.tableId).not.toBe(table2.tableId)

        const ns1Tables = await catalog.listTables('ns1')
        const ns2Tables = await catalog.listTables('ns2')

        expect(ns1Tables).toEqual(['events'])
        expect(ns2Tables).toEqual(['events'])
      })
    })

    describe('Snapshot consistency', () => {
      it('updates table metadata after commit', async () => {
        await catalog.createNamespace('test')
        await catalog.createTable('test', 'events', [{ name: 'id', type: 'string' }])

        const files: DataFile[] = [
          { path: 'data/part-001.parquet', format: 'parquet', recordCount: 100, fileSizeBytes: 5000, createdAt: new Date().toISOString() },
        ]

        const snapshot = await catalog.commitSnapshot('test', 'events', files)

        const table = await catalog.loadTable('test', 'events')
        expect(table?.currentSnapshotId).toBe(snapshot.snapshotId)
        expect(table?.snapshots).toHaveLength(1)
        expect(table?.snapshotLog).toHaveLength(1)
      })
    })

    describe('Empty operations', () => {
      it('commits snapshot with empty file list', async () => {
        await catalog.createNamespace('test')
        await catalog.createTable('test', 'events', [{ name: 'id', type: 'string' }])

        const snapshot = await catalog.commitSnapshot('test', 'events', [])

        expect(snapshot.summary.addedFiles).toBe(0)
        expect(snapshot.summary.addedRecords).toBe(0)
        expect(snapshot.manifestList).toEqual([])
      })
    })

    describe('Schema field types', () => {
      it('supports all field types', async () => {
        await catalog.createNamespace('types')

        const schema: SchemaField[] = [
          { name: 'str_field', type: 'string' },
          { name: 'int32_field', type: 'int32' },
          { name: 'int64_field', type: 'int64' },
          { name: 'float_field', type: 'float' },
          { name: 'double_field', type: 'double' },
          { name: 'bool_field', type: 'boolean' },
          { name: 'ts_field', type: 'timestamp' },
          { name: 'json_field', type: 'json' },
        ]

        const table = await catalog.createTable('types', 'all_types', schema)

        expect(table.schemas[0]?.fields).toHaveLength(8)
        expect(table.schemas[0]?.fields.map((f) => f.type)).toEqual([
          'string', 'int32', 'int64', 'float', 'double', 'boolean', 'timestamp', 'json',
        ])
      })

      it('supports nullable field annotation', async () => {
        await catalog.createNamespace('nullable')

        const schema: SchemaField[] = [
          { name: 'required', type: 'string', nullable: false },
          { name: 'optional', type: 'string', nullable: true },
          { name: 'default', type: 'string' }, // Default should be undefined/falsy
        ]

        const table = await catalog.createTable('nullable', 'test', schema)

        expect(table.schemas[0]?.fields[0]?.nullable).toBe(false)
        expect(table.schemas[0]?.fields[1]?.nullable).toBe(true)
        expect(table.schemas[0]?.fields[2]?.nullable).toBeUndefined()
      })

      it('supports field documentation', async () => {
        await catalog.createNamespace('docs')

        const schema: SchemaField[] = [
          { name: 'user_id', type: 'string', doc: 'Unique identifier for the user' },
          { name: 'created_at', type: 'timestamp', doc: 'When the record was created' },
        ]

        const table = await catalog.createTable('docs', 'documented', schema)

        expect(table.schemas[0]?.fields[0]?.doc).toBe('Unique identifier for the user')
        expect(table.schemas[0]?.fields[1]?.doc).toBe('When the record was created')
      })
    })

    describe('Concurrent operations', () => {
      it('handles multiple simultaneous namespace creations', async () => {
        const promises = [
          catalog.createNamespace('ns1'),
          catalog.createNamespace('ns2'),
          catalog.createNamespace('ns3'),
        ]

        await Promise.all(promises)

        const namespaces = await catalog.listNamespaces()
        expect(namespaces).toHaveLength(3)
      })

      it('handles multiple table creations in same namespace', async () => {
        await catalog.createNamespace('concurrent')

        const schema: SchemaField[] = [{ name: 'id', type: 'string' }]
        const promises = [
          catalog.createTable('concurrent', 't1', schema),
          catalog.createTable('concurrent', 't2', schema),
          catalog.createTable('concurrent', 't3', schema),
        ]

        await Promise.all(promises)

        const tables = await catalog.listTables('concurrent')
        expect(tables).toHaveLength(3)
      })
    })
  })
})
