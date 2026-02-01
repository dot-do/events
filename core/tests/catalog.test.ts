/**
 * events-rl8: CatalogDO Tests
 *
 * Tests for the Iceberg-style catalog Durable Object.
 * Since CatalogDO requires SQLite (ctx.storage.sql), we mock the SQL storage
 * with an in-memory table simulation.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock SQLite Storage
// ============================================================================

interface Row {
  [key: string]: unknown
}

/**
 * Creates a mock SqlStorage that simulates SQLite with in-memory tables.
 * Supports basic CREATE TABLE, INSERT, SELECT, UPDATE, DELETE operations.
 */
function createMockSqlStorage() {
  const tables: Record<string, Row[]> = {}

  function parseInsertValues(sql: string, params: unknown[]): { table: string; columns: string[]; values: unknown[] } {
    // INSERT INTO table (col1, col2) VALUES (?, ?)
    const match = sql.match(/INSERT INTO (\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i)
    if (!match) throw new Error(`Cannot parse INSERT: ${sql}`)
    const table = match[1]
    const columns = match[2].split(',').map(c => c.trim())
    return { table, columns, values: params }
  }

  function matchesConditions(row: Row, sql: string, params: unknown[]): boolean {
    // Simple WHERE clause parser for = and AND
    const whereMatch = sql.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s+GROUP|\s*$)/is)
    if (!whereMatch) return true

    const wherePart = whereMatch[1].trim()
    const conditions = wherePart.split(/\s+AND\s+/i)
    let paramIdx = 0

    // Count params used before WHERE
    const beforeWhere = sql.substring(0, sql.indexOf('WHERE'))
    const questionsBefore = (beforeWhere.match(/\?/g) || []).length

    // Use params starting after the ones consumed before WHERE
    const whereParams = params.slice(questionsBefore)
    paramIdx = 0

    for (const cond of conditions) {
      const eqMatch = cond.trim().match(/^(\w+)\s*=\s*\?$/i)
      if (eqMatch) {
        const col = eqMatch[1]
        if (row[col] !== whereParams[paramIdx]) return false
        paramIdx++
        continue
      }

      const leMatch = cond.trim().match(/^(\w+)\s*<=\s*\?$/i)
      if (leMatch) {
        const col = leMatch[1]
        if ((row[col] as number) > (whereParams[paramIdx] as number)) return false
        paramIdx++
        continue
      }

      const eqNullMatch = cond.trim().match(/^1=1$/i)
      if (eqNullMatch) continue

      // For LIKE, IN, etc. - skip for simplicity
      paramIdx++
    }
    return true
  }

  const sqlStorage = {
    exec: vi.fn((sql: string, ...params: unknown[]) => {
      const trimmed = sql.trim()

      // CREATE TABLE / CREATE INDEX - no-op
      if (trimmed.startsWith('CREATE')) {
        return {
          toArray: () => [],
          one: () => null,
          rowsWritten: 0,
        }
      }

      // INSERT
      if (trimmed.startsWith('INSERT')) {
        const { table, columns, values } = parseInsertValues(trimmed, params)
        if (!tables[table]) tables[table] = []

        const row: Row = {}
        columns.forEach((col, i) => {
          row[col] = values[i] !== undefined ? values[i] : null
        })

        // Check UNIQUE constraints for tables
        if (table === 'tables') {
          const existing = tables[table].find(
            r => r.namespace === row.namespace && r.name === row.name
          )
          if (existing) {
            throw new Error('UNIQUE constraint failed: tables.namespace, tables.name')
          }
        }

        if (table === 'namespaces') {
          const existing = tables[table].find(r => r.name === row.name)
          if (existing) {
            throw new Error('UNIQUE constraint failed: namespaces.name')
          }
        }

        tables[table].push(row)
        return {
          toArray: () => [],
          one: () => null,
          rowsWritten: 1,
        }
      }

      // SELECT
      if (trimmed.startsWith('SELECT')) {
        // Determine the table
        const fromMatch = trimmed.match(/FROM\s+(\w+)/i)
        if (!fromMatch) return { toArray: () => [], one: () => null }

        const table = fromMatch[1]
        const rows = tables[table] || []

        // COUNT(*)
        if (trimmed.includes('COUNT(*)')) {
          const filtered = rows.filter(r => matchesConditions(r, trimmed, params))
          return {
            toArray: () => [{ count: filtered.length }],
            one: () => ({ count: filtered.length }),
          }
        }

        // Filter by WHERE
        let result = rows.filter(r => matchesConditions(r, trimmed, params))

        // ORDER BY
        const orderMatch = trimmed.match(/ORDER BY\s+(\w+)(?:\s+(ASC|DESC))?/i)
        if (orderMatch) {
          const col = orderMatch[1]
          const dir = (orderMatch[2] || 'ASC').toUpperCase()
          result = [...result].sort((a, b) => {
            const va = a[col] as string | number
            const vb = b[col] as string | number
            if (va < vb) return dir === 'ASC' ? -1 : 1
            if (va > vb) return dir === 'ASC' ? 1 : -1
            return 0
          })
        }

        // LIMIT
        const limitMatch = trimmed.match(/LIMIT\s+(\d+)/i)
        if (limitMatch) {
          result = result.slice(0, parseInt(limitMatch[1]))
        }

        return {
          toArray: () => result,
          one: () => result[0] || null,
        }
      }

      // UPDATE
      if (trimmed.startsWith('UPDATE')) {
        const tableMatch = trimmed.match(/UPDATE\s+(\w+)/i)
        if (!tableMatch) return { toArray: () => [], one: () => null, rowsWritten: 0 }

        const table = tableMatch[1]
        const rows = tables[table] || []

        // Parse SET clause
        const setMatch = trimmed.match(/SET\s+(.+?)\s+WHERE/i)
        if (!setMatch) return { toArray: () => [], one: () => null, rowsWritten: 0 }

        const setClauses = setMatch[1].split(',').map(s => s.trim())
        const setParamCount = setClauses.filter(s => s.includes('?')).length
        const setParams = params.slice(0, setParamCount)
        const whereParams = params.slice(setParamCount)

        let updated = 0
        for (const row of rows) {
          if (matchesConditions(row, trimmed, params)) {
            let paramIdx = 0
            for (const clause of setClauses) {
              const m = clause.match(/^(\w+)\s*=\s*\?$/i)
              if (m) {
                row[m[1]] = setParams[paramIdx]
                paramIdx++
              }
            }
            updated++
          }
        }

        return {
          toArray: () => [],
          one: () => null,
          rowsWritten: updated,
        }
      }

      // DELETE
      if (trimmed.startsWith('DELETE')) {
        const tableMatch = trimmed.match(/DELETE FROM\s+(\w+)/i)
        if (!tableMatch) return { toArray: () => [], one: () => null, rowsWritten: 0 }

        const table = tableMatch[1]
        if (!tables[table]) return { toArray: () => [], one: () => null, rowsWritten: 0 }

        const before = tables[table].length
        tables[table] = tables[table].filter(r => !matchesConditions(r, trimmed, params))
        const deleted = before - tables[table].length

        return {
          toArray: () => [],
          one: () => null,
          rowsWritten: deleted,
        }
      }

      return { toArray: () => [], one: () => null, rowsWritten: 0 }
    }),

    // Expose internals for debugging
    _tables: tables,
  }

  return sqlStorage
}

/**
 * Creates a mock DurableObjectState for CatalogDO
 */
function createMockCtx() {
  const sqlStorage = createMockSqlStorage()

  return {
    id: {
      toString: () => 'catalog-do-id',
      equals: (other: any) => other.toString() === 'catalog-do-id',
    },
    storage: {
      sql: sqlStorage,
      get: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
      getAlarm: vi.fn(async () => null),
      setAlarm: vi.fn(),
    },
    waitUntil: vi.fn(),
    blockConcurrencyWhile: vi.fn(<T>(fn: () => Promise<T>) => fn()),
  }
}

// ============================================================================
// Import CatalogDO - we test the actual class with mocked ctx
// ============================================================================

// Since CatalogDO extends DurableObject from 'cloudflare:workers' which is not
// available in test environment, we need to mock the module before import.
vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    ctx: any
    env: any
    constructor(ctx: any, env: any) {
      this.ctx = ctx
      this.env = env
    }
  },
}))

import { CatalogDO } from '../src/catalog'

// ============================================================================
// Tests
// ============================================================================

describe('CatalogDO', () => {
  let catalog: CatalogDO
  let mockCtx: ReturnType<typeof createMockCtx>

  beforeEach(() => {
    mockCtx = createMockCtx()
    catalog = new CatalogDO(mockCtx as any, {})
  })

  // --------------------------------------------------------------------------
  // Namespace CRUD
  // --------------------------------------------------------------------------

  describe('Namespace operations', () => {
    it('creates a namespace', async () => {
      await catalog.createNamespace('production')
      const namespaces = await catalog.listNamespaces()
      expect(namespaces).toContain('production')
    })

    it('creates a namespace with properties', async () => {
      await catalog.createNamespace('staging', { region: 'us-west-2' })
      const namespaces = await catalog.listNamespaces()
      expect(namespaces).toContain('staging')
    })

    it('lists multiple namespaces in alphabetical order', async () => {
      await catalog.createNamespace('beta')
      await catalog.createNamespace('alpha')
      await catalog.createNamespace('gamma')

      const namespaces = await catalog.listNamespaces()
      expect(namespaces).toEqual(['alpha', 'beta', 'gamma'])
    })

    it('lists empty namespaces', async () => {
      const namespaces = await catalog.listNamespaces()
      expect(namespaces).toEqual([])
    })

    it('drops an empty namespace', async () => {
      await catalog.createNamespace('temp')
      let namespaces = await catalog.listNamespaces()
      expect(namespaces).toContain('temp')

      await catalog.dropNamespace('temp')
      namespaces = await catalog.listNamespaces()
      expect(namespaces).not.toContain('temp')
    })

    it('throws when dropping a non-empty namespace', async () => {
      await catalog.createNamespace('production')
      await catalog.createTable('production', 'events', [
        { name: 'ts', type: 'timestamp' },
        { name: 'type', type: 'string' },
      ])

      await expect(catalog.dropNamespace('production')).rejects.toThrow(
        'Namespace production is not empty'
      )
    })

    it('throws when creating duplicate namespace', async () => {
      await catalog.createNamespace('production')
      await expect(catalog.createNamespace('production')).rejects.toThrow('UNIQUE constraint')
    })
  })

  // --------------------------------------------------------------------------
  // Table operations
  // --------------------------------------------------------------------------

  describe('Table operations', () => {
    beforeEach(async () => {
      await catalog.createNamespace('default')
    })

    it('creates a table with schema', async () => {
      const table = await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
        { name: 'type', type: 'string' },
        { name: 'data', type: 'json', nullable: true },
      ])

      expect(table.name).toBe('events')
      expect(table.namespace).toBe('default')
      expect(table.schemas).toHaveLength(1)
      expect(table.schemas[0].fields).toHaveLength(3)
      expect(table.schemas[0].fields[0].name).toBe('ts')
      expect(table.schemas[0].fields[0].type).toBe('timestamp')
    })

    it('creates a table with custom location', async () => {
      const table = await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
      ], {
        location: 'custom/path/events',
      })

      expect(table.location).toBe('custom/path/events')
    })

    it('uses default location based on namespace/name', async () => {
      const table = await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
      ])

      expect(table.location).toBe('default/events')
    })

    it('lists tables in a namespace', async () => {
      await catalog.createTable('default', 'events', [{ name: 'ts', type: 'timestamp' }])
      await catalog.createTable('default', 'metrics', [{ name: 'value', type: 'double' }])

      const tables = await catalog.listTables('default')
      expect(tables).toContain('events')
      expect(tables).toContain('metrics')
      expect(tables).toHaveLength(2)
    })

    it('lists tables in alphabetical order', async () => {
      await catalog.createTable('default', 'zebra', [{ name: 'id', type: 'string' }])
      await catalog.createTable('default', 'apple', [{ name: 'id', type: 'string' }])

      const tables = await catalog.listTables('default')
      expect(tables).toEqual(['apple', 'zebra'])
    })

    it('returns empty array for namespace with no tables', async () => {
      const tables = await catalog.listTables('default')
      expect(tables).toEqual([])
    })

    it('loads a table by namespace and name', async () => {
      await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
        { name: 'type', type: 'string' },
      ])

      const table = await catalog.loadTable('default', 'events')
      expect(table).not.toBeNull()
      expect(table!.name).toBe('events')
      expect(table!.namespace).toBe('default')
      expect(table!.schemas[0].fields).toHaveLength(2)
    })

    it('returns null for non-existent table', async () => {
      const table = await catalog.loadTable('default', 'nonexistent')
      expect(table).toBeNull()
    })

    it('drops a table and its associated data', async () => {
      await catalog.createTable('default', 'events', [{ name: 'ts', type: 'timestamp' }])

      await catalog.dropTable('default', 'events')

      const table = await catalog.loadTable('default', 'events')
      expect(table).toBeNull()

      const tables = await catalog.listTables('default')
      expect(tables).not.toContain('events')
    })

    it('handles dropping a non-existent table gracefully', async () => {
      // Should not throw
      await catalog.dropTable('default', 'nonexistent')
    })

    it('rejects duplicate table names in same namespace', async () => {
      await catalog.createTable('default', 'events', [{ name: 'ts', type: 'timestamp' }])
      await expect(
        catalog.createTable('default', 'events', [{ name: 'ts', type: 'timestamp' }])
      ).rejects.toThrow('UNIQUE constraint')
    })

    it('allows same table name in different namespaces', async () => {
      await catalog.createNamespace('staging')
      await catalog.createTable('default', 'events', [{ name: 'ts', type: 'timestamp' }])
      await catalog.createTable('staging', 'events', [{ name: 'ts', type: 'timestamp' }])

      const defaultTables = await catalog.listTables('default')
      const stagingTables = await catalog.listTables('staging')
      expect(defaultTables).toContain('events')
      expect(stagingTables).toContain('events')
    })
  })

  // --------------------------------------------------------------------------
  // Snapshot commits
  // --------------------------------------------------------------------------

  describe('Snapshot operations', () => {
    beforeEach(async () => {
      await catalog.createNamespace('default')
      await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
        { name: 'type', type: 'string' },
      ])
    })

    it('commits a snapshot with data files', async () => {
      const snapshot = await catalog.commitSnapshot('default', 'events', [
        {
          path: 'default/events/2024/01/data-001.parquet',
          format: 'parquet' as const,
          recordCount: 1000,
          fileSizeBytes: 50000,
          createdAt: new Date().toISOString(),
        },
      ])

      expect(snapshot.snapshotId).toBeDefined()
      expect(snapshot.operation).toBe('append')
      expect(snapshot.summary.addedRecords).toBe(1000)
      expect(snapshot.summary.addedFiles).toBe(1)
      expect(snapshot.summary.totalRecords).toBe(1000)
      expect(snapshot.summary.totalFiles).toBe(1)
    })

    it('commits multiple snapshots with cumulative totals', async () => {
      const snap1 = await catalog.commitSnapshot('default', 'events', [
        {
          path: 'default/events/2024/01/data-001.parquet',
          format: 'parquet' as const,
          recordCount: 500,
          fileSizeBytes: 25000,
          createdAt: new Date().toISOString(),
        },
      ])

      const snap2 = await catalog.commitSnapshot('default', 'events', [
        {
          path: 'default/events/2024/01/data-002.parquet',
          format: 'parquet' as const,
          recordCount: 300,
          fileSizeBytes: 15000,
          createdAt: new Date().toISOString(),
        },
      ])

      expect(snap2.parentSnapshotId).toBe(snap1.snapshotId)
      expect(snap2.summary.addedRecords).toBe(300)
      expect(snap2.summary.totalRecords).toBe(800) // 500 + 300
      expect(snap2.summary.totalFiles).toBe(2) // 1 + 1
    })

    it('commits snapshot with CDC bookmark', async () => {
      const snapshot = await catalog.commitSnapshot('default', 'events', [
        {
          path: 'default/events/data.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ], {
        cdcBookmark: 'bookmark-abc-123',
      })

      expect(snapshot.cdcBookmark).toBe('bookmark-abc-123')
    })

    it('throws when committing to non-existent table', async () => {
      await expect(
        catalog.commitSnapshot('default', 'nonexistent', [
          {
            path: 'data.parquet',
            format: 'parquet' as const,
            recordCount: 100,
            fileSizeBytes: 5000,
            createdAt: new Date().toISOString(),
          },
        ])
      ).rejects.toThrow('Table default.nonexistent not found')
    })

    it('records manifest list from file paths', async () => {
      const snapshot = await catalog.commitSnapshot('default', 'events', [
        {
          path: 'path/to/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
        {
          path: 'path/to/file2.parquet',
          format: 'parquet' as const,
          recordCount: 200,
          fileSizeBytes: 10000,
          createdAt: new Date().toISOString(),
        },
      ])

      expect(snapshot.manifestList).toEqual([
        'path/to/file1.parquet',
        'path/to/file2.parquet',
      ])
    })
  })

  // --------------------------------------------------------------------------
  // Query building
  // --------------------------------------------------------------------------

  describe('Query building', () => {
    beforeEach(async () => {
      await catalog.createNamespace('default')
      await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
        { name: 'type', type: 'string' },
      ])
    })

    it('returns empty string when table has no files', async () => {
      const sql = await catalog.buildQuery('default', 'events')
      expect(sql).toBe('')
    })

    it('generates SELECT query for parquet files', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      const sql = await catalog.buildQuery('default', 'events')
      expect(sql).toContain('SELECT *')
      expect(sql).toContain('read_parquet')
      expect(sql).toContain('data/file1.parquet')
    })

    it('generates SELECT query for jsonl files', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.jsonl',
          format: 'jsonl' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      const sql = await catalog.buildQuery('default', 'events')
      expect(sql).toContain('read_json_auto')
    })

    it('generates query with specific columns', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      const sql = await catalog.buildQuery('default', 'events', {
        columns: ['ts', 'type'],
      })
      expect(sql).toContain('SELECT ts, type')
    })

    it('generates query with WHERE clause', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      const sql = await catalog.buildQuery('default', 'events', {
        where: "type = 'rpc.call'",
      })
      expect(sql).toContain("WHERE type = 'rpc.call'")
    })

    it('generates query with LIMIT', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      const sql = await catalog.buildQuery('default', 'events', { limit: 50 })
      expect(sql).toContain('LIMIT 50')
    })

    it('rejects invalid column names', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      await expect(
        catalog.buildQuery('default', 'events', {
          columns: ['valid_col', "'; DROP TABLE --"],
        })
      ).rejects.toThrow('Invalid column name')
    })

    it('rejects unsafe WHERE clause', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      await expect(
        catalog.buildQuery('default', 'events', {
          where: "type = 'x'; DROP TABLE events; --",
        })
      ).rejects.toThrow('Invalid where clause')
    })

    it('allows safe WHERE expressions', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
      ])

      const sql = await catalog.buildQuery('default', 'events', {
        where: "type = 'rpc.call' AND ts > '2024-01-01'",
      })
      expect(sql).toContain("WHERE type = 'rpc.call' AND ts > '2024-01-01'")
    })
  })

  // --------------------------------------------------------------------------
  // File listing
  // --------------------------------------------------------------------------

  describe('File listing', () => {
    beforeEach(async () => {
      await catalog.createNamespace('default')
      await catalog.createTable('default', 'events', [
        { name: 'ts', type: 'timestamp' },
      ])
    })

    it('returns empty array when no snapshots', async () => {
      const files = await catalog.listFiles('default', 'events')
      expect(files).toEqual([])
    })

    it('returns files for the current snapshot', async () => {
      await catalog.commitSnapshot('default', 'events', [
        {
          path: 'data/file1.parquet',
          format: 'parquet' as const,
          recordCount: 100,
          fileSizeBytes: 5000,
          createdAt: new Date().toISOString(),
        },
        {
          path: 'data/file2.parquet',
          format: 'parquet' as const,
          recordCount: 200,
          fileSizeBytes: 10000,
          createdAt: new Date().toISOString(),
        },
      ])

      const files = await catalog.listFiles('default', 'events')
      expect(files).toHaveLength(2)
      expect(files[0].path).toBe('data/file1.parquet')
      expect(files[0].recordCount).toBe(100)
      expect(files[1].path).toBe('data/file2.parquet')
      expect(files[1].recordCount).toBe(200)
    })

    it('returns empty for non-existent table', async () => {
      const files = await catalog.listFiles('default', 'nonexistent')
      expect(files).toEqual([])
    })
  })
})
