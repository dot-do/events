/**
 * Migration System Tests
 *
 * Tests for SQLite schema versioning in Durable Objects.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  runMigrations,
  getSchemaVersion,
  createMigrationFromStatements,
  tableExists,
  columnExists,
  indexExists,
  safeAddColumn,
  safeCreateIndex,
  type Migration,
  DEFAULT_VERSION_TABLE,
} from '../migrations.js'

// ============================================================================
// Mock SqlStorage
// ============================================================================

interface MockSqlRow {
  [key: string]: unknown
}

/**
 * Creates a mock SqlStorage with in-memory SQLite-like behavior
 */
function createMockSqlStorage() {
  const tables = new Map<string, Map<string, MockSqlRow>>()
  const columns = new Map<string, string[]>() // table -> column names
  const indexes = new Map<string, { table: string; columns: string[] }>()

  // Initialize schema version table structure
  const schemaVersionData = new Map<string, MockSqlRow>()

  const exec = vi.fn((sql: string, ...params: unknown[]) => {
    const sqlLower = sql.toLowerCase().trim()

    // Handle CREATE TABLE IF NOT EXISTS _schema_version
    if (sqlLower.includes('create table') && sqlLower.includes('_schema_version')) {
      if (!tables.has('_schema_version')) {
        tables.set('_schema_version', schemaVersionData)
        columns.set('_schema_version', ['id', 'version', 'updated_at'])
      }
      return { toArray: () => [], one: () => null, rowsWritten: 0 }
    }

    // Handle INSERT OR IGNORE INTO _schema_version
    if (sqlLower.includes('insert or ignore into _schema_version')) {
      if (!schemaVersionData.has('1')) {
        schemaVersionData.set('1', { id: 1, version: 0, updated_at: new Date().toISOString() })
      }
      return { toArray: () => [], one: () => null, rowsWritten: 1 }
    }

    // Handle SELECT version FROM _schema_version
    if (sqlLower.includes('select version from _schema_version')) {
      const row = schemaVersionData.get('1')
      return {
        toArray: () => (row ? [row] : []),
        one: () => row ?? null,
        rowsWritten: 0,
      }
    }

    // Handle UPDATE _schema_version SET version
    if (sqlLower.includes('update _schema_version set version')) {
      const version = params[0] as number
      const row = schemaVersionData.get('1')
      if (row) {
        row.version = version
        row.updated_at = new Date().toISOString()
      }
      return { toArray: () => [], one: () => null, rowsWritten: 1 }
    }

    // Handle CREATE TABLE
    if (sqlLower.startsWith('create table')) {
      const match = sql.match(/create\s+table\s+(?:if\s+not\s+exists\s+)?(\w+)/i)
      if (match) {
        const tableName = match[1]!.toLowerCase()
        if (!tables.has(tableName)) {
          tables.set(tableName, new Map())
          // Extract column names from CREATE TABLE statement
          const colMatch = sql.match(/\(([^)]+)\)/s)
          if (colMatch) {
            const colDefs = colMatch[1]!
            const colNames = colDefs
              .split(',')
              .map((c) => c.trim().split(/\s+/)[0]!)
              .filter((c) => !c.toUpperCase().startsWith('PRIMARY') && !c.toUpperCase().startsWith('FOREIGN') && !c.toUpperCase().startsWith('UNIQUE') && !c.toUpperCase().startsWith('CHECK'))
            columns.set(tableName, colNames)
          }
        }
      }
      return { toArray: () => [], one: () => null, rowsWritten: 0 }
    }

    // Handle CREATE INDEX
    if (sqlLower.startsWith('create index') || sqlLower.startsWith('create unique index')) {
      const match = sql.match(/create\s+(?:unique\s+)?index\s+(?:if\s+not\s+exists\s+)?(\w+)\s+on\s+(\w+)\s*\(([^)]+)\)/i)
      if (match) {
        const indexName = match[1]!.toLowerCase()
        const tableName = match[2]!.toLowerCase()
        const indexCols = match[3]!.split(',').map((c) => c.trim())
        indexes.set(indexName, { table: tableName, columns: indexCols })
      }
      return { toArray: () => [], one: () => null, rowsWritten: 0 }
    }

    // Handle ALTER TABLE ADD COLUMN
    if (sqlLower.includes('alter table') && sqlLower.includes('add column')) {
      const match = sql.match(/alter\s+table\s+(\w+)\s+add\s+column\s+(\w+)/i)
      if (match) {
        const tableName = match[1]!.toLowerCase()
        const columnName = match[2]!.toLowerCase()
        const cols = columns.get(tableName) ?? []
        if (!cols.includes(columnName)) {
          cols.push(columnName)
          columns.set(tableName, cols)
        }
      }
      return { toArray: () => [], one: () => null, rowsWritten: 1 }
    }

    // Handle PRAGMA table_info
    if (sqlLower.includes('pragma table_info')) {
      const match = sql.match(/pragma\s+table_info\((\w+)\)/i)
      if (match) {
        const tableName = match[1]!.toLowerCase()
        const cols = columns.get(tableName) ?? []
        const rows = cols.map((name, idx) => ({ cid: idx, name, type: 'TEXT', notnull: 0, dflt_value: null, pk: 0 }))
        return { toArray: () => rows, one: () => rows[0] ?? null, rowsWritten: 0 }
      }
    }

    // Handle SELECT name FROM sqlite_master for tables
    if (sqlLower.includes('sqlite_master') && sqlLower.includes("type='table'")) {
      const tableName = params[0] as string
      const exists = tables.has(tableName?.toLowerCase())
      return {
        toArray: () => (exists ? [{ name: tableName }] : []),
        one: () => (exists ? { name: tableName } : null),
        rowsWritten: 0,
      }
    }

    // Handle SELECT name FROM sqlite_master for indexes
    if (sqlLower.includes('sqlite_master') && sqlLower.includes("type='index'")) {
      const indexName = params[0] as string
      const exists = indexes.has(indexName?.toLowerCase())
      return {
        toArray: () => (exists ? [{ name: indexName }] : []),
        one: () => (exists ? { name: indexName } : null),
        rowsWritten: 0,
      }
    }

    // Default: return empty result
    return { toArray: () => [], one: () => null, rowsWritten: 0 }
  })

  return {
    exec,
    _tables: tables,
    _columns: columns,
    _indexes: indexes,
    _schemaVersionData: schemaVersionData,
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('Migration System', () => {
  let sql: ReturnType<typeof createMockSqlStorage>

  beforeEach(() => {
    sql = createMockSqlStorage()
  })

  describe('getSchemaVersion', () => {
    it('returns 0 for a fresh database', () => {
      const version = getSchemaVersion(sql as unknown as SqlStorage)
      expect(version).toBe(0)
    })

    it('creates the version table if it does not exist', () => {
      getSchemaVersion(sql as unknown as SqlStorage)
      expect(sql._tables.has('_schema_version')).toBe(true)
    })

    it('returns the current version after migrations', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'Test migration', up: () => {} },
      ]
      runMigrations(sql as unknown as SqlStorage, migrations)

      const version = getSchemaVersion(sql as unknown as SqlStorage)
      expect(version).toBe(1)
    })
  })

  describe('runMigrations', () => {
    it('runs all migrations in order', () => {
      const executionOrder: number[] = []
      const migrations: Migration[] = [
        { version: 1, description: 'First', up: () => executionOrder.push(1) },
        { version: 2, description: 'Second', up: () => executionOrder.push(2) },
        { version: 3, description: 'Third', up: () => executionOrder.push(3) },
      ]

      const result = runMigrations(sql as unknown as SqlStorage, migrations)

      expect(result.success).toBe(true)
      expect(result.previousVersion).toBe(0)
      expect(result.currentVersion).toBe(3)
      expect(result.appliedMigrations).toEqual([1, 2, 3])
      expect(executionOrder).toEqual([1, 2, 3])
    })

    it('only runs pending migrations', () => {
      // First run
      const migrations1: Migration[] = [
        { version: 1, description: 'First', up: () => {} },
        { version: 2, description: 'Second', up: () => {} },
      ]
      runMigrations(sql as unknown as SqlStorage, migrations1)

      // Second run with additional migration
      const executionOrder: number[] = []
      const migrations2: Migration[] = [
        { version: 1, description: 'First', up: () => executionOrder.push(1) },
        { version: 2, description: 'Second', up: () => executionOrder.push(2) },
        { version: 3, description: 'Third', up: () => executionOrder.push(3) },
      ]
      const result = runMigrations(sql as unknown as SqlStorage, migrations2)

      expect(result.success).toBe(true)
      expect(result.previousVersion).toBe(2)
      expect(result.currentVersion).toBe(3)
      expect(result.appliedMigrations).toEqual([3])
      expect(executionOrder).toEqual([3])
    })

    it('returns early when no migrations are pending', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'First', up: () => {} },
      ]
      runMigrations(sql as unknown as SqlStorage, migrations)

      // Run again
      const result = runMigrations(sql as unknown as SqlStorage, migrations)

      expect(result.success).toBe(true)
      expect(result.previousVersion).toBe(1)
      expect(result.currentVersion).toBe(1)
      expect(result.appliedMigrations).toEqual([])
    })

    it('handles empty migration list', () => {
      const result = runMigrations(sql as unknown as SqlStorage, [])

      expect(result.success).toBe(true)
      expect(result.previousVersion).toBe(0)
      expect(result.currentVersion).toBe(0)
      expect(result.appliedMigrations).toEqual([])
    })

    it('validates migration versions for duplicates', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'First', up: () => {} },
        { version: 1, description: 'Duplicate', up: () => {} },
      ]

      const result = runMigrations(sql as unknown as SqlStorage, migrations)

      expect(result.success).toBe(false)
      expect(result.error).toContain('Duplicate migration version')
    })

    it('validates migration versions for gaps', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'First', up: () => {} },
        { version: 3, description: 'Third (missing 2)', up: () => {} },
      ]

      const result = runMigrations(sql as unknown as SqlStorage, migrations)

      expect(result.success).toBe(false)
      expect(result.error).toContain('Migration version gap')
    })

    it('handles migration failures gracefully', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'First', up: () => {} },
        { version: 2, description: 'Fails', up: () => { throw new Error('Migration failed!') } },
        { version: 3, description: 'Third', up: () => {} },
      ]

      const result = runMigrations(sql as unknown as SqlStorage, migrations)

      expect(result.success).toBe(false)
      expect(result.previousVersion).toBe(0)
      expect(result.currentVersion).toBe(1) // First migration succeeded
      expect(result.appliedMigrations).toEqual([1])
      expect(result.error).toContain('Migration failed!')
    })

    it('can skip version validation', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'First', up: () => {} },
        { version: 5, description: 'Fifth (gap allowed)', up: () => {} },
      ]

      const result = runMigrations(sql as unknown as SqlStorage, migrations, {
        validateVersions: false,
      })

      expect(result.success).toBe(true)
      expect(result.currentVersion).toBe(5)
    })

    it('uses custom version table name', () => {
      const migrations: Migration[] = [
        { version: 1, description: 'Test', up: () => {} },
      ]

      runMigrations(sql as unknown as SqlStorage, migrations, {
        versionTableName: '_custom_schema_version',
      })

      // Check that custom table was used
      expect(sql.exec).toHaveBeenCalledWith(
        expect.stringContaining('_custom_schema_version'),
        expect.anything()
      )
    })
  })

  describe('createMigrationFromStatements', () => {
    it('creates a migration function from SQL statements', () => {
      const statements = [
        'CREATE TABLE users (id TEXT PRIMARY KEY)',
        'CREATE TABLE posts (id TEXT PRIMARY KEY)',
        'CREATE INDEX idx_posts_user ON posts(user_id)',
      ]

      const up = createMigrationFromStatements(statements)
      up(sql as unknown as SqlStorage)

      expect(sql.exec).toHaveBeenCalledWith('CREATE TABLE users (id TEXT PRIMARY KEY)')
      expect(sql.exec).toHaveBeenCalledWith('CREATE TABLE posts (id TEXT PRIMARY KEY)')
      expect(sql.exec).toHaveBeenCalledWith('CREATE INDEX idx_posts_user ON posts(user_id)')
    })
  })

  describe('tableExists', () => {
    it('returns false for non-existent table', () => {
      const exists = tableExists(sql as unknown as SqlStorage, 'nonexistent')
      expect(exists).toBe(false)
    })

    it('returns true for existing table', () => {
      sql.exec('CREATE TABLE test_table (id TEXT)')
      const exists = tableExists(sql as unknown as SqlStorage, 'test_table')
      expect(exists).toBe(true)
    })
  })

  describe('columnExists', () => {
    it('returns false for non-existent column', () => {
      sql.exec('CREATE TABLE test_table (id TEXT)')
      const exists = columnExists(sql as unknown as SqlStorage, 'test_table', 'nonexistent')
      expect(exists).toBe(false)
    })

    it('returns true for existing column', () => {
      sql.exec('CREATE TABLE test_table (id TEXT, name TEXT)')
      const exists = columnExists(sql as unknown as SqlStorage, 'test_table', 'name')
      expect(exists).toBe(true)
    })
  })

  describe('indexExists', () => {
    it('returns false for non-existent index', () => {
      const exists = indexExists(sql as unknown as SqlStorage, 'nonexistent_idx')
      expect(exists).toBe(false)
    })

    it('returns true for existing index', () => {
      sql.exec('CREATE INDEX test_idx ON test_table(col)')
      const exists = indexExists(sql as unknown as SqlStorage, 'test_idx')
      expect(exists).toBe(true)
    })
  })

  describe('safeAddColumn', () => {
    it('adds column when it does not exist', () => {
      sql.exec('CREATE TABLE test_table (id TEXT)')
      const added = safeAddColumn(sql as unknown as SqlStorage, 'test_table', 'new_col', 'TEXT')

      expect(added).toBe(true)
      expect(sql.exec).toHaveBeenCalledWith('ALTER TABLE test_table ADD COLUMN new_col TEXT')
    })

    it('returns false when column already exists', () => {
      sql.exec('CREATE TABLE test_table (id TEXT, existing_col TEXT)')
      const added = safeAddColumn(sql as unknown as SqlStorage, 'test_table', 'existing_col', 'TEXT')

      expect(added).toBe(false)
    })
  })

  describe('safeCreateIndex', () => {
    it('creates index when it does not exist', () => {
      const created = safeCreateIndex(sql as unknown as SqlStorage, 'test_idx', 'test_table', ['col1', 'col2'])

      expect(created).toBe(true)
      expect(sql.exec).toHaveBeenCalledWith('CREATE INDEX test_idx ON test_table(col1, col2)')
    })

    it('creates unique index when specified', () => {
      const created = safeCreateIndex(sql as unknown as SqlStorage, 'test_idx', 'test_table', ['col1'], { unique: true })

      expect(created).toBe(true)
      expect(sql.exec).toHaveBeenCalledWith('CREATE UNIQUE INDEX test_idx ON test_table(col1)')
    })

    it('creates partial index with WHERE clause', () => {
      const created = safeCreateIndex(sql as unknown as SqlStorage, 'test_idx', 'test_table', ['col1'], { where: 'active = 1' })

      expect(created).toBe(true)
      expect(sql.exec).toHaveBeenCalledWith('CREATE INDEX test_idx ON test_table(col1) WHERE active = 1')
    })

    it('returns false when index already exists', () => {
      sql.exec('CREATE INDEX existing_idx ON test_table(col)')
      const created = safeCreateIndex(sql as unknown as SqlStorage, 'existing_idx', 'test_table', ['col'])

      expect(created).toBe(false)
    })
  })

  describe('Integration: Real Migration Scenarios', () => {
    it('handles typical initial schema migration', () => {
      const migrations: Migration[] = [
        {
          version: 1,
          description: 'Initial schema',
          up: (sql) => {
            sql.exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`)
            sql.exec(`CREATE TABLE posts (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, content TEXT)`)
            sql.exec(`CREATE INDEX idx_posts_user ON posts(user_id)`)
          },
        },
      ]

      const result = runMigrations(sql as unknown as SqlStorage, migrations)

      expect(result.success).toBe(true)
      expect(sql._tables.has('users')).toBe(true)
      expect(sql._tables.has('posts')).toBe(true)
      expect(sql._indexes.has('idx_posts_user')).toBe(true)
    })

    it('handles incremental schema evolution', () => {
      // Run initial migration
      const initialMigrations: Migration[] = [
        {
          version: 1,
          description: 'Initial schema',
          up: (sql) => {
            sql.exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)`)
          },
        },
      ]
      runMigrations(sql as unknown as SqlStorage, initialMigrations)

      // Add new migration
      const allMigrations: Migration[] = [
        ...initialMigrations,
        {
          version: 2,
          description: 'Add email column',
          up: (sql) => {
            sql.exec(`ALTER TABLE users ADD COLUMN email TEXT`)
          },
        },
      ]
      const result = runMigrations(sql as unknown as SqlStorage, allMigrations)

      expect(result.success).toBe(true)
      expect(result.appliedMigrations).toEqual([2])
      expect(sql._columns.get('users')).toContain('email')
    })
  })
})
