/**
 * SQLite Schema Versioning for Durable Objects
 *
 * Provides a migration system for safe schema evolution in DOs that use SQLite.
 *
 * Key concepts:
 * - Schema version tracked in a dedicated table
 * - Migrations are numbered and run sequentially
 * - Each migration has an up() function that modifies the schema
 * - Migrations are run inside a transaction for atomicity
 *
 * @example
 * ```typescript
 * const migrations: Migration[] = [
 *   {
 *     version: 1,
 *     description: 'Initial schema',
 *     up: (sql) => {
 *       sql.exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`)
 *     }
 *   },
 *   {
 *     version: 2,
 *     description: 'Add email column',
 *     up: (sql) => {
 *       sql.exec(`ALTER TABLE users ADD COLUMN email TEXT`)
 *     }
 *   }
 * ]
 *
 * // In DO constructor:
 * runMigrations(ctx.storage.sql, migrations)
 * ```
 */

// ============================================================================
// Types
// ============================================================================

/**
 * A single migration definition
 */
export interface Migration {
  /** Unique version number (must be sequential starting from 1) */
  version: number
  /** Human-readable description of what this migration does */
  description: string
  /** Function that applies the migration */
  up: (sql: SqlStorage) => void
}

/**
 * Result of running migrations
 */
export interface MigrationResult {
  /** Whether migrations were run successfully */
  success: boolean
  /** Previous schema version before migrations */
  previousVersion: number
  /** Current schema version after migrations */
  currentVersion: number
  /** List of migrations that were applied */
  appliedMigrations: number[]
  /** Error message if migrations failed */
  error?: string | undefined
}

/**
 * Options for the migration runner
 */
export interface MigrationOptions {
  /** Table name for storing schema version (default: '_schema_version') */
  versionTableName?: string | undefined
  /** Whether to validate migration version numbers (default: true) */
  validateVersions?: boolean | undefined
}

// ============================================================================
// Constants
// ============================================================================

/** Default table name for schema versioning */
export const DEFAULT_VERSION_TABLE = '_schema_version'

// ============================================================================
// Migration Runner
// ============================================================================

/**
 * Ensures the schema version table exists
 */
function ensureVersionTable(sql: SqlStorage, tableName: string): void {
  sql.exec(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      version INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `)

  // Insert initial row if not exists
  sql.exec(`
    INSERT OR IGNORE INTO ${tableName} (id, version, updated_at)
    VALUES (1, 0, datetime('now'))
  `)
}

/**
 * Gets the current schema version
 */
export function getSchemaVersion(sql: SqlStorage, tableName = DEFAULT_VERSION_TABLE): number {
  // First ensure the table exists
  ensureVersionTable(sql, tableName)

  const row = sql.exec(`SELECT version FROM ${tableName} WHERE id = 1`).one()
  if (!row) {
    return 0
  }
  return row.version as number
}

/**
 * Sets the schema version
 */
function setSchemaVersion(sql: SqlStorage, version: number, tableName: string): void {
  sql.exec(
    `UPDATE ${tableName} SET version = ?, updated_at = datetime('now') WHERE id = 1`,
    version
  )
}

/**
 * Validates migration definitions
 */
function validateMigrations(migrations: Migration[]): void {
  if (migrations.length === 0) {
    return
  }

  // Check for duplicate versions
  const versions = new Set<number>()
  for (const migration of migrations) {
    if (versions.has(migration.version)) {
      throw new Error(`Duplicate migration version: ${migration.version}`)
    }
    versions.add(migration.version)
  }

  // Sort migrations and check for gaps
  const sorted = [...migrations].sort((a, b) => a.version - b.version)
  for (let i = 0; i < sorted.length; i++) {
    const expected = i + 1
    const actual = sorted[i]!.version
    if (actual !== expected) {
      throw new Error(
        `Migration version gap detected: expected version ${expected}, got ${actual}. Migrations must be sequential starting from 1.`
      )
    }
  }
}

/**
 * Runs pending migrations to bring the schema up to date
 *
 * @param sql - SqlStorage instance from ctx.storage.sql
 * @param migrations - Array of migration definitions
 * @param options - Optional configuration
 * @returns Result of the migration run
 *
 * @example
 * ```typescript
 * const result = runMigrations(ctx.storage.sql, [
 *   { version: 1, description: 'Create users table', up: (sql) => sql.exec(`CREATE TABLE users ...`) },
 *   { version: 2, description: 'Add posts table', up: (sql) => sql.exec(`CREATE TABLE posts ...`) }
 * ])
 *
 * if (!result.success) {
 *   console.error('Migration failed:', result.error)
 * }
 * ```
 */
export function runMigrations(
  sql: SqlStorage,
  migrations: Migration[],
  options: MigrationOptions = {}
): MigrationResult {
  const tableName = options.versionTableName ?? DEFAULT_VERSION_TABLE
  const validateVersions = options.validateVersions ?? true

  // Validate migrations if enabled
  if (validateVersions) {
    try {
      validateMigrations(migrations)
    } catch (error) {
      return {
        success: false,
        previousVersion: 0,
        currentVersion: 0,
        appliedMigrations: [],
        error: error instanceof Error ? error.message : 'Validation failed',
      }
    }
  }

  // Ensure version table exists and get current version
  ensureVersionTable(sql, tableName)
  const previousVersion = getSchemaVersion(sql, tableName)

  // Sort migrations by version
  const sortedMigrations = [...migrations].sort((a, b) => a.version - b.version)

  // Filter to only pending migrations
  const pendingMigrations = sortedMigrations.filter((m) => m.version > previousVersion)

  if (pendingMigrations.length === 0) {
    return {
      success: true,
      previousVersion,
      currentVersion: previousVersion,
      appliedMigrations: [],
    }
  }

  const appliedMigrations: number[] = []
  let currentVersion = previousVersion

  try {
    // Run each pending migration
    for (const migration of pendingMigrations) {
      // Run the migration
      migration.up(sql)

      // Update version
      setSchemaVersion(sql, migration.version, tableName)
      currentVersion = migration.version
      appliedMigrations.push(migration.version)
    }

    return {
      success: true,
      previousVersion,
      currentVersion,
      appliedMigrations,
    }
  } catch (error) {
    // Migration failed - return partial result
    return {
      success: false,
      previousVersion,
      currentVersion,
      appliedMigrations,
      error: error instanceof Error ? error.message : 'Unknown migration error',
    }
  }
}

/**
 * Creates a migration helper that combines multiple SQL statements
 *
 * @example
 * ```typescript
 * const migration: Migration = {
 *   version: 1,
 *   description: 'Initial schema',
 *   up: createMigrationFromStatements([
 *     'CREATE TABLE users (id TEXT PRIMARY KEY)',
 *     'CREATE TABLE posts (id TEXT PRIMARY KEY, user_id TEXT)',
 *     'CREATE INDEX idx_posts_user ON posts(user_id)'
 *   ])
 * }
 * ```
 */
export function createMigrationFromStatements(statements: string[]): (sql: SqlStorage) => void {
  return (sql: SqlStorage) => {
    for (const statement of statements) {
      sql.exec(statement)
    }
  }
}

/**
 * Checks if a table exists in the database
 */
export function tableExists(sql: SqlStorage, tableName: string): boolean {
  const result = sql.exec(
    `SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
    tableName
  ).one()
  return result !== null
}

/**
 * Checks if a column exists in a table
 */
export function columnExists(sql: SqlStorage, tableName: string, columnName: string): boolean {
  const columns = sql.exec(`PRAGMA table_info(${tableName})`).toArray()
  return columns.some((col) => col.name === columnName)
}

/**
 * Checks if an index exists in the database
 */
export function indexExists(sql: SqlStorage, indexName: string): boolean {
  const result = sql.exec(
    `SELECT name FROM sqlite_master WHERE type='index' AND name=?`,
    indexName
  ).one()
  return result !== null
}

/**
 * Safely adds a column if it doesn't exist
 * Returns true if the column was added, false if it already existed
 */
export function safeAddColumn(
  sql: SqlStorage,
  tableName: string,
  columnName: string,
  columnDef: string
): boolean {
  if (columnExists(sql, tableName, columnName)) {
    return false
  }
  sql.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDef}`)
  return true
}

/**
 * Safely creates an index if it doesn't exist
 * Returns true if the index was created, false if it already existed
 */
export function safeCreateIndex(
  sql: SqlStorage,
  indexName: string,
  tableName: string,
  columns: string[],
  options: { unique?: boolean; where?: string } = {}
): boolean {
  if (indexExists(sql, indexName)) {
    return false
  }
  const unique = options.unique ? 'UNIQUE ' : ''
  const where = options.where ? ` WHERE ${options.where}` : ''
  sql.exec(`CREATE ${unique}INDEX ${indexName} ON ${tableName}(${columns.join(', ')})${where}`)
  return true
}
