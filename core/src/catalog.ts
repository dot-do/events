/**
 * DO-based Iceberg-style Catalog
 *
 * A Durable Object that serves as metadata coordinator for a lakehouse.
 * Tracks tables, schemas, snapshots, and manifests - similar to Iceberg catalog.
 *
 * Key concepts:
 * - Tables: Named collections of data files with schema
 * - Snapshots: Point-in-time views of a table (immutable)
 * - Manifests: Lists of data files with statistics
 * - PITR: Integration with CDC bookmarks for point-in-time recovery
 */

import { DurableObject } from 'cloudflare:workers'
import {
  getString,
  getNumber,
  getOptionalString,
  getOptionalNumber,
  getJson,
  getOptionalJson,
  typedExec,
  type SqlRow,
} from './sql-mapper.js'
import { runMigrations, type Migration } from './migrations.js'

// Env type - users should extend this for their specific bindings
// eslint-disable-next-line @typescript-eslint/no-empty-interface
interface Env {}

// ============================================================================
// Types
// ============================================================================

export interface TableSchema {
  schemaId: number
  fields: SchemaField[]
  createdAt: string
}

/** Represents a change to a single field in a schema */
export interface FieldChange {
  field: string
  changeType: 'added' | 'removed' | 'modified'
  oldType?: SchemaField['type'] | undefined
  newType?: SchemaField['type'] | undefined
  oldNullable?: boolean | undefined
  newNullable?: boolean | undefined
}

/** Represents a schema change event */
export interface SchemaChange {
  changeId: string
  fromVersion: number
  toVersion: number
  changes: FieldChange[]
  timestamp: string
  /** Optional: what triggered this change (e.g., snapshot commit, manual evolution) */
  trigger?: 'snapshot' | 'manual' | 'auto-detect' | undefined
}

/** Represents a version of a schema with its history */
export interface SchemaVersion {
  version: number
  fields: SchemaField[]
  createdAt: string
  /** The change that created this version (null for initial version) */
  change?: SchemaChange | undefined
}

export interface SchemaField {
  name: string
  type: 'string' | 'int32' | 'int64' | 'float' | 'double' | 'boolean' | 'timestamp' | 'json'
  nullable?: boolean | undefined
  doc?: string | undefined
}

export interface PartitionSpec {
  specId: number
  fields: PartitionField[]
}

export interface PartitionField {
  sourceId: number
  name: string
  transform: 'identity' | 'year' | 'month' | 'day' | 'hour' | 'bucket' | 'truncate'
  transformArg?: number | undefined
}

export interface DataFile {
  path: string
  format: 'parquet' | 'jsonl'
  recordCount: number
  fileSizeBytes: number
  columnStats?: Record<string, ColumnStats> | undefined
  partitionValues?: Record<string, string> | undefined
  createdAt: string
}

export interface ColumnStats {
  nullCount?: number | undefined
  minValue?: string | number | undefined
  maxValue?: string | number | undefined
  distinctCount?: number | undefined
}

export interface Manifest {
  manifestId: string
  path: string
  addedFiles: number
  deletedFiles: number
  existingFiles: number
  partitionSummary?: Record<string, { min: string; max: string }> | undefined
}

export interface Snapshot {
  snapshotId: string
  parentSnapshotId?: string | undefined
  timestampMs: number
  operation: 'append' | 'overwrite' | 'delete' | 'replace'
  manifestList: string[]
  summary: {
    addedRecords: number
    deletedRecords: number
    totalRecords: number
    addedFiles: number
    deletedFiles: number
    totalFiles: number
  }
  // PITR integration
  cdcBookmark?: string | undefined
}

export interface TableMetadata {
  tableId: string
  name: string
  namespace: string
  location: string // R2 path prefix
  currentSchemaId: number
  schemas: TableSchema[]
  currentSpecId: number
  partitionSpecs: PartitionSpec[]
  currentSnapshotId?: string | undefined
  snapshots: Snapshot[]
  snapshotLog: { timestampMs: number; snapshotId: string }[]
  properties: Record<string, string>
  createdAt: string
  updatedAt: string
}

// ============================================================================
// Migrations
// ============================================================================

/**
 * CatalogDO schema migrations
 * Add new migrations at the end with incrementing version numbers
 */
const CATALOG_MIGRATIONS: Migration[] = [
  {
    version: 1,
    description: 'Initial schema - namespaces, tables, snapshots, data_files, schema_history',
    up: (sql) => {
      sql.exec(`
        CREATE TABLE IF NOT EXISTS namespaces (
          name TEXT PRIMARY KEY,
          properties TEXT DEFAULT '{}',
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      `)

      sql.exec(`
        CREATE TABLE IF NOT EXISTS tables (
          id TEXT PRIMARY KEY,
          namespace TEXT NOT NULL,
          name TEXT NOT NULL,
          location TEXT NOT NULL,
          metadata TEXT NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(namespace, name)
        )
      `)

      sql.exec(`
        CREATE TABLE IF NOT EXISTS snapshots (
          id TEXT PRIMARY KEY,
          table_id TEXT NOT NULL,
          parent_id TEXT,
          timestamp_ms INTEGER NOT NULL,
          operation TEXT NOT NULL,
          manifest_list TEXT NOT NULL,
          summary TEXT NOT NULL,
          cdc_bookmark TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (table_id) REFERENCES tables(id)
        )
      `)

      sql.exec(`
        CREATE TABLE IF NOT EXISTS data_files (
          path TEXT PRIMARY KEY,
          table_id TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          format TEXT NOT NULL,
          record_count INTEGER NOT NULL,
          file_size_bytes INTEGER NOT NULL,
          column_stats TEXT,
          partition_values TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (table_id) REFERENCES tables(id),
          FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
        )
      `)

      sql.exec(`
        CREATE TABLE IF NOT EXISTS schema_history (
          id TEXT PRIMARY KEY,
          table_id TEXT NOT NULL,
          version INTEGER NOT NULL,
          fields TEXT NOT NULL,
          from_version INTEGER,
          changes TEXT,
          trigger TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (table_id) REFERENCES tables(id),
          UNIQUE(table_id, version)
        )
      `)

      sql.exec(`CREATE INDEX IF NOT EXISTS idx_tables_namespace ON tables(namespace)`)
      sql.exec(`CREATE INDEX IF NOT EXISTS idx_snapshots_table ON snapshots(table_id)`)
      sql.exec(`CREATE INDEX IF NOT EXISTS idx_files_table ON data_files(table_id)`)
      sql.exec(`CREATE INDEX IF NOT EXISTS idx_files_snapshot ON data_files(snapshot_id)`)
      sql.exec(`CREATE INDEX IF NOT EXISTS idx_schema_history_table ON schema_history(table_id)`)
    },
  },
  // Add future migrations here with incrementing version numbers
  // Example:
  // {
  //   version: 2,
  //   description: 'Add retention_policy column to tables',
  //   up: (sql) => {
  //     sql.exec(`ALTER TABLE tables ADD COLUMN retention_policy TEXT`)
  //   },
  // },
]

// ============================================================================
// Catalog DO
// ============================================================================

export class CatalogDO extends DurableObject<Env> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql

    // Run migrations to initialize/update schema
    runMigrations(this.sql, CATALOG_MIGRATIONS)
  }

  // ---------------------------------------------------------------------------
  // Namespace Operations
  // ---------------------------------------------------------------------------

  async createNamespace(name: string, properties: Record<string, string> = {}): Promise<void> {
    this.sql.exec(
      `INSERT INTO namespaces (name, properties) VALUES (?, ?)`,
      name,
      JSON.stringify(properties)
    )
  }

  async listNamespaces(): Promise<string[]> {
    const { rows } = typedExec(this.sql, `SELECT name FROM namespaces ORDER BY name`)
    return rows.map((r) => getString(r, 'name'))
  }

  async dropNamespace(name: string): Promise<void> {
    // Check for tables first
    const { one } = typedExec(
      this.sql,
      `SELECT COUNT(*) as count FROM tables WHERE namespace = ?`,
      name
    )
    const tables = one()
    if (tables && getNumber(tables, 'count') > 0) {
      throw new Error(`Namespace ${name} is not empty`)
    }
    this.sql.exec(`DELETE FROM namespaces WHERE name = ?`, name)
  }

  // ---------------------------------------------------------------------------
  // Table Operations
  // ---------------------------------------------------------------------------

  async createTable(
    namespace: string,
    name: string,
    schema: SchemaField[],
    options: {
      location?: string
      partitionSpec?: PartitionField[]
      properties?: Record<string, string>
    } = {}
  ): Promise<TableMetadata> {
    const tableId = crypto.randomUUID()
    const now = new Date().toISOString()

    const metadata: TableMetadata = {
      tableId,
      name,
      namespace,
      location: options.location || `${namespace}/${name}`,
      currentSchemaId: 0,
      schemas: [{ schemaId: 0, fields: schema, createdAt: now }],
      currentSpecId: 0,
      partitionSpecs: options.partitionSpec
        ? [{ specId: 0, fields: options.partitionSpec }]
        : [{ specId: 0, fields: [] }],
      snapshots: [],
      snapshotLog: [],
      properties: options.properties || {},
      createdAt: now,
      updatedAt: now,
    }

    this.sql.exec(
      `INSERT INTO tables (id, namespace, name, location, metadata) VALUES (?, ?, ?, ?, ?)`,
      tableId,
      namespace,
      name,
      metadata.location,
      JSON.stringify(metadata)
    )

    return metadata
  }

  async loadTable(namespace: string, name: string): Promise<TableMetadata | null> {
    const { one } = typedExec(
      this.sql,
      `SELECT metadata FROM tables WHERE namespace = ? AND name = ?`,
      namespace,
      name
    )
    const row = one()
    return row ? getJson<TableMetadata>(row, 'metadata') : null
  }

  async listTables(namespace: string): Promise<string[]> {
    const { rows } = typedExec(
      this.sql,
      `SELECT name FROM tables WHERE namespace = ? ORDER BY name`,
      namespace
    )
    return rows.map((r) => getString(r, 'name'))
  }

  async dropTable(namespace: string, name: string): Promise<void> {
    const table = await this.loadTable(namespace, name)
    if (!table) return

    // Delete in order due to foreign keys
    this.sql.exec(`DELETE FROM data_files WHERE table_id = ?`, table.tableId)
    this.sql.exec(`DELETE FROM snapshots WHERE table_id = ?`, table.tableId)
    this.sql.exec(`DELETE FROM tables WHERE id = ?`, table.tableId)
  }

  // ---------------------------------------------------------------------------
  // Snapshot Operations (Commits)
  // ---------------------------------------------------------------------------

  async commitSnapshot(
    namespace: string,
    tableName: string,
    files: DataFile[],
    options: {
      operation?: 'append' | 'overwrite'
      cdcBookmark?: string
      /** Skip schema evolution check (default: false) */
      skipSchemaEvolution?: boolean
    } = {}
  ): Promise<Snapshot> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) throw new Error(`Table ${namespace}.${tableName} not found`)

    const { operation = 'append', cdcBookmark, skipSchemaEvolution = false } = options

    // Check for schema evolution if files have column stats
    if (!skipSchemaEvolution) {
      await this.checkSchemaEvolution(namespace, tableName, files)
    }

    const snapshotId = crypto.randomUUID()
    const timestampMs = Date.now()

    // Calculate summary
    const addedRecords = files.reduce((sum, f) => sum + f.recordCount, 0)
    const addedBytes = files.reduce((sum, f) => sum + f.fileSizeBytes, 0)

    const snapshot: Snapshot = {
      snapshotId,
      parentSnapshotId: table.currentSnapshotId,
      timestampMs,
      operation,
      manifestList: files.map((f) => f.path),
      summary: {
        addedRecords,
        deletedRecords: 0,
        totalRecords: addedRecords + (table.snapshots.length > 0
          ? (table.snapshots[table.snapshots.length - 1]?.summary.totalRecords ?? 0)
          : 0),
        addedFiles: files.length,
        deletedFiles: 0,
        totalFiles: files.length + (table.snapshots.length > 0
          ? (table.snapshots[table.snapshots.length - 1]?.summary.totalFiles ?? 0)
          : 0),
      },
      cdcBookmark,
    }

    // Insert snapshot
    this.sql.exec(
      `INSERT INTO snapshots (id, table_id, parent_id, timestamp_ms, operation, manifest_list, summary, cdc_bookmark)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      snapshotId,
      table.tableId,
      table.currentSnapshotId || null,
      timestampMs,
      operation,
      JSON.stringify(snapshot.manifestList),
      JSON.stringify(snapshot.summary),
      cdcBookmark || null
    )

    // Insert data files
    for (const file of files) {
      this.sql.exec(
        `INSERT INTO data_files (path, table_id, snapshot_id, format, record_count, file_size_bytes, column_stats, partition_values)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        file.path,
        table.tableId,
        snapshotId,
        file.format,
        file.recordCount,
        file.fileSizeBytes,
        file.columnStats ? JSON.stringify(file.columnStats) : null,
        file.partitionValues ? JSON.stringify(file.partitionValues) : null
      )
    }

    // Update table metadata
    table.snapshots.push(snapshot)
    table.snapshotLog.push({ timestampMs, snapshotId })
    table.currentSnapshotId = snapshotId
    table.updatedAt = new Date().toISOString()

    this.sql.exec(
      `UPDATE tables SET metadata = ?, updated_at = ? WHERE id = ?`,
      JSON.stringify(table),
      table.updatedAt,
      table.tableId
    )

    return snapshot
  }

  // ---------------------------------------------------------------------------
  // PITR (Point-in-Time Recovery)
  // ---------------------------------------------------------------------------

  async getSnapshotByBookmark(
    namespace: string,
    tableName: string,
    cdcBookmark: string
  ): Promise<Snapshot | null> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) return null

    const { one } = typedExec(
      this.sql,
      `SELECT * FROM snapshots WHERE table_id = ? AND cdc_bookmark = ? LIMIT 1`,
      table.tableId,
      cdcBookmark
    )
    const row = one()

    if (!row) return null

    return {
      snapshotId: getString(row, 'id'),
      parentSnapshotId: getOptionalString(row, 'parent_id') ?? undefined,
      timestampMs: getNumber(row, 'timestamp_ms'),
      operation: getString(row, 'operation') as Snapshot['operation'],
      manifestList: getJson<string[]>(row, 'manifest_list'),
      summary: getJson<Snapshot['summary']>(row, 'summary'),
      cdcBookmark: getOptionalString(row, 'cdc_bookmark') ?? undefined,
    }
  }

  async getSnapshotAtTime(
    namespace: string,
    tableName: string,
    timestampMs: number
  ): Promise<Snapshot | null> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) return null

    const { one } = typedExec(
      this.sql,
      `SELECT * FROM snapshots
       WHERE table_id = ? AND timestamp_ms <= ?
       ORDER BY timestamp_ms DESC LIMIT 1`,
      table.tableId,
      timestampMs
    )
    const row = one()

    if (!row) return null

    return {
      snapshotId: getString(row, 'id'),
      parentSnapshotId: getOptionalString(row, 'parent_id') ?? undefined,
      timestampMs: getNumber(row, 'timestamp_ms'),
      operation: getString(row, 'operation') as Snapshot['operation'],
      manifestList: getJson<string[]>(row, 'manifest_list'),
      summary: getJson<Snapshot['summary']>(row, 'summary'),
      cdcBookmark: getOptionalString(row, 'cdc_bookmark') ?? undefined,
    }
  }

  // ---------------------------------------------------------------------------
  // File Listing (for queries)
  // ---------------------------------------------------------------------------

  async listFiles(
    namespace: string,
    tableName: string,
    snapshotId?: string
  ): Promise<DataFile[]> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) return []

    const targetSnapshot = snapshotId || table.currentSnapshotId
    if (!targetSnapshot) return []

    const { rows } = typedExec(
      this.sql,
      `SELECT * FROM data_files WHERE table_id = ? AND snapshot_id = ?`,
      table.tableId,
      targetSnapshot
    )

    return rows.map((r) => ({
      path: getString(r, 'path'),
      format: getString(r, 'format') as 'parquet' | 'jsonl',
      recordCount: getNumber(r, 'record_count'),
      fileSizeBytes: getNumber(r, 'file_size_bytes'),
      columnStats: getOptionalJson<Record<string, ColumnStats>>(r, 'column_stats') ?? undefined,
      partitionValues: getOptionalJson<Record<string, string>>(r, 'partition_values') ?? undefined,
      createdAt: getOptionalString(r, 'created_at') ?? new Date().toISOString(),
    }))
  }

  // ---------------------------------------------------------------------------
  // Query Builder (generates DuckDB SQL)
  // ---------------------------------------------------------------------------

  async buildQuery(
    namespace: string,
    tableName: string,
    options: {
      columns?: string[]
      where?: string
      snapshotId?: string
      limit?: number
    } = {}
  ): Promise<string> {
    // Validate column names - only allow alphanumeric and underscore characters
    const SAFE_COLUMN_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
    if (options.columns) {
      for (const col of options.columns) {
        if (!SAFE_COLUMN_RE.test(col)) {
          throw new Error(`Invalid column name: ${col}`)
        }
      }
    }

    // Validate where clause - only allow safe expressions
    if (options.where) {
      // Allow: column names (alphanum/_), comparison operators, numbers, quoted strings, AND/OR/NOT/IS/NULL/IN/LIKE, parens, commas, whitespace
      const SAFE_WHERE_RE = /^(?:[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|'[^']*'|[<>=!]+|[\s(),]|AND|OR|NOT|IS|NULL|IN|LIKE|BETWEEN|TRUE|FALSE)+$/i
      if (!SAFE_WHERE_RE.test(options.where)) {
        throw new Error(`Invalid where clause: contains disallowed characters or expressions`)
      }
    }

    const files = await this.listFiles(namespace, tableName, options.snapshotId)
    if (files.length === 0) return ''

    // Escape file paths to prevent SQL injection
    // Escape single quotes by doubling them (standard SQL escaping)
    const escapePath = (path: string): string => {
      // Validate path only contains safe characters for file paths
      // Allow: alphanumeric, forward/back slash, dash, underscore, period, colon (for drive letters)
      const SAFE_PATH_RE = /^[A-Za-z0-9/_\-.:]+$/
      if (!SAFE_PATH_RE.test(path)) {
        throw new Error(`Invalid file path: ${path}`)
      }
      // Even with validation, escape single quotes as a defense-in-depth measure
      return path.replace(/'/g, "''")
    }

    const paths = files.map((f) => `'${escapePath(f.path)}'`).join(', ')
    const format = files[0]!.format

    const columns = options.columns?.join(', ') || '*'

    let sql: string
    if (format === 'parquet') {
      sql = `SELECT ${columns} FROM read_parquet([${paths}])`
    } else {
      sql = `SELECT ${columns} FROM read_json_auto([${paths}])`
    }

    if (options.where) {
      sql += ` WHERE ${options.where}`
    }

    if (options.limit !== undefined) {
      // Validate limit is a safe integer
      if (!Number.isInteger(options.limit) || options.limit < 0) {
        throw new Error(`Invalid limit: ${options.limit}`)
      }
      sql += ` LIMIT ${options.limit}`
    }

    return sql
  }

  // ---------------------------------------------------------------------------
  // Schema Evolution
  // ---------------------------------------------------------------------------

  /**
   * Get the full schema history for a table
   */
  async getSchemaHistory(namespace: string, tableName: string): Promise<SchemaVersion[]> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) return []

    const { rows } = typedExec(
      this.sql,
      `SELECT * FROM schema_history WHERE table_id = ? ORDER BY version ASC`,
      table.tableId
    )

    // If no history in schema_history table, fall back to table metadata
    if (rows.length === 0) {
      return table.schemas.map((s) => ({
        version: s.schemaId,
        fields: s.fields,
        createdAt: s.createdAt,
      }))
    }

    return rows.map((r) => {
      const change = getOptionalJson<FieldChange[]>(r, 'changes')
      return {
        version: getNumber(r, 'version'),
        fields: getJson<SchemaField[]>(r, 'fields'),
        createdAt: getOptionalString(r, 'created_at') ?? new Date().toISOString(),
        change: change ? {
          changeId: getString(r, 'id'),
          fromVersion: getOptionalNumber(r, 'from_version') ?? 0,
          toVersion: getNumber(r, 'version'),
          changes: change,
          timestamp: getString(r, 'created_at'),
          trigger: getOptionalString(r, 'trigger') as SchemaChange['trigger'],
        } : undefined,
      }
    })
  }

  /**
   * Detect differences between two schemas
   */
  private detectSchemaChanges(
    oldFields: SchemaField[],
    newFields: SchemaField[]
  ): FieldChange[] {
    const changes: FieldChange[] = []
    const oldFieldMap = new Map(oldFields.map((f) => [f.name, f]))
    const newFieldMap = new Map(newFields.map((f) => [f.name, f]))

    // Check for added and modified fields
    for (const newField of newFields) {
      const oldField = oldFieldMap.get(newField.name)
      if (!oldField) {
        changes.push({
          field: newField.name,
          changeType: 'added',
          newType: newField.type,
          newNullable: newField.nullable,
        })
      } else if (oldField.type !== newField.type || oldField.nullable !== newField.nullable) {
        changes.push({
          field: newField.name,
          changeType: 'modified',
          oldType: oldField.type,
          newType: newField.type,
          oldNullable: oldField.nullable,
          newNullable: newField.nullable,
        })
      }
    }

    // Check for removed fields
    for (const oldField of oldFields) {
      if (!newFieldMap.has(oldField.name)) {
        changes.push({
          field: oldField.name,
          changeType: 'removed',
          oldType: oldField.type,
          oldNullable: oldField.nullable,
        })
      }
    }

    return changes
  }

  /**
   * Evolve schema by adding new fields detected from data
   * Returns the new schema version if changes were made, null otherwise
   */
  async evolveSchema(
    namespace: string,
    tableName: string,
    detectedFields: SchemaField[],
    trigger: SchemaChange['trigger'] = 'auto-detect'
  ): Promise<SchemaVersion | null> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) throw new Error(`Table ${namespace}.${tableName} not found`)

    const currentSchema = table.schemas[table.currentSchemaId]
    if (!currentSchema) throw new Error(`No current schema found for table ${namespace}.${tableName}`)

    // Merge current fields with detected fields (detected fields can add new ones)
    const currentFieldMap = new Map(currentSchema.fields.map((f) => [f.name, f]))
    const mergedFields = [...currentSchema.fields]

    for (const detected of detectedFields) {
      if (!currentFieldMap.has(detected.name)) {
        mergedFields.push({
          ...detected,
          nullable: true, // New fields should be nullable for backward compatibility
        })
      }
    }

    // Check if there are any changes
    const changes = this.detectSchemaChanges(currentSchema.fields, mergedFields)
    if (changes.length === 0) {
      return null // No evolution needed
    }

    const now = new Date().toISOString()
    const newVersion = table.currentSchemaId + 1
    const changeId = crypto.randomUUID()

    // Record the schema change in history
    this.sql.exec(
      `INSERT INTO schema_history (id, table_id, version, fields, from_version, changes, trigger)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      changeId,
      table.tableId,
      newVersion,
      JSON.stringify(mergedFields),
      table.currentSchemaId,
      JSON.stringify(changes),
      trigger
    )

    // Update table metadata
    const newSchema: TableSchema = {
      schemaId: newVersion,
      fields: mergedFields,
      createdAt: now,
    }
    table.schemas.push(newSchema)
    table.currentSchemaId = newVersion
    table.updatedAt = now

    this.sql.exec(
      `UPDATE tables SET metadata = ?, updated_at = ? WHERE id = ?`,
      JSON.stringify(table),
      table.updatedAt,
      table.tableId
    )

    return {
      version: newVersion,
      fields: mergedFields,
      createdAt: now,
      change: {
        changeId,
        fromVersion: table.currentSchemaId - 1, // Previous version
        toVersion: newVersion,
        changes,
        timestamp: now,
        trigger,
      },
    }
  }

  /**
   * Infer schema from column stats (useful during snapshot commit)
   */
  private inferFieldsFromColumnStats(columnStats: Record<string, ColumnStats>): SchemaField[] {
    const fields: SchemaField[] = []
    for (const [name, stats] of Object.entries(columnStats)) {
      let type: SchemaField['type'] = 'json'

      // Infer type from min/max values
      if (stats.minValue !== undefined) {
        if (typeof stats.minValue === 'number') {
          type = Number.isInteger(stats.minValue) ? 'int64' : 'double'
        } else if (typeof stats.minValue === 'string') {
          // Check if it looks like a timestamp
          if (/^\d{4}-\d{2}-\d{2}/.test(stats.minValue)) {
            type = 'timestamp'
          } else {
            type = 'string'
          }
        }
      }

      fields.push({
        name,
        type,
        nullable: (stats.nullCount ?? 0) > 0,
      })
    }
    return fields
  }

  /**
   * Check and evolve schema during snapshot commit if new fields are detected
   */
  async checkSchemaEvolution(
    namespace: string,
    tableName: string,
    files: DataFile[]
  ): Promise<SchemaVersion | null> {
    // Collect all unique column names from file stats
    const detectedFields: SchemaField[] = []
    const seenFields = new Set<string>()

    for (const file of files) {
      if (file.columnStats) {
        const inferred = this.inferFieldsFromColumnStats(file.columnStats)
        for (const field of inferred) {
          if (!seenFields.has(field.name)) {
            seenFields.add(field.name)
            detectedFields.push(field)
          }
        }
      }
    }

    if (detectedFields.length === 0) {
      return null
    }

    return this.evolveSchema(namespace, tableName, detectedFields, 'snapshot')
  }

  /**
   * Record initial schema in history (call after createTable)
   */
  async recordInitialSchema(namespace: string, tableName: string): Promise<void> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) return

    const currentSchema = table.schemas[0]
    if (!currentSchema) return

    // Check if already recorded
    const existing = this.sql.exec(
      `SELECT id FROM schema_history WHERE table_id = ? AND version = 0`,
      table.tableId
    ).one()

    if (existing) return

    this.sql.exec(
      `INSERT INTO schema_history (id, table_id, version, fields, from_version, changes, trigger)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      crypto.randomUUID(),
      table.tableId,
      0,
      JSON.stringify(currentSchema.fields),
      null,
      null,
      'manual'
    )
  }

  // ---------------------------------------------------------------------------
  // Quota Management
  // ---------------------------------------------------------------------------

  /**
   * Initialize quota tables (called during schema migration)
   */
  private initQuotaTables(): void {
    this.sql.exec(`
      -- Quota configuration per namespace
      CREATE TABLE IF NOT EXISTS namespace_quotas (
        namespace TEXT PRIMARY KEY,
        max_events_per_day INTEGER NOT NULL DEFAULT 0,
        max_storage_bytes INTEGER NOT NULL DEFAULT 0,
        max_subscriptions INTEGER NOT NULL DEFAULT 0,
        max_schemas INTEGER NOT NULL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      -- Usage tracking per namespace
      CREATE TABLE IF NOT EXISTS namespace_usage (
        namespace TEXT PRIMARY KEY,
        events_today INTEGER NOT NULL DEFAULT 0,
        events_date TEXT NOT NULL,
        storage_bytes INTEGER NOT NULL DEFAULT 0,
        subscriptions INTEGER NOT NULL DEFAULT 0,
        schemas INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );
    `)
  }

  /**
   * Get current date in YYYY-MM-DD format (UTC)
   */
  private getCurrentDateString(): string {
    return new Date().toISOString().slice(0, 10)
  }

  /**
   * Get quota configuration for a namespace
   */
  async getQuota(namespace: string): Promise<{
    maxEventsPerDay: number
    maxStorageBytes: number
    maxSubscriptions: number
    maxSchemas: number
  }> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const { one } = typedExec(
      this.sql,
      `SELECT * FROM namespace_quotas WHERE namespace = ?`,
      namespace
    )
    const row = one()

    if (!row) {
      // Return default (unlimited) quota
      return {
        maxEventsPerDay: 0,
        maxStorageBytes: 0,
        maxSubscriptions: 0,
        maxSchemas: 0,
      }
    }

    return {
      maxEventsPerDay: getNumber(row, 'max_events_per_day'),
      maxStorageBytes: getNumber(row, 'max_storage_bytes'),
      maxSubscriptions: getNumber(row, 'max_subscriptions'),
      maxSchemas: getNumber(row, 'max_schemas'),
    }
  }

  /**
   * Set quota configuration for a namespace
   */
  async setQuota(
    namespace: string,
    quota: {
      maxEventsPerDay?: number
      maxStorageBytes?: number
      maxSubscriptions?: number
      maxSchemas?: number
    }
  ): Promise<void> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const now = new Date().toISOString()

    // Check if quota already exists
    const { one } = typedExec(
      this.sql,
      `SELECT namespace FROM namespace_quotas WHERE namespace = ?`,
      namespace
    )
    const existing = one()

    if (existing) {
      // Update existing quota
      const updates: string[] = ['updated_at = ?']
      const values: unknown[] = [now]

      if (quota.maxEventsPerDay !== undefined) {
        updates.push('max_events_per_day = ?')
        values.push(quota.maxEventsPerDay)
      }
      if (quota.maxStorageBytes !== undefined) {
        updates.push('max_storage_bytes = ?')
        values.push(quota.maxStorageBytes)
      }
      if (quota.maxSubscriptions !== undefined) {
        updates.push('max_subscriptions = ?')
        values.push(quota.maxSubscriptions)
      }
      if (quota.maxSchemas !== undefined) {
        updates.push('max_schemas = ?')
        values.push(quota.maxSchemas)
      }

      values.push(namespace)
      this.sql.exec(
        `UPDATE namespace_quotas SET ${updates.join(', ')} WHERE namespace = ?`,
        ...values
      )
    } else {
      // Insert new quota
      this.sql.exec(
        `INSERT INTO namespace_quotas (namespace, max_events_per_day, max_storage_bytes, max_subscriptions, max_schemas, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        namespace,
        quota.maxEventsPerDay ?? 0,
        quota.maxStorageBytes ?? 0,
        quota.maxSubscriptions ?? 0,
        quota.maxSchemas ?? 0,
        now,
        now
      )
    }
  }

  /**
   * Get current usage for a namespace
   */
  async getUsage(namespace: string): Promise<{
    eventsToday: number
    eventsDate: string
    storageBytes: number
    subscriptions: number
    schemas: number
  }> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const today = this.getCurrentDateString()

    const { one } = typedExec(
      this.sql,
      `SELECT * FROM namespace_usage WHERE namespace = ?`,
      namespace
    )
    const row = one()

    if (!row) {
      return {
        eventsToday: 0,
        eventsDate: today,
        storageBytes: 0,
        subscriptions: 0,
        schemas: 0,
      }
    }

    const eventsDate = getString(row, 'events_date')
    const eventsToday = eventsDate === today ? getNumber(row, 'events_today') : 0

    return {
      eventsToday,
      eventsDate: today,
      storageBytes: getNumber(row, 'storage_bytes'),
      subscriptions: getNumber(row, 'subscriptions'),
      schemas: getNumber(row, 'schemas'),
    }
  }

  /**
   * Check if ingesting events would exceed quota
   * Returns { allowed: true } if allowed, or { allowed: false, reason: string } if not
   */
  async checkEventQuota(
    namespace: string,
    eventCount: number
  ): Promise<{ allowed: boolean; reason?: string }> {
    const quota = await this.getQuota(namespace)
    const usage = await this.getUsage(namespace)

    // Check daily event limit
    if (quota.maxEventsPerDay > 0) {
      if (usage.eventsToday + eventCount > quota.maxEventsPerDay) {
        return {
          allowed: false,
          reason: `Daily event quota exceeded: ${usage.eventsToday}/${quota.maxEventsPerDay} events used, cannot add ${eventCount} more`,
        }
      }
    }

    return { allowed: true }
  }

  /**
   * Increment event counter for a namespace
   * Automatically resets the counter if the date has changed
   */
  async incrementEventCount(namespace: string, count: number): Promise<void> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const today = this.getCurrentDateString()
    const now = new Date().toISOString()

    // Check if usage record exists
    const { one } = typedExec(
      this.sql,
      `SELECT events_date FROM namespace_usage WHERE namespace = ?`,
      namespace
    )
    const existing = one()

    if (!existing) {
      // Create new usage record
      this.sql.exec(
        `INSERT INTO namespace_usage (namespace, events_today, events_date, storage_bytes, subscriptions, schemas, updated_at)
         VALUES (?, ?, ?, 0, 0, 0, ?)`,
        namespace,
        count,
        today,
        now
      )
    } else {
      const existingDate = getString(existing, 'events_date')
      if (existingDate === today) {
        // Same day, increment counter
        this.sql.exec(
          `UPDATE namespace_usage SET events_today = events_today + ?, updated_at = ? WHERE namespace = ?`,
          count,
          now,
          namespace
        )
      } else {
        // New day, reset counter
        this.sql.exec(
          `UPDATE namespace_usage SET events_today = ?, events_date = ?, updated_at = ? WHERE namespace = ?`,
          count,
          today,
          now,
          namespace
        )
      }
    }
  }

  /**
   * Update storage usage for a namespace
   */
  async updateStorageUsage(namespace: string, bytes: number): Promise<void> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const now = new Date().toISOString()
    const today = this.getCurrentDateString()

    // Upsert usage record
    const { one } = typedExec(
      this.sql,
      `SELECT namespace FROM namespace_usage WHERE namespace = ?`,
      namespace
    )
    const existing = one()

    if (!existing) {
      this.sql.exec(
        `INSERT INTO namespace_usage (namespace, events_today, events_date, storage_bytes, subscriptions, schemas, updated_at)
         VALUES (?, 0, ?, ?, 0, 0, ?)`,
        namespace,
        today,
        bytes,
        now
      )
    } else {
      this.sql.exec(
        `UPDATE namespace_usage SET storage_bytes = ?, updated_at = ? WHERE namespace = ?`,
        bytes,
        now,
        namespace
      )
    }
  }

  /**
   * Update subscription count for a namespace
   */
  async updateSubscriptionCount(namespace: string, count: number): Promise<void> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const now = new Date().toISOString()
    const today = this.getCurrentDateString()

    const { one } = typedExec(
      this.sql,
      `SELECT namespace FROM namespace_usage WHERE namespace = ?`,
      namespace
    )
    const existing = one()

    if (!existing) {
      this.sql.exec(
        `INSERT INTO namespace_usage (namespace, events_today, events_date, storage_bytes, subscriptions, schemas, updated_at)
         VALUES (?, 0, ?, 0, ?, 0, ?)`,
        namespace,
        today,
        count,
        now
      )
    } else {
      this.sql.exec(
        `UPDATE namespace_usage SET subscriptions = ?, updated_at = ? WHERE namespace = ?`,
        count,
        now,
        namespace
      )
    }
  }

  /**
   * Update schema count for a namespace
   */
  async updateSchemaCount(namespace: string, count: number): Promise<void> {
    // Ensure quota tables exist
    this.initQuotaTables()

    const now = new Date().toISOString()
    const today = this.getCurrentDateString()

    const { one } = typedExec(
      this.sql,
      `SELECT namespace FROM namespace_usage WHERE namespace = ?`,
      namespace
    )
    const existing = one()

    if (!existing) {
      this.sql.exec(
        `INSERT INTO namespace_usage (namespace, events_today, events_date, storage_bytes, subscriptions, schemas, updated_at)
         VALUES (?, 0, ?, 0, 0, ?, ?)`,
        namespace,
        today,
        count,
        now
      )
    } else {
      this.sql.exec(
        `UPDATE namespace_usage SET schemas = ?, updated_at = ? WHERE namespace = ?`,
        count,
        now,
        namespace
      )
    }
  }

  /**
   * Check all quota limits for a namespace
   */
  async checkAllQuotas(
    namespace: string,
    toAdd?: {
      events?: number
      storageBytes?: number
      subscriptions?: number
      schemas?: number
    }
  ): Promise<{ allowed: boolean; reason?: string; usage?: ReturnType<typeof this.getUsage> extends Promise<infer T> ? T : never; quota?: ReturnType<typeof this.getQuota> extends Promise<infer T> ? T : never }> {
    const quota = await this.getQuota(namespace)
    const usage = await this.getUsage(namespace)

    // Check daily event limit
    if (quota.maxEventsPerDay > 0 && toAdd?.events) {
      if (usage.eventsToday + toAdd.events > quota.maxEventsPerDay) {
        return {
          allowed: false,
          reason: `Daily event quota exceeded: ${usage.eventsToday}/${quota.maxEventsPerDay} events used`,
          usage,
          quota,
        }
      }
    }

    // Check storage limit
    if (quota.maxStorageBytes > 0 && toAdd?.storageBytes) {
      if (usage.storageBytes + toAdd.storageBytes > quota.maxStorageBytes) {
        return {
          allowed: false,
          reason: `Storage quota exceeded: ${usage.storageBytes}/${quota.maxStorageBytes} bytes used`,
          usage,
          quota,
        }
      }
    }

    // Check subscription limit
    if (quota.maxSubscriptions > 0 && toAdd?.subscriptions) {
      if (usage.subscriptions + toAdd.subscriptions > quota.maxSubscriptions) {
        return {
          allowed: false,
          reason: `Subscription quota exceeded: ${usage.subscriptions}/${quota.maxSubscriptions} subscriptions used`,
          usage,
          quota,
        }
      }
    }

    // Check schema limit
    if (quota.maxSchemas > 0 && toAdd?.schemas) {
      if (usage.schemas + toAdd.schemas > quota.maxSchemas) {
        return {
          allowed: false,
          reason: `Schema quota exceeded: ${usage.schemas}/${quota.maxSchemas} schemas used`,
          usage,
          quota,
        }
      }
    }

    return { allowed: true, usage, quota }
  }
}

// Export type for wrangler config
export type CatalogDOType = typeof CatalogDO
