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
  type SqlRow,
} from './sql-mapper.js'

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
  oldType?: SchemaField['type']
  newType?: SchemaField['type']
  oldNullable?: boolean
  newNullable?: boolean
}

/** Represents a schema change event */
export interface SchemaChange {
  changeId: string
  fromVersion: number
  toVersion: number
  changes: FieldChange[]
  timestamp: string
  /** Optional: what triggered this change (e.g., snapshot commit, manual evolution) */
  trigger?: 'snapshot' | 'manual' | 'auto-detect'
}

/** Represents a version of a schema with its history */
export interface SchemaVersion {
  version: number
  fields: SchemaField[]
  createdAt: string
  /** The change that created this version (null for initial version) */
  change?: SchemaChange
}

export interface SchemaField {
  name: string
  type: 'string' | 'int32' | 'int64' | 'float' | 'double' | 'boolean' | 'timestamp' | 'json'
  nullable?: boolean
  doc?: string
}

export interface PartitionSpec {
  specId: number
  fields: PartitionField[]
}

export interface PartitionField {
  sourceId: number
  name: string
  transform: 'identity' | 'year' | 'month' | 'day' | 'hour' | 'bucket' | 'truncate'
  transformArg?: number
}

export interface DataFile {
  path: string
  format: 'parquet' | 'jsonl'
  recordCount: number
  fileSizeBytes: number
  columnStats?: Record<string, ColumnStats>
  partitionValues?: Record<string, string>
  createdAt: string
}

export interface ColumnStats {
  nullCount?: number
  minValue?: string | number
  maxValue?: string | number
  distinctCount?: number
}

export interface Manifest {
  manifestId: string
  path: string
  addedFiles: number
  deletedFiles: number
  existingFiles: number
  partitionSummary?: Record<string, { min: string; max: string }>
}

export interface Snapshot {
  snapshotId: string
  parentSnapshotId?: string
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
  cdcBookmark?: string
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
  currentSnapshotId?: string
  snapshots: Snapshot[]
  snapshotLog: { timestampMs: number; snapshotId: string }[]
  properties: Record<string, string>
  createdAt: string
  updatedAt: string
}

// ============================================================================
// Catalog DO
// ============================================================================

export class CatalogDO extends DurableObject<Env> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql

    // Initialize schema
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS namespaces (
        name TEXT PRIMARY KEY,
        properties TEXT DEFAULT '{}',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS tables (
        id TEXT PRIMARY KEY,
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        location TEXT NOT NULL,
        metadata TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(namespace, name)
      );

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
      );

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
      );

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
      );

      CREATE INDEX IF NOT EXISTS idx_tables_namespace ON tables(namespace);
      CREATE INDEX IF NOT EXISTS idx_snapshots_table ON snapshots(table_id);
      CREATE INDEX IF NOT EXISTS idx_files_table ON data_files(table_id);
      CREATE INDEX IF NOT EXISTS idx_files_snapshot ON data_files(snapshot_id);
      CREATE INDEX IF NOT EXISTS idx_schema_history_table ON schema_history(table_id);
    `)
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
    const rows = this.sql.exec(`SELECT name FROM namespaces ORDER BY name`).toArray()
    return rows.map((r) => getString(r as SqlRow, 'name'))
  }

  async dropNamespace(name: string): Promise<void> {
    // Check for tables first
    const tables = this.sql.exec(
      `SELECT COUNT(*) as count FROM tables WHERE namespace = ?`,
      name
    ).one()
    if (tables && getNumber(tables as SqlRow, 'count') > 0) {
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
    const row = this.sql.exec(
      `SELECT metadata FROM tables WHERE namespace = ? AND name = ?`,
      namespace,
      name
    ).one()
    return row ? getJson<TableMetadata>(row as SqlRow, 'metadata') : null
  }

  async listTables(namespace: string): Promise<string[]> {
    const rows = this.sql.exec(
      `SELECT name FROM tables WHERE namespace = ? ORDER BY name`,
      namespace
    ).toArray()
    return rows.map((r) => getString(r as SqlRow, 'name'))
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

    const row = this.sql.exec(
      `SELECT * FROM snapshots WHERE table_id = ? AND cdc_bookmark = ? LIMIT 1`,
      table.tableId,
      cdcBookmark
    ).one()

    if (!row) return null

    return {
      snapshotId: getString(row as SqlRow, 'id'),
      parentSnapshotId: getOptionalString(row as SqlRow, 'parent_id') ?? undefined,
      timestampMs: getNumber(row as SqlRow, 'timestamp_ms'),
      operation: getString(row as SqlRow, 'operation') as Snapshot['operation'],
      manifestList: getJson<string[]>(row as SqlRow, 'manifest_list'),
      summary: getJson<Snapshot['summary']>(row as SqlRow, 'summary'),
      cdcBookmark: getOptionalString(row as SqlRow, 'cdc_bookmark') ?? undefined,
    }
  }

  async getSnapshotAtTime(
    namespace: string,
    tableName: string,
    timestampMs: number
  ): Promise<Snapshot | null> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) return null

    const row = this.sql.exec(
      `SELECT * FROM snapshots
       WHERE table_id = ? AND timestamp_ms <= ?
       ORDER BY timestamp_ms DESC LIMIT 1`,
      table.tableId,
      timestampMs
    ).one()

    if (!row) return null

    return {
      snapshotId: getString(row as SqlRow, 'id'),
      parentSnapshotId: getOptionalString(row as SqlRow, 'parent_id') ?? undefined,
      timestampMs: getNumber(row as SqlRow, 'timestamp_ms'),
      operation: getString(row as SqlRow, 'operation') as Snapshot['operation'],
      manifestList: getJson<string[]>(row as SqlRow, 'manifest_list'),
      summary: getJson<Snapshot['summary']>(row as SqlRow, 'summary'),
      cdcBookmark: getOptionalString(row as SqlRow, 'cdc_bookmark') ?? undefined,
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

    const rows = this.sql.exec(
      `SELECT * FROM data_files WHERE table_id = ? AND snapshot_id = ?`,
      table.tableId,
      targetSnapshot
    ).toArray()

    return rows.map((r) => ({
      path: getString(r as SqlRow, 'path'),
      format: getString(r as SqlRow, 'format') as 'parquet' | 'jsonl',
      recordCount: getNumber(r as SqlRow, 'record_count'),
      fileSizeBytes: getNumber(r as SqlRow, 'file_size_bytes'),
      columnStats: getOptionalJson<Record<string, ColumnStats>>(r as SqlRow, 'column_stats') ?? undefined,
      partitionValues: getOptionalJson<Record<string, string>>(r as SqlRow, 'partition_values') ?? undefined,
      createdAt: getOptionalString(r as SqlRow, 'created_at') ?? new Date().toISOString(),
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

    const rows = this.sql.exec(
      `SELECT * FROM schema_history WHERE table_id = ? ORDER BY version ASC`,
      table.tableId
    ).toArray()

    // If no history in schema_history table, fall back to table metadata
    if (rows.length === 0) {
      return table.schemas.map((s) => ({
        version: s.schemaId,
        fields: s.fields,
        createdAt: s.createdAt,
      }))
    }

    return rows.map((r) => {
      const change = getOptionalJson<FieldChange[]>(r as SqlRow, 'changes')
      return {
        version: getNumber(r as SqlRow, 'version'),
        fields: getJson<SchemaField[]>(r as SqlRow, 'fields'),
        createdAt: getOptionalString(r as SqlRow, 'created_at') ?? new Date().toISOString(),
        change: change ? {
          changeId: getString(r as SqlRow, 'id'),
          fromVersion: getOptionalNumber(r as SqlRow, 'from_version') ?? 0,
          toVersion: getNumber(r as SqlRow, 'version'),
          changes: change,
          timestamp: getString(r as SqlRow, 'created_at'),
          trigger: getOptionalString(r as SqlRow, 'trigger') as SchemaChange['trigger'],
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
}

// Export type for wrangler config
export type CatalogDOType = typeof CatalogDO
