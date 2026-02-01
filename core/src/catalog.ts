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

      CREATE INDEX IF NOT EXISTS idx_tables_namespace ON tables(namespace);
      CREATE INDEX IF NOT EXISTS idx_snapshots_table ON snapshots(table_id);
      CREATE INDEX IF NOT EXISTS idx_files_table ON data_files(table_id);
      CREATE INDEX IF NOT EXISTS idx_files_snapshot ON data_files(snapshot_id);
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
    return rows.map((r) => r.name as string)
  }

  async dropNamespace(name: string): Promise<void> {
    // Check for tables first
    const tables = this.sql.exec(
      `SELECT COUNT(*) as count FROM tables WHERE namespace = ?`,
      name
    ).one()
    if ((tables?.count as number) > 0) {
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
    return row ? JSON.parse(row.metadata as string) : null
  }

  async listTables(namespace: string): Promise<string[]> {
    const rows = this.sql.exec(
      `SELECT name FROM tables WHERE namespace = ? ORDER BY name`,
      namespace
    ).toArray()
    return rows.map((r) => r.name as string)
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
    } = {}
  ): Promise<Snapshot> {
    const table = await this.loadTable(namespace, tableName)
    if (!table) throw new Error(`Table ${namespace}.${tableName} not found`)

    const { operation = 'append', cdcBookmark } = options

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
          ? table.snapshots[table.snapshots.length - 1].summary.totalRecords
          : 0),
        addedFiles: files.length,
        deletedFiles: 0,
        totalFiles: files.length + (table.snapshots.length > 0
          ? table.snapshots[table.snapshots.length - 1].summary.totalFiles
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
      snapshotId: row.id as string,
      parentSnapshotId: row.parent_id as string | undefined,
      timestampMs: row.timestamp_ms as number,
      operation: row.operation as Snapshot['operation'],
      manifestList: JSON.parse(row.manifest_list as string),
      summary: JSON.parse(row.summary as string),
      cdcBookmark: row.cdc_bookmark as string | undefined,
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
      snapshotId: row.id as string,
      parentSnapshotId: row.parent_id as string | undefined,
      timestampMs: row.timestamp_ms as number,
      operation: row.operation as Snapshot['operation'],
      manifestList: JSON.parse(row.manifest_list as string),
      summary: JSON.parse(row.summary as string),
      cdcBookmark: row.cdc_bookmark as string | undefined,
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
      path: r.path as string,
      format: r.format as 'parquet' | 'jsonl',
      recordCount: r.record_count as number,
      fileSizeBytes: r.file_size_bytes as number,
      columnStats: r.column_stats ? JSON.parse(r.column_stats as string) : undefined,
      partitionValues: r.partition_values ? JSON.parse(r.partition_values as string) : undefined,
      createdAt: r.created_at as string,
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

    const paths = files.map((f) => `'${f.path}'`).join(', ')
    const format = files[0].format

    let sql: string
    if (format === 'parquet') {
      sql = `SELECT ${options.columns?.join(', ') || '*'} FROM read_parquet([${paths}])`
    } else {
      sql = `SELECT ${options.columns?.join(', ') || '*'} FROM read_json_auto([${paths}])`
    }

    if (options.where) {
      sql += ` WHERE ${options.where}`
    }

    if (options.limit) {
      sql += ` LIMIT ${options.limit}`
    }

    return sql
  }
}

// Export type for wrangler config
export type CatalogDOType = typeof CatalogDO
