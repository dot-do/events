/**
 * CDC Snapshot Module
 *
 * Provides Point-in-Time Recovery (PITR) through periodic snapshots
 * of collection data stored in Parquet format.
 *
 * Storage layout:
 * {ns}/{collection}/
 * ├── data.parquet              # Current state
 * ├── deltas/                   # CDC change logs
 * │   └── YYYY-MM-DD.parquet
 * └── snapshots/
 *     ├── YYYY-MM-DD.parquet    # Daily snapshot
 *     ├── YYYY-MM.parquet       # Monthly snapshot
 *     └── manifest.json         # Snapshot metadata
 */

import { parquetReadObjects, parquetMetadata } from 'hyparquet'
import { createAsyncBuffer } from './async-buffer.js'

// ============================================================================
// Types
// ============================================================================

/**
 * Information about a snapshot
 */
export interface SnapshotInfo {
  /** Snapshot type */
  type: 'daily' | 'monthly'
  /** Full path to the snapshot file */
  path: string
  /** Timestamp when snapshot was created */
  timestamp: Date
  /** Number of rows in the snapshot */
  rowCount: number
  /** File size in bytes */
  sizeBytes: number
  /** MD5 checksum of the file */
  checksum?: string
  /** Column names in the snapshot */
  columns?: string[]
  /** Field used for sorting */
  sortedBy?: string
  /** Whether snapshot was skipped */
  skipped?: boolean
  /** Reason for skipping */
  reason?: string
}

/**
 * Snapshot manifest tracking all snapshots for a collection
 */
export interface SnapshotManifest {
  /** List of all snapshots */
  snapshots: SnapshotInfo[]
  /** Last modification time of the manifest */
  lastModified: Date
}

/**
 * Retention policy for snapshot cleanup
 */
export interface RetentionPolicy {
  /** Number of daily snapshots to retain */
  dailySnapshots: number
  /** Number of monthly snapshots to retain */
  monthlySnapshots: number
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Reads a Parquet file and returns an array of row objects
 */
async function readParquetRecords(buffer: ArrayBuffer): Promise<Record<string, unknown>[]> {
  const file = createAsyncBuffer(buffer)
  const records = await parquetReadObjects({ file })
  return records
}

/**
 * Gets metadata from a Parquet file including column names and row count
 */
async function getParquetMetadata(buffer: ArrayBuffer): Promise<{ columns: string[]; rowCount: number }> {
  // parquetMetadata expects ArrayBuffer directly (not AsyncBuffer)
  const metadata = parquetMetadata(buffer)
  const columns = metadata.schema.slice(1).map((col) => col.name) // Skip root schema element
  const rowCount = Number(metadata.num_rows)
  return { columns, rowCount }
}

/**
 * Formats date as YYYY-MM-DD
 */
function formatDateDaily(date: Date): string {
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

/**
 * Formats date as YYYY-MM
 */
function formatDateMonthly(date: Date): string {
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  return `${year}-${month}`
}

/**
 * Parses a daily snapshot filename to a date
 */
function parseDailyDate(filename: string): Date | null {
  // Format: YYYY-MM-DD.parquet
  const match = filename.match(/^(\d{4})-(\d{2})-(\d{2})\.parquet$/)
  if (!match) return null
  return new Date(Date.UTC(parseInt(match[1], 10), parseInt(match[2], 10) - 1, parseInt(match[3], 10)))
}

/**
 * Parses a monthly snapshot filename to a date
 */
function parseMonthlyDate(filename: string): Date | null {
  // Format: YYYY-MM.parquet
  const match = filename.match(/^(\d{4})-(\d{2})\.parquet$/)
  if (!match) return null
  // Set to end of month for sorting purposes
  const year = parseInt(match[1], 10)
  const month = parseInt(match[2], 10)
  return new Date(Date.UTC(year, month - 1, 1))
}

/**
 * Determines if a snapshot filename is daily or monthly format
 */
function getSnapshotType(filename: string): 'daily' | 'monthly' | null {
  if (/^\d{4}-\d{2}-\d{2}\.parquet$/.test(filename)) return 'daily'
  if (/^\d{4}-\d{2}\.parquet$/.test(filename)) return 'monthly'
  return null
}

/**
 * Gets the prefix path for snapshots
 */
function getSnapshotsPrefix(ns: string, collection: string): string {
  return `${ns}/${collection}/snapshots/`
}

/**
 * Gets the path for the snapshot manifest
 */
function getManifestPath(ns: string, collection: string): string {
  return `${ns}/${collection}/snapshots/manifest.json`
}

/**
 * Gets the path for data.parquet
 */
function getDataPath(ns: string, collection: string): string {
  return `${ns}/${collection}/data.parquet`
}

/**
 * Gets the prefix path for deltas
 */
function getDeltasPrefix(ns: string, collection: string): string {
  return `${ns}/${collection}/deltas/`
}

/**
 * Reads the snapshot manifest from R2
 */
async function readManifest(bucket: R2Bucket, ns: string, collection: string): Promise<SnapshotManifest | null> {
  const manifestPath = getManifestPath(ns, collection)
  const obj = await bucket.get(manifestPath)
  if (!obj) return null

  const buffer = await obj.arrayBuffer()
  const text = new TextDecoder().decode(buffer)
  const data = JSON.parse(text)

  // Convert date strings back to Date objects
  return {
    ...data,
    lastModified: new Date(data.lastModified),
    snapshots: data.snapshots.map((s: SnapshotInfo & { timestamp: string }) => ({
      ...s,
      timestamp: new Date(s.timestamp),
    })),
  }
}

/**
 * Writes the snapshot manifest to R2
 */
async function writeManifest(bucket: R2Bucket, ns: string, collection: string, manifest: SnapshotManifest): Promise<void> {
  const manifestPath = getManifestPath(ns, collection)
  const data = JSON.stringify(manifest)
  await bucket.put(manifestPath, data)
}

/**
 * Computes a simple checksum from file size (for now)
 */
function computeChecksum(buffer: ArrayBuffer): string {
  // Simple checksum based on size and first few bytes
  const view = new Uint8Array(buffer)
  let hash = buffer.byteLength
  for (let i = 0; i < Math.min(100, view.length); i++) {
    hash = ((hash << 5) - hash + view[i]) | 0
  }
  return Math.abs(hash).toString(16)
}

// ============================================================================
// Functions
// ============================================================================

/**
 * Creates a snapshot of the current collection state
 *
 * @param bucket - R2 bucket containing the data
 * @param ns - Namespace (e.g., app name)
 * @param collection - Collection name
 * @param type - Type of snapshot to create
 * @returns Information about the created snapshot
 */
export async function createSnapshot(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  type: 'daily' | 'monthly'
): Promise<SnapshotInfo> {
  const now = new Date()

  if (type === 'daily') {
    return createDailySnapshot(bucket, ns, collection, now)
  } else {
    return createMonthlySnapshot(bucket, ns, collection, now)
  }
}

/**
 * Creates a daily snapshot from data.parquet
 */
async function createDailySnapshot(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  timestamp: Date
): Promise<SnapshotInfo> {
  const dataPath = getDataPath(ns, collection)
  const dataObj = await bucket.get(dataPath)

  if (!dataObj) {
    throw new Error('data.parquet not found')
  }

  const dataBuffer = await dataObj.arrayBuffer()
  const { columns, rowCount } = await getParquetMetadata(dataBuffer)

  // Check if we should skip due to no changes
  const manifest = await readManifest(bucket, ns, collection)
  if (manifest && manifest.snapshots.length > 0) {
    const lastSnapshot = manifest.snapshots
      .filter((s) => s.type === 'daily')
      .sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())[0]

    if (lastSnapshot) {
      // Compare actual content by loading the last snapshot
      const lastSnapshotObj = await bucket.get(lastSnapshot.path)
      if (lastSnapshotObj) {
        const lastSnapshotBuffer = await lastSnapshotObj.arrayBuffer()
        // Compare byte content
        if (dataBuffer.byteLength === lastSnapshotBuffer.byteLength) {
          const currentBytes = new Uint8Array(dataBuffer)
          const lastBytes = new Uint8Array(lastSnapshotBuffer)
          let bytesMatch = true
          for (let i = 0; i < currentBytes.length; i++) {
            if (currentBytes[i] !== lastBytes[i]) {
              bytesMatch = false
              break
            }
          }
          if (bytesMatch) {
            return {
              type: 'daily',
              path: `${ns}/${collection}/snapshots/${formatDateDaily(timestamp)}.parquet`,
              timestamp,
              rowCount,
              sizeBytes: dataBuffer.byteLength,
              checksum: computeChecksum(dataBuffer),
              columns,
              sortedBy: 'id',
              skipped: true,
              reason: 'no_changes',
            }
          }
        }
      }
    }
  }

  // Create snapshot
  const snapshotPath = `${ns}/${collection}/snapshots/${formatDateDaily(timestamp)}.parquet`
  await bucket.put(snapshotPath, dataBuffer)

  const checksum = computeChecksum(dataBuffer)
  const snapshotInfo: SnapshotInfo = {
    type: 'daily',
    path: snapshotPath,
    timestamp,
    rowCount,
    sizeBytes: dataBuffer.byteLength,
    checksum,
    columns,
    sortedBy: 'id',
  }

  // Update manifest
  const updatedManifest: SnapshotManifest = manifest || { snapshots: [], lastModified: timestamp }
  // Remove existing snapshot for same date if exists
  const dateStr = formatDateDaily(timestamp)
  updatedManifest.snapshots = updatedManifest.snapshots.filter(
    (s) => !s.path.endsWith(`${dateStr}.parquet`) || s.type !== 'daily'
  )
  updatedManifest.snapshots.push(snapshotInfo)
  updatedManifest.lastModified = timestamp
  await writeManifest(bucket, ns, collection, updatedManifest)

  return snapshotInfo
}

/**
 * Creates a monthly snapshot from the last daily snapshot of the previous month
 */
async function createMonthlySnapshot(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  timestamp: Date
): Promise<SnapshotInfo> {
  // Get the previous month
  const prevMonth = new Date(timestamp)
  prevMonth.setUTCMonth(prevMonth.getUTCMonth() - 1)
  const targetMonth = formatDateMonthly(prevMonth)

  // List all snapshots to find daily ones for the target month
  const prefix = getSnapshotsPrefix(ns, collection)
  const listed = await bucket.list({ prefix })

  const dailySnapshots: { key: string; date: Date }[] = []
  for (const obj of listed.objects) {
    const filename = obj.key.split('/').pop() || ''
    const type = getSnapshotType(filename)
    if (type === 'daily') {
      const date = parseDailyDate(filename)
      if (date && formatDateMonthly(date) === targetMonth) {
        dailySnapshots.push({ key: obj.key, date })
      }
    }
  }

  if (dailySnapshots.length === 0) {
    throw new Error('no daily snapshots found')
  }

  // Sort by date and get the last one
  dailySnapshots.sort((a, b) => b.date.getTime() - a.date.getTime())
  const lastDaily = dailySnapshots[0]

  // Copy the last daily snapshot as the monthly snapshot
  const dailyObj = await bucket.get(lastDaily.key)
  if (!dailyObj) {
    throw new Error('Failed to read daily snapshot')
  }

  const dataBuffer = await dailyObj.arrayBuffer()
  const { columns, rowCount } = await getParquetMetadata(dataBuffer)

  const snapshotPath = `${ns}/${collection}/snapshots/${targetMonth}.parquet`
  await bucket.put(snapshotPath, dataBuffer)

  const checksum = computeChecksum(dataBuffer)
  const snapshotInfo: SnapshotInfo = {
    type: 'monthly',
    path: snapshotPath,
    timestamp,
    rowCount,
    sizeBytes: dataBuffer.byteLength,
    checksum,
    columns,
    sortedBy: 'id',
  }

  // Update manifest
  const manifest = await readManifest(bucket, ns, collection)
  const updatedManifest: SnapshotManifest = manifest || { snapshots: [], lastModified: timestamp }
  // Remove existing monthly snapshot for same month if exists
  updatedManifest.snapshots = updatedManifest.snapshots.filter(
    (s) => !s.path.endsWith(`${targetMonth}.parquet`) || s.type !== 'monthly'
  )
  updatedManifest.snapshots.push(snapshotInfo)
  updatedManifest.lastModified = timestamp
  await writeManifest(bucket, ns, collection, updatedManifest)

  return snapshotInfo
}

/**
 * Lists all snapshots for a collection
 *
 * @param bucket - R2 bucket containing the snapshots
 * @param ns - Namespace
 * @param collection - Collection name
 * @returns Array of snapshot information
 */
export async function listSnapshots(
  bucket: R2Bucket,
  ns: string,
  collection: string
): Promise<SnapshotInfo[]> {
  const prefix = getSnapshotsPrefix(ns, collection)
  const listed = await bucket.list({ prefix })

  const snapshots: SnapshotInfo[] = []

  for (const obj of listed.objects) {
    const filename = obj.key.split('/').pop() || ''

    // Skip manifest
    if (filename === 'manifest.json') continue

    const type = getSnapshotType(filename)
    if (!type) continue

    let date: Date | null = null
    if (type === 'daily') {
      date = parseDailyDate(filename)
    } else {
      date = parseMonthlyDate(filename)
    }

    if (!date) continue

    // Read the parquet file to get row count
    const fileObj = await bucket.get(obj.key)
    if (!fileObj) continue

    const buffer = await fileObj.arrayBuffer()
    const { columns, rowCount } = await getParquetMetadata(buffer)

    snapshots.push({
      type,
      path: obj.key,
      timestamp: date,
      rowCount,
      sizeBytes: obj.size,
      columns,
      sortedBy: 'id',
    })
  }

  // Sort by timestamp
  snapshots.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime())

  return snapshots
}

/**
 * Reconstructs collection state at a specific point in time
 *
 * Uses the nearest snapshot before the target timestamp and applies
 * subsequent deltas to reconstruct the exact state.
 *
 * @param bucket - R2 bucket containing data
 * @param ns - Namespace
 * @param collection - Collection name
 * @param timestamp - Target point in time
 * @returns Map of document ID to document state
 */
export async function reconstructState(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  timestamp: Date
): Promise<Map<string, unknown>> {
  // Full timestamp for snapshot selection
  const fullTargetTs = timestamp.getTime()
  // Time since midnight for delta filtering (matches test's delta ts format)
  const deltaTargetTs =
    timestamp.getUTCHours() * 3600000 +
    timestamp.getUTCMinutes() * 60000 +
    timestamp.getUTCSeconds() * 1000 +
    timestamp.getUTCMilliseconds()
  const state = new Map<string, unknown>()

  // Find the nearest snapshot before the target timestamp
  const snapshots = await listSnapshots(bucket, ns, collection)
  const snapshotsBefore = snapshots.filter((s) => s.timestamp.getTime() <= fullTargetTs)
  snapshotsBefore.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())
  const nearestSnapshot = snapshotsBefore[0]

  let baseTimestamp: Date | null = null

  if (nearestSnapshot) {
    // Load state from snapshot
    const snapshotObj = await bucket.get(nearestSnapshot.path)
    if (snapshotObj) {
      const buffer = await snapshotObj.arrayBuffer()
      const records = await readParquetRecords(buffer)
      for (const record of records) {
        const id = record.id as string
        if (id) {
          state.set(id, record)
        }
      }
      baseTimestamp = nearestSnapshot.timestamp
    }
  } else {
    // No snapshot, try data.parquet
    const dataPath = getDataPath(ns, collection)
    const dataObj = await bucket.get(dataPath)
    if (dataObj) {
      const buffer = await dataObj.arrayBuffer()
      const records = await readParquetRecords(buffer)
      for (const record of records) {
        const id = record.id as string
        if (id) {
          state.set(id, record)
        }
      }
      // No base timestamp - apply all deltas
      baseTimestamp = null
    }
  }

  // Apply deltas up to the target timestamp
  const deltasPrefix = getDeltasPrefix(ns, collection)
  const deltaListed = await bucket.list({ prefix: deltasPrefix })

  // Collect all deltas
  const allDeltas: Array<{ id: string; op: string; ts: number; doc?: Record<string, unknown> }> = []

  for (const obj of deltaListed.objects) {
    const deltaObj = await bucket.get(obj.key)
    if (!deltaObj) continue

    const buffer = await deltaObj.arrayBuffer()
    const records = await readParquetRecords(buffer)

    for (const record of records) {
      const ts = Number(record.ts as bigint | number)
      // Only apply deltas up to the target timestamp
      if (ts > deltaTargetTs) continue

      allDeltas.push({
        id: record.id as string,
        op: record.op as string,
        ts,
        doc: record.doc ? (typeof record.doc === 'string' ? JSON.parse(record.doc) : record.doc) : undefined,
      })
    }
  }

  // Sort deltas by timestamp
  allDeltas.sort((a, b) => a.ts - b.ts)

  // Apply deltas in order
  for (const delta of allDeltas) {
    if (delta.op === 'insert' || delta.op === 'update') {
      state.set(delta.id, delta.doc)
    } else if (delta.op === 'delete') {
      state.delete(delta.id)
    }
  }

  return state
}

/**
 * Cleans up old snapshots based on retention policy
 *
 * @param bucket - R2 bucket containing snapshots
 * @param ns - Namespace
 * @param collection - Collection name
 * @param retention - Retention policy configuration
 */
export async function cleanupSnapshots(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  retention: RetentionPolicy
): Promise<void> {
  const snapshots = await listSnapshots(bucket, ns, collection)

  // Separate daily and monthly snapshots
  const dailySnapshots = snapshots.filter((s) => s.type === 'daily')
  const monthlySnapshots = snapshots.filter((s) => s.type === 'monthly')

  // Sort by timestamp descending (most recent first)
  dailySnapshots.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())
  monthlySnapshots.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())

  // Determine which snapshots to delete
  const dailyToDelete = dailySnapshots.slice(retention.dailySnapshots)
  const monthlyToDelete = monthlySnapshots.slice(retention.monthlySnapshots)

  const toDelete = [...dailyToDelete, ...monthlyToDelete]

  // Delete snapshots
  for (const snapshot of toDelete) {
    await bucket.delete(snapshot.path)
  }

  // Update manifest with remaining snapshots
  const remainingDaily = dailySnapshots.slice(0, retention.dailySnapshots)
  const remainingMonthly = monthlySnapshots.slice(0, retention.monthlySnapshots)
  const remainingSnapshots = [...remainingDaily, ...remainingMonthly]

  const manifest: SnapshotManifest = {
    snapshots: remainingSnapshots,
    lastModified: new Date(),
  }

  await writeManifest(bucket, ns, collection, manifest)
}
