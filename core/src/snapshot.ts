/**
 * Snapshot/Backup utilities for Collections
 */

export interface SnapshotOptions {
  /** R2 bucket for snapshots */
  bucket: R2Bucket
  /** Prefix for snapshot keys (default: 'snapshots') */
  prefix?: string
  /** Include metadata in snapshot */
  includeMetadata?: boolean
}

export interface SnapshotResult {
  /** R2 key where snapshot was stored */
  key: string
  /** Collection names included in snapshot */
  collections: string[]
  /** Total document count */
  totalDocs: number
  /** Timestamp of snapshot */
  timestamp: string
}

/**
 * Create a point-in-time snapshot of all collections to R2
 * Enables time-travel queries when combined with CDC stream
 *
 * @example
 * ```typescript
 * const result = await createSnapshot(this.sql, this.ctx.id.toString(), {
 *   bucket: this.env.SNAPSHOTS_BUCKET,
 * })
 * console.log(`Snapshot: ${result.key}, ${result.totalDocs} docs`)
 * ```
 */
export async function createSnapshot(
  sql: SqlStorage,
  doId: string,
  options: SnapshotOptions
): Promise<SnapshotResult> {
  const prefix = options.prefix ?? 'snapshots'
  const timestamp = new Date().toISOString()
  const safeTimestamp = timestamp.replace(/[:.]/g, '-')
  const key = `${prefix}/${doId}/${safeTimestamp}.json`

  // Get all collections
  const collectionsResult = sql.exec<{ collection: string }>(
    `SELECT DISTINCT collection FROM _collections`
  ).toArray()

  const snapshot: Record<string, Record<string, unknown>[]> = {}
  let totalDocs = 0

  for (const { collection } of collectionsResult) {
    const docs = sql.exec<{ id: string; data: string }>(
      `SELECT id, data FROM _collections WHERE collection = ?`,
      collection
    ).toArray()

    snapshot[collection] = docs.map(d => ({
      _id: d.id,
      ...JSON.parse(d.data),
    }))
    totalDocs += docs.length
  }

  const body = JSON.stringify({
    doId,
    timestamp,
    collections: Object.keys(snapshot),
    totalDocs,
    data: snapshot,
  }, null, 2)

  await options.bucket.put(key, body, {
    httpMetadata: { contentType: 'application/json' },
    customMetadata: {
      doId,
      timestamp,
      collections: collectionsResult.map(c => c.collection).join(','),
      docCount: String(totalDocs),
    },
  })

  return {
    key,
    collections: collectionsResult.map(c => c.collection),
    totalDocs,
    timestamp,
  }
}

/**
 * Restore collections from a snapshot
 *
 * @example
 * ```typescript
 * const result = await restoreSnapshot(
 *   this.sql,
 *   this.env.SNAPSHOTS_BUCKET,
 *   'snapshots/abc123/2024-01-15T12-34-56-789Z.json'
 * )
 * console.log(`Restored ${result.totalDocs} docs`)
 * ```
 */
export async function restoreSnapshot(
  sql: SqlStorage,
  bucket: R2Bucket,
  snapshotKey: string
): Promise<{ collections: string[]; totalDocs: number }> {
  const object = await bucket.get(snapshotKey)
  if (!object) {
    throw new Error(`Snapshot not found: ${snapshotKey}`)
  }

  const snapshot = await object.json<{
    data: Record<string, Record<string, unknown>[]>
  }>()

  let totalDocs = 0

  for (const [collection, docs] of Object.entries(snapshot.data)) {
    // Clear existing collection data
    sql.exec(`DELETE FROM _collections WHERE collection = ?`, collection)

    // Insert snapshot data
    for (const doc of docs) {
      const { _id, ...data } = doc
      sql.exec(
        `INSERT INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)`,
        collection,
        _id as string,
        JSON.stringify(data),
        new Date().toISOString()
      )
      totalDocs++
    }
  }

  return {
    collections: Object.keys(snapshot.data),
    totalDocs,
  }
}

/**
 * List available snapshots for a DO
 */
export async function listSnapshots(
  bucket: R2Bucket,
  doId: string,
  options?: { prefix?: string; limit?: number }
): Promise<R2Objects> {
  const prefix = options?.prefix ?? 'snapshots'
  return bucket.list({
    prefix: `${prefix}/${doId}/`,
    limit: options?.limit ?? 100,
  })
}

/**
 * Delete a snapshot
 */
export async function deleteSnapshot(
  bucket: R2Bucket,
  snapshotKey: string
): Promise<void> {
  await bucket.delete(snapshotKey)
}
