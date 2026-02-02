/**
 * R2 Storage Adapter for @dotdo/deltalake
 *
 * Wraps Cloudflare R2 bucket to implement the StorageBackend interface
 * required by DeltaTable operations. This enables ACID-guaranteed writes
 * with Delta Lake transaction logs on R2.
 *
 * Features:
 * - Byte-range reads for efficient Parquet footer access
 * - Optimistic concurrency control via ETag-based versioning
 * - Automatic pagination for list operations
 * - In-memory write locks for concurrent write serialization
 */

import type { StorageBackend } from '@dotdo/deltalake'

/**
 * File metadata returned by stat().
 */
interface FileStat {
  size: number
  lastModified: Date
  etag?: string
}

// =============================================================================
// WRITE LOCK UTILITY
// =============================================================================

/**
 * Lock object for managing write serialization.
 */
interface Lock {
  promise: Promise<void>
  release: () => void
}

/**
 * Creates a new Lock object.
 */
function createLock(): Lock {
  let releaseRef: (() => void) | undefined
  const promise = new Promise<void>(resolve => {
    releaseRef = resolve
  })
  const release = () => {
    if (releaseRef) releaseRef()
  }
  return { promise, release }
}

/**
 * Acquires a write lock for a given path.
 */
async function acquireWriteLock(locks: Map<string, Lock>, path: string): Promise<Lock> {
  while (true) {
    const existingLock = locks.get(path)

    if (existingLock) {
      await existingLock.promise
      continue
    }

    const newLock = createLock()
    locks.set(path, newLock)
    return newLock
  }
}

/**
 * Releases a write lock for a given path.
 */
function releaseWriteLock(locks: Map<string, Lock>, path: string, lock: Lock): void {
  if (locks.get(path) === lock) {
    locks.delete(path)
  }
  lock.release()
}

// =============================================================================
// ERRORS (matching @dotdo/deltalake error types)
// =============================================================================

/**
 * Error thrown when a file is not found.
 */
export class FileNotFoundError extends Error {
  constructor(
    public readonly path: string,
    public readonly operation: string
  ) {
    super(`File not found: ${path} (${operation})`)
    this.name = 'FileNotFoundError'
  }
}

/**
 * Error thrown when version mismatch occurs during conditional write.
 */
export class VersionMismatchError extends Error {
  constructor(
    public readonly path: string,
    public readonly expectedVersion: string | null,
    public readonly actualVersion: string | null
  ) {
    super(
      `Version mismatch for ${path}: expected ${expectedVersion ?? 'null (create)'}, got ${actualVersion ?? 'null (not found)'}`
    )
    this.name = 'VersionMismatchError'
  }
}

// =============================================================================
// R2 STORAGE BACKEND
// =============================================================================

/**
 * R2 Storage Backend for @dotdo/deltalake.
 *
 * Implements the StorageBackend interface using Cloudflare R2 bucket operations.
 * Provides all operations needed for Delta Lake table management:
 * - File operations (read, write, delete, list)
 * - Byte-range reads for efficient Parquet access
 * - Conditional writes for optimistic concurrency control
 *
 * @example
 * ```typescript
 * import { R2StorageBackend } from '@dotdo/events/deltalake-storage'
 * import { DeltaTable } from '@dotdo/deltalake'
 *
 * const storage = new R2StorageBackend(env.EVENTS_BUCKET)
 * const events = new DeltaTable(storage, 'events')
 * ```
 */
export class R2StorageBackend implements StorageBackend {
  private writeLocks = new Map<string, Lock>()

  constructor(private readonly bucket: R2Bucket) {
    if (!bucket || typeof bucket.get !== 'function') {
      throw new Error('R2StorageBackend requires a valid R2Bucket instance')
    }
  }

  /**
   * Read the entire contents of a file.
   */
  async read(path: string): Promise<Uint8Array> {
    const object = await this.bucket.get(path)
    if (!object) {
      throw new FileNotFoundError(path, 'read')
    }
    const arrayBuffer = await object.arrayBuffer()
    return new Uint8Array(arrayBuffer)
  }

  /**
   * Write data to a file.
   */
  async write(path: string, data: Uint8Array): Promise<void> {
    await this.bucket.put(path, data)
  }

  /**
   * List all files matching a prefix.
   */
  async list(prefix: string): Promise<string[]> {
    const results: string[] = []
    let cursor: string | undefined = undefined
    let truncated = true

    while (truncated) {
      const response = await this.bucket.list({
        prefix,
        ...(cursor ? { cursor } : {}),
      })

      results.push(...response.objects.map(obj => obj.key))
      truncated = response.truncated
      if (response.truncated) {
        cursor = response.cursor
      }
    }

    return results
  }

  /**
   * Delete a file (idempotent).
   */
  async delete(path: string): Promise<void> {
    await this.bucket.delete(path)
  }

  /**
   * Check if a file exists.
   */
  async exists(path: string): Promise<boolean> {
    const object = await this.bucket.head(path)
    return object !== null
  }

  /**
   * Get file metadata.
   */
  async stat(path: string): Promise<FileStat | null> {
    const object = await this.bucket.head(path)
    if (!object) {
      return null
    }
    return {
      size: object.size,
      lastModified: object.uploaded,
      etag: object.etag,
    }
  }

  /**
   * Read a byte range from a file.
   * Essential for efficient Parquet file reading where metadata is at the end.
   */
  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const length = end - start
    const object = await this.bucket.get(path, {
      range: { offset: start, length },
    })
    if (!object) {
      throw new FileNotFoundError(path, 'readRange')
    }
    const arrayBuffer = await object.arrayBuffer()
    return new Uint8Array(arrayBuffer)
  }

  /**
   * Get the current version (ETag) of a file.
   */
  async getVersion(path: string): Promise<string | null> {
    const object = await this.bucket.head(path)
    if (!object) {
      return null
    }
    return object.etag
  }

  /**
   * Conditionally write a file only if the version matches.
   * Enables optimistic concurrency control for Delta Lake transactions.
   *
   * @param path - File path
   * @param data - Data to write
   * @param expectedVersion - Expected ETag, or null for create-if-not-exists
   * @returns New ETag after successful write
   * @throws VersionMismatchError if version doesn't match
   */
  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null
  ): Promise<string> {
    const lock = await acquireWriteLock(this.writeLocks, path)

    try {
      const currentVersion = await this.getVersion(path)

      if (expectedVersion === null) {
        // Create-if-not-exists: file should not exist
        if (currentVersion !== null) {
          throw new VersionMismatchError(path, expectedVersion, currentVersion)
        }
      } else {
        // Update: version should match
        if (currentVersion !== expectedVersion) {
          throw new VersionMismatchError(path, expectedVersion, currentVersion)
        }
      }

      // Version matches, perform the write
      await this.bucket.put(path, data)

      // Get the new version (ETag) after write
      const newVersion = await this.getVersion(path)
      return newVersion ?? ''
    } finally {
      releaseWriteLock(this.writeLocks, path, lock)
    }
  }
}

/**
 * Create an R2 storage backend from an R2Bucket.
 *
 * @param bucket - Cloudflare R2 bucket binding
 * @returns StorageBackend instance for use with DeltaTable
 *
 * @example
 * ```typescript
 * import { createR2Storage } from '@dotdo/events/deltalake-storage'
 * import { DeltaTable } from '@dotdo/deltalake'
 *
 * const storage = createR2Storage(env.EVENTS_BUCKET)
 * const events = new DeltaTable(storage, 'events')
 * ```
 */
export function createR2Storage(bucket: R2Bucket): StorageBackend {
  return new R2StorageBackend(bucket)
}
