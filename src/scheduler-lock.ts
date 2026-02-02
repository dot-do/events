/**
 * Distributed lock for scheduled tasks using R2 conditional puts.
 *
 * Uses R2's conditional upload (onlyIf with etagDoesNotMatch: "*") to ensure
 * only one worker can acquire the lock at a time. The lock includes:
 * - TTL-based expiration (10 minutes) to prevent deadlocks
 * - Hostname/timestamp for debugging
 * - Automatic cleanup of expired locks
 */

import { logger, logError } from './logger'

const log = logger.child({ component: 'SchedulerLock' })

export interface SchedulerLockOptions {
  /** Lock timeout in milliseconds (default: 10 minutes) */
  timeoutMs?: number
  /** Hostname for debugging */
  hostname?: string
}

export interface LockInfo {
  hostname: string
  timestamp: string
  acquiredAt: number
  expiresAt: number
}

const DEFAULT_LOCK_TIMEOUT_MS = 10 * 60 * 1000 // 10 minutes
const LOCK_KEY = '_locks/scheduled'

/**
 * Try to acquire a distributed lock for scheduled tasks.
 *
 * Uses R2 conditional put with etagDoesNotMatch: "*" for atomic acquisition.
 * If put succeeds, we have the lock. If put fails (object exists), we check
 * if the lock is expired (>10 min old). If expired, delete and retry.
 *
 * @returns Lock info if acquired, null if lock is held by another worker
 */
export async function acquireSchedulerLock(
  bucket: R2Bucket,
  options: SchedulerLockOptions = {}
): Promise<LockInfo | null> {
  const { timeoutMs = DEFAULT_LOCK_TIMEOUT_MS, hostname = `worker-${crypto.randomUUID().slice(0, 8)}` } = options

  const now = Date.now()
  const expiresAt = now + timeoutMs

  const lockInfo: LockInfo = {
    hostname,
    timestamp: new Date().toISOString(),
    acquiredAt: now,
    expiresAt,
  }

  // Try atomic lock acquisition using conditional put
  // onlyIf: { etagDoesNotMatch: "*" } ensures the put only succeeds if no object exists
  try {
    await bucket.put(LOCK_KEY, JSON.stringify(lockInfo), {
      httpMetadata: {
        contentType: 'application/json',
      },
      onlyIf: {
        etagDoesNotMatch: '*',
      },
    })

    log.info('Lock acquired', { hostname, expiresAt: new Date(expiresAt).toISOString() })
    return lockInfo
  } catch (err) {
    // Put failed - lock object exists, check if it's expired
    const existing = await bucket.get(LOCK_KEY)

    if (!existing) {
      // Lock was just deleted, retry acquisition
      log.info('Lock disappeared, retrying acquisition')
      return acquireSchedulerLock(bucket, options)
    }

    try {
      const existingLock: LockInfo = await existing.json()

      // If lock is still valid, another worker owns it
      if (existingLock.expiresAt > now) {
        log.info('Lock held by another worker', {
          holder: existingLock.hostname,
          expiresInSeconds: Math.round((existingLock.expiresAt - now) / 1000)
        })
        return null
      }

      // Lock is expired (>10 min old), delete and retry
      log.info('Found expired lock, deleting and retrying', {
        holder: existingLock.hostname,
        expiredSecondsAgo: Math.round((now - existingLock.expiresAt) / 1000)
      })

      await bucket.delete(LOCK_KEY)
      return acquireSchedulerLock(bucket, options)
    } catch (parseErr) {
      // Invalid lock data, delete and retry
      log.warn('Invalid lock data, deleting and retrying')
      await bucket.delete(LOCK_KEY)
      return acquireSchedulerLock(bucket, options)
    }
  }
}

/**
 * Release a distributed lock.
 *
 * @param lock - The lock info returned from acquireSchedulerLock
 * @returns true if released, false if lock was not owned by us
 */
export async function releaseSchedulerLock(
  bucket: R2Bucket,
  lock: LockInfo
): Promise<boolean> {
  try {
    // Verify we still own the lock before deleting
    const existing = await bucket.get(LOCK_KEY)

    if (!existing) {
      log.warn('Lock already released')
      return true
    }

    const existingLock: LockInfo = await existing.json()

    if (existingLock.hostname !== lock.hostname || existingLock.acquiredAt !== lock.acquiredAt) {
      log.warn('Lock owned by different worker', { holder: existingLock.hostname, expected: lock.hostname })
      return false
    }

    await bucket.delete(LOCK_KEY)
    log.info('Lock released', { hostname: lock.hostname })
    return true
  } catch (err) {
    logError(log, 'Failed to release lock', err)
    return false
  }
}

/**
 * Execute a function with a distributed lock.
 *
 * Acquires the lock, runs the function, and releases the lock.
 * If the lock cannot be acquired, returns null without running the function.
 *
 * @param bucket - R2 bucket for lock storage
 * @param fn - Function to execute while holding the lock
 * @param options - Lock options
 * @returns Result of fn, or null if lock could not be acquired
 */
export async function withSchedulerLock<T>(
  bucket: R2Bucket,
  fn: () => Promise<T>,
  options: SchedulerLockOptions = {}
): Promise<{ result: T; skipped: false } | { result: null; skipped: true }> {
  const lock = await acquireSchedulerLock(bucket, options)

  if (!lock) {
    return { result: null, skipped: true }
  }

  try {
    const result = await fn()
    return { result, skipped: false }
  } finally {
    await releaseSchedulerLock(bucket, lock)
  }
}
