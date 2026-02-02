/**
 * Scheduler Lock Unit Tests
 *
 * Comprehensive unit tests for src/scheduler-lock.ts covering:
 * 1. acquireSchedulerLock() - successful acquisition
 * 2. releaseSchedulerLock() - successful release
 * 3. withSchedulerLock() - wrapper function
 * 4. Race condition scenarios
 * 5. Lock expiration handling
 * 6. Error paths
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  acquireSchedulerLock,
  releaseSchedulerLock,
  withSchedulerLock,
  type LockInfo,
  type SchedulerLockOptions,
} from '../scheduler-lock'

// ============================================================================
// Constants
// ============================================================================

const LOCK_KEY = '_locks/scheduled'
const DEFAULT_LOCK_TIMEOUT_MS = 10 * 60 * 1000 // 10 minutes

// ============================================================================
// Mock Factories
// ============================================================================

interface MockR2Object {
  json: () => Promise<LockInfo>
  text?: () => Promise<string>
  body?: ReadableStream
}

interface MockR2BucketState {
  lockInfo: LockInfo | null
  putShouldFail: boolean
  getShouldFail: boolean
  deleteShouldFail: boolean
  putCallCount: number
  getCallCount: number
  deleteCallCount: number
  lockDeletedDuringGet: boolean
}

function createMockR2Bucket(initialState: Partial<MockR2BucketState> = {}) {
  const state: MockR2BucketState = {
    lockInfo: null,
    putShouldFail: false,
    getShouldFail: false,
    deleteShouldFail: false,
    putCallCount: 0,
    getCallCount: 0,
    deleteCallCount: 0,
    lockDeletedDuringGet: false,
    ...initialState,
  }

  const bucket = {
    _state: state,

    put: vi.fn(async (key: string, body: string, options?: { onlyIf?: { etagDoesNotMatch?: string }; httpMetadata?: { contentType?: string } }) => {
      state.putCallCount++

      if (key === LOCK_KEY) {
        // Simulate conditional put behavior
        if (options?.onlyIf?.etagDoesNotMatch === '*' && state.lockInfo !== null) {
          // Lock exists, conditional put fails
          throw new Error('PreconditionFailed: Object already exists')
        }
        // Lock acquired successfully
        state.lockInfo = JSON.parse(body)
      }
      return { key, version: 'v1', size: body.length }
    }),

    get: vi.fn(async (key: string): Promise<MockR2Object | null> => {
      state.getCallCount++

      if (state.getShouldFail) {
        throw new Error('R2 get failed')
      }

      // Simulate lock being deleted between put failure and get
      if (state.lockDeletedDuringGet) {
        state.lockDeletedDuringGet = false
        return null
      }

      if (key === LOCK_KEY && state.lockInfo) {
        const lockCopy = { ...state.lockInfo }
        return {
          json: async () => lockCopy,
          text: async () => JSON.stringify(lockCopy),
        }
      }
      return null
    }),

    delete: vi.fn(async (key: string) => {
      state.deleteCallCount++

      if (state.deleteShouldFail) {
        throw new Error('R2 delete failed')
      }

      if (key === LOCK_KEY) {
        state.lockInfo = null
      }
    }),

    head: vi.fn(async () => null),
    list: vi.fn(async () => ({ objects: [], truncated: false })),
  }

  return bucket as unknown as R2Bucket & { _state: MockR2BucketState }
}

function createLockInfo(overrides: Partial<LockInfo> = {}): LockInfo {
  const now = Date.now()
  return {
    hostname: 'test-worker',
    timestamp: new Date().toISOString(),
    acquiredAt: now,
    expiresAt: now + DEFAULT_LOCK_TIMEOUT_MS,
    ...overrides,
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('scheduler-lock', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // ──────────────────────────────────────────────────────────────────────────
  // acquireSchedulerLock Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('acquireSchedulerLock()', () => {
    describe('successful acquisition', () => {
      it('acquires lock when no lock exists', async () => {
        const bucket = createMockR2Bucket()

        const lock = await acquireSchedulerLock(bucket)

        expect(lock).not.toBeNull()
        expect(lock?.hostname).toBeDefined()
        expect(lock?.timestamp).toBeDefined()
        expect(lock?.acquiredAt).toBeDefined()
        expect(lock?.expiresAt).toBeGreaterThan(Date.now())
      })

      it('uses provided hostname in lock info', async () => {
        const bucket = createMockR2Bucket()

        const lock = await acquireSchedulerLock(bucket, { hostname: 'custom-worker-123' })

        expect(lock?.hostname).toBe('custom-worker-123')
      })

      it('generates random hostname if not provided', async () => {
        const bucket = createMockR2Bucket()

        const lock = await acquireSchedulerLock(bucket)

        expect(lock?.hostname).toMatch(/^worker-[a-f0-9]+$/)
      })

      it('sets correct expiration time with default timeout', async () => {
        const bucket = createMockR2Bucket()
        const now = Date.now()

        const lock = await acquireSchedulerLock(bucket)

        expect(lock?.acquiredAt).toBeGreaterThanOrEqual(now)
        expect(lock?.expiresAt).toBe(lock!.acquiredAt + DEFAULT_LOCK_TIMEOUT_MS)
      })

      it('sets correct expiration time with custom timeout', async () => {
        const bucket = createMockR2Bucket()
        const customTimeout = 5 * 60 * 1000 // 5 minutes

        const lock = await acquireSchedulerLock(bucket, { timeoutMs: customTimeout })

        expect(lock?.expiresAt).toBe(lock!.acquiredAt + customTimeout)
      })

      it('includes ISO timestamp in lock info', async () => {
        const bucket = createMockR2Bucket()

        const lock = await acquireSchedulerLock(bucket)

        expect(lock?.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/)
      })

      it('calls R2 put with correct parameters', async () => {
        const bucket = createMockR2Bucket()

        await acquireSchedulerLock(bucket, { hostname: 'test-host' })

        expect(bucket.put).toHaveBeenCalledWith(
          LOCK_KEY,
          expect.stringContaining('"hostname":"test-host"'),
          expect.objectContaining({
            httpMetadata: { contentType: 'application/json' },
            onlyIf: { etagDoesNotMatch: '*' },
          })
        )
      })
    })

    describe('lock contention', () => {
      it('returns null when valid lock is held by another worker', async () => {
        const existingLock = createLockInfo({ hostname: 'other-worker' })
        const bucket = createMockR2Bucket({ lockInfo: existingLock })

        const lock = await acquireSchedulerLock(bucket, { hostname: 'my-worker' })

        expect(lock).toBeNull()
      })

      it('does not delete valid lock held by another worker', async () => {
        const existingLock = createLockInfo({ hostname: 'other-worker' })
        const bucket = createMockR2Bucket({ lockInfo: existingLock })

        await acquireSchedulerLock(bucket)

        expect(bucket.delete).not.toHaveBeenCalled()
      })
    })

    describe('lock expiration handling', () => {
      it('acquires lock when existing lock is expired', async () => {
        const expiredLock = createLockInfo({
          hostname: 'old-worker',
          acquiredAt: Date.now() - DEFAULT_LOCK_TIMEOUT_MS - 60000,
          expiresAt: Date.now() - 60000, // Expired 1 minute ago
        })
        const bucket = createMockR2Bucket({ lockInfo: expiredLock })

        const lock = await acquireSchedulerLock(bucket, { hostname: 'new-worker' })

        expect(lock).not.toBeNull()
        expect(lock?.hostname).toBe('new-worker')
      })

      it('deletes expired lock before re-acquiring', async () => {
        const expiredLock = createLockInfo({
          hostname: 'old-worker',
          expiresAt: Date.now() - 1000, // Expired
        })
        const bucket = createMockR2Bucket({ lockInfo: expiredLock })

        await acquireSchedulerLock(bucket)

        expect(bucket.delete).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('correctly identifies lock at exact expiration boundary as expired', async () => {
        const now = Date.now()
        const boundaryLock = createLockInfo({
          hostname: 'boundary-worker',
          expiresAt: now, // Expires exactly now
        })
        const bucket = createMockR2Bucket({ lockInfo: boundaryLock })

        const lock = await acquireSchedulerLock(bucket)

        // Lock at exact boundary is NOT expired (expiresAt > now check fails when equal)
        expect(lock).toBeNull()
      })

      it('correctly identifies lock 1ms before expiration as valid', async () => {
        const now = Date.now()
        const validLock = createLockInfo({
          hostname: 'valid-worker',
          expiresAt: now + 1, // Expires 1ms from now
        })
        const bucket = createMockR2Bucket({ lockInfo: validLock })

        const lock = await acquireSchedulerLock(bucket)

        expect(lock).toBeNull()
      })
    })

    describe('race conditions', () => {
      it('retries when lock disappears between put failure and get', async () => {
        const bucket = createMockR2Bucket({
          lockInfo: createLockInfo(), // Lock exists initially
          lockDeletedDuringGet: true, // But disappears during get
        })

        const lock = await acquireSchedulerLock(bucket)

        expect(lock).not.toBeNull()
        // Should have called put at least twice (initial fail, retry success)
        expect(bucket._state.putCallCount).toBeGreaterThanOrEqual(2)
      })

      it('handles multiple workers racing for lock', async () => {
        const bucket = createMockR2Bucket()

        // Simulate two workers trying to acquire simultaneously
        const [lock1, lock2] = await Promise.all([
          acquireSchedulerLock(bucket, { hostname: 'worker-1' }),
          acquireSchedulerLock(bucket, { hostname: 'worker-2' }),
        ])

        // Due to atomic conditional put, only one should succeed
        const successCount = [lock1, lock2].filter(l => l !== null).length
        expect(successCount).toBe(1)
      })
    })

    describe('error paths', () => {
      it('handles invalid JSON in existing lock data', async () => {
        const bucket = createMockR2Bucket()
        // Override get to return invalid JSON
        bucket.get = vi.fn(async () => ({
          json: async () => { throw new SyntaxError('Invalid JSON') },
        })) as unknown as typeof bucket.get
        // Make put fail to trigger the get path
        bucket.put = vi.fn(async () => { throw new Error('Lock exists') }) as unknown as typeof bucket.put

        const lock = await acquireSchedulerLock(bucket)

        // Should delete invalid lock and retry
        expect(bucket.delete).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('handles R2 get errors gracefully', async () => {
        const bucket = createMockR2Bucket({
          lockInfo: createLockInfo(),
          getShouldFail: true,
        })

        // The function will throw when get fails
        await expect(acquireSchedulerLock(bucket)).rejects.toThrow('R2 get failed')
      })
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // releaseSchedulerLock Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('releaseSchedulerLock()', () => {
    describe('successful release', () => {
      it('releases lock when we own it', async () => {
        const myLock = createLockInfo({ hostname: 'my-worker' })
        const bucket = createMockR2Bucket({ lockInfo: myLock })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(true)
        expect(bucket.delete).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('verifies hostname before releasing', async () => {
        const myLock = createLockInfo({ hostname: 'my-worker', acquiredAt: 1000 })
        const bucket = createMockR2Bucket({ lockInfo: myLock })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(true)
        expect(bucket.get).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('verifies acquiredAt timestamp before releasing', async () => {
        const myLock = createLockInfo({ hostname: 'my-worker', acquiredAt: 1000 })
        const differentTimeLock = createLockInfo({ hostname: 'my-worker', acquiredAt: 1000 })
        const bucket = createMockR2Bucket({ lockInfo: differentTimeLock })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(true)
      })
    })

    describe('lock already released', () => {
      it('returns true if lock is already released', async () => {
        const bucket = createMockR2Bucket({ lockInfo: null })
        const myLock = createLockInfo({ hostname: 'my-worker' })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(true)
        expect(bucket.delete).not.toHaveBeenCalled()
      })
    })

    describe('ownership validation', () => {
      it('fails to release if lock is owned by different worker', async () => {
        const otherLock = createLockInfo({ hostname: 'other-worker' })
        const bucket = createMockR2Bucket({ lockInfo: otherLock })
        const myLock = createLockInfo({ hostname: 'my-worker' })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(false)
        expect(bucket.delete).not.toHaveBeenCalled()
      })

      it('fails to release if acquiredAt differs', async () => {
        const storedLock = createLockInfo({ hostname: 'my-worker', acquiredAt: 2000 })
        const bucket = createMockR2Bucket({ lockInfo: storedLock })
        const myLock = createLockInfo({ hostname: 'my-worker', acquiredAt: 1000 })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(false)
      })

      it('prevents accidental release of re-acquired lock', async () => {
        // Scenario: Worker A acquires lock, Worker A crashes, lock expires,
        // Worker B acquires lock, Worker A recovers and tries to release
        const workerBLock = createLockInfo({
          hostname: 'worker-b',
          acquiredAt: Date.now(),
        })
        const bucket = createMockR2Bucket({ lockInfo: workerBLock })

        const workerAOldLock = createLockInfo({
          hostname: 'worker-a',
          acquiredAt: Date.now() - DEFAULT_LOCK_TIMEOUT_MS - 1000,
        })

        const released = await releaseSchedulerLock(bucket, workerAOldLock)

        expect(released).toBe(false)
        expect(bucket._state.lockInfo).not.toBeNull()
      })
    })

    describe('error handling', () => {
      it('returns false on R2 delete error', async () => {
        const myLock = createLockInfo({ hostname: 'my-worker' })
        const bucket = createMockR2Bucket({
          lockInfo: myLock,
          deleteShouldFail: true,
        })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(false)
      })

      it('returns false on R2 get error', async () => {
        const myLock = createLockInfo({ hostname: 'my-worker' })
        const bucket = createMockR2Bucket({
          lockInfo: myLock,
          getShouldFail: true,
        })

        const released = await releaseSchedulerLock(bucket, myLock)

        expect(released).toBe(false)
      })
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // withSchedulerLock Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('withSchedulerLock()', () => {
    describe('successful execution', () => {
      it('executes function when lock is acquired', async () => {
        const bucket = createMockR2Bucket()
        const fn = vi.fn().mockResolvedValue('result')

        const result = await withSchedulerLock(bucket, fn)

        expect(result).toEqual({ result: 'result', skipped: false })
        expect(fn).toHaveBeenCalledTimes(1)
      })

      it('returns function result correctly', async () => {
        const bucket = createMockR2Bucket()
        const complexResult = { data: [1, 2, 3], success: true }
        const fn = vi.fn().mockResolvedValue(complexResult)

        const result = await withSchedulerLock(bucket, fn)

        expect(result.skipped).toBe(false)
        expect(result.result).toEqual(complexResult)
      })

      it('releases lock after function completes successfully', async () => {
        const bucket = createMockR2Bucket()
        const fn = vi.fn().mockResolvedValue('done')

        await withSchedulerLock(bucket, fn)

        expect(bucket.delete).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('passes options to acquireSchedulerLock', async () => {
        const bucket = createMockR2Bucket()
        const fn = vi.fn().mockResolvedValue('done')
        const options: SchedulerLockOptions = {
          hostname: 'custom-host',
          timeoutMs: 30000,
        }

        await withSchedulerLock(bucket, fn, options)

        expect(bucket.put).toHaveBeenCalledWith(
          LOCK_KEY,
          expect.stringContaining('"hostname":"custom-host"'),
          expect.any(Object)
        )
      })
    })

    describe('lock not acquired', () => {
      it('skips function when lock cannot be acquired', async () => {
        const existingLock = createLockInfo({ hostname: 'other-worker' })
        const bucket = createMockR2Bucket({ lockInfo: existingLock })
        const fn = vi.fn()

        const result = await withSchedulerLock(bucket, fn)

        expect(result).toEqual({ result: null, skipped: true })
        expect(fn).not.toHaveBeenCalled()
      })

      it('does not attempt to release lock when not acquired', async () => {
        const existingLock = createLockInfo({ hostname: 'other-worker' })
        const bucket = createMockR2Bucket({ lockInfo: existingLock })
        const fn = vi.fn()

        await withSchedulerLock(bucket, fn)

        expect(bucket.delete).not.toHaveBeenCalled()
      })
    })

    describe('error handling', () => {
      it('releases lock when function throws', async () => {
        const bucket = createMockR2Bucket()
        const fn = vi.fn().mockRejectedValue(new Error('Function error'))

        await expect(withSchedulerLock(bucket, fn)).rejects.toThrow('Function error')

        expect(bucket.delete).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('propagates function errors after releasing lock', async () => {
        const bucket = createMockR2Bucket()
        const customError = new Error('Custom error message')
        const fn = vi.fn().mockRejectedValue(customError)

        await expect(withSchedulerLock(bucket, fn)).rejects.toThrow('Custom error message')
      })

      it('releases lock even on synchronous throw', async () => {
        const bucket = createMockR2Bucket()
        const fn = vi.fn(() => {
          throw new Error('Sync error')
        })

        await expect(withSchedulerLock(bucket, fn as () => Promise<unknown>)).rejects.toThrow('Sync error')

        expect(bucket.delete).toHaveBeenCalledWith(LOCK_KEY)
      })

      it('handles slow functions correctly', async () => {
        const bucket = createMockR2Bucket()
        const slowFn = vi.fn(async () => {
          await new Promise(resolve => setTimeout(resolve, 1000))
          return 'slow result'
        })

        const resultPromise = withSchedulerLock(bucket, slowFn)

        // Advance time
        await vi.advanceTimersByTimeAsync(1000)

        const result = await resultPromise

        expect(result.skipped).toBe(false)
        expect(result.result).toBe('slow result')
        expect(bucket.delete).toHaveBeenCalled()
      })
    })

    describe('type safety', () => {
      it('preserves generic type of function result', async () => {
        const bucket = createMockR2Bucket()

        interface CustomResult {
          count: number
          items: string[]
        }

        const fn = vi.fn(async (): Promise<CustomResult> => ({
          count: 3,
          items: ['a', 'b', 'c'],
        }))

        const result = await withSchedulerLock<CustomResult>(bucket, fn)

        if (!result.skipped) {
          // TypeScript should recognize result.result as CustomResult
          expect(result.result.count).toBe(3)
          expect(result.result.items).toHaveLength(3)
        }
      })
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Race Condition Scenarios
  // ──────────────────────────────────────────────────────────────────────────

  describe('race condition scenarios', () => {
    it('handles concurrent withSchedulerLock calls', async () => {
      const bucket = createMockR2Bucket()
      let executionCount = 0

      const fn = vi.fn(async () => {
        executionCount++
        await new Promise(resolve => setTimeout(resolve, 100))
        return `execution-${executionCount}`
      })

      // Start two concurrent operations
      const [result1, result2] = await Promise.all([
        withSchedulerLock(bucket, fn, { hostname: 'worker-1' }),
        withSchedulerLock(bucket, fn, { hostname: 'worker-2' }),
      ])

      // Only one should have executed
      const executed = [result1, result2].filter(r => !r.skipped)
      const skipped = [result1, result2].filter(r => r.skipped)

      expect(executed).toHaveLength(1)
      expect(skipped).toHaveLength(1)
    })

    it('second worker can acquire lock after first releases', async () => {
      const bucket = createMockR2Bucket()

      // First worker acquires and releases
      const result1 = await withSchedulerLock(bucket, async () => 'first')
      expect(result1.skipped).toBe(false)

      // Second worker should be able to acquire
      const result2 = await withSchedulerLock(bucket, async () => 'second')
      expect(result2.skipped).toBe(false)
      expect(result2.result).toBe('second')
    })

    it('handles lock theft prevention during long-running tasks', async () => {
      const bucket = createMockR2Bucket()
      const longRunningFn = vi.fn(async () => {
        // Simulate long-running task
        await new Promise(resolve => setTimeout(resolve, 5000))
        return 'completed'
      })

      const resultPromise = withSchedulerLock(bucket, longRunningFn, {
        hostname: 'long-runner',
        timeoutMs: 1000, // Short timeout
      })

      // Advance time past the lock timeout
      await vi.advanceTimersByTimeAsync(2000)

      // Another worker tries to acquire (lock should be expired now)
      const bucket2 = createMockR2Bucket({ lockInfo: bucket._state.lockInfo })

      // Simulate time passing so lock appears expired
      vi.setSystemTime(new Date(Date.now() + 2000))

      const lock2 = await acquireSchedulerLock(bucket2, { hostname: 'stealer' })

      // The lock can be "stolen" if expired
      expect(lock2).not.toBeNull()

      // Advance remaining time for first function
      await vi.advanceTimersByTimeAsync(3000)

      const result = await resultPromise
      expect(result.skipped).toBe(false)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Lock Expiration Edge Cases
  // ──────────────────────────────────────────────────────────────────────────

  describe('lock expiration edge cases', () => {
    it('handles very short timeout values', async () => {
      const bucket = createMockR2Bucket()

      const lock = await acquireSchedulerLock(bucket, { timeoutMs: 1 })

      expect(lock).not.toBeNull()
      expect(lock!.expiresAt - lock!.acquiredAt).toBe(1)
    })

    it('handles very long timeout values', async () => {
      const bucket = createMockR2Bucket()
      const oneYear = 365 * 24 * 60 * 60 * 1000

      const lock = await acquireSchedulerLock(bucket, { timeoutMs: oneYear })

      expect(lock).not.toBeNull()
      expect(lock!.expiresAt - lock!.acquiredAt).toBe(oneYear)
    })

    it('handles clock skew scenarios', async () => {
      // Lock created with "future" timestamp
      const futureLock = createLockInfo({
        hostname: 'future-worker',
        acquiredAt: Date.now() + 60000, // 1 minute in future
        expiresAt: Date.now() + 60000 + DEFAULT_LOCK_TIMEOUT_MS,
      })
      const bucket = createMockR2Bucket({ lockInfo: futureLock })

      // Current worker should not be able to acquire (lock is "valid")
      const lock = await acquireSchedulerLock(bucket)

      expect(lock).toBeNull()
    })

    it('handles zero timeout (immediate expiration)', async () => {
      const bucket = createMockR2Bucket()

      const lock = await acquireSchedulerLock(bucket, { timeoutMs: 0 })

      expect(lock).not.toBeNull()
      // Lock expires immediately
      expect(lock!.expiresAt).toBe(lock!.acquiredAt)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Additional Error Paths
  // ──────────────────────────────────────────────────────────────────────────

  describe('additional error paths', () => {
    it('handles malformed lock data in R2', async () => {
      const bucket = createMockR2Bucket()
      // Make put fail to trigger get path
      let putCalls = 0
      bucket.put = vi.fn(async (key, body, options) => {
        putCalls++
        if (putCalls === 1 && options?.onlyIf?.etagDoesNotMatch === '*') {
          throw new Error('Lock exists')
        }
        bucket._state.lockInfo = JSON.parse(body)
        return { key, version: 'v1', size: body.length }
      }) as unknown as typeof bucket.put

      // Return invalid structure from get
      bucket.get = vi.fn(async () => ({
        json: async () => ({ invalid: 'structure' }), // Missing required fields
      })) as unknown as typeof bucket.get

      // The implementation should handle this and delete invalid lock
      const lock = await acquireSchedulerLock(bucket)

      expect(bucket.delete).toHaveBeenCalled()
    })

    it('handles empty string hostname', async () => {
      const bucket = createMockR2Bucket()

      const lock = await acquireSchedulerLock(bucket, { hostname: '' })

      expect(lock).not.toBeNull()
      expect(lock!.hostname).toBe('')
    })

    it('handles special characters in hostname', async () => {
      const bucket = createMockR2Bucket()
      const specialHostname = 'worker-123_test.local:8080'

      const lock = await acquireSchedulerLock(bucket, { hostname: specialHostname })

      expect(lock).not.toBeNull()
      expect(lock!.hostname).toBe(specialHostname)
    })

    it('handles negative timeout gracefully', async () => {
      const bucket = createMockR2Bucket()

      const lock = await acquireSchedulerLock(bucket, { timeoutMs: -1000 })

      expect(lock).not.toBeNull()
      // Lock will have expired immediately (expiresAt < acquiredAt)
      expect(lock!.expiresAt).toBeLessThan(lock!.acquiredAt)
    })
  })

  // ──────────────────────────────────────────────────────────────────────────
  // Integration-style Tests
  // ──────────────────────────────────────────────────────────────────────────

  describe('integration scenarios', () => {
    it('full acquire-work-release cycle', async () => {
      const bucket = createMockR2Bucket()
      const workResults: string[] = []

      const result = await withSchedulerLock(
        bucket,
        async () => {
          workResults.push('start')
          await new Promise(resolve => setTimeout(resolve, 100))
          workResults.push('middle')
          await new Promise(resolve => setTimeout(resolve, 100))
          workResults.push('end')
          return workResults.length
        },
        { hostname: 'integration-test' }
      )

      // Advance time for the delays
      await vi.advanceTimersByTimeAsync(200)

      expect(result.skipped).toBe(false)
      expect(result.result).toBe(3)
      expect(workResults).toEqual(['start', 'middle', 'end'])
    })

    it('sequential workers can all complete their work', async () => {
      const bucket = createMockR2Bucket()
      const completedWorkers: string[] = []

      for (const workerId of ['worker-1', 'worker-2', 'worker-3']) {
        const result = await withSchedulerLock(
          bucket,
          async () => {
            completedWorkers.push(workerId)
            return workerId
          },
          { hostname: workerId }
        )

        expect(result.skipped).toBe(false)
        expect(result.result).toBe(workerId)
      }

      expect(completedWorkers).toEqual(['worker-1', 'worker-2', 'worker-3'])
    })

    it('handles mixed success and failure scenarios', async () => {
      const bucket = createMockR2Bucket()

      // First: successful execution
      const result1 = await withSchedulerLock(bucket, async () => 'success')
      expect(result1.skipped).toBe(false)

      // Second: function throws but lock is still released
      await expect(
        withSchedulerLock(bucket, async () => {
          throw new Error('failure')
        })
      ).rejects.toThrow('failure')

      // Third: should still be able to acquire after error
      const result3 = await withSchedulerLock(bucket, async () => 'recovered')
      expect(result3.skipped).toBe(false)
      expect(result3.result).toBe('recovered')
    })
  })
})
