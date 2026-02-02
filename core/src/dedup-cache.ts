/**
 * Deduplication cache for queue processing
 *
 * Optimizes dedup checks by:
 * 1. Using an in-memory bloom filter for recent events
 * 2. Batching R2 list operations instead of individual HEAD requests
 * 3. Caching recently seen event IDs
 *
 * This reduces R2 API calls while maintaining idempotency guarantees.
 */

/**
 * Simple bloom filter for probabilistic set membership testing.
 * False positives are acceptable (extra R2 check), false negatives are not allowed.
 */
export class BloomFilter {
  private bits: Uint8Array
  private readonly size: number
  private readonly hashCount: number

  constructor(expectedItems: number = 10000, falsePositiveRate: number = 0.01) {
    // Calculate optimal size and hash count
    // size = -n * ln(p) / (ln(2)^2)
    this.size = Math.ceil(-expectedItems * Math.log(falsePositiveRate) / (Math.LN2 * Math.LN2))
    // hashCount = (m/n) * ln(2)
    this.hashCount = Math.ceil((this.size / expectedItems) * Math.LN2)
    this.bits = new Uint8Array(Math.ceil(this.size / 8))
  }

  private hash(str: string, seed: number): number {
    // Simple FNV-1a hash with seed
    let hash = 2166136261 ^ seed
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i)
      hash = Math.imul(hash, 16777619)
    }
    return Math.abs(hash) % this.size
  }

  add(item: string): void {
    for (let i = 0; i < this.hashCount; i++) {
      const pos = this.hash(item, i)
      const byteIndex = Math.floor(pos / 8)
      const bitMask = 1 << (pos % 8)
      this.bits[byteIndex] = (this.bits[byteIndex] ?? 0) | bitMask
    }
  }

  mayContain(item: string): boolean {
    for (let i = 0; i < this.hashCount; i++) {
      const pos = this.hash(item, i)
      const byteIndex = Math.floor(pos / 8)
      const bitMask = 1 << (pos % 8)
      if (((this.bits[byteIndex] ?? 0) & bitMask) === 0) {
        return false
      }
    }
    return true
  }

  clear(): void {
    this.bits.fill(0)
  }
}

/**
 * LRU cache for recently processed event IDs.
 * Provides definite "yes" answers (unlike bloom filter's "maybe yes").
 */
export class LRUCache<T> {
  private cache: Map<string, T>
  private readonly maxSize: number

  constructor(maxSize: number = 5000) {
    this.cache = new Map()
    this.maxSize = maxSize
  }

  get(key: string): T | undefined {
    const value = this.cache.get(key)
    if (value !== undefined) {
      // Move to end (most recently used)
      this.cache.delete(key)
      this.cache.set(key, value)
    }
    return value
  }

  set(key: string, value: T): void {
    // Delete first to update order if exists
    this.cache.delete(key)
    this.cache.set(key, value)

    // Evict oldest if over capacity
    if (this.cache.size > this.maxSize) {
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      }
    }
  }

  has(key: string): boolean {
    return this.cache.has(key)
  }

  clear(): void {
    this.cache.clear()
  }

  get size(): number {
    return this.cache.size
  }
}

/**
 * Result of a batch dedup check
 */
export interface DedupCheckResult {
  /** Event IDs that have already been processed (should be skipped) */
  duplicates: Set<string>
  /** Event IDs that are new (should be processed) */
  newEvents: Set<string>
  /** Number of R2 API calls made */
  r2Calls: number
  /** Number of cache hits (bloom + LRU) */
  cacheHits: number
}

/**
 * Dedup cache manager for queue processing.
 * Uses a two-level cache: LRU for definite hits, bloom filter for probabilistic filtering.
 */
export class DedupCache {
  private readonly bloom: BloomFilter
  private readonly lru: LRUCache<boolean>
  private lastClear: number = Date.now()
  private readonly clearIntervalMs: number

  constructor(options: {
    expectedItems?: number
    falsePositiveRate?: number
    lruMaxSize?: number
    clearIntervalMs?: number
  } = {}) {
    this.bloom = new BloomFilter(
      options.expectedItems ?? 10000,
      options.falsePositiveRate ?? 0.01
    )
    this.lru = new LRUCache(options.lruMaxSize ?? 5000)
    // Clear bloom filter periodically to prevent fill-up (default: 5 minutes)
    this.clearIntervalMs = options.clearIntervalMs ?? 5 * 60 * 1000
  }

  /**
   * Check if we should skip processing an event (likely duplicate).
   * Returns true if the event was definitely or probably already processed.
   */
  mightBeDuplicate(eventId: string): boolean {
    // Check LRU first (definite answer)
    if (this.lru.has(eventId)) {
      return true
    }
    // Check bloom filter (probabilistic - may have false positives)
    return this.bloom.mayContain(eventId)
  }

  /**
   * Mark an event as processed.
   */
  markProcessed(eventId: string): void {
    this.bloom.add(eventId)
    this.lru.set(eventId, true)
    this.maybeClearBloom()
  }

  /**
   * Mark multiple events as processed.
   */
  markProcessedBatch(eventIds: string[]): void {
    for (const id of eventIds) {
      this.bloom.add(id)
      this.lru.set(id, true)
    }
    this.maybeClearBloom()
  }

  /**
   * Periodically clear the bloom filter to prevent fill-up.
   * LRU cache persists for recency-based dedup.
   */
  private maybeClearBloom(): void {
    const now = Date.now()
    if (now - this.lastClear > this.clearIntervalMs) {
      this.bloom.clear()
      this.lastClear = now
    }
  }

  /**
   * Clear both caches (for testing or reset scenarios).
   */
  clear(): void {
    this.bloom.clear()
    this.lru.clear()
    this.lastClear = Date.now()
  }

  get stats(): { lruSize: number; lastClear: number } {
    return {
      lruSize: this.lru.size,
      lastClear: this.lastClear,
    }
  }
}

/**
 * Batch check for duplicates using R2 list operations.
 * Much more efficient than N individual HEAD requests.
 *
 * @param bucket - R2 bucket to check
 * @param prefix - Dedup key prefix (e.g., 'dedup/cdc/' or 'dedup/sub/')
 * @param eventIds - Event IDs to check
 * @param cache - Optional dedup cache for memory-based filtering
 * @returns Result containing duplicate and new event sets
 */
export async function batchCheckDuplicates(
  bucket: R2Bucket,
  prefix: string,
  eventIds: string[],
  cache?: DedupCache
): Promise<DedupCheckResult> {
  const duplicates = new Set<string>()
  const newEvents = new Set<string>()
  let r2Calls = 0
  let cacheHits = 0

  if (eventIds.length === 0) {
    return { duplicates, newEvents, r2Calls, cacheHits }
  }

  // First pass: check in-memory cache
  const needsR2Check: string[] = []
  for (const eventId of eventIds) {
    if (cache?.mightBeDuplicate(eventId)) {
      // Might be duplicate - still need to verify with R2 unless LRU hit
      if (cache['lru'].has(eventId)) {
        duplicates.add(eventId)
        cacheHits++
      } else {
        // Bloom filter hit - need to verify
        needsR2Check.push(eventId)
      }
    } else {
      // Definitely not in cache - might still be in R2
      needsR2Check.push(eventId)
    }
  }

  if (needsR2Check.length === 0) {
    return { duplicates, newEvents, r2Calls, cacheHits }
  }

  // Use R2 list to batch-check multiple keys at once
  // Group by common prefix portions to minimize list calls
  const existingKeys = new Set<string>()

  // For small batches, we can list all keys with the prefix at once
  // R2 list returns up to 1000 objects per call
  if (needsR2Check.length <= 100) {
    // Build a set of expected keys
    const expectedKeys = new Set(needsR2Check.map(id => `${prefix}${id}`))

    // List all objects with the prefix
    // This is O(1) API call vs O(n) HEAD requests
    const listed = await bucket.list({ prefix, limit: 1000 })
    r2Calls++

    for (const obj of listed.objects) {
      existingKeys.add(obj.key)
    }

    // Handle truncated results
    let cursor = listed.truncated ? listed.cursor : undefined
    while (cursor) {
      const more = await bucket.list({ prefix, limit: 1000, cursor })
      r2Calls++
      for (const obj of more.objects) {
        existingKeys.add(obj.key)
      }
      cursor = more.truncated ? more.cursor : undefined
    }

    // Check which of our event IDs exist
    for (const eventId of needsR2Check) {
      const key = `${prefix}${eventId}`
      if (existingKeys.has(key)) {
        duplicates.add(eventId)
        cache?.markProcessed(eventId)
      } else {
        newEvents.add(eventId)
      }
    }
  } else {
    // For larger batches, use parallel HEAD requests with concurrency limit
    // This is a fallback for very large batches where listing might be expensive
    const concurrency = 50
    const results = await parallelMap(
      needsR2Check,
      async (eventId) => {
        const key = `${prefix}${eventId}`
        const exists = await bucket.head(key)
        return { eventId, exists: exists !== null }
      },
      concurrency
    )
    r2Calls += needsR2Check.length

    for (const { eventId, exists } of results) {
      if (exists) {
        duplicates.add(eventId)
        cache?.markProcessed(eventId)
      } else {
        newEvents.add(eventId)
      }
    }
  }

  return { duplicates, newEvents, r2Calls, cacheHits }
}

/**
 * Write dedup markers in batch.
 * Uses parallel puts with concurrency limit.
 */
export async function batchWriteDedupMarkers(
  bucket: R2Bucket,
  prefix: string,
  eventIds: string[],
  metadata: { processedAt?: string; deliveredAt?: string },
  cache?: DedupCache
): Promise<{ written: number; r2Calls: number }> {
  if (eventIds.length === 0) {
    return { written: 0, r2Calls: 0 }
  }

  const now = new Date().toISOString()
  const customMetadata: Record<string, string> = metadata.processedAt
    ? { processedAt: metadata.processedAt }
    : { deliveredAt: metadata.deliveredAt ?? now }

  // Write in parallel with concurrency limit
  const concurrency = 50
  await parallelMap(
    eventIds,
    async (eventId) => {
      const key = `${prefix}${eventId}`
      await bucket.put(key, '', { customMetadata })
    },
    concurrency
  )

  // Update cache
  if (cache) {
    cache.markProcessedBatch(eventIds)
  }

  return { written: eventIds.length, r2Calls: eventIds.length }
}

/**
 * Execute async operations in parallel with a concurrency limit.
 */
async function parallelMap<T, R>(
  items: T[],
  fn: (item: T) => Promise<R>,
  concurrency: number
): Promise<R[]> {
  const results: R[] = []
  const executing: Promise<void>[] = []

  for (const item of items) {
    const p = fn(item).then((r) => {
      results.push(r)
    })

    executing.push(p as unknown as Promise<void>)

    if (executing.length >= concurrency) {
      await Promise.race(executing)
      // Remove completed promises
      for (let i = executing.length - 1; i >= 0; i--) {
        const promise = executing[i]
        if (promise) {
          // Check if promise is settled by racing with a resolved promise
          const settled = await Promise.race([
            promise.then(() => true).catch(() => true),
            Promise.resolve(false),
          ])
          if (settled) {
            executing.splice(i, 1)
          }
        }
      }
    }
  }

  await Promise.all(executing)
  return results
}

// Global dedup cache instance (persists across queue batch invocations within same isolate)
let globalCdcCache: DedupCache | undefined
let globalSubCache: DedupCache | undefined

/**
 * Get or create the global CDC dedup cache.
 */
export function getCdcDedupCache(): DedupCache {
  if (!globalCdcCache) {
    globalCdcCache = new DedupCache({
      expectedItems: 10000,
      falsePositiveRate: 0.01,
      lruMaxSize: 5000,
      clearIntervalMs: 5 * 60 * 1000,
    })
  }
  return globalCdcCache
}

/**
 * Get or create the global subscription dedup cache.
 */
export function getSubDedupCache(): DedupCache {
  if (!globalSubCache) {
    globalSubCache = new DedupCache({
      expectedItems: 10000,
      falsePositiveRate: 0.01,
      lruMaxSize: 5000,
      clearIntervalMs: 5 * 60 * 1000,
    })
  }
  return globalSubCache
}
