/**
 * Dedup Cache Tests
 *
 * Tests for the bloom filter, LRU cache, and batch dedup checking utilities.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  BloomFilter,
  LRUCache,
  DedupCache,
  batchCheckDuplicates,
  batchWriteDedupMarkers,
} from '../dedup-cache'

// ============================================================================
// BloomFilter Tests
// ============================================================================

describe('BloomFilter', () => {
  it('returns false for items never added', () => {
    const bloom = new BloomFilter(1000, 0.01)
    expect(bloom.mayContain('never-added')).toBe(false)
    expect(bloom.mayContain('also-never-added')).toBe(false)
  })

  it('returns true for items that were added', () => {
    const bloom = new BloomFilter(1000, 0.01)
    bloom.add('item-1')
    bloom.add('item-2')
    bloom.add('item-3')

    expect(bloom.mayContain('item-1')).toBe(true)
    expect(bloom.mayContain('item-2')).toBe(true)
    expect(bloom.mayContain('item-3')).toBe(true)
  })

  it('has acceptable false positive rate', () => {
    const bloom = new BloomFilter(1000, 0.01)

    // Add 1000 items
    for (let i = 0; i < 1000; i++) {
      bloom.add(`added-${i}`)
    }

    // Check 1000 items that were never added
    let falsePositives = 0
    for (let i = 0; i < 1000; i++) {
      if (bloom.mayContain(`never-added-${i}`)) {
        falsePositives++
      }
    }

    // False positive rate should be around 1% (allow 5% margin)
    expect(falsePositives / 1000).toBeLessThan(0.05)
  })

  it('clears the filter', () => {
    const bloom = new BloomFilter(1000, 0.01)
    bloom.add('item-1')
    bloom.add('item-2')

    expect(bloom.mayContain('item-1')).toBe(true)

    bloom.clear()

    expect(bloom.mayContain('item-1')).toBe(false)
    expect(bloom.mayContain('item-2')).toBe(false)
  })

  it('handles empty strings', () => {
    const bloom = new BloomFilter(1000, 0.01)
    bloom.add('')
    expect(bloom.mayContain('')).toBe(true)
  })

  it('handles long strings', () => {
    const bloom = new BloomFilter(1000, 0.01)
    const longString = 'a'.repeat(10000)
    bloom.add(longString)
    expect(bloom.mayContain(longString)).toBe(true)
  })

  it('handles hash overflow edge case (MIN_INT)', () => {
    // Math.imul can produce 0x80000000 (MIN_INT = -2147483648)
    // Math.abs(-2147483648) returns -2147483648 due to overflow
    // The fix uses (hash >>> 0) for unsigned conversion instead
    const bloom = new BloomFilter(1000, 0.01)

    // Test many strings to increase chance of hitting overflow edge cases
    const testStrings = [
      'overflow-test-1',
      'overflow-test-2',
      '\x00\x00\x00\x00', // null bytes
      '\xff\xff\xff\xff', // high bytes
      String.fromCharCode(0x80, 0x00, 0x00, 0x00),
    ]

    for (const str of testStrings) {
      // Should not throw and should always return valid positions
      bloom.add(str)
      const result = bloom.mayContain(str)
      expect(result).toBe(true)
    }
  })
})

// ============================================================================
// LRUCache Tests
// ============================================================================

describe('LRUCache', () => {
  it('stores and retrieves values', () => {
    const cache = new LRUCache<string>(100)
    cache.set('key1', 'value1')
    cache.set('key2', 'value2')

    expect(cache.get('key1')).toBe('value1')
    expect(cache.get('key2')).toBe('value2')
  })

  it('returns undefined for missing keys', () => {
    const cache = new LRUCache<string>(100)
    expect(cache.get('missing')).toBeUndefined()
  })

  it('evicts oldest item when capacity is exceeded', () => {
    const cache = new LRUCache<string>(3)
    cache.set('key1', 'value1')
    cache.set('key2', 'value2')
    cache.set('key3', 'value3')
    cache.set('key4', 'value4') // This should evict key1

    expect(cache.get('key1')).toBeUndefined()
    expect(cache.get('key2')).toBe('value2')
    expect(cache.get('key3')).toBe('value3')
    expect(cache.get('key4')).toBe('value4')
  })

  it('updates LRU order on get', () => {
    const cache = new LRUCache<string>(3)
    cache.set('key1', 'value1')
    cache.set('key2', 'value2')
    cache.set('key3', 'value3')

    // Access key1 to make it most recently used
    cache.get('key1')

    // Add key4 - should evict key2 (oldest)
    cache.set('key4', 'value4')

    expect(cache.get('key1')).toBe('value1') // Still there (was accessed)
    expect(cache.get('key2')).toBeUndefined() // Evicted
    expect(cache.get('key3')).toBe('value3')
    expect(cache.get('key4')).toBe('value4')
  })

  it('updates LRU order on set of existing key', () => {
    const cache = new LRUCache<string>(3)
    cache.set('key1', 'value1')
    cache.set('key2', 'value2')
    cache.set('key3', 'value3')

    // Update key1 to make it most recently used
    cache.set('key1', 'updated-value1')

    // Add key4 - should evict key2 (oldest)
    cache.set('key4', 'value4')

    expect(cache.get('key1')).toBe('updated-value1')
    expect(cache.get('key2')).toBeUndefined()
  })

  it('reports correct size', () => {
    const cache = new LRUCache<string>(100)
    expect(cache.size).toBe(0)

    cache.set('key1', 'value1')
    expect(cache.size).toBe(1)

    cache.set('key2', 'value2')
    expect(cache.size).toBe(2)
  })

  it('has() returns true for existing keys', () => {
    const cache = new LRUCache<string>(100)
    cache.set('key1', 'value1')

    expect(cache.has('key1')).toBe(true)
    expect(cache.has('missing')).toBe(false)
  })

  it('clear() removes all items', () => {
    const cache = new LRUCache<string>(100)
    cache.set('key1', 'value1')
    cache.set('key2', 'value2')

    cache.clear()

    expect(cache.size).toBe(0)
    expect(cache.get('key1')).toBeUndefined()
    expect(cache.get('key2')).toBeUndefined()
  })
})

// ============================================================================
// DedupCache Tests
// ============================================================================

describe('DedupCache', () => {
  it('marks items as processed and detects duplicates', () => {
    const cache = new DedupCache()

    expect(cache.mightBeDuplicate('event-1')).toBe(false)

    cache.markProcessed('event-1')

    expect(cache.mightBeDuplicate('event-1')).toBe(true)
  })

  it('batch marks items as processed', () => {
    const cache = new DedupCache()

    cache.markProcessedBatch(['event-1', 'event-2', 'event-3'])

    expect(cache.mightBeDuplicate('event-1')).toBe(true)
    expect(cache.mightBeDuplicate('event-2')).toBe(true)
    expect(cache.mightBeDuplicate('event-3')).toBe(true)
    expect(cache.mightBeDuplicate('event-4')).toBe(false)
  })

  it('clears both caches', () => {
    const cache = new DedupCache()

    cache.markProcessedBatch(['event-1', 'event-2'])
    expect(cache.mightBeDuplicate('event-1')).toBe(true)

    cache.clear()

    expect(cache.mightBeDuplicate('event-1')).toBe(false)
  })

  it('provides stats', () => {
    const cache = new DedupCache()

    const stats = cache.stats
    expect(stats.lruSize).toBe(0)
    expect(typeof stats.lastClear).toBe('number')

    cache.markProcessed('event-1')

    expect(cache.stats.lruSize).toBe(1)
  })
})

// ============================================================================
// batchCheckDuplicates Tests
// ============================================================================

describe('batchCheckDuplicates', () => {
  interface MockR2Object {
    key: string
  }

  interface MockR2ListResult {
    objects: MockR2Object[]
    truncated: boolean
    cursor?: string
  }

  function createMockBucket(existingKeys: string[] = []) {
    const keySet = new Set(existingKeys)

    return {
      head: vi.fn(async (key: string) => {
        return keySet.has(key) ? { key } : null
      }),
      list: vi.fn(async ({ prefix }: { prefix: string; limit?: number; cursor?: string }): Promise<MockR2ListResult> => {
        const matchingObjects = existingKeys
          .filter(k => k.startsWith(prefix))
          .map(key => ({ key }))
        return {
          objects: matchingObjects,
          truncated: false,
        }
      }),
      put: vi.fn(async () => {}),
    } as unknown as R2Bucket
  }

  it('returns empty sets for empty input', async () => {
    const bucket = createMockBucket()
    const result = await batchCheckDuplicates(bucket, 'dedup/', [])

    expect(result.duplicates.size).toBe(0)
    expect(result.newEvents.size).toBe(0)
    expect(result.r2Calls).toBe(0)
  })

  it('identifies new events when none exist in R2', async () => {
    const bucket = createMockBucket([])
    const result = await batchCheckDuplicates(bucket, 'dedup/', ['event-1', 'event-2', 'event-3'])

    expect(result.duplicates.size).toBe(0)
    expect(result.newEvents.size).toBe(3)
    expect(result.newEvents.has('event-1')).toBe(true)
    expect(result.newEvents.has('event-2')).toBe(true)
    expect(result.newEvents.has('event-3')).toBe(true)
  })

  it('identifies duplicates from R2', async () => {
    const bucket = createMockBucket(['dedup/event-1', 'dedup/event-2'])
    const result = await batchCheckDuplicates(bucket, 'dedup/', ['event-1', 'event-2', 'event-3'])

    expect(result.duplicates.size).toBe(2)
    expect(result.duplicates.has('event-1')).toBe(true)
    expect(result.duplicates.has('event-2')).toBe(true)
    expect(result.newEvents.size).toBe(1)
    expect(result.newEvents.has('event-3')).toBe(true)
  })

  it('uses cache for LRU hits', async () => {
    const bucket = createMockBucket([])
    const cache = new DedupCache()

    // Pre-populate cache
    cache.markProcessed('event-1')

    const result = await batchCheckDuplicates(bucket, 'dedup/', ['event-1', 'event-2'], cache)

    expect(result.duplicates.has('event-1')).toBe(true)
    expect(result.cacheHits).toBe(1)
    expect(result.newEvents.has('event-2')).toBe(true)
  })

  it('uses R2 list for small batches (efficient)', async () => {
    const bucket = createMockBucket([])
    const result = await batchCheckDuplicates(bucket, 'dedup/', ['event-1', 'event-2'])

    // Should use list, not individual HEAD requests
    expect((bucket.list as ReturnType<typeof vi.fn>).mock.calls.length).toBeGreaterThanOrEqual(1)
  })
})

// ============================================================================
// batchWriteDedupMarkers Tests
// ============================================================================

describe('batchWriteDedupMarkers', () => {
  function createMockBucket() {
    return {
      put: vi.fn(async () => {}),
    } as unknown as R2Bucket
  }

  it('returns zeros for empty input', async () => {
    const bucket = createMockBucket()
    const result = await batchWriteDedupMarkers(bucket, 'dedup/', [], {})

    expect(result.written).toBe(0)
    expect(result.r2Calls).toBe(0)
  })

  it('writes markers for all event IDs', async () => {
    const bucket = createMockBucket()
    const result = await batchWriteDedupMarkers(
      bucket,
      'dedup/',
      ['event-1', 'event-2', 'event-3'],
      { processedAt: '2024-01-01T00:00:00Z' }
    )

    expect(result.written).toBe(3)
    expect(result.r2Calls).toBe(3)

    const putMock = bucket.put as ReturnType<typeof vi.fn>
    expect(putMock).toHaveBeenCalledWith('dedup/event-1', '', expect.any(Object))
    expect(putMock).toHaveBeenCalledWith('dedup/event-2', '', expect.any(Object))
    expect(putMock).toHaveBeenCalledWith('dedup/event-3', '', expect.any(Object))
  })

  it('updates cache when provided', async () => {
    const bucket = createMockBucket()
    const cache = new DedupCache()

    await batchWriteDedupMarkers(
      bucket,
      'dedup/',
      ['event-1', 'event-2'],
      { processedAt: '2024-01-01T00:00:00Z' },
      cache
    )

    expect(cache.mightBeDuplicate('event-1')).toBe(true)
    expect(cache.mightBeDuplicate('event-2')).toBe(true)
  })
})
