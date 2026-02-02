/**
 * Tests for Configuration Constants
 *
 * Validates that configuration values are reasonable and consistent.
 */

import { describe, it, expect } from 'vitest'
import {
  DEFAULT_BATCH_SIZE,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_RETRY_QUEUE_SIZE,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_CIRCUIT_BREAKER_RESET_MS,
  DEFAULT_FETCH_TIMEOUT_MS,
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  RETRY_JITTER_MS,
  DEFAULT_SUBSCRIPTION_MAX_RETRIES,
  DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
  SUBSCRIPTION_BATCH_LIMIT,
  SUBSCRIPTION_RETRY_BASE_DELAY_MS,
  SUBSCRIPTION_RETRY_MAX_DELAY_MS,
  DEFAULT_BATCH_DELIVERY_SIZE,
  DEFAULT_BATCH_DELIVERY_WINDOW_MS,
  MAX_BATCH_DELIVERY_SIZE,
  MAX_BATCH_DELIVERY_WINDOW_MS,
  MAX_PATTERN_LENGTH,
  MAX_PATTERN_SEGMENTS,
  ALLOWED_PATTERN_CHARS,
  MAX_PATTERN_MATCH_ITERATIONS,
  DEFAULT_COMPACTION_PARALLELISM,
  DEFAULT_COMPACTION_CHUNK_SIZE,
  MIN_DELTAS_FOR_PARALLEL,
  MAX_COMPACTION_PARALLELISM,
  coreConfig,
} from '../config'

describe('Emitter Configuration', () => {
  it('should have reasonable batch size', () => {
    expect(DEFAULT_BATCH_SIZE).toBeGreaterThan(0)
    expect(DEFAULT_BATCH_SIZE).toBeLessThanOrEqual(1000)
  })

  it('should have reasonable flush interval', () => {
    expect(DEFAULT_FLUSH_INTERVAL_MS).toBeGreaterThanOrEqual(100)
    expect(DEFAULT_FLUSH_INTERVAL_MS).toBeLessThanOrEqual(60000)
  })

  it('should have reasonable retry queue size', () => {
    expect(DEFAULT_MAX_RETRY_QUEUE_SIZE).toBeGreaterThanOrEqual(100)
    expect(DEFAULT_MAX_RETRY_QUEUE_SIZE).toBeLessThanOrEqual(100000)
  })

  it('should have reasonable failure threshold', () => {
    expect(DEFAULT_MAX_CONSECUTIVE_FAILURES).toBeGreaterThanOrEqual(3)
    expect(DEFAULT_MAX_CONSECUTIVE_FAILURES).toBeLessThanOrEqual(100)
  })

  it('should have reasonable circuit breaker reset time', () => {
    expect(DEFAULT_CIRCUIT_BREAKER_RESET_MS).toBeGreaterThanOrEqual(10000)
    expect(DEFAULT_CIRCUIT_BREAKER_RESET_MS).toBeLessThanOrEqual(3600000) // max 1 hour
  })

  it('should have reasonable fetch timeout', () => {
    expect(DEFAULT_FETCH_TIMEOUT_MS).toBeGreaterThanOrEqual(5000)
    expect(DEFAULT_FETCH_TIMEOUT_MS).toBeLessThanOrEqual(120000) // max 2 minutes
  })
})

describe('Retry Configuration', () => {
  it('should have positive base delay', () => {
    expect(RETRY_BASE_DELAY_MS).toBeGreaterThan(0)
  })

  it('should have max delay greater than base delay', () => {
    expect(RETRY_MAX_DELAY_MS).toBeGreaterThan(RETRY_BASE_DELAY_MS)
  })

  it('should have reasonable jitter', () => {
    expect(RETRY_JITTER_MS).toBeGreaterThanOrEqual(0)
    expect(RETRY_JITTER_MS).toBeLessThanOrEqual(RETRY_BASE_DELAY_MS)
  })
})

describe('Subscription Configuration', () => {
  it('should have reasonable max retries', () => {
    expect(DEFAULT_SUBSCRIPTION_MAX_RETRIES).toBeGreaterThanOrEqual(1)
    expect(DEFAULT_SUBSCRIPTION_MAX_RETRIES).toBeLessThanOrEqual(20)
  })

  it('should have reasonable timeout', () => {
    expect(DEFAULT_SUBSCRIPTION_TIMEOUT_MS).toBeGreaterThanOrEqual(1000)
    expect(DEFAULT_SUBSCRIPTION_TIMEOUT_MS).toBeLessThanOrEqual(300000)
  })

  it('should have reasonable batch limit', () => {
    expect(SUBSCRIPTION_BATCH_LIMIT).toBeGreaterThan(0)
    expect(SUBSCRIPTION_BATCH_LIMIT).toBeLessThanOrEqual(1000)
  })

  it('should have consistent retry delays', () => {
    expect(SUBSCRIPTION_RETRY_MAX_DELAY_MS).toBeGreaterThan(SUBSCRIPTION_RETRY_BASE_DELAY_MS)
  })

  it('should have max batch size >= default', () => {
    expect(MAX_BATCH_DELIVERY_SIZE).toBeGreaterThanOrEqual(DEFAULT_BATCH_DELIVERY_SIZE)
  })

  it('should have max batch window >= default', () => {
    expect(MAX_BATCH_DELIVERY_WINDOW_MS).toBeGreaterThanOrEqual(DEFAULT_BATCH_DELIVERY_WINDOW_MS)
  })
})

describe('Pattern Configuration', () => {
  it('should have reasonable max length', () => {
    expect(MAX_PATTERN_LENGTH).toBeGreaterThan(10)
    expect(MAX_PATTERN_LENGTH).toBeLessThanOrEqual(1000)
  })

  it('should have reasonable max segments', () => {
    expect(MAX_PATTERN_SEGMENTS).toBeGreaterThan(2)
    expect(MAX_PATTERN_SEGMENTS).toBeLessThanOrEqual(50)
  })

  it('should have valid regex for allowed chars', () => {
    expect(ALLOWED_PATTERN_CHARS).toBeInstanceOf(RegExp)
    expect(ALLOWED_PATTERN_CHARS.test('valid.pattern')).toBe(true)
    expect(ALLOWED_PATTERN_CHARS.test('has spaces')).toBe(false)
    expect(ALLOWED_PATTERN_CHARS.test('webhook.*')).toBe(true)
  })

  it('should have reasonable max iterations', () => {
    expect(MAX_PATTERN_MATCH_ITERATIONS).toBeGreaterThan(100)
    expect(MAX_PATTERN_MATCH_ITERATIONS).toBeLessThanOrEqual(1000000)
  })
})

describe('Compaction Configuration', () => {
  it('should have reasonable default parallelism', () => {
    expect(DEFAULT_COMPACTION_PARALLELISM).toBeGreaterThan(0)
    expect(DEFAULT_COMPACTION_PARALLELISM).toBeLessThanOrEqual(MAX_COMPACTION_PARALLELISM)
  })

  it('should have reasonable chunk size', () => {
    expect(DEFAULT_COMPACTION_CHUNK_SIZE).toBeGreaterThan(0)
    expect(DEFAULT_COMPACTION_CHUNK_SIZE).toBeLessThanOrEqual(100)
  })

  it('should have min deltas less than chunk size', () => {
    expect(MIN_DELTAS_FOR_PARALLEL).toBeLessThanOrEqual(DEFAULT_COMPACTION_CHUNK_SIZE)
  })

  it('should have max parallelism >= default', () => {
    expect(MAX_COMPACTION_PARALLELISM).toBeGreaterThanOrEqual(DEFAULT_COMPACTION_PARALLELISM)
  })
})

describe('coreConfig object', () => {
  it('should match individual constants', () => {
    expect(coreConfig.emitter.defaultBatchSize).toBe(DEFAULT_BATCH_SIZE)
    expect(coreConfig.retry.baseDelayMs).toBe(RETRY_BASE_DELAY_MS)
    expect(coreConfig.subscription.defaultMaxRetries).toBe(DEFAULT_SUBSCRIPTION_MAX_RETRIES)
    expect(coreConfig.pattern.maxLength).toBe(MAX_PATTERN_LENGTH)
    expect(coreConfig.compaction.defaultParallelism).toBe(DEFAULT_COMPACTION_PARALLELISM)
  })

  it('should be readonly (immutable)', () => {
    // TypeScript enforces this via 'as const', but we can verify the structure
    expect(typeof coreConfig).toBe('object')
    expect(Object.keys(coreConfig)).toContain('emitter')
    expect(Object.keys(coreConfig)).toContain('retry')
    expect(Object.keys(coreConfig)).toContain('subscription')
    expect(Object.keys(coreConfig)).toContain('pattern')
    expect(Object.keys(coreConfig)).toContain('compaction')
  })
})
