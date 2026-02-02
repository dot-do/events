/**
 * Quota System Tests
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { isQuotaExceeded, wouldExceedQuota, getCurrentDateString, createDefaultUsage, DEFAULT_QUOTA } from '../quota'

describe('isQuotaExceeded', () => {
  it('returns false when limit is 0 (unlimited)', () => expect(isQuotaExceeded(999999, 0)).toBe(false))
  it('returns false when current is below limit', () => expect(isQuotaExceeded(5, 10)).toBe(false))
  it('returns true when current equals limit', () => expect(isQuotaExceeded(10, 10)).toBe(true))
  it('returns true when current exceeds limit', () => expect(isQuotaExceeded(15, 10)).toBe(true))
})

describe('wouldExceedQuota', () => {
  it('returns false when limit is 0 (unlimited)', () => expect(wouldExceedQuota(100, 50, 0)).toBe(false))
  it('returns false when adding stays within limit', () => expect(wouldExceedQuota(5, 3, 10)).toBe(false))
  it('returns false when adding exactly reaches limit', () => expect(wouldExceedQuota(5, 5, 10)).toBe(false))
  it('returns true when adding would exceed limit', () => expect(wouldExceedQuota(5, 6, 10)).toBe(true))
  it('handles boundary at limit', () => {
    expect(wouldExceedQuota(10, 0, 10)).toBe(false)
    expect(wouldExceedQuota(10, 1, 10)).toBe(true)
  })
})

describe('getCurrentDateString', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())
  it('returns YYYY-MM-DD format', () => {
    vi.setSystemTime(new Date('2024-06-15T12:00:00Z'))
    expect(getCurrentDateString()).toBe('2024-06-15')
  })
  it('uses UTC timezone', () => {
    vi.setSystemTime(new Date('2024-12-31T23:59:59Z'))
    expect(getCurrentDateString()).toBe('2024-12-31')
  })
  it('handles new year boundary in UTC', () => {
    vi.setSystemTime(new Date('2025-01-01T00:00:01Z'))
    expect(getCurrentDateString()).toBe('2025-01-01')
  })
})

describe('createDefaultUsage', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())
  it('returns zero counts', () => {
    vi.setSystemTime(new Date('2024-06-15T12:00:00Z'))
    const usage = createDefaultUsage()
    expect(usage.eventsToday).toBe(0)
    expect(usage.storageBytes).toBe(0)
    expect(usage.subscriptions).toBe(0)
    expect(usage.schemas).toBe(0)
  })
  it('sets current date for eventsDate', () => {
    vi.setSystemTime(new Date('2024-06-15T12:00:00Z'))
    expect(createDefaultUsage().eventsDate).toBe('2024-06-15')
  })
})

describe('DEFAULT_QUOTA', () => {
  it('has all limits set to unlimited (0)', () => {
    expect(DEFAULT_QUOTA.maxEventsPerDay).toBe(0)
    expect(DEFAULT_QUOTA.maxStorageBytes).toBe(0)
    expect(DEFAULT_QUOTA.maxSubscriptions).toBe(0)
    expect(DEFAULT_QUOTA.maxSchemas).toBe(0)
  })
})
