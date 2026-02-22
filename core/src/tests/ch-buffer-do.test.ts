/**
 * Unit tests for ClickHouseBufferDO exports.
 *
 * Pure unit tests — no miniflare needed. Tests exported constants and the
 * toNdjsonRow mapping function.
 */

import { describe, it, expect } from 'vitest'
import {
  FLUSH_INTERVAL_MS,
  FIRST_SHARD_LIMITS,
  DEFAULT_LIMITS,
  toNdjsonRow,
} from '../ch-buffer-do'

describe('ClickHouseBufferDO constants', () => {
  it('FIRST_SHARD_LIMITS has lower thresholds than DEFAULT_LIMITS', () => {
    expect(FIRST_SHARD_LIMITS.bytes).toBeLessThan(DEFAULT_LIMITS.bytes)
    expect(FIRST_SHARD_LIMITS.count).toBeLessThan(DEFAULT_LIMITS.count)
    expect(FIRST_SHARD_LIMITS.conns).toBeLessThan(DEFAULT_LIMITS.conns)
  })

  it('FIRST_SHARD_LIMITS values: 10MB / 10k / 200', () => {
    expect(FIRST_SHARD_LIMITS.bytes).toBe(10 * 1024 * 1024)
    expect(FIRST_SHARD_LIMITS.count).toBe(10_000)
    expect(FIRST_SHARD_LIMITS.conns).toBe(200)
  })

  it('DEFAULT_LIMITS values: 50MB / 50k / 1000', () => {
    expect(DEFAULT_LIMITS.bytes).toBe(50 * 1024 * 1024)
    expect(DEFAULT_LIMITS.count).toBe(50_000)
    expect(DEFAULT_LIMITS.conns).toBe(1000)
  })

  it('FLUSH_INTERVAL_MS is 100', () => {
    expect(FLUSH_INTERVAL_MS).toBe(100)
  })
})

describe('toNdjsonRow', () => {
  it('maps fields with defaults for missing values', () => {
    const row = JSON.parse(toNdjsonRow({}))

    // id should be a UUID
    expect(row.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
    // ts should be an ISO string
    expect(new Date(row.ts).toISOString()).toBe(row.ts)
    // string defaults
    expect(row.ns).toBe('')
    expect(row.type).toBe('')
    expect(row.event).toBe('')
    expect(row.url).toBe('')
    expect(row.source).toBe('')
    expect(row.actor).toBe('')
    // data and meta default to JSON-stringified empty objects
    expect(row.data).toBe('{}')
    expect(row.meta).toBe('{}')
  })

  it('preserves provided field values', () => {
    const input = {
      id: 'evt_123',
      ns: 'acme',
      ts: '2026-01-01T00:00:00.000Z',
      type: 'track',
      event: 'page_view',
      url: 'https://example.com',
      source: 'web',
      actor: 'user_1',
      data: { page: '/home' },
      meta: { ua: 'Chrome' },
    }

    const row = JSON.parse(toNdjsonRow(input))

    expect(row.id).toBe('evt_123')
    expect(row.ns).toBe('acme')
    expect(row.ts).toBe('2026-01-01T00:00:00.000Z')
    expect(row.type).toBe('track')
    expect(row.event).toBe('page_view')
    expect(row.url).toBe('https://example.com')
    expect(row.source).toBe('web')
    expect(row.actor).toBe('user_1')
    expect(row.data).toBe('{"page":"/home"}')
    expect(row.meta).toBe('{"ua":"Chrome"}')
  })

  it('passes through data/meta when already strings', () => {
    const input = {
      data: '{"preserialised":true}',
      meta: '{"preserialised":true}',
    }

    const row = JSON.parse(toNdjsonRow(input))

    expect(row.data).toBe('{"preserialised":true}')
    expect(row.meta).toBe('{"preserialised":true}')
  })

  it('returns valid JSON', () => {
    const result = toNdjsonRow({ type: 'test' })
    expect(() => JSON.parse(result)).not.toThrow()
  })
})
