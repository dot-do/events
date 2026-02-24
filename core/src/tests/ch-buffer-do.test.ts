/**
 * Unit tests for ClickHouseBufferDO exports.
 *
 * Pure unit tests — no miniflare needed. Tests exported constants and the
 * toNdjsonRow mapping function.
 */

import { describe, it, expect } from 'vitest'
import {
  FLUSH_INTERVAL_MS,
  SHARD_LIMITS,
  toNdjsonRow,
} from '../ch-buffer-do'

describe('ClickHouseBufferDO constants', () => {
  it('SHARD_LIMITS values: 50MB / 50k / 1000', () => {
    expect(SHARD_LIMITS.bytes).toBe(50 * 1024 * 1024)
    expect(SHARD_LIMITS.count).toBe(50_000)
    expect(SHARD_LIMITS.conns).toBe(1000)
  })

  it('FLUSH_INTERVAL_MS is 50', () => {
    expect(FLUSH_INTERVAL_MS).toBe(50)
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
    // JSON columns are raw objects (not stringified) for ClickHouse JSONEachRow
    expect(row.actor).toEqual({})
    expect(row.data).toEqual({})
    expect(row.meta).toEqual({})
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
      actor: { id: 'user_1' },
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
    expect(row.actor).toEqual({ id: 'user_1' })
    expect(row.data).toEqual({ page: '/home' })
    expect(row.meta).toEqual({ ua: 'Chrome' })
  })

  it('parses pre-serialized data/meta strings back to objects', () => {
    const input = {
      data: '{"preserialised":true}',
      meta: '{"preserialised":true}',
    }

    const row = JSON.parse(toNdjsonRow(input))

    // Pre-serialized strings are parsed back to objects for ClickHouse JSON columns
    expect(row.data).toEqual({ preserialised: true })
    expect(row.meta).toEqual({ preserialised: true })
  })

  it('normalizes string actor to { id } wrapper', () => {
    const row = JSON.parse(toNdjsonRow({ actor: 'user_123' }))
    expect(row.actor).toEqual({ id: 'user_123' })
  })

  it('normalizes empty string actor to empty object', () => {
    const row = JSON.parse(toNdjsonRow({ actor: '' }))
    expect(row.actor).toEqual({})
  })

  it('passes through object actor as raw object', () => {
    const row = JSON.parse(toNdjsonRow({ actor: { id: 'u1', role: 'admin' } }))
    expect(row.actor).toEqual({ id: 'u1', role: 'admin' })
  })

  it('returns valid JSON', () => {
    const result = toNdjsonRow({ type: 'test' })
    expect(() => JSON.parse(result)).not.toThrow()
  })
})
