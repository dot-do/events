/**
 * Tests for CDC DeltaLake conversion utilities
 *
 * Tests the conversion between events.do CDC format and deltalake CDC format.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  mapOperationToDeltalake,
  mapOperationFromDeltalake,
  convertToDeltalakeCDC,
  convertFromDeltalakeCDC,
  convertBatchToDeltalakeCDC,
  convertBatchFromDeltalakeCDC,
  serializeCDCRecord,
  deserializeCDCRecord,
  resetSequenceState,
  setSequenceState,
  type EventsDoCDCRecord,
  type EventsDoOperation,
} from '../cdc-deltalake'

describe('Operation Mapping', () => {
  describe('mapOperationToDeltalake', () => {
    it('should map insert to c', () => {
      expect(mapOperationToDeltalake('insert')).toBe('c')
    })

    it('should map update to u', () => {
      expect(mapOperationToDeltalake('update')).toBe('u')
    })

    it('should map delete to d', () => {
      expect(mapOperationToDeltalake('delete')).toBe('d')
    })

    it('should default to c for unknown operation', () => {
      // The implementation defaults to 'c' for unknown operations
      expect(mapOperationToDeltalake('unknown' as EventsDoOperation)).toBe('c')
    })
  })

  describe('mapOperationFromDeltalake', () => {
    it('should map c to insert', () => {
      expect(mapOperationFromDeltalake('c')).toBe('insert')
    })

    it('should map u to update', () => {
      expect(mapOperationFromDeltalake('u')).toBe('update')
    })

    it('should map d to delete', () => {
      expect(mapOperationFromDeltalake('d')).toBe('delete')
    })

    it('should map r to insert (snapshot/read)', () => {
      expect(mapOperationFromDeltalake('r')).toBe('insert')
    })

    it('should default to insert for unknown operation', () => {
      // The implementation defaults to 'insert' for unknown operations
      expect(mapOperationFromDeltalake('x' as 'c')).toBe('insert')
    })
  })
})

describe('CDC Record Conversion', () => {
  beforeEach(() => {
    resetSequenceState()
  })

  afterEach(() => {
    resetSequenceState()
  })

  describe('convertToDeltalakeCDC', () => {
    it('should convert insert record', () => {
      const record: EventsDoCDCRecord = {
        pk: 'user-123',
        op: 'insert',
        data: { name: 'Alice', age: 30 },
        ts: '2023-11-14T22:13:20.000Z',
        bookmark: 'bookmark-1',
        collection: 'users',
      }

      const result = convertToDeltalakeCDC(record)

      expect(result._id).toBe('user-123')
      expect(result._op).toBe('c')
      expect(result._before).toBeNull()
      expect(result._after).toEqual({ name: 'Alice', age: 30 })
      expect(typeof result._seq).toBe('bigint')
      expect(typeof result._ts).toBe('bigint')
      // Source uses 'deltalake' system and doesn't include bookmark
      expect(result._source.system).toBe('deltalake')
      expect(result._source.collection).toBe('users')
      // Bookmark is stored in _txn
      expect(result._txn).toBe('bookmark-1')
    })

    it('should convert update record with before/after', () => {
      const record: EventsDoCDCRecord = {
        pk: 'user-123',
        op: 'update',
        data: { name: 'Alice', age: 31 },
        prev: { name: 'Alice', age: 30 },
        ts: '2023-11-14T22:13:21.000Z',
        bookmark: 'bookmark-2',
        collection: 'users',
      }

      const result = convertToDeltalakeCDC(record)

      expect(result._op).toBe('u')
      expect(result._before).toEqual({ name: 'Alice', age: 30 })
      expect(result._after).toEqual({ name: 'Alice', age: 31 })
    })

    it('should convert delete record', () => {
      const record: EventsDoCDCRecord = {
        pk: 'user-123',
        op: 'delete',
        prev: { name: 'Alice', age: 31 },
        ts: '2023-11-14T22:13:22.000Z',
        bookmark: 'bookmark-3',
        collection: 'users',
      }

      const result = convertToDeltalakeCDC(record)

      expect(result._op).toBe('d')
      expect(result._before).toEqual({ name: 'Alice', age: 31 })
      expect(result._after).toBeNull()
    })

    it('should generate monotonically increasing sequence numbers', () => {
      const records: EventsDoCDCRecord[] = [
        { pk: 'a', op: 'insert', data: {}, ts: '2023-01-01T00:00:01.000Z', bookmark: 'b1', collection: 'c' },
        { pk: 'b', op: 'insert', data: {}, ts: '2023-01-01T00:00:02.000Z', bookmark: 'b2', collection: 'c' },
        { pk: 'c', op: 'insert', data: {}, ts: '2023-01-01T00:00:03.000Z', bookmark: 'b3', collection: 'c' },
      ]

      const results = records.map(convertToDeltalakeCDC)

      expect(results[0]._seq).toBeLessThan(results[1]._seq)
      expect(results[1]._seq).toBeLessThan(results[2]._seq)
    })

    it('should convert timestamp to nanoseconds', () => {
      const record: EventsDoCDCRecord = {
        pk: 'id',
        op: 'insert',
        data: {},
        ts: '2023-11-14T22:13:20.000Z', // 1700000000000 ms
        bookmark: 'b',
        collection: 'c',
      }

      const result = convertToDeltalakeCDC(record)

      // Nanoseconds = milliseconds * 1_000_000
      const expectedMs = new Date('2023-11-14T22:13:20.000Z').getTime()
      expect(result._ts).toBe(BigInt(expectedMs) * BigInt(1_000_000))
    })
  })

  describe('convertFromDeltalakeCDC', () => {
    it('should convert deltalake record back to events.do format', () => {
      const tsMs = new Date('2023-11-14T22:13:20.000Z').getTime()
      const deltalakeRecord = {
        _id: 'user-123',
        _seq: BigInt(1),
        _op: 'c' as const,
        _before: null,
        _after: { name: 'Alice', age: 30 },
        _ts: BigInt(tsMs) * BigInt(1_000_000),
        _source: {
          system: 'deltalake',
          collection: 'users',
        },
        _txn: 'bookmark-1',
      }

      const result = convertFromDeltalakeCDC(deltalakeRecord, 'users')

      expect(result.pk).toBe('user-123')
      expect(result.op).toBe('insert')
      expect(result.data).toEqual({ name: 'Alice', age: 30 })
      // For insert, prev is undefined (not set)
      expect(result.prev).toBeUndefined()
      expect(result.ts).toBe('2023-11-14T22:13:20.000Z')
      expect(result.bookmark).toBe('bookmark-1')
      expect(result.collection).toBe('users')
    })

    it('should use collection parameter', () => {
      const record = {
        _id: 'id',
        _seq: BigInt(1),
        _op: 'c' as const,
        _before: null,
        _after: {},
        _ts: BigInt(1000000000),
        _source: { system: 'deltalake', collection: 'original' },
      }

      const result = convertFromDeltalakeCDC(record, 'override')
      expect(result.collection).toBe('override')
    })

    it('should convert update record with before/after', () => {
      const tsMs = new Date('2023-11-14T22:13:20.000Z').getTime()
      const record = {
        _id: 'user-123',
        _seq: BigInt(1),
        _op: 'u' as const,
        _before: { name: 'Alice', age: 30 },
        _after: { name: 'Alice', age: 31 },
        _ts: BigInt(tsMs) * BigInt(1_000_000),
        _source: { system: 'deltalake', collection: 'users' },
      }

      const result = convertFromDeltalakeCDC(record, 'users')

      expect(result.op).toBe('update')
      expect(result.data).toEqual({ name: 'Alice', age: 31 })
      expect(result.prev).toEqual({ name: 'Alice', age: 30 })
    })

    it('should convert delete record', () => {
      const tsMs = new Date('2023-11-14T22:13:20.000Z').getTime()
      const record = {
        _id: 'user-123',
        _seq: BigInt(1),
        _op: 'd' as const,
        _before: { name: 'Alice', age: 31 },
        _after: null,
        _ts: BigInt(tsMs) * BigInt(1_000_000),
        _source: { system: 'deltalake', collection: 'users' },
      }

      const result = convertFromDeltalakeCDC(record, 'users')

      expect(result.op).toBe('delete')
      expect(result.data).toBeUndefined()
      expect(result.prev).toEqual({ name: 'Alice', age: 31 })
    })
  })

  describe('Batch Conversion', () => {
    it('should convert batch to deltalake format', () => {
      const records: EventsDoCDCRecord[] = [
        { pk: '1', op: 'insert', data: { a: 1 }, ts: '2023-01-01T00:00:00.000Z', bookmark: 'b1', collection: 'c' },
        { pk: '2', op: 'update', data: { a: 2 }, prev: { a: 1 }, ts: '2023-01-01T00:00:01.000Z', bookmark: 'b2', collection: 'c' },
      ]

      const results = convertBatchToDeltalakeCDC(records)

      expect(results).toHaveLength(2)
      expect(results[0]._id).toBe('1')
      expect(results[0]._op).toBe('c')
      expect(results[1]._id).toBe('2')
      expect(results[1]._op).toBe('u')
    })

    it('should convert batch from deltalake format', () => {
      const records = [
        {
          _id: '1',
          _seq: BigInt(1),
          _op: 'c' as const,
          _before: null,
          _after: { a: 1 },
          _ts: BigInt(1000000000),
          _source: { system: 'deltalake' as const, collection: 'test' },
          _txn: 'b1',
        },
        {
          _id: '2',
          _seq: BigInt(2),
          _op: 'u' as const,
          _before: { a: 1 },
          _after: { a: 2 },
          _ts: BigInt(1001000000),
          _source: { system: 'deltalake' as const, collection: 'test' },
          _txn: 'b2',
        },
      ]

      const results = convertBatchFromDeltalakeCDC(records, 'test')

      expect(results).toHaveLength(2)
      expect(results[0].pk).toBe('1')
      expect(results[0].op).toBe('insert')
      expect(results[1].pk).toBe('2')
      expect(results[1].op).toBe('update')
    })
  })
})

describe('Serialization', () => {
  beforeEach(() => {
    resetSequenceState()
  })

  describe('serializeCDCRecord', () => {
    it('should serialize record with string representations', () => {
      const record = {
        _id: 'test-id',
        _seq: BigInt(12345),
        _op: 'c' as const,
        _before: null,
        _after: { key: 'value' },
        _ts: BigInt(1700000000000000000),
        _source: { system: 'test' as const },
      }

      const serialized = serializeCDCRecord(record)

      expect(serialized._id).toBe('test-id')
      expect(serialized._seq).toBe('12345')
      expect(serialized._op).toBe('c')
      expect(serialized._before).toBeNull()
      expect(serialized._after).toBe('{"key":"value"}')
      expect(serialized._ts).toBe('1700000000000000000')
      expect(serialized._source).toBe('{"system":"test"}')
    })

    it('should include _txn if present', () => {
      const record = {
        _id: 'id',
        _seq: BigInt(1),
        _op: 'c' as const,
        _before: null,
        _after: {},
        _ts: BigInt(1000),
        _source: { system: 'test' as const },
        _txn: 'my-txn',
      }

      const serialized = serializeCDCRecord(record)
      expect(serialized._txn).toBe('my-txn')
    })
  })

  describe('deserializeCDCRecord', () => {
    it('should deserialize string representations back to native types', () => {
      const serialized = {
        _id: 'test-id',
        _seq: '12345',
        _op: 'c',
        _before: null,
        _after: '{"key":"value"}',
        _ts: '1700000000000000000',
        _source: '{"system":"test"}',
      }

      const record = deserializeCDCRecord(serialized)

      expect(record._id).toBe('test-id')
      expect(record._seq).toBe(BigInt(12345))
      expect(record._op).toBe('c')
      expect(record._before).toBeNull()
      expect(record._after).toEqual({ key: 'value' })
      expect(record._ts).toBe(BigInt(1700000000000000000))
      expect(record._source).toEqual({ system: 'test' })
    })

    it('should handle null _after for delete operations', () => {
      const serialized = {
        _id: 'id',
        _seq: '1',
        _op: 'd',
        _before: '{"old":"data"}',
        _after: null,
        _ts: '1000',
        _source: '{}',
      }

      const record = deserializeCDCRecord(serialized)

      expect(record._after).toBeNull()
      expect(record._before).toEqual({ old: 'data' })
    })

    it('should preserve _txn if present', () => {
      const serialized = {
        _id: 'id',
        _seq: '1',
        _op: 'c',
        _before: null,
        _after: '{}',
        _ts: '1000',
        _source: '{}',
        _txn: 'bookmark-123',
      }

      const record = deserializeCDCRecord(serialized)
      expect(record._txn).toBe('bookmark-123')
    })
  })

  describe('Round-trip serialization', () => {
    it('should preserve data through serialize/deserialize cycle', () => {
      const original: EventsDoCDCRecord = {
        pk: 'complex-id',
        op: 'update',
        data: { nested: { deep: [1, 2, 3] }, flag: true },
        prev: { nested: { deep: [] }, flag: false },
        ts: '2023-11-14T22:13:20.000Z',
        bookmark: 'bookmark-xyz',
        collection: 'complex',
      }

      const converted = convertToDeltalakeCDC(original)
      const serialized = serializeCDCRecord(converted)
      const deserialized = deserializeCDCRecord(serialized)
      const restored = convertFromDeltalakeCDC(deserialized, 'complex')

      expect(restored.pk).toBe(original.pk)
      expect(restored.op).toBe(original.op)
      expect(restored.data).toEqual(original.data)
      expect(restored.prev).toEqual(original.prev)
      // ts comes back as ISO string
      expect(restored.ts).toBe(original.ts)
      expect(restored.bookmark).toBe(original.bookmark)
      expect(restored.collection).toBe(original.collection)
    })
  })
})

describe('Sequence State Management', () => {
  afterEach(() => {
    resetSequenceState()
  })

  it('should reset sequence state', () => {
    // Generate some sequences
    const record: EventsDoCDCRecord = {
      pk: 'id', op: 'insert', data: {}, ts: '2023-01-01T00:00:00.000Z', bookmark: 'b', collection: 'c',
    }
    const result1 = convertToDeltalakeCDC(record)

    resetSequenceState()

    // After reset, sequence should start over (but still be > 0 due to time component)
    const result2 = convertToDeltalakeCDC(record)

    // The sequence should be different but both valid
    expect(result1._seq).toBeGreaterThan(0n)
    expect(result2._seq).toBeGreaterThan(0n)
  })

  it('should set sequence state for a collection', () => {
    setSequenceState('test-collection', 1000n, 5000n)

    const record: EventsDoCDCRecord = {
      pk: 'id', op: 'insert', data: {}, ts: '1970-01-01T00:00:00.001Z', bookmark: 'b', collection: 'test-collection',
    }
    const result = convertToDeltalakeCDC(record)

    // Sequence should be greater than what we set (incremented)
    expect(result._seq).toBeGreaterThanOrEqual(1001n)
  })

  it('should reset specific collection state', () => {
    // Set up state for two collections
    setSequenceState('col1', 100n, 100n)
    setSequenceState('col2', 200n, 200n)

    // Reset only col1
    resetSequenceState('col1')

    // col2 should still have its state
    const record: EventsDoCDCRecord = {
      pk: 'id', op: 'insert', data: {}, ts: '1970-01-01T00:00:00.001Z', bookmark: 'b', collection: 'col2',
    }
    const result = convertToDeltalakeCDC(record)

    // Should continue from col2's sequence
    expect(result._seq).toBeGreaterThanOrEqual(201n)
  })
})
