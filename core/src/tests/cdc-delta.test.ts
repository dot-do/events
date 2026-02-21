/**
 * CDC Delta File Tests
 *
 * TDD Red Phase: Tests for delta file operations in CDC
 *
 * Delta files store incremental changes with schema:
 * - pk: STRING (primary key)
 * - op: STRING (created/updated/deleted)
 * - data: VARIANT (new document state, null for delete)
 * - prev: VARIANT (previous state, optional)
 * - ts: TIMESTAMP
 * - bookmark: STRING (SQLite bookmark for PITR)
 */

import { describe, it, expect } from 'vitest'
import {
  createDeltaRecord,
  writeDeltaFile,
  readDeltaFile,
  generateDeltaPath,
  accumulateDeltas,
  type DeltaRecord,
  type CDCEvent,
} from '../cdc-delta.js'

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Creates a mock CDC event for testing
 */
function createMockCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    type: 'collection.created',
    collection: 'users',
    docId: 'user-123',
    doc: { name: 'Alice', active: true },
    bookmark: 'bookmark-abc123',
    ts: '2024-01-15T10:30:00.000Z',
    do: {
      id: 'do-123',
      name: 'my-do',
    },
    ...overrides,
  }
}

// ============================================================================
// Delta Record Creation Tests
// ============================================================================

describe('createDeltaRecord', () => {
  describe('insert operations', () => {
    it('creates insert delta with correct fields', () => {
      const event = createMockCDCEvent({
        type: 'collection.created',
        docId: 'user-123',
        doc: { name: 'Alice', active: true },
        bookmark: 'bookmark-abc',
      })

      const delta = createDeltaRecord(event)

      expect(delta.pk).toBe('user-123')
      expect(delta.op).toBe('created')
      expect(delta.data).toEqual({ name: 'Alice', active: true })
      expect(delta.prev).toBeNull()
      expect(delta.bookmark).toBe('bookmark-abc')
    })

    it('includes timestamp from event', () => {
      const event = createMockCDCEvent({
        ts: '2024-01-15T10:30:00.000Z',
      })

      const delta = createDeltaRecord(event)

      expect(delta.ts).toBe('2024-01-15T10:30:00.000Z')
    })

    it('handles nested document data', () => {
      const event = createMockCDCEvent({
        type: 'collection.created',
        doc: {
          name: 'Alice',
          metadata: {
            tags: ['admin', 'verified'],
            preferences: { theme: 'dark' },
          },
        },
      })

      const delta = createDeltaRecord(event)

      expect(delta.data).toEqual({
        name: 'Alice',
        metadata: {
          tags: ['admin', 'verified'],
          preferences: { theme: 'dark' },
        },
      })
    })
  })

  describe('update operations', () => {
    it('creates update delta with prev state captured', () => {
      const event = createMockCDCEvent({
        type: 'collection.updated',
        docId: 'user-123',
        doc: { name: 'Alice', active: false },
        prev: { name: 'Alice', active: true },
        bookmark: 'bookmark-def',
      })

      const delta = createDeltaRecord(event)

      expect(delta.pk).toBe('user-123')
      expect(delta.op).toBe('updated')
      expect(delta.data).toEqual({ name: 'Alice', active: false })
      expect(delta.prev).toEqual({ name: 'Alice', active: true })
      expect(delta.bookmark).toBe('bookmark-def')
    })

    it('handles update without prev state', () => {
      const event = createMockCDCEvent({
        type: 'collection.updated',
        docId: 'user-123',
        doc: { name: 'Bob' },
        prev: undefined,
      })

      const delta = createDeltaRecord(event)

      expect(delta.op).toBe('updated')
      expect(delta.prev).toBeNull()
    })
  })

  describe('delete operations', () => {
    it('creates delete delta with null data', () => {
      const event = createMockCDCEvent({
        type: 'collection.deleted',
        docId: 'user-123',
        doc: undefined,
        prev: { name: 'Alice', active: true },
        bookmark: 'bookmark-ghi',
      })

      const delta = createDeltaRecord(event)

      expect(delta.pk).toBe('user-123')
      expect(delta.op).toBe('deleted')
      expect(delta.data).toBeNull()
      expect(delta.prev).toEqual({ name: 'Alice', active: true })
      expect(delta.bookmark).toBe('bookmark-ghi')
    })

    it('handles delete without prev state', () => {
      const event = createMockCDCEvent({
        type: 'collection.deleted',
        docId: 'user-123',
        doc: undefined,
        prev: undefined,
      })

      const delta = createDeltaRecord(event)

      expect(delta.op).toBe('deleted')
      expect(delta.data).toBeNull()
      expect(delta.prev).toBeNull()
    })
  })

  describe('bookmark handling', () => {
    it('includes SQLite bookmark in each record', () => {
      const events = [
        createMockCDCEvent({ type: 'collection.created', bookmark: 'bookmark-1' }),
        createMockCDCEvent({ type: 'collection.updated', bookmark: 'bookmark-2' }),
        createMockCDCEvent({ type: 'collection.deleted', bookmark: 'bookmark-3' }),
      ]

      for (const event of events) {
        const delta = createDeltaRecord(event)
        expect(delta.bookmark).toBe(event.bookmark)
      }
    })

    it('handles missing bookmark gracefully', () => {
      const event = createMockCDCEvent({
        bookmark: undefined,
      })

      const delta = createDeltaRecord(event)

      expect(delta.bookmark).toBeNull()
    })
  })
})

// ============================================================================
// Delta File Writing Tests
// ============================================================================

describe('writeDeltaFile', () => {
  it('writes delta records to Parquet format', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'user-1',
        op: 'created',
        data: { name: 'Alice' },
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bookmark-1',
      },
      {
        pk: 'user-2',
        op: 'created',
        data: { name: 'Bob' },
        prev: null,
        ts: '2024-01-15T10:01:00.000Z',
        bookmark: 'bookmark-2',
      },
    ]

    const buffer = writeDeltaFile(records)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
  })

  it('uses correct column schema (pk, op, data, prev, ts, bookmark)', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'doc-1',
        op: 'updated',
        data: { value: 42 },
        prev: { value: 41 },
        ts: '2024-01-15T12:00:00.000Z',
        bookmark: 'bm-123',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack).toHaveLength(1)
    expect(readBack[0]).toHaveProperty('pk')
    expect(readBack[0]).toHaveProperty('op')
    expect(readBack[0]).toHaveProperty('data')
    expect(readBack[0]).toHaveProperty('prev')
    expect(readBack[0]).toHaveProperty('ts')
    expect(readBack[0]).toHaveProperty('bookmark')
  })

  it('handles empty delta list (no-op)', () => {
    const records: DeltaRecord[] = []

    const buffer = writeDeltaFile(records)

    // Should return an empty or minimal buffer
    expect(buffer).toBeInstanceOf(ArrayBuffer)
  })

  it('preserves data field as JSON string for VARIANT type', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'doc-1',
        op: 'created',
        data: { nested: { deeply: { value: [1, 2, 3] } } },
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-1',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].data).toEqual({ nested: { deeply: { value: [1, 2, 3] } } })
  })

  it('handles null data for delete operations', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'doc-1',
        op: 'deleted',
        data: null,
        prev: { name: 'Old' },
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-1',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].data).toBeNull()
  })

  it('handles null prev for insert operations', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'doc-1',
        op: 'created',
        data: { name: 'New' },
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-1',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].prev).toBeNull()
  })
})

// ============================================================================
// Delta File Reading Tests
// ============================================================================

describe('readDeltaFile', () => {
  it('reads delta records from Parquet file', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'user-1',
        op: 'created',
        data: { name: 'Alice', age: 30 },
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bookmark-abc',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack).toHaveLength(1)
    expect(readBack[0].pk).toBe('user-1')
  })

  it('preserves all fields correctly', async () => {
    const original: DeltaRecord = {
      pk: 'doc-xyz',
      op: 'updated',
      data: { status: 'active', count: 42 },
      prev: { status: 'inactive', count: 41 },
      ts: '2024-01-15T14:30:00.000Z',
      bookmark: 'bookmark-456',
    }

    const buffer = writeDeltaFile([original])
    const readBack = await readDeltaFile(buffer)

    expect(readBack).toHaveLength(1)
    expect(readBack[0].pk).toBe(original.pk)
    expect(readBack[0].op).toBe(original.op)
    expect(readBack[0].data).toEqual(original.data)
    expect(readBack[0].prev).toEqual(original.prev)
    expect(readBack[0].ts).toBe(original.ts)
    expect(readBack[0].bookmark).toBe(original.bookmark)
  })

  it('handles VARIANT data types (complex nested objects)', async () => {
    const complexData = {
      users: [
        { id: 1, name: 'Alice', tags: ['admin'] },
        { id: 2, name: 'Bob', tags: ['user', 'verified'] },
      ],
      metadata: {
        version: 2,
        flags: { active: true, beta: false },
      },
    }

    const records: DeltaRecord[] = [
      {
        pk: 'complex-doc',
        op: 'created',
        data: complexData,
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-complex',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].data).toEqual(complexData)
  })

  it('handles multiple records', async () => {
    const records: DeltaRecord[] = [
      { pk: 'a', op: 'created', data: { v: 1 }, prev: null, ts: '2024-01-15T10:00:00.000Z', bookmark: 'b1' },
      { pk: 'b', op: 'updated', data: { v: 2 }, prev: { v: 1 }, ts: '2024-01-15T10:01:00.000Z', bookmark: 'b2' },
      { pk: 'c', op: 'deleted', data: null, prev: { v: 3 }, ts: '2024-01-15T10:02:00.000Z', bookmark: 'b3' },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack).toHaveLength(3)
    expect(readBack.map((r) => r.pk)).toEqual(['a', 'b', 'c'])
    expect(readBack.map((r) => r.op)).toEqual(['created', 'updated', 'deleted'])
  })

  it('handles empty parquet file', async () => {
    const records: DeltaRecord[] = []

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack).toHaveLength(0)
  })
})

// ============================================================================
// Delta Accumulation Tests
// ============================================================================

describe('accumulateDeltas', () => {
  it('accumulates multiple changes to same record', () => {
    const deltas: DeltaRecord[] = [
      { pk: 'user-1', op: 'created', data: { name: 'Alice' }, prev: null, ts: '2024-01-15T10:00:00.000Z', bookmark: 'b1' },
      {
        pk: 'user-1',
        op: 'updated',
        data: { name: 'Alice', age: 30 },
        prev: { name: 'Alice' },
        ts: '2024-01-15T10:01:00.000Z',
        bookmark: 'b2',
      },
      {
        pk: 'user-1',
        op: 'updated',
        data: { name: 'Alice Updated', age: 30 },
        prev: { name: 'Alice', age: 30 },
        ts: '2024-01-15T10:02:00.000Z',
        bookmark: 'b3',
      },
    ]

    const accumulated = accumulateDeltas(deltas)

    // Last-write-wins: should have final state
    expect(accumulated.get('user-1')).toEqual({
      pk: 'user-1',
      op: 'updated',
      data: { name: 'Alice Updated', age: 30 },
      prev: { name: 'Alice', age: 30 },
      ts: '2024-01-15T10:02:00.000Z',
      bookmark: 'b3',
    })
  })

  it('last-write-wins for conflicts', () => {
    const deltas: DeltaRecord[] = [
      { pk: 'doc-1', op: 'created', data: { v: 1 }, prev: null, ts: '2024-01-15T10:00:00.000Z', bookmark: 'b1' },
      { pk: 'doc-1', op: 'updated', data: { v: 2 }, prev: { v: 1 }, ts: '2024-01-15T10:01:00.000Z', bookmark: 'b2' },
      { pk: 'doc-1', op: 'updated', data: { v: 3 }, prev: { v: 2 }, ts: '2024-01-15T10:02:00.000Z', bookmark: 'b3' },
    ]

    const accumulated = accumulateDeltas(deltas)

    expect(accumulated.get('doc-1')?.data).toEqual({ v: 3 })
    expect(accumulated.get('doc-1')?.bookmark).toBe('b3')
  })

  it('maintains operation order within batch', () => {
    const deltas: DeltaRecord[] = [
      { pk: 'a', op: 'created', data: { v: 1 }, prev: null, ts: '2024-01-15T10:00:00.000Z', bookmark: 'b1' },
      { pk: 'b', op: 'created', data: { v: 2 }, prev: null, ts: '2024-01-15T10:00:01.000Z', bookmark: 'b2' },
      { pk: 'a', op: 'updated', data: { v: 10 }, prev: { v: 1 }, ts: '2024-01-15T10:00:02.000Z', bookmark: 'b3' },
      { pk: 'c', op: 'created', data: { v: 3 }, prev: null, ts: '2024-01-15T10:00:03.000Z', bookmark: 'b4' },
    ]

    const accumulated = accumulateDeltas(deltas)

    expect(accumulated.size).toBe(3)
    expect(accumulated.get('a')?.data).toEqual({ v: 10 })
    expect(accumulated.get('b')?.data).toEqual({ v: 2 })
    expect(accumulated.get('c')?.data).toEqual({ v: 3 })
  })

  it('handles insert-then-delete sequence', () => {
    const deltas: DeltaRecord[] = [
      { pk: 'temp', op: 'created', data: { v: 1 }, prev: null, ts: '2024-01-15T10:00:00.000Z', bookmark: 'b1' },
      { pk: 'temp', op: 'deleted', data: null, prev: { v: 1 }, ts: '2024-01-15T10:01:00.000Z', bookmark: 'b2' },
    ]

    const accumulated = accumulateDeltas(deltas)

    expect(accumulated.get('temp')?.op).toBe('deleted')
    expect(accumulated.get('temp')?.data).toBeNull()
  })

  it('handles delete-then-insert (resurrection)', () => {
    const deltas: DeltaRecord[] = [
      { pk: 'zombie', op: 'deleted', data: null, prev: { v: 'old' }, ts: '2024-01-15T10:00:00.000Z', bookmark: 'b1' },
      { pk: 'zombie', op: 'created', data: { v: 'resurrected' }, prev: null, ts: '2024-01-15T10:01:00.000Z', bookmark: 'b2' },
    ]

    const accumulated = accumulateDeltas(deltas)

    expect(accumulated.get('zombie')?.op).toBe('created')
    expect(accumulated.get('zombie')?.data).toEqual({ v: 'resurrected' })
  })

  it('handles empty delta list', () => {
    const accumulated = accumulateDeltas([])

    expect(accumulated.size).toBe(0)
  })
})

// ============================================================================
// Delta File Path Tests
// ============================================================================

describe('generateDeltaPath', () => {
  it('generates correct path: {ns}/{collection}/deltas/{seq}_{timestamp}.parquet', () => {
    const path = generateDeltaPath('my-namespace', 'users', 1, new Date('2024-01-15T10:30:00.000Z'))

    expect(path).toMatch(/^my-namespace\/users\/deltas\/001_2024-01-15T10-30-00-000Z\.parquet$/)
  })

  it('sequence numbers are zero-padded (001, 002, etc.)', () => {
    const path1 = generateDeltaPath('ns', 'col', 1, new Date('2024-01-15T10:00:00.000Z'))
    const path5 = generateDeltaPath('ns', 'col', 5, new Date('2024-01-15T10:00:00.000Z'))
    const path99 = generateDeltaPath('ns', 'col', 99, new Date('2024-01-15T10:00:00.000Z'))
    const path100 = generateDeltaPath('ns', 'col', 100, new Date('2024-01-15T10:00:00.000Z'))
    const path1000 = generateDeltaPath('ns', 'col', 1000, new Date('2024-01-15T10:00:00.000Z'))

    expect(path1).toContain('/001_')
    expect(path5).toContain('/005_')
    expect(path99).toContain('/099_')
    expect(path100).toContain('/100_')
    expect(path1000).toContain('/1000_')
  })

  it('timestamp format is ISO 8601 safe for filenames', () => {
    const path = generateDeltaPath('ns', 'col', 1, new Date('2024-01-15T10:30:45.123Z'))

    // Should not contain colons (invalid for filenames on Windows)
    expect(path).not.toContain(':')
    // Should use dashes instead
    expect(path).toContain('2024-01-15T10-30-45-123Z')
  })

  it('handles special characters in namespace', () => {
    const path = generateDeltaPath('my-org_project', 'users', 1, new Date('2024-01-15T10:00:00.000Z'))

    expect(path).toMatch(/^my-org_project\/users\/deltas\//)
  })

  it('handles special characters in collection name', () => {
    const path = generateDeltaPath('ns', 'user_profiles', 1, new Date('2024-01-15T10:00:00.000Z'))

    expect(path).toMatch(/\/user_profiles\/deltas\//)
  })

  it('uses current time if not provided', () => {
    const before = new Date()
    const path = generateDeltaPath('ns', 'col', 1)
    const after = new Date()

    // Path should contain a timestamp between before and after
    const match = path.match(/(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z)/)
    expect(match).not.toBeNull()

    // Convert back to date format for comparison
    const tsStr = match![1].replace(/-(\d{2})-(\d{2})-(\d{3})Z$/, ':$1:$2.$3Z')
    const ts = new Date(tsStr)

    expect(ts.getTime()).toBeGreaterThanOrEqual(before.getTime())
    expect(ts.getTime()).toBeLessThanOrEqual(after.getTime())
  })
})

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

describe('edge cases', () => {
  it('handles very large documents', async () => {
    const largeObject: Record<string, unknown> = {}
    for (let i = 0; i < 1000; i++) {
      largeObject[`field_${i}`] = `value_${i}_${'x'.repeat(100)}`
    }

    const records: DeltaRecord[] = [
      {
        pk: 'large-doc',
        op: 'created',
        data: largeObject,
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-large',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].pk).toBe('large-doc')
    expect(Object.keys(readBack[0].data as Record<string, unknown>).length).toBe(1000)
  })

  it('handles unicode in documents', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'unicode-doc',
        op: 'created',
        data: {
          japanese: 'nihongo',
          emoji: 'test',
          chinese: 'zhongwen',
        },
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-unicode',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].data).toEqual({
      japanese: 'nihongo',
      emoji: 'test',
      chinese: 'zhongwen',
    })
  })

  it('handles special characters in primary key', () => {
    const event = createMockCDCEvent({
      docId: 'doc:with/special@chars#and$symbols',
    })

    const delta = createDeltaRecord(event)

    expect(delta.pk).toBe('doc:with/special@chars#and$symbols')
  })

  it('handles empty string bookmark', () => {
    const event = createMockCDCEvent({
      bookmark: '',
    })

    const delta = createDeltaRecord(event)

    // Empty string should be converted to null for consistency
    expect(delta.bookmark).toBe('')
  })

  it('handles null values in document data', async () => {
    const records: DeltaRecord[] = [
      {
        pk: 'null-values',
        op: 'created',
        data: { name: 'Test', optional: null, nested: { value: null } },
        prev: null,
        ts: '2024-01-15T10:00:00.000Z',
        bookmark: 'bm-null',
      },
    ]

    const buffer = writeDeltaFile(records)
    const readBack = await readDeltaFile(buffer)

    expect(readBack[0].data).toEqual({ name: 'Test', optional: null, nested: { value: null } })
  })
})
