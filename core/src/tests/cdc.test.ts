/**
 * CDCCollection Tests
 *
 * TDD Red Phase: Tests for CDC (Change Data Capture) Collection wrapper
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { CDCCollection, type Collection } from '../cdc.js'
import { createMockCollection, createMockEmitter } from './mocks.js'

// ============================================================================
// Tests
// ============================================================================

describe('CDCCollection', () => {
  let mockCollection: ReturnType<typeof createMockCollection<{ name: string; active?: boolean }>>
  let mockEmitter: ReturnType<typeof createMockEmitter>
  let cdcCollection: CDCCollection<{ name: string; active?: boolean }>

  beforeEach(() => {
    mockCollection = createMockCollection<{ name: string; active?: boolean }>()
    mockEmitter = createMockEmitter()
    cdcCollection = new CDCCollection(mockCollection, mockEmitter, 'users')
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('get()', () => {
    it('should return document without emitting event', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      const result = cdcCollection.get('user-1')

      expect(result).toEqual({ name: 'Alice' })
      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })

    it('should return null for non-existent document', () => {
      const result = cdcCollection.get('non-existent')

      expect(result).toBeNull()
      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })
  })

  describe('put() - new document', () => {
    it('should emit insert event for new document', () => {
      cdcCollection.put('user-1', { name: 'Alice' })

      expect(mockEmitter.emitChange).toHaveBeenCalledWith(
        'insert',
        'users',
        'user-1',
        { name: 'Alice' }
      )
    })

    it('should call underlying collection put', () => {
      cdcCollection.put('user-1', { name: 'Alice' })

      expect(mockCollection.put).toHaveBeenCalledWith('user-1', { name: 'Alice' })
    })

    it('should store document in collection', () => {
      cdcCollection.put('user-1', { name: 'Alice' })

      expect(mockCollection._data.get('user-1')).toEqual({ name: 'Alice' })
    })
  })

  describe('put() - existing document', () => {
    it('should emit update event for existing document', () => {
      // Pre-populate
      mockCollection._data.set('user-1', { name: 'Alice' })

      cdcCollection.put('user-1', { name: 'Bob' })

      expect(mockEmitter.emitChange).toHaveBeenCalledWith(
        'update',
        'users',
        'user-1',
        { name: 'Bob' },
        { name: 'Alice' }
      )
    })

    it('should include previous value in update event', () => {
      mockCollection._data.set('user-1', { name: 'Alice', active: true })

      cdcCollection.put('user-1', { name: 'Alice', active: false })

      const [type, collection, docId, doc, prev] = mockEmitter.emitChange.mock.calls[0]
      expect(type).toBe('update')
      expect(prev).toEqual({ name: 'Alice', active: true })
    })

    it('should update document in collection', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      cdcCollection.put('user-1', { name: 'Bob' })

      expect(mockCollection._data.get('user-1')).toEqual({ name: 'Bob' })
    })
  })

  describe('delete()', () => {
    it('should emit delete event with previous value', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      cdcCollection.delete('user-1')

      expect(mockEmitter.emitChange).toHaveBeenCalledWith(
        'delete',
        'users',
        'user-1',
        undefined,
        { name: 'Alice' }
      )
    })

    it('should return true when document existed', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      const result = cdcCollection.delete('user-1')

      expect(result).toBe(true)
    })

    it('should return false when document did not exist', () => {
      const result = cdcCollection.delete('non-existent')

      expect(result).toBe(false)
    })

    it('should not emit event when document did not exist', () => {
      cdcCollection.delete('non-existent')

      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })

    it('should remove document from collection', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      cdcCollection.delete('user-1')

      expect(mockCollection._data.has('user-1')).toBe(false)
    })
  })

  describe('clear()', () => {
    it('should emit delete event for each document', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })
      mockCollection._data.set('user-3', { name: 'Charlie' })

      cdcCollection.clear()

      expect(mockEmitter.emitChange).toHaveBeenCalledTimes(3)
    })

    it('should emit delete events with correct document IDs', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      cdcCollection.clear()

      const ids = mockEmitter.emitChange.mock.calls.map((call: unknown[]) => call[2])
      expect(ids).toContain('user-1')
      expect(ids).toContain('user-2')
    })

    it('should return count of cleared documents', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      const count = cdcCollection.clear()

      expect(count).toBe(2)
    })

    it('should clear underlying collection', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      cdcCollection.clear()

      expect(mockCollection.clear).toHaveBeenCalled()
    })

    it('should not emit events when collection is empty', () => {
      cdcCollection.clear()

      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })
  })

  describe('has()', () => {
    it('should return true when document exists', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      expect(cdcCollection.has('user-1')).toBe(true)
    })

    it('should return false when document does not exist', () => {
      expect(cdcCollection.has('non-existent')).toBe(false)
    })

    it('should not emit events', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      cdcCollection.has('user-1')

      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })
  })

  describe('find()', () => {
    it('should return matching documents without emitting events', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      const results = cdcCollection.find()

      expect(results).toHaveLength(2)
      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })

    it('should pass filter to underlying collection', () => {
      cdcCollection.find({ active: true })

      expect(mockCollection.find).toHaveBeenCalledWith({ active: true }, undefined)
    })
  })

  describe('count()', () => {
    it('should return count without emitting events', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      const count = cdcCollection.count()

      expect(count).toBe(2)
      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })
  })

  describe('list()', () => {
    it('should return all documents without emitting events', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      const results = cdcCollection.list()

      expect(results).toHaveLength(2)
      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })
  })

  describe('keys()', () => {
    it('should return all keys without emitting events', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      const keys = cdcCollection.keys()

      expect(keys).toContain('user-1')
      expect(keys).toContain('user-2')
      expect(mockEmitter.emitChange).not.toHaveBeenCalled()
    })
  })

  describe('putMany() - bulk insert', () => {
    it('should emit insert/update events for each document', () => {
      cdcCollection.putMany([
        { id: 'user-1', doc: { name: 'Alice' } },
        { id: 'user-2', doc: { name: 'Bob' } },
      ])

      expect(mockEmitter.emitChange).toHaveBeenCalledTimes(2)
    })

    it('should correctly identify inserts vs updates', () => {
      // Pre-populate one document
      mockCollection._data.set('user-1', { name: 'OldAlice' })

      cdcCollection.putMany([
        { id: 'user-1', doc: { name: 'Alice' } }, // update
        { id: 'user-2', doc: { name: 'Bob' } }, // insert
      ])

      const calls = mockEmitter.emitChange.mock.calls
      const updateCall = calls.find((c: unknown[]) => c[0] === 'update')
      const insertCall = calls.find((c: unknown[]) => c[0] === 'insert')

      expect(updateCall?.[2]).toBe('user-1')
      expect(insertCall?.[2]).toBe('user-2')
    })
  })

  describe('deleteMany() - bulk delete', () => {
    it('should emit delete events for each deleted document', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      cdcCollection.deleteMany(['user-1', 'user-2'])

      expect(mockEmitter.emitChange).toHaveBeenCalledTimes(2)
    })

    it('should return count of deleted documents', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })
      mockCollection._data.set('user-2', { name: 'Bob' })

      const count = cdcCollection.deleteMany(['user-1', 'user-2', 'non-existent'])

      expect(count).toBe(2)
    })

    it('should not emit events for non-existent documents', () => {
      mockCollection._data.set('user-1', { name: 'Alice' })

      cdcCollection.deleteMany(['user-1', 'non-existent'])

      expect(mockEmitter.emitChange).toHaveBeenCalledTimes(1)
    })
  })

  describe('collection name in events', () => {
    it('should use configured collection name in all events', () => {
      const customCdc = new CDCCollection(mockCollection, mockEmitter, 'custom_collection')
      mockCollection._data.set('doc-1', { name: 'Test' })

      customCdc.put('doc-2', { name: 'New' })
      customCdc.put('doc-1', { name: 'Updated' })
      customCdc.delete('doc-1')

      for (const call of mockEmitter.emitChange.mock.calls) {
        expect(call[1]).toBe('custom_collection')
      }
    })
  })

  describe('edge cases', () => {
    it('should handle empty document', () => {
      cdcCollection.put('empty-doc', {} as any)

      expect(mockEmitter.emitChange).toHaveBeenCalledWith(
        'insert',
        'users',
        'empty-doc',
        {}
      )
    })

    it('should handle document with nested objects', () => {
      const nestedDoc = {
        name: 'Alice',
        metadata: {
          created: new Date().toISOString(),
          tags: ['admin', 'active'],
        },
      } as any

      cdcCollection.put('nested-doc', nestedDoc)

      expect(mockCollection._data.get('nested-doc')).toEqual(nestedDoc)
    })

    it('should handle rapid put/delete cycles', () => {
      // Insert
      cdcCollection.put('volatile', { name: 'First' })
      // Update
      cdcCollection.put('volatile', { name: 'Second' })
      // Delete
      cdcCollection.delete('volatile')
      // Re-insert
      cdcCollection.put('volatile', { name: 'Third' })

      // Should have emitted: insert, update, delete, insert
      expect(mockEmitter.emitChange).toHaveBeenCalledTimes(4)

      const types = mockEmitter.emitChange.mock.calls.map((call: unknown[]) => call[0])
      expect(types).toEqual(['insert', 'update', 'delete', 'insert'])
    })

    it('should handle special characters in document id', () => {
      const specialId = 'doc:with/special@chars#and$symbols'

      cdcCollection.put(specialId, { name: 'Special' })

      expect(mockEmitter.emitChange).toHaveBeenCalledWith(
        'insert',
        'users',
        specialId,
        { name: 'Special' }
      )
    })

    it('should maintain data integrity across operations', () => {
      // Initial state
      cdcCollection.put('user-1', { name: 'Alice' })
      cdcCollection.put('user-2', { name: 'Bob' })

      // Update one
      cdcCollection.put('user-1', { name: 'Alice Updated' })

      // Verify state
      expect(cdcCollection.get('user-1')).toEqual({ name: 'Alice Updated' })
      expect(cdcCollection.get('user-2')).toEqual({ name: 'Bob' })

      // Delete one
      cdcCollection.delete('user-1')

      // Verify state
      expect(cdcCollection.get('user-1')).toBeNull()
      expect(cdcCollection.get('user-2')).toEqual({ name: 'Bob' })
    })
  })

  describe('clear() event ordering', () => {
    it('should emit delete events in deterministic order', () => {
      mockCollection._data.set('a', { name: 'A' })
      mockCollection._data.set('b', { name: 'B' })
      mockCollection._data.set('c', { name: 'C' })

      cdcCollection.clear()

      // All deletes should be emitted
      expect(mockEmitter.emitChange).toHaveBeenCalledTimes(3)

      // Each delete should reference the correct collection
      for (const call of mockEmitter.emitChange.mock.calls) {
        expect(call[0]).toBe('delete')
        expect(call[1]).toBe('users')
      }
    })
  })
})
