/**
 * CDC (Change Data Capture) Collection Wrapper
 */

import type { EventEmitter } from './emitter.js'

/**
 * Generic Collection interface (compatible with @dotdo/rpc collections)
 */
export interface Collection<T extends Record<string, unknown>> {
  get(id: string): T | null
  put(id: string, doc: T): void
  delete(id: string): boolean
  has(id: string): boolean
  find(filter?: Record<string, unknown>, options?: Record<string, unknown>): T[]
  count(filter?: Record<string, unknown>): number
  list(options?: Record<string, unknown>): T[]
  keys(): string[]
  clear(): number
}

/**
 * Wraps a Collection with CDC event emission
 *
 * Every mutation (put, delete, clear) emits a CDC event with:
 * - The new document state
 * - The previous document state (if trackPrevious is enabled)
 * - SQLite bookmark for PITR
 *
 * @example
 * ```typescript
 * const users = new CDCCollection(
 *   this.collection<User>('users'),
 *   this.events,
 *   'users'
 * )
 *
 * // This emits a CDC event
 * users.put('user-123', { name: 'Alice', active: true })
 * ```
 */
export class CDCCollection<T extends Record<string, unknown>> {
  constructor(
    private collection: Collection<T>,
    private emitter: EventEmitter,
    private name: string
  ) {}

  /**
   * Get a document by ID (no event emitted)
   */
  get(id: string): T | null {
    return this.collection.get(id)
  }

  /**
   * Put a document - emits insert or update CDC event
   */
  put(id: string, doc: T): void {
    const prev = this.collection.get(id)
    this.collection.put(id, doc)

    if (prev) {
      this.emitter.emitChange('updated', this.name, id, doc, prev)
    } else {
      this.emitter.emitChange('created', this.name, id, doc)
    }
  }

  /**
   * Delete a document - emits delete CDC event
   */
  delete(id: string): boolean {
    const prev = this.collection.get(id)
    const deleted = this.collection.delete(id)

    if (deleted && prev) {
      this.emitter.emitChange('deleted', this.name, id, undefined, prev)
    }

    return deleted
  }

  /**
   * Check if document exists (no event emitted)
   */
  has(id: string): boolean {
    return this.collection.has(id)
  }

  /**
   * Find documents matching filter (no event emitted)
   */
  find(filter?: Record<string, unknown>, options?: Record<string, unknown>): T[] {
    return this.collection.find(filter, options)
  }

  /**
   * Count documents matching filter (no event emitted)
   */
  count(filter?: Record<string, unknown>): number {
    return this.collection.count(filter)
  }

  /**
   * List all documents (no event emitted)
   */
  list(options?: Record<string, unknown>): T[] {
    return this.collection.list(options)
  }

  /**
   * Get all document IDs (no event emitted)
   */
  keys(): string[] {
    return this.collection.keys()
  }

  /**
   * Clear all documents - emits delete CDC event for each
   */
  clear(): number {
    const keys = this.keys()
    const count = this.collection.clear()

    // Emit delete for each document
    for (const id of keys) {
      this.emitter.emitChange('deleted', this.name, id)
    }

    return count
  }

  /**
   * Bulk put - emits CDC events for each document
   */
  putMany(docs: Array<{ id: string; doc: T }>): void {
    for (const { id, doc } of docs) {
      this.put(id, doc)
    }
  }

  /**
   * Bulk delete - emits CDC events for each document
   */
  deleteMany(ids: string[]): number {
    let deleted = 0
    for (const id of ids) {
      if (this.delete(id)) deleted++
    }
    return deleted
  }
}
