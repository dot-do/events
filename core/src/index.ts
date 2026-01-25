/**
 * @dotdo/events - Event streaming, CDC, and lakehouse analytics for Durable Objects
 *
 * Lightweight event streaming with:
 * - Batched emission to events.do (or any endpoint)
 * - Alarm-based retries for reliability
 * - CDC (Change Data Capture) for collections
 * - PITR (Point-in-time recovery) with SQLite bookmarks
 * - R2 streaming for lakehouse/time-travel queries
 */

export * from './emitter.js'
export * from './types.js'
export * from './cdc.js'
export * from './snapshot.js'
export * from './query.js'
