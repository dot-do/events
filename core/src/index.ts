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

// === Core: EventEmitter ===

export { EventEmitter } from './emitter.js'
export type {
  BaseEvent,
  RpcCallEvent,
  CollectionChangeEvent,
  LifecycleEvent,
  WebSocketEvent,
  ClientEvent,
  CustomEvent,
  DurableEvent,
  EmitInput,
  EventBatch,
  EventEmitterOptions,
} from './types.js'
export {
  isRpcCallEvent,
  isCollectionChangeEvent,
  isLifecycleEvent,
  isWebSocketEvent,
  isClientEvent,
  isCustomEvent,
} from './types.js'

// === CDC: Change Data Capture ===

export type { Collection } from './cdc.js'
export { CDCCollection } from './cdc.js'

// CDC Delta files (incremental changes in Parquet format)
export type { CDCEvent, DeltaRecord } from './cdc-delta.js'
export {
  createDeltaRecord,
  writeDeltaFile,
  readDeltaFile,
  accumulateDeltas,
  generateDeltaPath,
} from './cdc-delta.js'

// CDC Compaction (delta → data.parquet)
export type { CompactionDeltaRecord, CompactionOptions, CompactionResult, CollectionManifest } from './cdc-compaction.js'
export {
  compactCollection,
  applyDeltasToState,
  writeDataParquet,
} from './cdc-compaction.js'

// CDC Processor (Durable Object)
export type { ProcessorState, DocumentState } from './cdc-processor.js'
export { CDCProcessorDO } from './cdc-processor.js'

// CDC Snapshots (PITR - Point-in-time recovery)
export type { SnapshotInfo, SnapshotManifest, RetentionPolicy } from './cdc-snapshot.js'
export {
  createSnapshot as createCDCSnapshot,
  listSnapshots as listCDCSnapshots,
  reconstructState,
  cleanupSnapshots,
} from './cdc-snapshot.js'

// === Query: DuckDB SQL Builders ===

export type { QueryOptions } from './query.js'
export { buildQuery, buildHistoryQuery, buildLatencyQuery, buildPITRRangeQuery } from './query.js'

// === Webhooks: Signature Verification ===

export type { VerificationResult } from './webhooks.js'
export {
  verifyGitHubSignature,
  verifyStripeSignature,
  verifyWorkOSSignature,
  verifySlackSignature,
  verifyLinearSignature,
  verifySvixSignature,
  generateGitHubSignature,
  generateStripeSignature,
  generateWorkOSSignature,
  generateSlackSignature,
  generateLinearSignature,
  generateSvixSignature,
} from './webhooks.js'

// === Catalog: Iceberg-style Metadata ===

export type {
  TableSchema,
  SchemaField,
  PartitionSpec,
  PartitionField,
  DataFile,
  ColumnStats,
  Manifest,
  Snapshot,
  TableMetadata,
} from './catalog.js'
export { CatalogDO } from './catalog.js'

// === Subscriptions: Pub/Sub ===

export type {
  Subscription,
  Delivery,
  DeliveryLog,
  DeadLetter,
  SubscriptionStats,
} from './subscription.js'
export { SubscriptionDO } from './subscription.js'

// Pattern matching for subscriptions
export {
  matchPattern,
  extractPatternPrefix,
  findMatchingSubscriptions,
  clearPatternCache,
} from './pattern-matcher.js'

// === Compaction: Parquet ===

export {
  readParquetRecords,
  mergeParquetRecords,
  writeCompactedParquet,
  listFilesForCompaction,
} from './compaction.js'

// === Snapshots: Legacy ===

export type { SnapshotOptions, SnapshotResult } from './snapshot.js'
export { createSnapshot, restoreSnapshot, listSnapshots, deleteSnapshot } from './snapshot.js'

// === Utilities ===

export { ulid } from './ulid.js'
export { createAsyncBuffer } from './async-buffer.js'
