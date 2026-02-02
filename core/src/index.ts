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
  EventBufferFullError,
  CircuitBreakerOpenError,
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
export type {
  CompactionDeltaRecord,
  CompactionOptions,
  CompactionResult,
  CollectionManifest as CompactionManifest,
  ParallelCompactionOptions,
  ParallelCompactionProgress,
} from './cdc-compaction.js'
export {
  compactCollection,
  compactCollectionParallel,
  applyDeltasToState,
  writeDataParquet,
  mergeChunkStates,
} from './cdc-compaction.js'

// CDC Processor (Durable Object)
export type { ProcessorState, DocumentState, ProcessorManifest, DeltaRef } from './cdc-processor.js'
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

export type { QueryOptions, OrderByColumn, OrderDirection, OrderBy } from './query.js'
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
  // Schema evolution types
  FieldChange,
  SchemaChange,
  SchemaVersion,
} from './catalog.js'
export { CatalogDO } from './catalog.js'

// === Subscriptions: Pub/Sub ===

export type {
  Subscription,
  Delivery,
  DeliveryLog,
  DeadLetter,
  SubscriptionStats,
  BatchDeliveryConfig,
  BatchDeliveryResult,
  PendingBatch,
} from './subscription.js'
export { SubscriptionDO } from './subscription.js'

// Worker ID validation (SSRF prevention for subscription delivery)
export type { ValidationResult as WorkerIdValidationResult } from './worker-id-validation.js'
export {
  validateWorkerId,
  validateRpcMethod,
  assertValidDeliveryTarget,
  buildSafeDeliveryUrl,
  WORKER_ID_PATTERN,
  RPC_METHOD_PATTERN,
  MAX_WORKER_ID_LENGTH,
  MAX_RPC_METHOD_LENGTH,
  SSRF_BLOCKLIST_PATTERNS,
} from './worker-id-validation.js'

// === Schema Registry: Event Validation ===

export type {
  JsonSchema,
  SchemaRegistration,
  ValidationResult,
  ValidationError,
  NamespaceConfig,
} from './schema-registry.js'
export { SchemaRegistryDO, validateAgainstSchema } from './schema-registry.js'

// === Safe Regex: ReDoS Prevention ===

export type { RegexSafetyResult, SafeRegexOptions, SafeRegexResult } from './safe-regex.js'
export {
  analyzeRegexSafety,
  createSafeRegex,
  safeRegexTest,
  safeRegexMatch,
  validateSchemaPattern,
  getCachedSafeRegex,
  clearRegexCache,
  MAX_PATTERN_LENGTH as REGEX_MAX_PATTERN_LENGTH,
  MAX_INPUT_LENGTH as REGEX_MAX_INPUT_LENGTH,
  MAX_QUANTIFIER as REGEX_MAX_QUANTIFIER,
  MAX_REGEX_EXECUTION_MS,
} from './safe-regex.js'

// Pattern matching for subscriptions
export {
  matchPattern,
  extractPatternPrefix,
  findMatchingSubscriptions,
  clearPatternCache,
  getPatternCacheSize,
  PATTERN_CACHE_MAX_SIZE,
} from './pattern-matcher.js'

// === Compaction: Parquet ===

export {
  readParquetRecords,
  mergeParquetRecords,
  writeCompactedParquet,
  listFilesForCompaction,
} from './compaction.js'

// Event stream compaction (hourly files → daily files)
export type {
  EventCompactionOptions,
  DayCompactionResult,
  EventCompactionResult,
} from './event-compaction.js'
export { compactEventStream } from './event-compaction.js'

// === Snapshots: Legacy ===

export type { SnapshotOptions, SnapshotResult } from './snapshot.js'
export { createSnapshot, restoreSnapshot, listSnapshots, deleteSnapshot } from './snapshot.js'

// === Utilities ===

export { ulid } from './ulid.js'
export { createAsyncBuffer } from './async-buffer.js'

// === R2 Path Sanitization ===

export {
  InvalidR2PathError,
  sanitizePathSegment,
  sanitizeR2Path,
  buildSafeR2Path,
  validateNamespaceOrCollection,
} from './r2-path.js'

// === SQL Row Mapper ===

export type { SqlRow, JsonValidator, JsonValidationResult } from './sql-mapper.js'
export {
  SqlTypeError,
  JsonValidationError,
  getString,
  getNumber,
  getBoolean,
  getOptionalString,
  getOptionalNumber,
  getOptionalBoolean,
  getJson,
  getOptionalJson,
  getValidatedJson,
  getValidatedOptionalJson,
  // Type guard utilities
  isObject,
  isArray,
  isArrayOf,
  isString,
  isNumber,
  isBoolean,
  isNull,
  createObjectValidator,
  nullable,
  optional,
} from './sql-mapper.js'

// === Dedup Cache ===
// Optimized deduplication for queue processing using bloom filters and R2 batch operations

export type { DedupCheckResult } from './dedup-cache.js'
export {
  BloomFilter,
  LRUCache,
  DedupCache,
  batchCheckDuplicates,
  batchWriteDedupMarkers,
  getCdcDedupCache,
  getSubDedupCache,
} from './dedup-cache.js'

// === Browser SDK ===
// Note: For browser usage, import from '@dotdo/events/browser' for the standalone bundle
export type { BrowserConfig, EncryptionConfig } from './browser.js'
export {
  EventsSDK,
  StorageEncryption,
  init as initBrowser,
  initAsync as initBrowserAsync,
  page,
  track,
  identify,
  flush as flushBrowser
} from './browser.js'

// === End-to-End Encryption ===
// Provides AES-256-GCM encryption for sensitive event payloads
export type {
  EncryptedPayload,
  EncryptionKeyInfo,
  EncryptionKeyStore,
  PayloadEncryptionConfig,
  EncryptOptions,
} from './encryption.js'
export {
  // Key management
  generateEncryptionKey,
  generateEncryptionKeyInfo,
  createKeyStore,
  initializeKeyStore,
  rotateEncryptionKey,
  // Encryption/decryption
  encryptPayload,
  decryptPayload,
  encryptFields,
  decryptFields,
  reencryptPayload,
  // Utilities
  isEncryptedPayload,
  shouldEncryptEvent,
  constantTimeCompare,
  ENCRYPTED_MARKER,
} from './encryption.js'

// === Configuration ===
// Centralized configuration constants for tuning and defaults
export { coreConfig } from './config.js'
export {
  // Emitter defaults
  DEFAULT_EMITTER_ENDPOINT,
  DEFAULT_BATCH_SIZE,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_RETRY_QUEUE_SIZE,
  DEFAULT_MAX_CONSECUTIVE_FAILURES,
  DEFAULT_CIRCUIT_BREAKER_RESET_MS,
  // Retry configuration
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  RETRY_JITTER_MS,
  // Subscription defaults
  DEFAULT_SUBSCRIPTION_MAX_RETRIES,
  DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
  SUBSCRIPTION_BATCH_LIMIT,
  SUBSCRIPTION_RETRY_BASE_DELAY_MS,
  SUBSCRIPTION_RETRY_MAX_DELAY_MS,
  // Batched delivery configuration
  DEFAULT_BATCH_DELIVERY_SIZE,
  DEFAULT_BATCH_DELIVERY_WINDOW_MS,
  MAX_BATCH_DELIVERY_SIZE,
  MAX_BATCH_DELIVERY_WINDOW_MS,
  // Pattern validation
  MAX_PATTERN_LENGTH,
  MAX_PATTERN_SEGMENTS,
  ALLOWED_PATTERN_CHARS,
  MAX_PATTERN_MATCH_ITERATIONS,
  // Storage keys
  STORAGE_KEY_RETRY,
  STORAGE_KEY_RETRY_COUNT,
  STORAGE_KEY_BATCH,
  STORAGE_KEY_CIRCUIT_BREAKER,
  // Parallel compaction configuration
  DEFAULT_COMPACTION_PARALLELISM,
  DEFAULT_COMPACTION_CHUNK_SIZE,
  MIN_DELTAS_FOR_PARALLEL,
  MAX_COMPACTION_PARALLELISM,
} from './config.js'
