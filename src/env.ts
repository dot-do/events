/**
 * Shared Env interface for events.do worker
 *
 * All worker-side files should import Env from here rather than
 * defining their own. Files that only need a subset should use
 * Pick<Env, 'EVENTS_BUCKET' | ...> or similar.
 */

import type { CatalogDO } from '../core/src/catalog'
import type { SubscriptionDO } from '../core/src/subscription'
import type { CDCProcessorDO } from '../core/src/cdc-processor'
import type { SchemaRegistryDO } from '../core/src/schema-registry'
import type { EventWriterDO } from './event-writer-do'
import type { ShardCoordinatorDO } from './shard-coordinator-do'
import type { SubscriptionShardCoordinatorDO } from './subscription-shard-coordinator-do'
import type { CDCShardCoordinatorDO } from './cdc-shard-coordinator-do'
import type { RateLimiterDO } from './middleware/rate-limiter-do'
import type { WebhookEnv } from './webhook-handler'
import type { EventBatch } from '@dotdo/events'
import type { AuthBinding } from 'oauth.do/rpc'
import type { AuthRequest } from 'oauth.do/itty'
import type { AnalyticsEngineDataset } from './metrics'

export type { WebhookEnv, AuthRequest }

/**
 * RPC interface for the OAuth worker service binding.
 * Methods match the RPC entrypoints on OAuthWorker (`.do/oauth/workers/oauth/index.ts`).
 */
export interface OAuthRPC {
  /** Initiate login — returns a redirect Response from the upstream auth provider */
  login(returnTo: string): Promise<Response>
  /** Exchange an authorization code for tokens — returns the DO's JSON Response */
  exchange(code: string): Promise<Response>
  /** HTTP fetch fallback */
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
}

export interface Env extends WebhookEnv {
  EVENTS_BUCKET: R2Bucket
  PIPELINE_BUCKET: R2Bucket
  BENCHMARK_BUCKET: R2Bucket
  /** Cloudflare Pipeline for event ingestion into ClickHouse */
  EVENTS_PIPELINE?: Pipeline<Record<string, unknown>>
  CATALOG: DurableObjectNamespace<CatalogDO>
  SUBSCRIPTIONS: DurableObjectNamespace<SubscriptionDO>
  CDC_PROCESSOR: DurableObjectNamespace<CDCProcessorDO>
  EVENT_WRITER: DurableObjectNamespace<EventWriterDO>
  /** Shard coordinator for EventWriterDO dynamic sharding (optional - falls back to static shards if not bound) */
  SHARD_COORDINATOR?: DurableObjectNamespace<ShardCoordinatorDO>
  /** Shard coordinator for SubscriptionDO dynamic sharding (optional - falls back to static shards if not bound) */
  SUBSCRIPTION_SHARD_COORDINATOR?: DurableObjectNamespace<SubscriptionShardCoordinatorDO>
  /** Shard coordinator for CDCProcessorDO dynamic sharding (optional - falls back to hash-based routing if not bound) */
  CDC_SHARD_COORDINATOR?: DurableObjectNamespace<CDCShardCoordinatorDO>
  RATE_LIMITER: DurableObjectNamespace<RateLimiterDO>
  /** Schema registry for event validation (optional - validation disabled if not bound) */
  SCHEMA_REGISTRY?: DurableObjectNamespace<SchemaRegistryDO>
  EVENTS_QUEUE?: Queue<EventBatch>
  /** When true and EVENTS_QUEUE is bound, use queue for CDC/subscription fanout instead of direct DO calls */
  USE_QUEUE_FANOUT?: string
  AUTH_TOKEN?: string
  /** When true, allows unauthenticated access to /ingest endpoint. Only use for development/testing. */
  ALLOW_UNAUTHENTICATED_INGEST?: string
  AUTH: AuthBinding
  OAUTH: OAuthRPC
  ENVIRONMENT: string
  ALLOWED_ORIGINS?: string
  TAIL_AUTH_SECRET?: string
  /** Max requests per minute for /ingest endpoint (default: 1000) */
  RATE_LIMIT_REQUESTS_PER_MINUTE?: string
  /** Max events per minute for /ingest endpoint (default: 100000) */
  RATE_LIMIT_EVENTS_PER_MINUTE?: string
  /** Analytics Engine dataset for metrics (optional) */
  ANALYTICS?: AnalyticsEngineDataset
  /** Namespace-scoped API keys stored as JSON: { "ns_acme_xxx": "acme", "ns_beta_yyy": "beta" } */
  NAMESPACE_API_KEYS?: string
  /** Default namespace for legacy keys without namespace prefix (default: "default") */
  DEFAULT_NAMESPACE?: string
  /** Enable schema validation at ingest time (default: false if SCHEMA_REGISTRY not bound) */
  ENABLE_SCHEMA_VALIDATION?: string

  // ============================================================================
  // End-to-End Encryption Configuration
  // ============================================================================

  /** JSON-encoded encryption key store for E2E payload encryption */
  ENCRYPTION_KEYS?: string
  /** JSON-encoded namespace encryption configs: { "namespace": EncryptionConfig } */
  NAMESPACE_ENCRYPTION_CONFIGS?: string
  /** Default encryption config (JSON) - applied when namespace has no specific config */
  DEFAULT_ENCRYPTION_CONFIG?: string
  /** Enable encryption for all events (default: false) */
  ENABLE_ENCRYPTION?: string

  /** Max request body size in bytes for ingest endpoint (default: 1048576 = 1MB) */
  MAX_INGEST_BODY_SIZE?: string
  /** Max request body size in bytes for webhook endpoint (default: 1048576 = 1MB) */
  MAX_WEBHOOK_BODY_SIZE?: string
  /** Max request body size in bytes for query endpoint (default: 65536 = 64KB) */
  MAX_QUERY_BODY_SIZE?: string

  // ============================================================================
  // DeltaLake Integration Configuration
  // ============================================================================

  /** Enable DeltaTable for event storage (default: false) */
  USE_DELTALAKE?: string
  /** Enable DeltaTable for CDC storage (default: false) */
  USE_DELTALAKE_CDC?: string
  /** Enable dual-write mode during migration - writes to both legacy and DeltaTable (default: false) */
  DELTALAKE_DUAL_WRITE?: string
}
