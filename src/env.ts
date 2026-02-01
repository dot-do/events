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
import type { EventWriterDO } from './event-writer-do'
import type { RateLimiterDO } from './middleware/rate-limiter-do'
import type { WebhookEnv } from './webhook-handler'
import type { EventBatch } from '@dotdo/events'
import type { AuthBinding } from 'oauth.do/rpc'
import type { AuthRequest } from 'oauth.do/itty'

export type { WebhookEnv, AuthRequest }

export interface Env extends WebhookEnv {
  EVENTS_BUCKET: R2Bucket
  PIPELINE_BUCKET: R2Bucket
  BENCHMARK_BUCKET: R2Bucket
  CATALOG: DurableObjectNamespace<CatalogDO>
  SUBSCRIPTIONS: DurableObjectNamespace<SubscriptionDO>
  CDC_PROCESSOR: DurableObjectNamespace<CDCProcessorDO>
  EVENT_WRITER: DurableObjectNamespace<EventWriterDO>
  RATE_LIMITER: DurableObjectNamespace<RateLimiterDO>
  EVENTS_QUEUE?: Queue<EventBatch>
  /** When true and EVENTS_QUEUE is bound, use queue for CDC/subscription fanout instead of direct DO calls */
  USE_QUEUE_FANOUT?: string
  AUTH_TOKEN?: string
  /** When true, allows unauthenticated access to /ingest endpoint. Only use for development/testing. */
  ALLOW_UNAUTHENTICATED_INGEST?: string
  AUTH: AuthBinding
  OAUTH: Fetcher
  ENVIRONMENT: string
  ALLOWED_ORIGINS?: string
  TAIL_AUTH_SECRET?: string
  /** Max requests per minute for /ingest endpoint (default: 1000) */
  RATE_LIMIT_REQUESTS_PER_MINUTE?: string
  /** Max events per minute for /ingest endpoint (default: 100000) */
  RATE_LIMIT_EVENTS_PER_MINUTE?: string
}
