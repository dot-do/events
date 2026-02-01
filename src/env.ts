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
import type { WebhookEnv } from './webhook-handler'
import type { EventBatch } from '@dotdo/events'
import type { AuthBinding } from 'oauth.do/rpc'
import type { AuthRequest } from 'oauth.do/rpc/itty'

export type { WebhookEnv, AuthRequest }

export interface Env extends WebhookEnv {
  EVENTS_BUCKET: R2Bucket
  PIPELINE_BUCKET: R2Bucket
  BENCHMARK_BUCKET: R2Bucket
  CATALOG: DurableObjectNamespace<CatalogDO>
  SUBSCRIPTIONS: DurableObjectNamespace<SubscriptionDO>
  CDC_PROCESSOR: DurableObjectNamespace<CDCProcessorDO>
  EVENT_WRITER: DurableObjectNamespace<EventWriterDO>
  EVENTS_QUEUE?: Queue<EventBatch>
  AUTH_TOKEN?: string
  AUTH: AuthBinding
  OAUTH: Fetcher
  ENVIRONMENT: string
  ALLOWED_ORIGINS?: string
  TAIL_AUTH_SECRET?: string
}
