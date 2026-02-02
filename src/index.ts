/**
 * events.do - Event Ingestion Worker
 *
 * This is the main entry point that wires together the handler modules.
 *
 * Routes:
 * - events.do (primary)
 * - events.workers.do (alias)
 * - apis.do/events, apis.do/e (client-side with cookies)
 *
 * Endpoints:
 * - POST /ingest - Receive batched events
 * - POST /webhooks?provider=xxx - Receive and verify webhooks (github, stripe, workos, slack, linear, svix)
 * - GET /health - Health check
 * - POST /query - Generate DuckDB query
 * - GET /recent - List recent events (debug)
 * - GET /pipeline - Check pipeline bucket for Parquet files
 * - GET /catalog/* - Catalog API
 * - /subscriptions/* - Subscription management API
 */

import type { Env } from './env'
import type { EventBatch } from '@dotdo/events'

// Import DOs from local source (not package) for proper bundling
import { CatalogDO } from '../core/src/catalog'
import { SubscriptionDO } from '../core/src/subscription'
import { CDCProcessorDO } from '../core/src/cdc-processor'
import { SchemaRegistryDO } from '../core/src/schema-registry'
import { EventWriterDO } from './event-writer-do'
import { RateLimiterDO } from './middleware/rate-limiter-do'

// Import handlers
import { handleFetch } from './handlers/fetch'
import { handleQueue } from './handlers/queue'
import { handleScheduled } from './handlers/scheduled'

// Re-export DOs for wrangler
export { CatalogDO, SubscriptionDO, CDCProcessorDO, SchemaRegistryDO, EventWriterDO, RateLimiterDO }

export default {
  fetch: handleFetch,

  scheduled: handleScheduled,

  queue(batch: MessageBatch<EventBatch>, env: Env): Promise<void> {
    return handleQueue(batch, env)
  },
}
