/**
 * events.do - Event Ingestion, Streaming & Analytics Worker
 *
 * Uses API() from @dotdo/api for the public surface:
 * - Landing page with discovery links
 * - Event browsing via eventsConvention (EVENTS self-binding)
 * - MCP tools for event search, count, and SQL
 *
 * Infrastructure routes (ingest, webhooks, auth, admin) are mounted
 * via the `routes` callback using mountInfraRoutes().
 *
 * Routes:
 * - events.do (primary)
 * - events.workers.do (alias)
 * - webhooks.do (webhook-specific domain)
 *
 * Public Endpoints:
 * - GET / — API discovery
 * - GET /events — Faceted event browsing
 * - GET /commits, /errors, /traces, /webhooks, /analytics, /ai, /cdc, /tail — Category views
 * - GET /mcp — MCP endpoint
 * - POST /e — Event ingestion (canonical)
 * - POST /ingest — Event ingestion (deprecated)
 * - POST /webhooks?provider=xxx — Webhook processing
 * - GET /health — Health check
 *
 * Admin Endpoints (require auth):
 * - GET /recent, /pipeline, /benchmark, /dashboard
 * - POST /query
 * - /catalog/*, /subscriptions/*, /schemas/*, /shards/*
 */

import { API } from '@dotdo/api'
import type { Env } from './env'
import type { EventBatch } from '@dotdo/events'

// Import DOs from local source (not package) for proper bundling
import { CatalogDO } from '../core/src/catalog'
import { SubscriptionDO } from '../core/src/subscription'
import { CDCProcessorDO } from '../core/src/cdc-processor'
import { SchemaRegistryDO } from '../core/src/schema-registry'
import { ClickHouseBufferDO } from '../core/src/ch-buffer-do'
import { EventWriterDO } from './event-writer-do'
import { ShardCoordinatorDO } from './shard-coordinator-do'
import { SubscriptionShardCoordinatorDO } from './subscription-shard-coordinator-do'
import { RateLimiterDO } from './middleware/rate-limiter-do'
import { EventsService } from './events-service'

// Import handlers
import { mountInfraRoutes } from './handlers/infra-routes'
import { handleQueue } from './handlers/queue'
import { handleScheduled } from './handlers/scheduled'

// Re-export DOs for wrangler
export { CatalogDO, SubscriptionDO, CDCProcessorDO, SchemaRegistryDO, EventWriterDO, ShardCoordinatorDO, SubscriptionShardCoordinatorDO, RateLimiterDO, ClickHouseBufferDO, EventsService }

// Backward-compat alias — existing consumers that reference BufferService keep working
export { EventsService as BufferService }

// ---------------------------------------------------------------------------
// API() — public surface + infrastructure routes
// ---------------------------------------------------------------------------

const app = API({
  name: 'events.do',
  description: 'Event streaming, ingestion, and analytics for the .do platform',
  version: '0.2.0',
  auth: { mode: 'optional' },
  mcp: { name: 'events.do', version: '0.2.0' },
  events: {
    scope: '*',
    topLevelRoutes: true,
    auth: 'superadmin',
  },
  landing: async (c) => {
    const url = new URL(c.req.url)
    const base = url.origin
    return c.var.respond({
      data: {
        'Ingest Events': `POST ${base}/e`,
        'Commits': `${base}/commits`,
        'Errors': `${base}/errors`,
        'Traces': `${base}/traces`,
        'Webhooks': `${base}/webhooks`,
        'Analytics': `${base}/analytics`,
        'AI Events': `${base}/ai`,
        'CDC Events': `${base}/cdc`,
        'Tail Events': `${base}/tail`,
        'All Events': `${base}/events`,
      },
      key: 'discover',
      links: {
        mcp: `${base}/mcp`,
        ingest: `${base}/e`,
      },
    })
  },
  routes: (app) => mountInfraRoutes(app),
})

// ---------------------------------------------------------------------------
// Worker default export — fetch delegates to API(), scheduled + queue unchanged
// ---------------------------------------------------------------------------

export default {
  fetch: app.fetch,

  scheduled: handleScheduled,

  queue(batch: MessageBatch<EventBatch>, env: Env): Promise<void> {
    return handleQueue(batch, env)
  },
}
