/**
 * Project templates for events init command
 */

export const TEMPLATES = {
  common: {
    tsconfig: () => `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "lib": ["ES2022"],
    "types": ["@cloudflare/workers-types"]
  },
  "include": ["src/**/*.ts"],
  "exclude": ["node_modules"]
}
`,

    gitignore: () => `# Dependencies
node_modules/
.pnpm-store/

# Build outputs
dist/
.wrangler/

# Environment files
.env
.env.local
.dev.vars

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log
`,

    eventSchema: () => JSON.stringify(
      {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        $id: 'events.json',
        title: 'Event Schema',
        description: 'Base schema for events.do events',
        type: 'object',
        properties: {
          type: {
            type: 'string',
            description: 'Event type identifier',
          },
          ts: {
            type: 'string',
            format: 'date-time',
            description: 'ISO 8601 timestamp',
          },
          do: {
            type: 'object',
            properties: {
              id: { type: 'string' },
              name: { type: 'string' },
              class: { type: 'string' },
            },
            required: ['id'],
          },
        },
        required: ['type', 'ts', 'do'],
      },
      null,
      2
    ),
  },

  basic: {
    packageJson: (name: string) => ({
      name,
      version: '0.1.0',
      private: true,
      type: 'module',
      scripts: {
        dev: 'wrangler dev',
        deploy: 'wrangler deploy',
        test: 'vitest run',
      },
      dependencies: {
        '@dotdo/events': '^0.1.0',
      },
      devDependencies: {
        '@cloudflare/workers-types': '^4.20250121.0',
        typescript: '^5.7.3',
        vitest: '^2.1.8',
        wrangler: '^4.0.0',
      },
    }),

    wranglerConfig: (name: string) => `{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "${name}",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-25",
  "compatibility_flags": ["nodejs_compat"],

  // Enable workers.dev subdomain
  "workers_dev": true,

  // R2 bucket for event storage
  "r2_buckets": [
    { "binding": "EVENTS_BUCKET", "bucket_name": "${name}-events" }
  ],

  // Development environment
  "env": {
    "dev": {
      "vars": {
        "ENVIRONMENT": "development"
      }
    }
  }
}
`,

    indexTs: () => `import { EventEmitter, type DurableEvent } from '@dotdo/events'

export interface Env {
  EVENTS_BUCKET: R2Bucket
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    // Event ingestion endpoint
    if ((url.pathname === '/e' || url.pathname === '/ingest') && request.method === 'POST') {
      try {
        const events = await request.json() as { events: DurableEvent[] }

        // Store events in R2
        const key = \`events/\${new Date().toISOString().split('T')[0]}/\${crypto.randomUUID()}.jsonl\`
        const content = events.events.map(e => JSON.stringify(e)).join('\\n')
        await env.EVENTS_BUCKET.put(key, content)

        return Response.json({ success: true, count: events.events.length })
      } catch (error) {
        return Response.json({ error: 'Invalid event payload' }, { status: 400 })
      }
    }

    // Health check
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', timestamp: new Date().toISOString() })
    }

    return new Response('events.do - Event Streaming Service', {
      headers: { 'Content-Type': 'text/plain' },
    })
  },
}
`,
  },

  cdc: {
    packageJson: (name: string) => ({
      name,
      version: '0.1.0',
      private: true,
      type: 'module',
      scripts: {
        dev: 'wrangler dev',
        deploy: 'wrangler deploy',
        test: 'vitest run',
      },
      dependencies: {
        '@dotdo/events': '^0.1.0',
      },
      devDependencies: {
        '@cloudflare/workers-types': '^4.20250121.0',
        typescript: '^5.7.3',
        vitest: '^2.1.8',
        wrangler: '^4.0.0',
      },
    }),

    wranglerConfig: (name: string) => `{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "${name}",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-25",
  "compatibility_flags": ["nodejs_compat"],

  // Enable workers.dev subdomain
  "workers_dev": true,

  // R2 bucket for event/CDC storage
  "r2_buckets": [
    { "binding": "EVENTS_BUCKET", "bucket_name": "${name}-events" }
  ],

  // Durable Objects for CDC
  "durable_objects": {
    "bindings": [
      { "name": "ITEMS", "class_name": "ItemsDO" }
    ]
  },

  // Migrations
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["ItemsDO"] }
  ],

  // Development environment
  "env": {
    "dev": {
      "vars": {
        "ENVIRONMENT": "development"
      }
    }
  }
}
`,

    indexTs: () => `import { EventEmitter, CDCCollection, type DurableEvent, type PipelineLike } from '@dotdo/events'
import { DurableObject } from 'cloudflare:workers'

export interface Env {
  EVENTS_PIPELINE: PipelineLike
  EVENTS_BUCKET: R2Bucket
  ITEMS: DurableObjectNamespace<ItemsDO>
}

interface Item {
  id: string
  name: string
  status: 'active' | 'archived'
  createdAt: string
  updatedAt: string
}

/**
 * Example Durable Object with CDC-enabled collection
 */
export class ItemsDO extends DurableObject<Env> {
  private items: CDCCollection<Item>
  private emitter: EventEmitter

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    // Initialize CDC collection
    this.items = new CDCCollection<Item>(ctx, 'items')

    // Initialize event emitter — Pipeline-first, alarm retry on failure
    this.emitter = new EventEmitter(
      env.EVENTS_PIPELINE,
      { cdc: true, trackPrevious: true },
      ctx,
    )
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // List items
    if (request.method === 'GET' && url.pathname === '/items') {
      const items = this.items.list()
      return Response.json({ items })
    }

    // Create item
    if (request.method === 'POST' && url.pathname === '/items') {
      const data = await request.json() as Partial<Item>
      const item: Item = {
        id: crypto.randomUUID(),
        name: data.name || 'Untitled',
        status: 'active',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      }

      // Insert with CDC tracking
      this.items.insert(item.id, item)

      // Emit CDC event
      await this.emitter.emit({
        type: 'collection.insert',
        collection: 'items',
        docId: item.id,
        doc: item,
      })

      return Response.json({ item })
    }

    // Update item
    if (request.method === 'PUT' && url.pathname.startsWith('/items/')) {
      const id = url.pathname.split('/')[2]
      const data = await request.json() as Partial<Item>

      const prev = this.items.get(id)
      if (!prev) {
        return Response.json({ error: 'Item not found' }, { status: 404 })
      }

      const updated: Item = {
        ...prev,
        ...data,
        id: prev.id,
        updatedAt: new Date().toISOString(),
      }

      // Update with CDC tracking
      this.items.update(id, updated)

      // Emit CDC event
      await this.emitter.emit({
        type: 'collection.update',
        collection: 'items',
        docId: id,
        doc: updated,
        prev,
      })

      return Response.json({ item: updated })
    }

    return new Response('Not Found', { status: 404 })
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    // Route to Durable Object
    if (url.pathname.startsWith('/items')) {
      const id = env.ITEMS.idFromName('default')
      const stub = env.ITEMS.get(id)
      return stub.fetch(request)
    }

    // Health check
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', timestamp: new Date().toISOString() })
    }

    return new Response('events.do - CDC Example', {
      headers: { 'Content-Type': 'text/plain' },
    })
  },
}
`,
  },

  analytics: {
    packageJson: (name: string) => ({
      name,
      version: '0.1.0',
      private: true,
      type: 'module',
      scripts: {
        dev: 'wrangler dev',
        deploy: 'wrangler deploy',
        test: 'vitest run',
      },
      dependencies: {
        '@dotdo/events': '^0.1.0',
        'hyparquet': '^1.24.1',
      },
      devDependencies: {
        '@cloudflare/workers-types': '^4.20250121.0',
        typescript: '^5.7.3',
        vitest: '^2.1.8',
        wrangler: '^4.0.0',
      },
    }),

    wranglerConfig: (name: string) => `{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "${name}",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-25",
  "compatibility_flags": ["nodejs_compat"],

  // Enable workers.dev subdomain
  "workers_dev": true,

  // R2 buckets for lakehouse
  "r2_buckets": [
    { "binding": "EVENTS_BUCKET", "bucket_name": "${name}-events" },
    { "binding": "LAKEHOUSE_BUCKET", "bucket_name": "${name}-lakehouse" }
  ],

  // Analytics Engine for metrics
  "analytics_engine_datasets": [
    { "binding": "ANALYTICS", "dataset": "${name}_metrics" }
  ],

  // Durable Objects
  "durable_objects": {
    "bindings": [
      { "name": "CATALOG", "class_name": "CatalogDO" }
    ]
  },

  // Migrations
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["CatalogDO"] }
  ],

  // Scheduled compaction
  "triggers": {
    "crons": ["0 * * * *"]
  },

  // Development environment
  "env": {
    "dev": {
      "vars": {
        "ENVIRONMENT": "development"
      }
    }
  }
}
`,

    indexTs: () => `import {
  EventEmitter,
  CatalogDO as BaseCatalogDO,
  buildQuery,
  type DurableEvent,
  type QueryOptions,
} from '@dotdo/events'
import { DurableObject } from 'cloudflare:workers'

export interface Env {
  EVENTS_BUCKET: R2Bucket
  LAKEHOUSE_BUCKET: R2Bucket
  ANALYTICS: AnalyticsEngineDataset
  CATALOG: DurableObjectNamespace<CatalogDO>
}

/**
 * Catalog DO for Iceberg-style metadata management
 */
export class CatalogDO extends BaseCatalogDO {}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    // Event ingestion
    if ((url.pathname === '/e' || url.pathname === '/ingest') && request.method === 'POST') {
      try {
        const { events } = await request.json() as { events: DurableEvent[] }

        // Store events in lakehouse format
        const date = new Date()
        const key = \`events/year=\${date.getUTCFullYear()}/month=\${String(date.getUTCMonth() + 1).padStart(2, '0')}/day=\${String(date.getUTCDate()).padStart(2, '0')}/hour=\${String(date.getUTCHours()).padStart(2, '0')}/\${crypto.randomUUID()}.jsonl\`

        const content = events.map(e => JSON.stringify(e)).join('\\n')
        await env.LAKEHOUSE_BUCKET.put(key, content)

        // Write analytics
        for (const event of events) {
          env.ANALYTICS.writeDataPoint({
            blobs: [event.type, event.do?.class || 'unknown'],
            doubles: [1],
            indexes: [event.do?.id || 'unknown'],
          })
        }

        return Response.json({ success: true, count: events.length, key })
      } catch (error) {
        return Response.json({ error: 'Invalid event payload' }, { status: 400 })
      }
    }

    // Query endpoint - generates DuckDB SQL
    if (url.pathname === '/query' && request.method === 'POST') {
      try {
        const options = await request.json() as QueryOptions

        // Build the DuckDB query
        const sql = buildQuery(options)

        return Response.json({
          sql,
          note: 'Execute this SQL with DuckDB against the R2 lakehouse bucket',
        })
      } catch (error) {
        return Response.json({ error: 'Invalid query options' }, { status: 400 })
      }
    }

    // List recent events
    if (url.pathname === '/events' && request.method === 'GET') {
      const limit = parseInt(url.searchParams.get('limit') || '100')
      const prefix = url.searchParams.get('prefix') || 'events/'

      const listed = await env.LAKEHOUSE_BUCKET.list({ prefix, limit: 10 })
      const events: DurableEvent[] = []

      for (const obj of listed.objects.slice(0, 5)) {
        const content = await env.LAKEHOUSE_BUCKET.get(obj.key)
        if (content) {
          const text = await content.text()
          const lines = text.split('\\n').filter(Boolean)
          for (const line of lines) {
            if (events.length >= limit) break
            try {
              events.push(JSON.parse(line))
            } catch {
              // Skip invalid lines
            }
          }
        }
        if (events.length >= limit) break
      }

      return Response.json({ events, count: events.length })
    }

    // Health check
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', timestamp: new Date().toISOString() })
    }

    return new Response('events.do - Analytics Lakehouse', {
      headers: { 'Content-Type': 'text/plain' },
    })
  },

  // Scheduled compaction
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    console.log('Running scheduled compaction...')
    // TODO: Implement hourly -> daily compaction
  },
}
`,
  },
}
