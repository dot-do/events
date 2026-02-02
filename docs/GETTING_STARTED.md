# Getting Started with events.do

This guide will walk you through setting up event streaming, CDC (Change Data Capture), and analytics for your Cloudflare Durable Objects using `@dotdo/events`.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Basic Setup with a Durable Object](#basic-setup-with-a-durable-object)
4. [Adding EventEmitter to a DO](#adding-eventemitter-to-a-do)
5. [Using CDCCollection for Change Tracking](#using-cdccollection-for-change-tracking)
6. [Querying Events with the API](#querying-events-with-the-api)
7. [Setting Up Webhooks](#setting-up-webhooks)
8. [Browser SDK Usage](#browser-sdk-usage)
9. [Complete Example](#complete-example)

---

## Prerequisites

Before you begin, make sure you have:

- **Node.js** (v18 or later) - [Download Node.js](https://nodejs.org/)
- **pnpm** (recommended) or npm - Install pnpm with `npm install -g pnpm`
- **Cloudflare account** - [Sign up for Cloudflare](https://dash.cloudflare.com/sign-up)
- **Wrangler CLI** - Install with `pnpm add -g wrangler`

Verify your setup:

```bash
node --version    # Should be v18+
pnpm --version    # Should be v8+
wrangler --version
```

Log in to Cloudflare:

```bash
wrangler login
```

---

## Installation

Add `@dotdo/events` to your Cloudflare Workers project:

```bash
pnpm add @dotdo/events
```

Or with npm:

```bash
npm install @dotdo/events
```

---

## Basic Setup with a Durable Object

If you are new to Durable Objects, here is a minimal example of a Worker with a Durable Object:

### 1. Create your project structure

```
my-project/
  src/
    index.ts      # Worker entrypoint
    my-do.ts      # Durable Object class
  wrangler.jsonc  # Wrangler configuration
  package.json
  tsconfig.json
```

### 2. Configure wrangler.jsonc

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-worker",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-25",
  "compatibility_flags": ["nodejs_compat"],

  "durable_objects": {
    "bindings": [
      { "name": "MY_DO", "class_name": "MyDO" }
    ]
  },

  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["MyDO"] }
  ]
}
```

### 3. Create your Durable Object (src/my-do.ts)

```typescript
import { DurableObject } from 'cloudflare:workers'

interface Env {
  MY_DO: DurableObjectNamespace
}

export class MyDO extends DurableObject<Env> {
  async sayHello(name: string): Promise<string> {
    return `Hello, ${name}!`
  }
}
```

### 4. Create the Worker entrypoint (src/index.ts)

```typescript
import { MyDO } from './my-do'

interface Env {
  MY_DO: DurableObjectNamespace
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.MY_DO.idFromName('singleton')
    const stub = env.MY_DO.get(id)
    const greeting = await stub.sayHello('World')
    return new Response(greeting)
  }
}

export { MyDO }
```

---

## Adding EventEmitter to a DO

Now let us add event streaming to your Durable Object. The `EventEmitter` class batches events and sends them to events.do for storage and analytics.

### Update your Durable Object

```typescript
import { DurableObject } from 'cloudflare:workers'
import { EventEmitter } from '@dotdo/events'

interface Env {
  MY_DO: DurableObjectNamespace
  EVENTS_API_KEY?: string      // Optional: API key for events.do
  EVENTS_BUCKET?: R2Bucket     // Optional: R2 bucket for lakehouse storage
}

export class MyDO extends DurableObject<Env> {
  // Create the EventEmitter instance
  events = new EventEmitter(this.ctx, this.env, {
    // Enable CDC (Change Data Capture) for collections
    cdc: true,
    // Include previous document state in update events (enables diffs)
    trackPrevious: true,
    // Optional: Stream events to R2 for lakehouse analytics
    r2Bucket: this.env.EVENTS_BUCKET,
    // Optional: API key for events.do authentication
    apiKey: this.env.EVENTS_API_KEY,
  })

  async fetch(request: Request): Promise<Response> {
    // Enrich events with request context (colo, worker name, etc.)
    this.events.enrichFromRequest(request)

    // Your request handling logic here
    return new Response('OK')
  }

  async doSomething(userId: string, action: string): Promise<void> {
    // Emit a custom event (non-blocking, batched)
    this.events.emit({
      type: 'custom.user_action',
      data: {
        userId,
        action,
        timestamp: new Date().toISOString(),
      },
    })
  }

  // REQUIRED: Forward alarm to EventEmitter for retry handling
  async alarm(): Promise<void> {
    await this.events.handleAlarm()
  }

  // OPTIONAL: Persist events before hibernation
  async webSocketClose(ws: WebSocket): Promise<void> {
    await this.events.persistBatch()
  }
}
```

### Event Types

The `@dotdo/events` package supports several event types:

| Type | Description | Example |
|------|-------------|---------|
| `rpc.call` | RPC method invocations | Method name, duration, success/error |
| `collection.insert` | New document created | Collection name, document ID, document |
| `collection.update` | Document updated | Collection name, document ID, new/previous doc |
| `collection.delete` | Document deleted | Collection name, document ID, previous doc |
| `do.create` | DO instance created | Reason |
| `do.alarm` | Alarm triggered | - |
| `ws.connect` | WebSocket connected | Connection count |
| `ws.message` | WebSocket message | - |
| `custom.*` | Your custom events | Any data |

---

## Using CDCCollection for Change Tracking

The `CDCCollection` wrapper automatically emits CDC events whenever you modify documents. This is perfect for building audit logs, syncing data, or enabling time-travel queries.

### Wrap your collections

```typescript
import { DurableObject } from 'cloudflare:workers'
import { EventEmitter, CDCCollection } from '@dotdo/events'

// Define your document type
interface User {
  name: string
  email: string
  role: 'admin' | 'user'
  active: boolean
  createdAt: string
  updatedAt: string
}

interface Env {
  MY_DO: DurableObjectNamespace
}

export class UserService extends DurableObject<Env> {
  // Create EventEmitter with CDC enabled
  events = new EventEmitter(this.ctx, this.env, {
    cdc: true,
    trackPrevious: true,  // Include previous doc in updates
  })

  // Wrap your collection with CDCCollection
  // Note: this.collection() is available when extending DurableRPC from @dotdo/rpc
  // For vanilla DOs, you would use SQLite directly
  users = new CDCCollection<User>(
    this.collection<User>('users'),  // The underlying collection
    this.events,                      // EventEmitter instance
    'users'                           // Collection name for events
  )

  async createUser(data: { name: string; email: string; role?: 'admin' | 'user' }): Promise<string> {
    const id = crypto.randomUUID()
    const now = new Date().toISOString()

    // This automatically emits a collection.insert event
    this.users.put(id, {
      name: data.name,
      email: data.email,
      role: data.role ?? 'user',
      active: true,
      createdAt: now,
      updatedAt: now,
    })

    return id
  }

  async updateUser(id: string, updates: Partial<User>): Promise<User | null> {
    const existing = this.users.get(id)
    if (!existing) return null

    const updated = {
      ...existing,
      ...updates,
      updatedAt: new Date().toISOString(),
    }

    // This automatically emits a collection.update event
    // with both the new document and previous document
    this.users.put(id, updated)

    return updated
  }

  async deleteUser(id: string): Promise<boolean> {
    // This automatically emits a collection.delete event
    // with the previous document included
    return this.users.delete(id)
  }

  async getUser(id: string): Promise<User | null> {
    // Read operations do not emit events
    return this.users.get(id)
  }

  async listUsers(): Promise<User[]> {
    // Read operations do not emit events
    return this.users.list()
  }

  async alarm(): Promise<void> {
    await this.events.handleAlarm()
  }
}
```

### CDCCollection Methods

| Method | Emits Event | Event Type |
|--------|-------------|------------|
| `get(id)` | No | - |
| `put(id, doc)` | Yes | `collection.insert` or `collection.update` |
| `delete(id)` | Yes | `collection.delete` |
| `has(id)` | No | - |
| `find(filter)` | No | - |
| `list()` | No | - |
| `keys()` | No | - |
| `clear()` | Yes | `collection.delete` for each document |
| `putMany(docs)` | Yes | One event per document |
| `deleteMany(ids)` | Yes | One event per document |

---

## Querying Events with the API

Once events are flowing to events.do, you can query them using the API. The API returns DuckDB SQL queries that you can execute against your R2 lakehouse.

### Query endpoint

```bash
# Get events for a specific date range
curl -X POST https://events.do/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{
    "dateRange": {
      "start": "2026-01-01T00:00:00Z",
      "end": "2026-01-31T23:59:59Z"
    },
    "eventTypes": ["collection.update"],
    "collection": "users",
    "limit": 100
  }'
```

### Query parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dateRange` | `{ start, end }` | Filter by timestamp range (ISO 8601) |
| `doId` | `string` | Filter by Durable Object ID |
| `doClass` | `string` | Filter by DO class name |
| `eventTypes` | `string[]` | Filter by event types |
| `collection` | `string` | Filter by collection name (CDC events) |
| `colo` | `string` | Filter by Cloudflare colo |
| `limit` | `number` | Maximum results (1-10000) |
| `namespace` | `string` | Multi-tenant namespace |

### Using the query builders in code

```typescript
import { buildQuery, buildHistoryQuery, buildLatencyQuery } from '@dotdo/events'

// Build a general query
const sql = buildQuery({
  bucket: 'my-events',
  dateRange: {
    start: new Date('2026-01-01'),
    end: new Date('2026-01-31')
  },
  eventTypes: ['rpc.call'],
  doClass: 'UserService',
  limit: 1000,
})

// Build a document history query (time-travel)
const historySql = buildHistoryQuery({
  bucket: 'my-events',
  collection: 'users',
  docId: 'user-123',
})

// Build a latency analysis query
const latencySql = buildLatencyQuery({
  bucket: 'my-events',
  doClass: 'UserService',
  method: 'createUser',
})
// Returns: p50, p95, p99, avg, max latency grouped by class and method
```

---

## Setting Up Webhooks

events.do can receive webhooks from popular services and normalize them into a consistent event format.

### Supported webhook providers

- **GitHub** - Repository events (push, pull_request, issues, etc.)
- **Stripe** - Payment events (charge.succeeded, customer.created, etc.)
- **WorkOS** - SSO and directory sync events
- **Slack** - Slash commands and interactive components
- **Linear** - Issue and project events
- **Svix** - Webhook delivery service

### Configuring webhook endpoints

#### Option 1: Query parameter style

```
POST https://events.do/webhooks?provider=github
POST https://events.do/webhooks?provider=stripe
```

#### Option 2: Dedicated domain (if configured)

```
POST https://webhooks.do/github
POST https://webhooks.do/stripe
```

### Setting up webhook secrets

Set your webhook secrets as environment variables:

```bash
wrangler secret put GITHUB_WEBHOOK_SECRET
wrangler secret put STRIPE_WEBHOOK_SECRET
wrangler secret put WORKOS_WEBHOOK_SECRET
```

### Example: GitHub webhook

1. Go to your GitHub repository Settings > Webhooks
2. Add webhook URL: `https://events.do/webhooks?provider=github`
3. Content type: `application/json`
4. Secret: Your `GITHUB_WEBHOOK_SECRET` value
5. Select events you want to receive

### Receiving webhook events in your worker

You can subscribe to webhook events and have them delivered to your worker:

```typescript
// Subscribe to GitHub push events
const result = await subscriptionDO.subscribe({
  workerId: 'my-worker',
  workerBinding: 'MY_HANDLER',  // Optional: use service binding
  pattern: 'webhook.github.push',  // Pattern matching
  rpcMethod: 'handleGitHubPush',
  maxRetries: 5,
  timeoutMs: 30000,
})

// Your handler worker
export class MyHandler {
  async handleGitHubPush(payload: unknown): Promise<void> {
    // Process the GitHub push event
    console.log('Received push:', payload)
  }
}
```

### Subscription patterns

| Pattern | Matches |
|---------|---------|
| `webhook.github.push` | Exact match |
| `webhook.github.*` | Any single-segment GitHub event |
| `webhook.**` | All webhook events |
| `collection.users.*` | All user collection CDC events |
| `**` | All events |

---

## Browser SDK Usage

The `@dotdo/events` package includes a lightweight browser SDK for client-side analytics.

### Installation

The browser SDK is included in the main package:

```typescript
import { init, page, track, identify, flush } from '@dotdo/events/browser'
```

Or use the CDN version:

```html
<script src="https://events.do/sdk.js"></script>
<script>
  Events.init({ endpoint: '/e' })
</script>
```

### Initialize the SDK

```typescript
import { init, page, track, identify } from '@dotdo/events/browser'

// Initialize with configuration
const sdk = init({
  endpoint: '/e',           // Your events endpoint (default: /e)
  batchSize: 10,            // Flush after 10 events (default: 10)
  flushInterval: 5000,      // Flush every 5 seconds (default: 5000)
  maxRetries: 3,            // Retry failed requests 3 times (default: 3)
  maxRetryQueueSize: 100,   // Max events to queue for retry (default: 100)

  // Optional callbacks
  onError: (error, events) => {
    console.error('Failed to send events:', error)
  },
  onSuccess: (events) => {
    console.log('Events sent successfully:', events.length)
  },
})
```

### Track page views

```typescript
// Track a page view (captures URL, path, referrer automatically)
page()

// Track with custom properties
page({
  section: 'blog',
  author: 'john-doe',
})
```

### Track custom events

```typescript
// Track a button click
track('button_clicked', {
  buttonId: 'signup-cta',
  location: 'header',
})

// Track a form submission
track('form_submitted', {
  formId: 'contact-form',
  fields: ['name', 'email', 'message'],
})

// Track a purchase
track('purchase_completed', {
  orderId: 'order-123',
  total: 99.99,
  currency: 'USD',
  items: 3,
})
```

### Identify users

```typescript
// Identify a user after login
identify('user-123', {
  email: 'user@example.com',
  name: 'John Doe',
  plan: 'pro',
  createdAt: '2026-01-15T10:00:00Z',
})
```

### Manual flush

```typescript
import { flush } from '@dotdo/events/browser'

// Force send all pending events (useful before navigation)
await flush()
```

### Automatic behaviors

The browser SDK automatically:

- Generates an anonymous ID (stored in localStorage)
- Generates a session ID (stored in sessionStorage)
- Batches events and flushes periodically
- Flushes on page hide/visibility change
- Retries failed requests with exponential backoff
- Persists failed events for recovery on next page load

---

## Complete Example

Here is a complete example that ties everything together:

### wrangler.jsonc

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-25",
  "compatibility_flags": ["nodejs_compat"],

  "r2_buckets": [
    { "binding": "EVENTS_BUCKET", "bucket_name": "my-events" }
  ],

  "durable_objects": {
    "bindings": [
      { "name": "TASKS", "class_name": "TasksDO" }
    ]
  },

  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["TasksDO"] }
  ],

  "vars": {
    "ENVIRONMENT": "production"
  }
}
```

### src/tasks-do.ts

```typescript
import { DurableObject } from 'cloudflare:workers'
import { EventEmitter, CDCCollection } from '@dotdo/events'

interface Task {
  title: string
  description: string
  status: 'pending' | 'in_progress' | 'completed'
  priority: 'low' | 'medium' | 'high'
  assigneeId?: string
  createdAt: string
  updatedAt: string
  completedAt?: string
}

interface Env {
  TASKS: DurableObjectNamespace
  EVENTS_BUCKET?: R2Bucket
  EVENTS_API_KEY?: string
}

export class TasksDO extends DurableObject<Env> {
  private sql = this.ctx.storage.sql

  // Initialize EventEmitter
  events = new EventEmitter(this.ctx, this.env, {
    cdc: true,
    trackPrevious: true,
    r2Bucket: this.env.EVENTS_BUCKET,
    apiKey: this.env.EVENTS_API_KEY,
  })

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.initSchema()
  }

  private initSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_tasks_updated ON tasks(updated_at);
    `)
  }

  // Simple collection wrapper for CDCCollection
  private taskCollection = {
    get: (id: string): Task | null => {
      const row = this.sql.exec('SELECT data FROM tasks WHERE id = ?', id).one()
      return row ? JSON.parse(row.data as string) : null
    },
    put: (id: string, doc: Task): void => {
      const now = Date.now()
      this.sql.exec(
        `INSERT OR REPLACE INTO tasks (id, data, created_at, updated_at)
         VALUES (?, ?, COALESCE((SELECT created_at FROM tasks WHERE id = ?), ?), ?)`,
        id, JSON.stringify(doc), id, now, now
      )
    },
    delete: (id: string): boolean => {
      const result = this.sql.exec('DELETE FROM tasks WHERE id = ?', id)
      return result.rowsWritten > 0
    },
    has: (id: string): boolean => {
      return this.sql.exec('SELECT 1 FROM tasks WHERE id = ? LIMIT 1', id).one() !== null
    },
    find: (): Task[] => this.list(),
    count: (): number => {
      const row = this.sql.exec('SELECT COUNT(*) as count FROM tasks').one()
      return row ? Number(row.count) : 0
    },
    list: (): Task[] => {
      return this.sql.exec('SELECT data FROM tasks ORDER BY updated_at DESC')
        .toArray()
        .map(row => JSON.parse(row.data as string))
    },
    keys: (): string[] => {
      return this.sql.exec('SELECT id FROM tasks')
        .toArray()
        .map(row => row.id as string)
    },
    clear: (): number => {
      const count = this.count()
      this.sql.exec('DELETE FROM tasks')
      return count
    },
  }

  // Wrap with CDCCollection for automatic change tracking
  tasks = new CDCCollection<Task>(this.taskCollection, this.events, 'tasks')

  async fetch(request: Request): Promise<Response> {
    this.events.enrichFromRequest(request)

    const url = new URL(request.url)

    try {
      if (request.method === 'GET' && url.pathname === '/tasks') {
        return Response.json(this.tasks.list())
      }

      if (request.method === 'GET' && url.pathname.startsWith('/tasks/')) {
        const id = url.pathname.split('/')[2]
        const task = this.tasks.get(id)
        if (!task) {
          return Response.json({ error: 'Task not found' }, { status: 404 })
        }
        return Response.json(task)
      }

      if (request.method === 'POST' && url.pathname === '/tasks') {
        const body = await request.json() as Partial<Task>
        const id = crypto.randomUUID()
        const now = new Date().toISOString()

        const task: Task = {
          title: body.title || 'Untitled',
          description: body.description || '',
          status: 'pending',
          priority: body.priority || 'medium',
          assigneeId: body.assigneeId,
          createdAt: now,
          updatedAt: now,
        }

        // Automatically emits collection.insert event
        this.tasks.put(id, task)

        // Emit custom business event
        this.events.emit({
          type: 'custom.task_created',
          data: { taskId: id, priority: task.priority },
        })

        return Response.json({ id, ...task }, { status: 201 })
      }

      if (request.method === 'PATCH' && url.pathname.startsWith('/tasks/')) {
        const id = url.pathname.split('/')[2]
        const existing = this.tasks.get(id)
        if (!existing) {
          return Response.json({ error: 'Task not found' }, { status: 404 })
        }

        const updates = await request.json() as Partial<Task>
        const wasCompleted = existing.status !== 'completed' && updates.status === 'completed'

        const updated: Task = {
          ...existing,
          ...updates,
          updatedAt: new Date().toISOString(),
          completedAt: wasCompleted ? new Date().toISOString() : existing.completedAt,
        }

        // Automatically emits collection.update event with previous doc
        this.tasks.put(id, updated)

        // Emit custom event for completion
        if (wasCompleted) {
          this.events.emit({
            type: 'custom.task_completed',
            data: { taskId: id, completedAt: updated.completedAt },
          })
        }

        return Response.json({ id, ...updated })
      }

      if (request.method === 'DELETE' && url.pathname.startsWith('/tasks/')) {
        const id = url.pathname.split('/')[2]

        // Automatically emits collection.delete event
        const deleted = this.tasks.delete(id)

        if (!deleted) {
          return Response.json({ error: 'Task not found' }, { status: 404 })
        }

        return Response.json({ ok: true })
      }

      return Response.json({ error: 'Not found' }, { status: 404 })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      return Response.json({ error: message }, { status: 500 })
    }
  }

  // Required: Handle alarms for event retry
  async alarm(): Promise<void> {
    await this.events.handleAlarm()
  }
}
```

### src/index.ts

```typescript
import { TasksDO } from './tasks-do'

interface Env {
  TASKS: DurableObjectNamespace
  EVENTS_BUCKET?: R2Bucket
  EVENTS_API_KEY?: string
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    // Route to Tasks DO
    if (url.pathname.startsWith('/tasks')) {
      const id = env.TASKS.idFromName('default')
      const stub = env.TASKS.get(id)
      return stub.fetch(request)
    }

    // Serve browser SDK example
    if (url.pathname === '/') {
      return new Response(getIndexHtml(), {
        headers: { 'Content-Type': 'text/html' },
      })
    }

    // Handle browser events
    if (url.pathname === '/e' && request.method === 'POST') {
      const body = await request.json()
      console.log('Browser events:', body)
      return Response.json({ ok: true })
    }

    return new Response('Not found', { status: 404 })
  },
}

function getIndexHtml(): string {
  return `<!DOCTYPE html>
<html>
<head>
  <title>Tasks App</title>
  <script type="module">
    import { init, page, track } from '@dotdo/events/browser'

    init({ endpoint: '/e' })
    page()

    document.getElementById('create').onclick = () => {
      track('button_clicked', { button: 'create_task' })
    }
  </script>
</head>
<body>
  <h1>Tasks</h1>
  <button id="create">Create Task</button>
</body>
</html>`
}

export { TasksDO }
```

### Running the example

```bash
# Install dependencies
pnpm install

# Start local development
pnpm dev

# Test the API
curl http://localhost:8787/tasks
curl -X POST http://localhost:8787/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "My first task", "priority": "high"}'

# Deploy to production
pnpm deploy
```

---

## Next Steps

- Read the [API Reference](/core/README.md) for detailed documentation
- Learn about [Operations](/docs/OPERATIONS.md) for monitoring and maintenance
- Set up [R2 Lakehouse](/docs/LAKEHOUSE.md) for analytics (coming soon)
- Configure [Subscriptions](/docs/SUBSCRIPTIONS.md) for event-driven workflows (coming soon)

## Support

- GitHub Issues: [github.com/dotdo/events/issues](https://github.com/dotdo/events/issues)
- Documentation: [events.do/docs](https://events.do/docs)
