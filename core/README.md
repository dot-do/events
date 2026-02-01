# @dotdo/events

Event streaming, CDC (Change Data Capture), and lakehouse analytics for Cloudflare Durable Objects.

[![npm version](https://img.shields.io/npm/v/@dotdo/events.svg)](https://www.npmjs.com/package/@dotdo/events)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Installation

```bash
pnpm add @dotdo/events
```

## Quick Start

```typescript
import { EventEmitter } from '@dotdo/events'

export class MyDO extends DurableObject {
  events = new EventEmitter(this.ctx, this.env, {
    cdc: true,
  })

  async doSomething() {
    // Emit custom events
    this.events.emit({ type: 'user.action', userId: '123', action: 'click' })
  }

  // Required: forward alarm to EventEmitter for retry handling
  async alarm() {
    await this.events.handleAlarm()
  }
}
```

## Features

- **Event batching and streaming** - Batched emission to [events.do](https://events.do) with configurable batch size and flush intervals
- **CDC (Change Data Capture)** - Automatic change events for collection mutations with SQLite bookmarks for PITR (Point-in-Time Recovery)
- **R2 lakehouse storage** - Stream events to R2 in Parquet-friendly JSON Lines format organized by time partitions
- **DuckDB query generation** - Built-in query builders for analytics, time-travel, and latency analysis
- **Alarm-based retry** - Reliable delivery with exponential backoff using DO alarms
- **Hibernation support** - Persist and restore event batches across hibernation cycles

## API Reference

### EventEmitter

The core class for emitting events from Durable Objects.

```typescript
import { EventEmitter } from '@dotdo/events'

const events = new EventEmitter(ctx, env, options)
```

#### Constructor

```typescript
new EventEmitter(
  ctx: DurableObjectState,
  env: Record<string, unknown>,
  options?: EventEmitterOptions
)
```

#### Methods

| Method | Description |
|--------|-------------|
| `emit(event)` | Emit a custom event (batched, non-blocking) |
| `emitChange(type, collection, docId, doc?, prev?)` | Emit a CDC event for collection changes |
| `flush()` | Manually flush pending events to endpoint |
| `handleAlarm()` | Handle alarm for retry - call from your DO's `alarm()` method |
| `enrichFromRequest(request)` | Enrich event identity from incoming request (colo, worker, etc.) |
| `persistBatch()` | Persist pending batch before hibernation |

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `pendingCount` | `number` | Number of events in current batch |
| `doIdentity` | `object` | Current DO identity info |

#### Example

```typescript
export class ChatRoom extends DurableObject {
  events = new EventEmitter(this.ctx, this.env, {
    cdc: true,
    trackPrevious: true,
    r2Bucket: this.env.EVENTS_BUCKET,
  })

  async fetch(request: Request) {
    // Enrich events with request context
    this.events.enrichFromRequest(request)
    // ... handle request
  }

  async sendMessage(userId: string, text: string) {
    this.events.emit({
      type: 'message.sent',
      userId,
      textLength: text.length,
      roomId: this.ctx.id.toString(),
    })
  }

  async alarm() {
    await this.events.handleAlarm()
  }

  async webSocketClose(ws: WebSocket) {
    // Persist events before hibernation
    await this.events.persistBatch()
  }
}
```

### CDCCollection

Wraps a collection with automatic CDC event emission for all mutations.

```typescript
import { CDCCollection } from '@dotdo/events'

const users = new CDCCollection<User>(
  this.collection<User>('users'),
  this.events,
  'users'
)
```

#### Constructor

```typescript
new CDCCollection<T>(
  collection: Collection<T>,
  emitter: EventEmitter,
  name: string
)
```

#### Methods

| Method | Description | Emits Event |
|--------|-------------|-------------|
| `get(id)` | Get document by ID | No |
| `put(id, doc)` | Insert or update document | `collection.insert` or `collection.update` |
| `delete(id)` | Delete document | `collection.delete` |
| `has(id)` | Check if document exists | No |
| `find(filter?, options?)` | Find documents matching filter | No |
| `count(filter?)` | Count documents | No |
| `list(options?)` | List all documents | No |
| `keys()` | Get all document IDs | No |
| `clear()` | Delete all documents | `collection.delete` for each |
| `putMany(docs)` | Bulk insert/update | Event per document |
| `deleteMany(ids)` | Bulk delete | Event per document |

#### Example

```typescript
export class UserService extends DurableRPC {
  events = new EventEmitter(this.ctx, this.env, { cdc: true, trackPrevious: true })
  users = new CDCCollection<User>(this.collection<User>('users'), this.events, 'users')

  async createUser(data: { name: string; email: string }) {
    const id = crypto.randomUUID()
    // Automatically emits collection.insert event
    this.users.put(id, {
      name: data.name,
      email: data.email,
      createdAt: new Date().toISOString(),
    })
    return id
  }

  async updateUser(id: string, updates: Partial<User>) {
    const user = this.users.get(id)
    if (!user) return null
    // Automatically emits collection.update with prev doc
    this.users.put(id, { ...user, ...updates })
    return { ...user, ...updates }
  }
}
```

### Event Types

```typescript
import type {
  DurableEvent,
  BaseEvent,
  RpcCallEvent,
  CollectionChangeEvent,
  LifecycleEvent,
  WebSocketEvent,
  ClientEvent,
} from '@dotdo/events'
```

#### DurableEvent (Union Type)

All events extend `BaseEvent` and include:

```typescript
interface BaseEvent {
  type: string        // Event type identifier
  ts: string          // ISO timestamp
  do: {
    id: string        // DO ID
    name?: string     // DO name (if named)
    class?: string    // DO class name
    colo?: string     // Cloudflare colo
    worker?: string   // Worker name
  }
}
```

#### Event Type Reference

| Type | Description | Additional Fields |
|------|-------------|-------------------|
| `RpcCallEvent` | RPC method call | `method`, `namespace`, `durationMs`, `success`, `error` |
| `CollectionChangeEvent` | CDC mutation | `collection`, `docId`, `doc`, `prev`, `bookmark` |
| `LifecycleEvent` | DO lifecycle | `reason` |
| `WebSocketEvent` | WebSocket activity | `connectionCount`, `code`, `reason` |
| `ClientEvent` | Browser analytics | `event`, `properties`, `traits`, `userId`, `anonymousId` |

### Query Builders

Generate DuckDB queries for analyzing events stored in R2.

```typescript
import { buildQuery, buildHistoryQuery, buildLatencyQuery, buildPITRRangeQuery } from '@dotdo/events'
```

#### buildQuery(options)

Build a general-purpose query for events.

```typescript
const sql = buildQuery({
  bucket: 'my-events',
  dateRange: { start: new Date('2024-01-01'), end: new Date('2024-01-31') },
  eventTypes: ['rpc.call'],
  doClass: 'ChatRoom',
  limit: 1000,
})
```

#### buildHistoryQuery(options)

Reconstruct document history for time-travel queries.

```typescript
const sql = buildHistoryQuery({
  bucket: 'my-events',
  collection: 'users',
  docId: 'user-123',
})
```

#### buildLatencyQuery(options)

Analyze RPC latency with percentiles.

```typescript
const sql = buildLatencyQuery({
  bucket: 'my-events',
  doClass: 'ChatRoom',
  method: 'sendMessage',
})
// Returns p50, p95, p99, avg, max latency grouped by class and method
```

#### buildPITRRangeQuery(options)

Find events between two SQLite bookmarks for point-in-time recovery.

```typescript
const sql = buildPITRRangeQuery({
  bucket: 'my-events',
  startBookmark: 'bookmark-abc',
  endBookmark: 'bookmark-xyz',
  collection: 'users',
})
```

### Snapshot Utilities

Create and restore point-in-time snapshots of collections.

```typescript
import { createSnapshot, restoreSnapshot, listSnapshots, deleteSnapshot } from '@dotdo/events'
```

#### createSnapshot(sql, doId, options)

```typescript
const result = await createSnapshot(this.sql, this.ctx.id.toString(), {
  bucket: this.env.SNAPSHOTS_BUCKET,
  prefix: 'snapshots', // optional
})
// { key: 'snapshots/abc123/2024-01-15T12-34-56-789Z.json', collections: ['users'], totalDocs: 42, timestamp: '...' }
```

#### restoreSnapshot(sql, bucket, snapshotKey)

```typescript
const result = await restoreSnapshot(
  this.sql,
  this.env.SNAPSHOTS_BUCKET,
  'snapshots/abc123/2024-01-15T12-34-56-789Z.json'
)
// { collections: ['users'], totalDocs: 42 }
```

#### listSnapshots(bucket, doId, options?)

```typescript
const snapshots = await listSnapshots(this.env.SNAPSHOTS_BUCKET, this.ctx.id.toString(), {
  limit: 10,
})
```

#### deleteSnapshot(bucket, snapshotKey)

```typescript
await deleteSnapshot(this.env.SNAPSHOTS_BUCKET, 'snapshots/abc123/2024-01-15T12-34-56-789Z.json')
```

## Configuration Options

```typescript
interface EventEmitterOptions {
  /** Endpoint to send events (default: 'https://events.do/ingest') */
  endpoint?: string

  /** Batch size before auto-flush (default: 100) */
  batchSize?: number

  /** Max time to hold events before flush in ms (default: 1000) */
  flushIntervalMs?: number

  /** Enable CDC for collections (default: false) */
  cdc?: boolean

  /** Include previous doc in CDC updates for diffs (default: false) */
  trackPrevious?: boolean

  /** R2 bucket for lakehouse streaming (optional) */
  r2Bucket?: R2Bucket

  /** API key for authentication (optional) */
  apiKey?: string
}
```

## Integration with @dotdo/rpc

When using `@dotdo/rpc`, events integrate seamlessly with the DurableRPC base class:

```typescript
import { DurableRPC } from '@dotdo/rpc'
import { EventEmitter, CDCCollection } from '@dotdo/events'

interface User {
  name: string
  email: string
  role: 'admin' | 'user'
  active: boolean
}

interface Env {
  EVENTS_BUCKET?: R2Bucket
  EVENTS_API_KEY?: string
}

export class MyDO extends DurableRPC {
  // Create event emitter with CDC enabled
  events = new EventEmitter(this.ctx, this.env as Env, {
    cdc: true,
    trackPrevious: true,
    r2Bucket: (this.env as Env).EVENTS_BUCKET,
    apiKey: (this.env as Env).EVENTS_API_KEY,
  })

  // Wrap collections with CDC
  users = new CDCCollection<User>(this.collection<User>('users'), this.events, 'users')

  async createUser(data: { name: string; email: string; role?: 'admin' | 'user' }) {
    const id = crypto.randomUUID()
    const user: User = {
      name: data.name,
      email: data.email,
      role: data.role ?? 'user',
      active: true,
    }

    // CDC event emitted automatically
    this.users.put(id, user)

    // Emit custom business event
    this.events.emit({
      type: 'user.registered',
      userId: id,
      email: user.email,
      role: user.role,
    })

    return user
  }

  async updateUserRole(userId: string, role: 'admin' | 'user') {
    const user = this.users.get(userId)
    if (!user) return null

    // CDC update event with previous doc included
    this.users.put(userId, { ...user, role })
    return { ...user, role }
  }

  // Required: forward alarm to event emitter
  async alarm() {
    await this.events.handleAlarm()
  }

  // Enrich events with request context
  async fetch(request: Request) {
    this.events.enrichFromRequest(request)
    return super.fetch(request)
  }

  // Persist batch before hibernation
  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
    await this.events.persistBatch()
    await super.webSocketClose(ws, code, reason, wasClean)
  }
}
```

## Architecture

```
+----------------+     +------------------+     +------------------+
|  Durable       |     |                  |     |                  |
|  Object        | --> |  EventEmitter    | --> |  events.do       |
|                |     |  (batching)      |     |  (ingestion)     |
+----------------+     +------------------+     +------------------+
       |                       |                        |
       |                       v                        v
       |               +------------------+     +------------------+
       |               |  R2 Bucket       |     |  Analytics       |
       |               |  (lakehouse)     | <-- |  (DuckDB)        |
       |               +------------------+     +------------------+
       v
+------------------+
|  SQLite          |
|  (bookmarks)     |
+------------------+
```

### Data Flow

1. **Event Emission**: Your DO emits events via `EventEmitter.emit()` or CDC wrapper mutations
2. **Batching**: Events are batched in memory (configurable size/interval)
3. **Streaming**: Batches are sent to events.do for real-time processing
4. **Lakehouse**: Optionally, events stream to R2 in time-partitioned JSON Lines format
5. **PITR**: CDC events include SQLite bookmarks for point-in-time recovery
6. **Analytics**: Query events with DuckDB using generated queries

### R2 Storage Format

Events are stored in R2 with the following path structure:

```
events/{year}/{month}/{day}/{hour}/{do_id}_{timestamp}.jsonl
```

Example:
```
events/2024/01/15/14/abc123_1705329600000.jsonl
```

This format is optimized for:
- Time-based partitioning (efficient date range queries)
- Parquet conversion (compatible with lakehouse tools)
- DuckDB glob patterns

## License

MIT
