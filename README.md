# events.do

Event streaming, CDC (Change Data Capture), and lakehouse analytics for Durable Objects.

Built on [ducklytics](https://github.com/dotdo/duckdb/tree/main/packages/ducklytics) for storage and querying with DuckDB.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DurableRPC Instance                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│  │  RPC Call   │  │ Collection  │  │  Lifecycle  │                         │
│  │   Events    │  │    CDC      │  │   Events    │                         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                         │
│         │         + bookmark│                │                              │
│         └────────────────┼────────────────┘                                 │
│                          ▼                                                  │
│                  ┌───────────────┐                                          │
│                  │ EventEmitter  │ ◄── SQLite bookmark (PITR)               │
│                  │  (batched)    │                                          │
│                  └───────┬───────┘                                          │
│                          │ flush()                                          │
│                          ▼                                                  │
│                  ┌───────────────┐       ┌───────────────┐                  │
│                  │    Alarm      │◄──────│ Retry Queue   │                  │
│                  │ (on failure)  │       │  (storage)    │                  │
│                  └───────┬───────┘       └───────────────┘                  │
└──────────────────────────┼──────────────────────────────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
   ┌───────────────┐               ┌───────────────┐
   │events.workers │               │   R2 Bucket   │
   │   .do/ingest  │               │  (lakehouse)  │
   │  (ducklytics) │               │  YYYY/MM/DD/  │
   └───────┬───────┘               └───────┬───────┘
           │                               │
           ▼                               ▼
   ┌───────────────┐               ┌───────────────┐
   │ DuckDB Query  │◄──────────────│   DuckDB      │
   │   Dashboard   │               │ read_json_auto│
   └───────────────┘               └───────────────┘
```

## Packages

- `@dotdo/events` - EventEmitter SDK for Durable Objects with CDC and PITR
- `events.do` - Cloudflare Worker deployment (uses ducklytics)

## Features

- **Event Batching** - Efficient batched event emission
- **Alarm Retries** - Reliable delivery with DO alarm-based retries
- **CDC** - Change Data Capture for collections with previous doc tracking
- **PITR** - Point-in-time recovery with SQLite bookmark capture
- **R2 Lakehouse** - Stream events to R2 for DuckDB queries
- **Time Travel** - Reconstruct any document's history
- **Subscriptions** - Pattern-based event routing to workers with retry and dead letter queue

## Documentation

- [Getting Started](./docs/GETTING_STARTED.md) - Installation and basic setup
- [Subscriptions](./docs/SUBSCRIPTIONS.md) - Event subscriptions and webhook delivery
- [Operations](./docs/OPERATIONS.md) - Monitoring and maintenance

## Quick Start

```typescript
import { DurableRPC } from '@dotdo/rpc'
import { EventEmitter, CDCCollection } from '@dotdo/events'

export class MyDO extends DurableRPC {
  events = new EventEmitter(this.ctx, this.env, {
    pipeline: this.env.EVENTS_PIPELINE,
    cdc: true,
    trackPrevious: true,
  })

  // CDC-enabled collection
  users = new CDCCollection(
    this.collection('users'),
    this.events,
    'users'
  )

  async createUser(data: User) {
    // CDC event emitted automatically with PITR bookmark
    this.users.put(data.id, data)
  }

  // Handle alarm for retry
  async alarm() {
    await this.events.handleAlarm()
  }
}
```

## Query with DuckDB

```sql
-- Reconstruct document history (time travel)
SELECT ts, type, docId, doc, prev, bookmark
FROM read_json_auto('r2://events/2024/**/*.jsonl')
WHERE collection = 'users' AND docId = 'user-123'
ORDER BY ts ASC;

-- Find all changes between two PITR bookmarks
SELECT * FROM read_json_auto('r2://events/2024/**/*.jsonl')
WHERE bookmark > 'start_bookmark' AND bookmark <= 'end_bookmark';

-- Aggregate RPC latencies by method
SELECT method, AVG(durationMs), P95(durationMs), COUNT(*)
FROM read_json_auto('r2://events/2024/01/**/*.jsonl')
WHERE type = 'rpc.call'
GROUP BY method;
```

## License

MIT
