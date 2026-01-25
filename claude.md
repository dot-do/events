# events.do - Claude Context

## Project Overview

Event streaming, CDC (Change Data Capture), and lakehouse analytics for Durable Objects.

**Domains:**
- `events.do` (primary, once domain is set up)
- `events.workers.do` (alias)

## Architecture

- `core/` - `@dotdo/events` package
  - EventEmitter for DO event batching with alarm retries
  - CDC (Change Data Capture) for collections
  - PITR (Point-in-time recovery) with SQLite bookmarks
  - CDCCollection wrapper for automatic change tracking
  - Snapshot/restore utilities

- `src/` - `events.do` worker
  - Ingest endpoint (`/ingest`) - receives batched events
  - R2 streaming for lakehouse storage
  - Uses ducklytics for storage backend
  - DuckDB query support

## Key Concepts

### Event Types
- `rpc.call` - RPC method invocations with latency
- `collection.insert/update/delete` - CDC events with previous doc
- `do.create/alarm/hibernate` - Lifecycle events
- `ws.connect/message/close` - WebSocket events

### PITR (Point-in-time Recovery)
Every CDC event captures the SQLite bookmark via `ctx.storage.getCurrentBookmark()`. This enables:
- Restoring any DO to exact state at any CDC event
- Time-travel queries across the entire system
- Disaster recovery

### R2 Lakehouse
Events streamed to R2 in JSONL format:
```
events/{year}/{month}/{day}/{hour}/{uuid}.jsonl
```

Query with DuckDB:
```sql
SELECT * FROM read_json_auto('r2://events/2024/**/*.jsonl')
WHERE type = 'collection.update' AND collection = 'users'
```

## Dependencies

- `ducklytics` - Analytics dashboard and query engine
- `@dotdo/rpc` - DurableRPC integration
- `colo.do` - Location awareness (optional)

## Related Repos

- `/Users/nathanclevenger/projects/rpc.do/core` - DurableRPC
- `/Users/nathanclevenger/projects/duckdb/packages/ducklytics` - Backend
- `/Users/nathanclevenger/projects/colo` - Colo awareness

## Commands

```bash
# Development
pnpm dev              # Run worker locally
pnpm test             # Run tests

# Deployment
pnpm deploy           # Deploy to events.workers.do
```
