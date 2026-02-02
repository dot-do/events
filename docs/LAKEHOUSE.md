# R2 Lakehouse Storage

This document describes the R2-based lakehouse architecture for events.do, including storage layout, file formats, querying with DuckDB, compaction processes, and configuration options.

## Table of Contents

1. [Overview](#overview)
2. [Storage Layout](#storage-layout)
3. [File Formats](#file-formats)
4. [Querying with DuckDB](#querying-with-duckdb)
5. [Compaction](#compaction)
6. [Retention and Cleanup](#retention-and-cleanup)
7. [Configuration Options](#configuration-options)
8. [Example Queries](#example-queries)

---

## Overview

events.do uses Cloudflare R2 as a lakehouse storage layer for event streaming and CDC (Change Data Capture) data. The architecture provides:

- **Time-partitioned event storage** for efficient range queries
- **Columnar Parquet format** with statistics for predicate pushdown
- **Automatic compaction** to merge small files into larger ones
- **Point-in-time recovery (PITR)** through CDC snapshots and deltas
- **DuckDB-compatible** file layout for ad-hoc analytics

### R2 Buckets

| Bucket | Binding | Purpose |
|--------|---------|---------|
| `events` | `EVENTS_BUCKET` | Primary event storage |
| `platform-events` | `PIPELINE_BUCKET` | Pipeline events |
| `events-parquet-test` | `BENCHMARK_BUCKET` | Benchmark testing |

---

## Storage Layout

### Event Streams

Events are stored in a time-partitioned directory structure:

```
events/
  {year}/
    {month}/
      {day}/
        {hour}/
          {ulid}.parquet           # Raw event files (ingested hourly)
          _compact_{ulid}.parquet  # Hourly compacted file
        _compact_{ulid}.parquet    # Daily compacted file
```

**Example paths:**
```
events/2026/02/02/14/01JJHK5X2MABCD1234567890.parquet
events/2026/02/02/14/_compact_01JJHK6Y3NABCD2345678901.parquet
events/2026/02/02/_compact_01JJHK7Z4OABCD3456789012.parquet
```

### Multi-tenant Isolation

For namespaced (multi-tenant) deployments, events are isolated under namespace prefixes:

```
ns/{namespace}/events/
  {year}/{month}/{day}/{hour}/{ulid}.parquet
```

### CDC (Change Data Capture) Storage

CDC data for collections is stored separately:

```
cdc/{namespace}/{collection}/
  data.parquet              # Current state snapshot
  manifest.json             # Collection metadata
  deltas/
    001_2026-02-02T14-30.parquet  # Sequential delta files
    002_2026-02-02T15-00.parquet
  processed/                # Archived deltas (optional)
  snapshots/
    2026-02-02.parquet      # Daily snapshot
    2026-02.parquet         # Monthly snapshot
    manifest.json           # Snapshot metadata
```

### Deduplication Markers

Dedup markers prevent duplicate event ingestion:

```
dedup/{event-id}            # Empty marker files with TTL metadata
```

### Dead Letters

Failed messages are stored for debugging:

```
dead-letter/
  queue/{date}/{id}.json    # Failed queue messages
```

---

## File Formats

### Parquet (Primary Format)

Events are stored in Apache Parquet format with the following schema:

```
Column              Type                Notes
----------------------------------------------------------------------
ts                  TIMESTAMP_MILLIS    Event timestamp (required, indexed)
type                UTF8                Event type (required, indexed)
source              UTF8                Event source (optional)
provider            UTF8                Webhook provider (optional)
event_type          UTF8                Provider event type (optional)
verified            BOOLEAN             Signature verified (optional)
script_name         UTF8                Worker name (tail events)
outcome             UTF8                Request outcome (tail events)
method              UTF8                HTTP method (optional)
url                 UTF8                Request URL (optional)
status_code         INT32               HTTP status (optional)
duration_ms         DOUBLE              Request duration (optional)
payload             UTF8 (JSON)         Full event payload
```

**Parquet Features:**
- **Compression:** SNAPPY (for raw files) or UNCOMPRESSED (for compacted files with dictionary encoding)
- **Statistics:** Min/max values per column for predicate pushdown
- **Row Groups:** Optimized for analytics queries

**Custom Metadata (R2 object):**
```json
{
  "events": "1000",
  "bytes": "45678",
  "bytesPerEvent": "45",
  "cpuMs": "12.34",
  "minTs": "1706878800000",
  "maxTs": "1706882400000",
  "types": "rpc.call,collection.update",
  "format": "parquet-v1"
}
```

### JSONL (Legacy Format)

Older events may be stored as newline-delimited JSON:

```
events/{year}/{month}/{day}/{hour}/{uuid}.jsonl
```

The query layer automatically unions Parquet and JSONL files for compatibility.

### CDC Delta Format

CDC deltas use Parquet with the following schema:

```
Column    Type      Notes
---------------------------------
op        UTF8      Operation: insert, update, delete
pk        UTF8      Primary key (document ID)
ts        UTF8      ISO timestamp
data      UTF8      JSON-encoded document (for insert/update)
prev      UTF8      JSON-encoded previous state (optional)
bookmark  UTF8      SQLite bookmark for PITR
```

---

## Querying with DuckDB

events.do generates DuckDB-compatible SQL queries for lakehouse analytics.

### Basic Query Structure

```sql
SELECT *
FROM read_parquet('r2://events/events/2026/02/**/*.parquet',
  filename=true,
  hive_partitioning=false
)
WHERE type = 'rpc.call'
ORDER BY ts DESC
LIMIT 100
```

### Path Optimization

Queries automatically optimize the glob pattern based on date range:

| Date Range | Path Pattern |
|------------|--------------|
| Same day | `events/2026/02/02/*/` |
| Same month | `events/2026/02/*/*/` |
| Same year | `events/2026/*/*/*/` |
| Cross-year | `events/*/*/*/*/` |

### Query Builder API

```typescript
import { buildQuery } from '@dotdo/events'

const sql = buildQuery({
  bucket: 'events',
  dateRange: { start: new Date('2026-02-01'), end: new Date('2026-02-02') },
  eventTypes: ['rpc.call', 'collection.update'],
  doClass: 'ChatRoom',
  limit: 1000,
})
```

### Multi-format Support

The query endpoint unions Parquet and JSONL for backwards compatibility:

```sql
SELECT *
FROM read_parquet('events/2026/02/02/*/*.parquet', filename=true, hive_partitioning=false)
UNION ALL
SELECT *
FROM read_json_auto('events/2026/02/02/*/*.jsonl', filename=true, hive_partitioning=false)
WHERE type IN ('rpc.call')
ORDER BY ts DESC
```

---

## Compaction

Compaction merges small files into larger ones to improve query performance.

### Compaction Levels

| Level | Trigger | Input | Output |
|-------|---------|-------|--------|
| **Hourly** | Cron (5 min past hour) | Raw Parquet files | `_compact_{ulid}.parquet` in hour directory |
| **Daily** | Cron (midnight UTC) | Hourly compact files | `_compact_{ulid}.parquet` in day directory |

### Event Stream Compaction

```typescript
import { compactEventStream } from '@dotdo/events'

const result = await compactEventStream(bucket, {
  prefixes: ['events', 'webhooks', 'tail'],
  daysBack: 7,
  maxBytes: 100 * 1024 * 1024,  // 100MB per file
  maxEvents: 1_000_000,         // 1M events per file
  dryRun: false,
})

console.log(`Compacted ${result.totalRecords} records across ${result.days.length} days`)
```

**Compaction Process:**
1. List all Parquet files for completed days (excludes today)
2. For each day with multiple files, merge records sorted by timestamp
3. Write new compacted file with metadata
4. Delete source files after successful merge

### CDC Compaction

CDC delta files are compacted into `data.parquet`:

```typescript
import { compactCollection } from '@dotdo/events'

const result = await compactCollection(bucket, 'myworker', 'users', {
  deleteDeltas: true,  // Remove deltas after compaction
  archiveDeltas: false // Or archive to processed/ folder
})

console.log(`Compacted ${result.recordCount} records from ${result.deltasProcessed} deltas`)
```

**CDC Compaction Process:**
1. Load existing `data.parquet` (if exists)
2. Read all delta files in sequence order
3. Apply deltas: insert, update, delete operations
4. Write new `data.parquet` sorted by primary key
5. Update `manifest.json` with stats
6. Delete or archive processed deltas

### Compacted File Metadata

Compacted files include metadata:

```json
{
  "sourceFiles": "24",
  "recordCount": "150000",
  "compactedAt": "2026-02-02T01:30:00.000Z",
  "compactionLevel": "day"
}
```

---

## Retention and Cleanup

### Automatic Cleanup (Scheduled Tasks)

The scheduled handler runs cleanup tasks at different hours:

| Task | Schedule | TTL |
|------|----------|-----|
| Dedup markers | Hourly | 24 hours |
| Dead letters (R2) | 2:30 UTC daily | 30 days |
| Subscription dead letters | 3:30 UTC daily | 30 days |

### CDC Snapshot Retention

```typescript
import { cleanupSnapshots } from '@dotdo/events'

await cleanupSnapshots(bucket, 'namespace', 'collection', {
  dailySnapshots: 30,    // Keep 30 daily snapshots
  monthlySnapshots: 12,  // Keep 12 monthly snapshots
})
```

### Manual Cleanup

```bash
# List old dedup markers
wrangler r2 object list events --prefix="dedup/" --limit=100

# Delete specific prefix
wrangler r2 object delete events "dedup/old-marker-id"
```

---

## Configuration Options

### EventEmitter Options

```typescript
interface EventEmitterOptions {
  endpoint?: string           // events.do endpoint
  batchSize?: number          // Events before auto-flush (default: 100)
  flushIntervalMs?: number    // Max hold time in ms (default: 1000)
  r2Bucket?: R2Bucket         // R2 bucket for lakehouse streaming
  trackPrevious?: boolean     // Include prev doc in CDC updates
  maxRetryQueueSize?: number  // Max retry queue (default: 10000)
}
```

### Event Buffer Options

```typescript
const buffer = getEventBuffer(bucket, 'events', {
  countThreshold: 50,       // Flush after N events
  timeThresholdMs: 5000,    // Flush after N ms
  maxBufferSize: 10000,     // Max buffer before force flush
})
```

### Compaction Options

```typescript
interface EventCompactionOptions {
  maxBytes?: number      // Max bytes per compacted file (default: 100MB)
  maxEvents?: number     // Max events per compacted file (default: 1M)
  prefixes?: string[]    // Prefixes to compact (default: ['events'])
  daysBack?: number      // Days to look back (default: 7)
  dryRun?: boolean       // List files without compacting
}
```

### Wrangler Configuration

```jsonc
// wrangler.jsonc
{
  "r2_buckets": [
    { "binding": "EVENTS_BUCKET", "bucket_name": "events" }
  ],
  "triggers": {
    "crons": ["30 * * * *"]  // Scheduled compaction
  }
}
```

---

## Example Queries

### Query Recent Events

```sql
-- Events from the last hour
SELECT type, COUNT(*) as count
FROM read_parquet('r2://events/events/2026/02/02/14/*.parquet')
GROUP BY type
ORDER BY count DESC
```

### Query by Event Type

```sql
-- All RPC calls with latency
SELECT
  ts,
  "do".class as do_class,
  method,
  durationMs,
  success
FROM read_parquet('r2://events/events/2026/02/**/*.parquet')
WHERE type = 'rpc.call'
ORDER BY ts DESC
LIMIT 1000
```

### Query CDC Changes

```sql
-- Track changes to a specific document
SELECT ts, op, pk, data
FROM read_parquet('r2://events/cdc/myworker/users/deltas/*.parquet')
WHERE pk = 'user-123'
ORDER BY ts ASC
```

### Latency Analytics

```sql
-- RPC latency percentiles by method
SELECT
  "do".class,
  method,
  COUNT(*) as call_count,
  AVG(durationMs) as avg_ms,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY durationMs) as p50_ms,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY durationMs) as p95_ms,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY durationMs) as p99_ms,
  MAX(durationMs) as max_ms
FROM read_parquet('r2://events/events/2026/02/**/*.parquet')
WHERE type = 'rpc.call'
GROUP BY "do".class, method
ORDER BY call_count DESC
```

### Time Travel Query (PITR)

```sql
-- Find events between two bookmarks
SELECT *
FROM read_parquet('r2://events/cdc/myworker/users/deltas/*.parquet')
WHERE bookmark > 'bookmark-start'
  AND bookmark <= 'bookmark-end'
ORDER BY ts ASC
```

### Error Analysis

```sql
-- Failed requests by endpoint
SELECT
  url,
  status_code,
  COUNT(*) as error_count
FROM read_parquet('r2://events/events/2026/02/**/*.parquet')
WHERE status_code >= 400
GROUP BY url, status_code
ORDER BY error_count DESC
LIMIT 20
```

### Daily Event Volume

```sql
-- Event counts by day and type
SELECT
  DATE_TRUNC('day', ts) as day,
  type,
  COUNT(*) as event_count
FROM read_parquet('r2://events/events/2026/**/*.parquet')
GROUP BY day, type
ORDER BY day DESC, event_count DESC
```

---

## Related Documentation

- [OPERATIONS.md](./OPERATIONS.md) - Operational runbook
- [GETTING_STARTED.md](./GETTING_STARTED.md) - Quick start guide
- [CLAUDE.md](/CLAUDE.md) - Project context
