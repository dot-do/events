# ADR-0004: SQLite vs KV for Durable Object Storage

## Status

Accepted

## Context

Durable Objects provide two storage backends:

1. **KV Storage API**: Key-value store with `get`, `put`, `delete`, `list` operations
2. **SQLite Storage**: Full SQL database with transactions, indexes, and complex queries

Our Durable Objects have varying storage needs:

| Durable Object | Data Pattern | Query Needs |
|----------------|--------------|-------------|
| SubscriptionDO | Subscriptions, deliveries, logs | Pattern matching, time-based queries |
| CDCProcessorDO | Document state, deltas, manifest | Collection scans, timestamp ordering |
| CatalogDO | Table metadata, schemas, snapshots | Version history, schema evolution |
| RateLimiterDO | Request counts, window timestamps | Sliding window aggregations |
| SchemaRegistryDO | Schemas, validations, configs | Lookup by type pattern |
| EventWriterDO | Buffered events | Batch flush only |
| ShardCoordinatorDO | Shard assignments, load stats | Simple lookups |

### Key Considerations

- **Query complexity**: Some DOs need joins, aggregations, time-range queries
- **Data volume**: CDC and subscription DOs may store thousands of records
- **Consistency**: Need ACID transactions for delivery tracking
- **Migration**: Ability to evolve schema over time
- **Performance**: Index support for common query patterns

## Decision

We will use **SQLite storage** for DOs that require complex queries or manage relational data:

- `SubscriptionDO` - SQLite (subscriptions + deliveries + logs)
- `CDCProcessorDO` - SQLite (document state + manifest)
- `CatalogDO` - SQLite (table metadata + schemas)
- `RateLimiterDO` - SQLite (sliding window counters)
- `SchemaRegistryDO` - SQLite (schema registry)

We will use **KV storage** for DOs with simple access patterns:

- `EventWriterDO` - KV (event buffer, batch flush)
- `ShardCoordinatorDO` - KV (shard assignments)

### Migration Configuration

```jsonc
// wrangler.jsonc
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["CatalogDO"] },
  { "tag": "v2", "new_sqlite_classes": ["SubscriptionDO"] },
  { "tag": "v3", "new_sqlite_classes": ["CDCProcessorDO"] },
  { "tag": "v4", "new_classes": ["EventWriterDO"] },
  { "tag": "v5", "new_sqlite_classes": ["RateLimiterDO"] },
  { "tag": "v6", "new_sqlite_classes": ["SchemaRegistryDO"] },
  { "tag": "v7", "new_classes": ["ShardCoordinatorDO"] }
]
```

## Consequences

### Positive

1. **Rich query capabilities**: SQLite enables complex queries that would require multiple KV operations:
   ```sql
   -- Find subscriptions matching an event type pattern
   SELECT * FROM subscriptions
   WHERE pattern GLOB 'webhook.github.*' AND active = 1

   -- Get pending deliveries ordered by retry time
   SELECT * FROM deliveries
   WHERE status = 'pending' AND next_attempt_at <= ?
   ORDER BY next_attempt_at LIMIT 100
   ```

2. **ACID transactions**: Subscription delivery requires atomic updates across tables:
   ```sql
   BEGIN;
   UPDATE deliveries SET status = 'delivered', delivered_at = ? WHERE id = ?;
   INSERT INTO delivery_log (delivery_id, attempt, status, duration_ms) VALUES (?, ?, 'success', ?);
   COMMIT;
   ```

3. **Efficient indexing**: SQLite indexes accelerate common queries without scanning all data:
   ```sql
   CREATE INDEX idx_deliveries_pending ON deliveries(status, next_attempt_at)
   WHERE status = 'pending';
   ```

4. **Schema evolution**: ALTER TABLE and migrations enable evolving data models over time.

5. **Point-in-time recovery**: SQLite bookmarks (`getCurrentBookmark()`) enable PITR for CDC:
   ```typescript
   const bookmark = await ctx.storage.getCurrentBookmark()
   // Restore to this exact state later
   ```

6. **Aggregations**: Rate limiting benefits from SQL aggregations:
   ```sql
   SELECT COUNT(*) FROM requests
   WHERE api_key = ? AND timestamp > ?
   ```

### Negative

1. **Storage overhead**: SQLite has higher per-row overhead than KV for simple key-value data.

2. **Learning curve**: Developers need SQL knowledge; KV API is simpler.

3. **Migration complexity**: Schema changes require migration scripts; KV is schemaless.

4. **Query optimization**: Complex queries can be slow without proper indexes.

5. **Size limits**: SQLite databases in DOs have size limits. Large datasets must overflow to R2.

### Trade-off Matrix

| Factor | SQLite | KV Storage |
|--------|--------|------------|
| Query flexibility | High (full SQL) | Low (key lookup, prefix scan) |
| Transaction support | Full ACID | Per-key only |
| Index support | Multiple indexes | Primary key only |
| Schema flexibility | Requires migration | Schemaless |
| Per-row overhead | Higher | Lower |
| Maximum dataset | Limited by DO | Limited by DO |
| PITR support | Yes (bookmarks) | No |

### Risks

| Risk | Mitigation |
|------|------------|
| SQL injection | Use parameterized queries exclusively; never interpolate user input |
| Schema migration failures | Test migrations in staging; implement rollback procedures |
| Query performance degradation | Monitor query latency; add indexes for slow patterns |
| Storage exhaustion | Monitor database size; implement overflow to R2 for large collections |

## Implementation Examples

### SubscriptionDO Schema

```sql
-- Subscriptions
CREATE TABLE subscriptions (
  id TEXT PRIMARY KEY,
  worker_name TEXT NOT NULL,
  pattern TEXT NOT NULL,
  rpc_method TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  active INTEGER DEFAULT 1
);
CREATE INDEX idx_subscriptions_pattern ON subscriptions(pattern);

-- Pending deliveries
CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  subscription_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT DEFAULT 'pending',
  attempts INTEGER DEFAULT 0,
  next_attempt_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (subscription_id) REFERENCES subscriptions(id)
);
CREATE INDEX idx_deliveries_pending ON deliveries(status, next_attempt_at)
WHERE status = 'pending';

-- Delivery log
CREATE TABLE delivery_log (
  id TEXT PRIMARY KEY,
  delivery_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL,
  duration_ms INTEGER,
  error TEXT,
  created_at INTEGER NOT NULL
);
```

### EventWriterDO (KV Pattern)

```typescript
// Simple buffer - KV is sufficient
class EventWriterDO {
  async bufferEvents(events: Event[]) {
    const existing = await this.ctx.storage.get<Event[]>('buffer') ?? []
    await this.ctx.storage.put('buffer', [...existing, ...events])
  }

  async flush() {
    const buffer = await this.ctx.storage.get<Event[]>('buffer') ?? []
    await this.writeToR2(buffer)
    await this.ctx.storage.delete('buffer')
  }
}
```

## References

- [Durable Objects SQLite Storage](https://developers.cloudflare.com/durable-objects/api/storage-api/)
- [SQLite in Durable Objects Announcement](https://blog.cloudflare.com/sqlite-in-durable-objects/)
- [DO Storage Best Practices](https://developers.cloudflare.com/durable-objects/best-practices/access-durable-objects-storage/)
