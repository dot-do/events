# ADR-0001: Durable Objects for State Management

## Status

Accepted

## Context

events.do is an event streaming, CDC (Change Data Capture), and lakehouse analytics platform for Cloudflare Workers. The system needs to manage several types of stateful operations:

1. **Event subscriptions** - Track which workers subscribe to which event patterns, manage delivery queues, and handle retries with exponential backoff
2. **CDC processing** - Maintain document state, track changes, and coordinate delta file generation
3. **Catalog metadata** - Store Iceberg-style table metadata, snapshots, and schema evolution
4. **Rate limiting** - Track request counts per tenant/API key with sliding windows
5. **Schema registry** - Store and validate event schemas per namespace

We evaluated several options for state management:

- **KV Storage**: Simple key-value store, eventually consistent
- **D1 Database**: Serverless SQLite, shared across requests
- **Durable Objects**: Single-threaded actors with strong consistency
- **External databases**: PostgreSQL, Redis, etc. via network calls

### Key Requirements

- Strong consistency for subscription delivery (at-least-once semantics)
- Transactional updates for CDC state
- Low latency for rate limiting decisions
- Isolation between tenants
- No cold start penalty for hot paths

## Decision

We will use **Durable Objects (DOs)** as the primary state management layer for all stateful components:

- `SubscriptionDO` - Manages subscriptions, delivery queues, and retry logic
- `CDCProcessorDO` - Maintains per-collection document state and generates delta files
- `CatalogDO` - Stores Iceberg-style metadata with schema evolution
- `RateLimiterDO` - Implements distributed rate limiting with sliding windows
- `SchemaRegistryDO` - Validates events against registered schemas
- `EventWriterDO` - Buffers events before flushing to R2
- `ShardCoordinatorDO` - Manages dynamic sharding for subscription routing

Each DO instance is identified by a unique name (e.g., namespace/collection for CDC, event type prefix for subscriptions) enabling natural sharding and isolation.

## Consequences

### Positive

1. **Strong consistency**: Single-threaded execution model guarantees serializable transactions within each DO instance. No race conditions for subscription delivery or CDC state updates.

2. **Built-in persistence**: DOs automatically persist state to durable storage. SQLite-backed DOs provide ACID transactions with automatic replication.

3. **Natural sharding**: Each DO instance handles a subset of work (one collection, one event type prefix). Horizontal scaling is automatic as load distributes across instances.

4. **Zero network overhead**: DOs run on Cloudflare's edge, eliminating external database round trips. State access is local memory speed.

5. **Alarm-based scheduling**: DOs can schedule alarms for future execution, enabling retry backoff and periodic compaction without external cron.

6. **Tenant isolation**: Each tenant can have isolated DO instances, preventing noisy neighbor issues and enabling per-tenant scaling.

7. **Hibernation**: Idle DOs automatically hibernate, reducing costs while maintaining fast wake-up times.

### Negative

1. **Single point of throughput**: Each DO instance can only process one request at a time. High-traffic event types need sharding strategies (addressed by `ShardCoordinatorDO`).

2. **Memory limits**: DOs have 128MB memory limit. Large CDC collections must flush to R2 and implement streaming access.

3. **No cross-DO transactions**: Atomic updates spanning multiple DOs require saga patterns or careful design. We mitigate this by designing boundaries around consistency requirements.

4. **Platform lock-in**: Durable Objects are Cloudflare-specific. Migration to other platforms would require significant refactoring.

5. **Debugging complexity**: Distributed state across many DO instances can be harder to debug than a centralized database.

### Risks

| Risk | Mitigation |
|------|------------|
| DO instance becomes hotspot | Implement dynamic sharding via `ShardCoordinatorDO`; fan out high-traffic patterns across multiple sub-shards |
| Memory exhaustion for large collections | Implement streaming access to R2; flush CDC state periodically; use SQLite for large datasets |
| Platform lock-in | Abstract DO interfaces behind clean APIs; document migration path to other actor systems |
| Data loss during hibernation | Use SQLite storage class for critical data; implement write-ahead logging for in-memory buffers |

## References

- [Cloudflare Durable Objects Documentation](https://developers.cloudflare.com/durable-objects/)
- [Durable Objects SQLite Storage](https://developers.cloudflare.com/durable-objects/api/storage-api/)
- [Actor Model Pattern](https://en.wikipedia.org/wiki/Actor_model)
