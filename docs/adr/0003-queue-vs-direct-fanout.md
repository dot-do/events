# ADR-0003: Queue vs Direct Fanout Architecture

## Status

Accepted

## Context

When events are ingested into events.do, they need to be distributed (fanned out) to multiple consumers:

1. **CDC Processors**: Track changes to collections and generate delta files
2. **Subscription Deliveries**: Deliver events to subscribed workers via RPC

We needed to decide between two fanout architectures:

### Option A: Direct Fanout
Events are fanned out synchronously during the ingest request by calling Durable Objects directly.

```
Ingest Request -> Validate -> Write to R2 -> Call CDCProcessorDO -> Call SubscriptionDO -> Response
```

### Option B: Queue-based Fanout
Events are written to a Cloudflare Queue, and a separate consumer processes CDC and subscription fanout asynchronously.

```
Ingest Request -> Validate -> Write to R2 -> Send to Queue -> Response
                                                    |
Queue Consumer <- <- <- <- <- <- <- <- <- <- <- <- -+
        |
        +-> Call CDCProcessorDO
        +-> Call SubscriptionDO
```

### Key Considerations

- **Latency**: Direct fanout adds latency to ingest response
- **Reliability**: Queue provides automatic retries on failure
- **Throughput**: Queue decouples ingest from processing capacity
- **Complexity**: Queue adds operational overhead
- **Cost**: Queue operations have associated costs
- **Consistency**: Direct fanout fails fast; queue may delay error detection

## Decision

We will support **both fanout modes**, configurable via the `USE_QUEUE_FANOUT` environment variable:

- `USE_QUEUE_FANOUT=false` (default): Direct fanout via Durable Object calls
- `USE_QUEUE_FANOUT=true`: Queue-based fanout via `EVENTS_QUEUE`

**Mutual exclusion** is enforced: events are processed by either the direct path OR the queue consumer, never both.

### Direct Fanout (Default)

```typescript
// In ingest handler
if (!useQueueFanout) {
  ctx.waitUntil(executeDirectFanout(context))
}
```

- CDC and subscription fanout run in `waitUntil` (non-blocking)
- Suitable for low-to-medium event volumes
- Simpler operational model

### Queue Fanout (Opt-in)

```typescript
// In ingest handler
if (useQueueFanout) {
  ctx.waitUntil(sendToQueue(env, batch))
}
```

- Queue consumer handles CDC and subscription fanout
- Automatic retries with exponential backoff (up to 5 attempts)
- Dead letter storage in R2 for failed messages
- Event-level idempotency via dedup markers

## Consequences

### Positive

1. **Flexibility**: Operators can choose the appropriate mode based on their workload characteristics.

2. **Graceful scaling**: Queue mode absorbs traffic spikes by buffering events, preventing DO overload.

3. **Reliable delivery**: Queue provides automatic retries with configurable backoff. Failed events don't block subsequent processing.

4. **Lower ingest latency** (queue mode): Ingest response returns immediately after queueing, not after fanout completion.

5. **Dead letter handling**: Permanently failed events are stored in R2 for inspection and manual reprocessing.

6. **Idempotency**: Both modes implement deduplication using bloom filters and R2 markers, preventing duplicate processing on retries.

### Negative

1. **Operational complexity**: Two code paths to maintain and test. Configuration errors could cause duplicate or missed events.

2. **Queue costs**: Queue operations (send, receive) incur costs. High-volume workloads should evaluate cost impact.

3. **Delayed error detection** (queue mode): Errors surface in queue consumer logs, not ingest response. Requires monitoring setup.

4. **Ordering not guaranteed** (queue mode): Events may be processed out of order across queue batches. CDC processors handle this via timestamp ordering.

5. **Additional latency** (queue mode): End-to-end latency increases due to queue polling interval (up to 5 seconds batch timeout).

### Trade-off Summary

| Aspect | Direct Fanout | Queue Fanout |
|--------|---------------|--------------|
| Ingest latency | Higher (includes fanout) | Lower (queue-and-return) |
| Error visibility | Immediate | Delayed (consumer logs) |
| Retry handling | Manual (via alarm) | Automatic (queue retry) |
| Throughput ceiling | DO concurrency limit | Queue throughput limit |
| Operational complexity | Lower | Higher |
| Cost | DO invocations only | DO + Queue operations |

### Risks

| Risk | Mitigation |
|------|------------|
| Duplicate processing on mode switch | Idempotency via event ID dedup markers in R2 |
| Queue backlog during outage | Monitor queue depth; scale consumer concurrency |
| Dead letters accumulating | Alerting on dead letter writes; periodic review process |
| Configuration drift | Environment validation at startup; integration tests |

## Implementation Details

### Fanout Mode Determination

```typescript
export function determineFanoutMode(env: Env): boolean {
  return env.USE_QUEUE_FANOUT === 'true' && !!env.EVENTS_QUEUE
}
```

### Queue Consumer Deduplication

Uses a two-tier dedup strategy:
1. **Bloom filter**: Fast in-memory check for recent event IDs
2. **R2 markers**: Persistent dedup markers for reliability

```typescript
const dedupResult = await batchCheckDuplicates(
  env.EVENTS_BUCKET,
  'dedup/cdc/',
  eventIds,
  cdcCache  // Bloom filter + LRU cache
)
```

### Dead Letter Storage

```
dead-letter/
  queue/
    {date}/
      {message-id}.json
```

Failed messages after MAX_RETRIES (5) are written to R2 with full context for debugging.

## References

- [Cloudflare Queues Documentation](https://developers.cloudflare.com/queues/)
- [Queue Consumer Configuration](https://developers.cloudflare.com/queues/configuration/consumer-concurrency/)
- [Idempotency Patterns](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)
