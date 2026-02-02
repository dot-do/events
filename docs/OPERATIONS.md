# events.do Operational Runbook

This document provides operational procedures for deploying, monitoring, and maintaining the events.do production system.

## Table of Contents

1. [System Overview](#system-overview)
2. [Deployment Procedures](#deployment-procedures)
3. [Environment Variables and Secrets](#environment-variables-and-secrets)
4. [Monitoring and Alerting](#monitoring-and-alerting)
5. [Common Troubleshooting Scenarios](#common-troubleshooting-scenarios)
6. [Incident Response Procedures](#incident-response-procedures)
7. [Backup and Recovery (PITR)](#backup-and-recovery-pitr)
8. [Scaling Considerations](#scaling-considerations)

---

## System Overview

events.do is an event streaming, CDC (Change Data Capture), and lakehouse analytics platform built on Cloudflare Workers and Durable Objects.

### Architecture Components

| Component | Purpose |
|-----------|---------|
| **Main Worker** (`events-do`) | Primary API endpoint for event ingestion, queries, and webhooks |
| **EventWriterDO** | Sharded Durable Object for batched event writes to R2 |
| **CDCProcessorDO** | Processes CDC events and maintains collection state |
| **CatalogDO** | Manages namespace and table metadata |
| **SubscriptionDO** | Handles event subscriptions and fanout |
| **RateLimiterDO** | Sliding window rate limiting per API key/IP |
| **R2 Buckets** | `events` (primary), `platform-events` (pipeline), `events-parquet-test` (benchmarks) |
| **Queue** | `events` queue for CDC/subscription fanout |

### Domains

- `events.do` - Primary production domain
- `webhooks.do` - Webhook ingestion endpoint
- `events.workers.do` - Workers.dev alias

---

## Deployment Procedures

### Prerequisites

- Node.js 18+ and pnpm installed
- Cloudflare Wrangler CLI authenticated (`wrangler login`)
- Access to the Cloudflare account (`b6641681fe423910342b9ffa1364c76d`)

### Standard Deployment

```bash
# Install dependencies
pnpm install

# Run tests before deployment
pnpm test

# Deploy to production
pnpm deploy
```

### Deployment Checklist

1. **Pre-deployment**
   - [ ] Run `pnpm test` - all tests pass
   - [ ] Run `pnpm typecheck` - no TypeScript errors
   - [ ] Review changes in git diff
   - [ ] Verify secrets are configured (see [Environment Variables](#environment-variables-and-secrets))

2. **Deployment**
   - [ ] Run `pnpm deploy`
   - [ ] Monitor deployment logs for errors
   - [ ] Verify deployment completed in Cloudflare Dashboard

3. **Post-deployment**
   - [ ] Check `/health` endpoint returns `200 OK`
   - [ ] Verify recent events are being ingested (check R2 bucket)
   - [ ] Monitor error rates for 15 minutes

### Deploying Auxiliary Workers

```bash
# Deploy compactor worker (CDC compaction)
pnpm deploy:compactor

# Deploy tail worker (worker trace streaming)
pnpm deploy:tail
```

### Rollback Procedure

```bash
# View deployment history
wrangler deployments list

# Rollback to previous deployment
wrangler rollback
```

---

## Environment Variables and Secrets

### Required Secrets

Set secrets using `wrangler secret put <SECRET_NAME>`:

| Secret | Purpose | Required |
|--------|---------|----------|
| `AUTH_TOKEN` | Bearer token for `/ingest` authentication | Yes (production) |
| `GITHUB_WEBHOOK_SECRET` | GitHub webhook signature verification | For GitHub webhooks |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signature verification | For Stripe webhooks |
| `WORKOS_WEBHOOK_SECRET` | WorkOS webhook signature verification | For WorkOS webhooks |
| `SLACK_SIGNING_SECRET` | Slack webhook signature verification | For Slack webhooks |
| `LINEAR_WEBHOOK_SECRET` | Linear webhook signature verification | For Linear webhooks |
| `SVIX_WEBHOOK_SECRET` | Svix webhook signature verification | For Svix webhooks |
| `TAIL_AUTH_SECRET` | Authentication for tail worker | Optional |

### Configuration Variables

Set in `wrangler.jsonc` or via `wrangler vars`:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `production` | Environment identifier |
| `ALLOWED_ORIGINS` | `""` | CORS allowed origins (comma-separated) |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | `1000` | Max requests per minute per client |
| `RATE_LIMIT_EVENTS_PER_MINUTE` | `100000` | Max events per minute per client |
| `ALLOW_UNAUTHENTICATED_INGEST` | `false` | Allow unauthenticated `/ingest` (dev only) |
| `USE_QUEUE_FANOUT` | `false` | Use queue for CDC/subscription fanout |

### Setting Secrets

```bash
# Set production AUTH_TOKEN
wrangler secret put AUTH_TOKEN
# Enter your secure token when prompted

# Set webhook secrets
wrangler secret put GITHUB_WEBHOOK_SECRET
wrangler secret put STRIPE_WEBHOOK_SECRET
```

### Verifying Secrets

```bash
# List configured secrets (values are hidden)
wrangler secret list
```

---

## Monitoring and Alerting

### Health Check Endpoint

```bash
# Check service health
curl https://events.do/health
```

Expected response:
```json
{
  "status": "ok",
  "service": "events.do",
  "ts": "2026-02-02T12:00:00.000Z",
  "env": "production",
  "cpuTimeMs": 0.5
}
```

### Key Metrics to Monitor

#### Cloudflare Dashboard

1. **Workers Analytics** (Cloudflare Dashboard > Workers > events-do)
   - Request rate and latency (p50, p99)
   - Error rate (target: < 0.1%)
   - CPU time per request

2. **Durable Objects** (Workers > events-do > Durable Objects)
   - Request count per DO class
   - Storage usage
   - Alarm triggers

3. **R2 Storage** (R2 > events bucket)
   - Object count and size
   - Operations per second
   - Storage growth rate

4. **Queues** (Queues > events)
   - Messages processed
   - Backlog size
   - Retry rate

### Log Analysis

Access logs via Wrangler:

```bash
# Stream live logs
wrangler tail

# Filter by status
wrangler tail --status=error

# Filter by search term
wrangler tail --search="CDC"
```

### Key Log Patterns

| Pattern | Meaning | Action |
|---------|---------|--------|
| `[ingest] Buffered X events` | Events successfully ingested | Normal |
| `[ingest] Rate limit exceeded` | Client hitting rate limits | Investigate client |
| `[EventWriterDO] Backpressure triggered` | Shard overloaded | Check scaling |
| `[CDC processor error]` | CDC processing failed | Check CDC logs |
| `[webhook] Signature verification failed` | Invalid webhook signature | Verify secret |
| `[SCHEDULED] Skipped - another worker running` | Lock contention on scheduled task | Normal |

### Alerting Recommendations

Set up alerts for:

1. **Error Rate > 1%** for 5 minutes
2. **P99 Latency > 5s** for 10 minutes
3. **Queue Backlog > 10,000** messages
4. **DO Storage > 80%** of limit
5. **Health endpoint failure** for 1 minute

---

## Common Troubleshooting Scenarios

### Scenario 1: Events Not Being Ingested

**Symptoms:** POST to `/ingest` returns success but events not appearing in R2

**Diagnosis:**
```bash
# Check recent R2 writes
wrangler r2 object list events --prefix="events/" --limit=10

# Check worker logs
wrangler tail --search="EventWriterDO"
```

**Resolution:**
1. Verify AUTH_TOKEN is set correctly
2. Check EventWriterDO is receiving events
3. Force flush any buffered events:
   - EventWriterDO uses alarm-based flushing (5s debounce)
   - Wait for flush or check DO alarms

### Scenario 2: Rate Limiting Issues

**Symptoms:** Clients receiving 429 Too Many Requests

**Diagnosis:**
```bash
# Check rate limit headers in response
curl -v https://events.do/ingest -H "Authorization: Bearer $TOKEN"

# Check logs for rate limit hits
wrangler tail --search="rate limit"
```

**Resolution:**
1. Increase `RATE_LIMIT_REQUESTS_PER_MINUTE` or `RATE_LIMIT_EVENTS_PER_MINUTE`
2. Distribute client load across multiple API keys
3. Implement exponential backoff in client

### Scenario 3: Webhook Signature Failures

**Symptoms:** Webhooks returning 401 with "Signature verification failed"

**Diagnosis:**
```bash
# Check webhook logs
wrangler tail --search="webhook"
```

**Resolution:**
1. Verify webhook secret matches provider configuration
2. Check for whitespace or encoding issues in secret
3. Ensure raw body is not modified before verification
4. Re-configure secret: `wrangler secret put GITHUB_WEBHOOK_SECRET`

### Scenario 4: High Latency on Ingest

**Symptoms:** P99 latency > 1s on `/ingest` endpoint

**Diagnosis:**
1. Check EventWriterDO backpressure logs
2. Check R2 operation latency in dashboard
3. Review batch sizes being sent

**Resolution:**
1. EventWriterDO automatically shards on backpressure
2. Consider reducing batch sizes in clients
3. Enable queue fanout: `USE_QUEUE_FANOUT=true`

### Scenario 5: CDC Compaction Not Running

**Symptoms:** Delta files accumulating without compaction

**Diagnosis:**
```bash
# Check scheduled task logs
wrangler tail --search="SCHEDULED"

# Check for lock contention
wrangler tail --search="Skipped - another worker"
```

**Resolution:**
1. Verify cron trigger is configured: `30 * * * *`
2. Check scheduler lock in R2: `scheduler-lock.json`
3. Manually trigger compaction via scheduled event

### Scenario 6: Durable Object Errors

**Symptoms:** DO requests failing with errors

**Diagnosis:**
```bash
# Check DO-specific logs
wrangler tail --search="DO"

# Check DO analytics in dashboard
```

**Resolution:**
1. Check DO storage limits (10GB per DO)
2. Verify migrations are applied
3. Consider resetting DO state (data loss - use with caution)

---

## Incident Response Procedures

### Severity Levels

| Level | Description | Response Time | Examples |
|-------|-------------|---------------|----------|
| **P0** | Complete outage | Immediate | All requests failing, data loss |
| **P1** | Partial outage | < 30 min | Major feature unavailable |
| **P2** | Degraded service | < 2 hours | High latency, intermittent errors |
| **P3** | Minor issue | < 24 hours | Non-critical bug, cosmetic issue |

### Incident Response Steps

#### 1. Detection and Assessment
- [ ] Confirm the incident via health check and logs
- [ ] Determine severity level
- [ ] Identify affected components

#### 2. Communication
- [ ] Update status page (if available)
- [ ] Notify stakeholders based on severity

#### 3. Mitigation

**For deployment-related issues:**
```bash
# Immediate rollback
wrangler rollback
```

**For traffic-related issues:**
- Increase rate limits temporarily
- Enable maintenance mode (return 503)

**For data issues:**
- Identify affected time window
- Prepare PITR recovery (see next section)

#### 4. Resolution
- [ ] Implement fix
- [ ] Test in development environment
- [ ] Deploy fix to production
- [ ] Verify resolution

#### 5. Post-Incident
- [ ] Document timeline and actions taken
- [ ] Identify root cause
- [ ] Create follow-up tasks to prevent recurrence

---

## Backup and Recovery (PITR)

events.do provides Point-in-Time Recovery (PITR) through CDC snapshots and delta files.

### How PITR Works

1. **CDC Events** capture every insert/update/delete with SQLite bookmark
2. **Snapshots** store periodic collection state (daily/monthly)
3. **Delta Files** store incremental changes between snapshots
4. **Reconstruction** applies deltas to nearest snapshot

### Storage Layout

```
{namespace}/{collection}/
├── data.parquet              # Current state
├── deltas/                   # CDC change logs
│   └── 001_YYYY-MM-DDTHH-MM.parquet
├── snapshots/
│   ├── YYYY-MM-DD.parquet    # Daily snapshot
│   ├── YYYY-MM.parquet       # Monthly snapshot
│   └── manifest.json         # Snapshot metadata
```

### Creating Manual Snapshot

Snapshots are created automatically via scheduled tasks, but you can trigger manually:

```typescript
import { createSnapshot } from '@dotdo/events'

// Create daily snapshot
const result = await createSnapshot(bucket, 'namespace', 'collection', 'daily')
console.log(`Snapshot created: ${result.path}`)
```

### Listing Available Snapshots

```typescript
import { listSnapshots } from '@dotdo/events'

const snapshots = await listSnapshots(bucket, 'namespace', 'collection')
for (const s of snapshots) {
  console.log(`${s.type}: ${s.path} (${s.rowCount} rows)`)
}
```

### Restoring to Point-in-Time

```typescript
import { reconstructState } from '@dotdo/events'

// Reconstruct state at specific timestamp
const targetTime = new Date('2026-01-15T14:30:00Z')
const state = await reconstructState(bucket, 'namespace', 'collection', targetTime)

// state is Map<docId, document>
for (const [id, doc] of state) {
  console.log(`Document ${id}:`, doc)
}
```

### Full Collection Restore

```typescript
import { restoreSnapshot } from '@dotdo/events'

// Restore from specific snapshot
const result = await restoreSnapshot(
  sql,
  bucket,
  'namespace/collection/snapshots/2026-01-15.parquet'
)
console.log(`Restored ${result.totalDocs} documents`)
```

### Retention Policy

Default retention (configurable):
- **Daily snapshots:** 30 days
- **Monthly snapshots:** 12 months

Cleanup runs automatically via scheduled tasks:

```typescript
import { cleanupSnapshots } from '@dotdo/events'

await cleanupSnapshots(bucket, 'namespace', 'collection', {
  dailySnapshots: 30,
  monthlySnapshots: 12,
})
```

### Recovery Scenarios

| Scenario | Action |
|----------|--------|
| Accidental deletion | Use `reconstructState` to find last known state |
| Data corruption | Restore from last known good snapshot |
| Complete DO loss | Restore collection from R2 snapshots |
| Audit query | Use PITR to query historical state |

---

## Scaling Considerations

### Current Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Worker CPU time | 30s (paid) | Per request |
| Worker memory | 128MB | Per isolate |
| Durable Object storage | 10GB | Per DO instance |
| R2 object size | 5GB | Per object |
| Queue batch size | 100 messages | Per batch |
| Rate limit | 1000 req/min | Per client (configurable) |

### Horizontal Scaling

#### EventWriterDO Sharding

EventWriterDO automatically shards under load:
- Default shard: `events`
- Overflow shards: `events:shard-1`, `events:shard-2`, etc.
- Backpressure triggers at 100 pending writes
- Max 16 overflow shards

#### Subscription Sharding

SubscriptionDO shards by event type prefix:
- Events `collection.insert` -> shard `collection`
- Events `webhook.github.push` -> shard `webhook`

#### CDC Processor Sharding

CDCProcessorDO shards by namespace/collection:
- Each `{namespace}/{collection}` has dedicated processor
- Prevents cross-collection contention

### Scaling Recommendations

1. **High ingest volume (>10K events/sec)**
   - Enable queue fanout: `USE_QUEUE_FANOUT=true`
   - Increase EventWriterDO shard count
   - Consider dedicated namespace sharding

2. **Large collections (>1M documents)**
   - Enable regular compaction
   - Increase snapshot frequency
   - Consider partitioning by time

3. **High query volume**
   - Use DuckDB on cached Parquet files
   - Implement query result caching
   - Consider dedicated query workers

4. **Multi-region**
   - Deploy to multiple Cloudflare regions
   - Use R2 for global data access
   - Consider regional DO placement hints

### Performance Tuning

```jsonc
// wrangler.jsonc - Queue tuning
"queues": {
  "consumers": [{
    "queue": "events",
    "max_batch_size": 100,    // Increase for throughput
    "max_batch_timeout": 5,   // Decrease for latency
    "max_retries": 3
  }]
}
```

### Monitoring Scale

Watch these metrics as you scale:
- EventWriterDO shard distribution
- Queue backlog growth rate
- R2 operation latency
- DO storage growth
- CPU time per request

---

## Appendix

### Useful Commands

```bash
# View worker configuration
wrangler whoami
wrangler deployments list

# R2 operations
wrangler r2 object list events --prefix="events/"
wrangler r2 object get events events/2026/02/02/12/event.parquet

# Queue operations
wrangler queues list

# DO operations (via dashboard or API)
# Check storage: Cloudflare Dashboard > Workers > events-do > Durable Objects
```

### Support Contacts

- Cloudflare Support: https://support.cloudflare.com
- Workers Discord: https://discord.gg/cloudflaredev

### Related Documentation

- [CLAUDE.md](/CLAUDE.md) - Project context
- [ARCHITECTURE.md](/ARCHITECTURE.md) - System architecture
- [README.md](/README.md) - Getting started
