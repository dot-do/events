# Disaster Recovery Runbook

This document defines the disaster recovery procedures, recovery objectives, and runbooks for events.do production systems.

## Table of Contents

1. [Recovery Objectives](#recovery-objectives)
2. [Backup Systems Inventory](#backup-systems-inventory)
3. [Recovery Procedures](#recovery-procedures)
4. [Testing Schedule and Procedures](#testing-schedule-and-procedures)
5. [Contact and Escalation](#contact-and-escalation)
6. [Runbook: Common Failure Scenarios](#runbook-common-failure-scenarios)

---

## Recovery Objectives

### Recovery Time Objective (RTO)

| Component | RTO Target | Notes |
|-----------|------------|-------|
| **API Availability** | 5 minutes | Worker rollback via Wrangler |
| **Event Ingestion** | 15 minutes | Restore EventWriterDO routing |
| **Query Service** | 30 minutes | R2 lakehouse always available |
| **Single DO Recovery** | 1 hour | Restore from snapshots + deltas |
| **Full System Recovery** | 4 hours | Complete restoration from R2 |
| **Cross-Region Failover** | 15 minutes | Cloudflare global network |

### Recovery Point Objective (RPO)

| Data Type | RPO Target | Mechanism |
|-----------|------------|-----------|
| **Event Streams** | Near-zero | Events written to R2 immediately |
| **CDC Data** | 5 seconds | Alarm-based flush (5s debounce) |
| **DO State (SQLite)** | 0 | Cloudflare-managed replication |
| **Snapshots** | 24 hours | Daily snapshot schedule |
| **Collection State** | 5 minutes | CDC delta files + PITR |

### Service Level Objectives

| Metric | Target | Measurement |
|--------|--------|-------------|
| Availability | 99.9% | Monthly uptime |
| Data Durability | 99.999999999% (11 9s) | R2 storage durability |
| Event Delivery | At-least-once | Deduplication at ingest |

### Known Data Loss Windows

#### Bookmark-to-Flush Gap (Events)

**What it is:** There is a 0-5 second window between when an event is accepted via `/ingest` and when it is durably written to R2 storage. During this window, events are held in-memory in EventWriterDO.

**Expected Data Loss Window:**
- **Normal operation:** 0-5 seconds of events (alarm debounce period)
- **DO crash/eviction:** Up to 5 seconds of in-flight events
- **Worker redeployment:** Buffered events are lost (no graceful drain)

**How It Works:**
1. Events arrive at `/ingest` → immediately buffered in EventWriterDO
2. EventWriterDO sets an alarm for 5 seconds (or uses existing alarm)
3. Alarm fires → batch written to R2 as Parquet file
4. Response already returned to client (201 status)

**Mitigation Strategies:**

| Strategy | Description | Trade-off |
|----------|-------------|-----------|
| **Shorter flush interval** | Reduce alarm debounce from 5s to 1s | More R2 writes, higher cost |
| **Synchronous flush** | Block response until R2 write completes | Higher latency (50-200ms) |
| **Client retry** | Clients retry on 5xx or timeout | Requires idempotent batchId |
| **Queue-based ingestion** | Use Cloudflare Queue for persistence | Added complexity, higher latency |

**Alarm Flush Guarantees:**
- Alarms are guaranteed to fire (Cloudflare manages scheduling)
- If a DO is evicted before alarm fires, buffered data is lost
- After alarm fires and R2 write succeeds, data is durable (11 9s)
- Failed R2 writes are retried via alarm reschedule

**Monitoring:**
- Alert on EventWriterDO alarm failures
- Track `r2_write_errors` metrics
- Monitor DO eviction rates

**When This Matters:**
- High-value events requiring guaranteed delivery → use synchronous flush
- Audit/compliance events → use queue-based ingestion
- Analytics events → default behavior is acceptable

#### Bookmark-to-Flush Gap (CDC)

**What it is:** CDC events are debounced for up to 5 seconds before being flushed to R2 delta files.

**Expected Data Loss Window:**
- **DO crash:** Up to 5 seconds of CDC changes
- **DO state is preserved:** SQLite bookmark survives DO hibernation

**Critical Difference from Events:**
- CDC events include SQLite bookmarks that allow replaying changes from DO storage
- If CDC flush fails but DO survives, changes can be recovered by re-emitting from the bookmark
- If DO is destroyed, changes since last flush are lost (but collection state is in DO's SQLite)

**Recovery:** Use PITR to reconstruct state from the last successful CDC delta flush.

---

## Backup Systems Inventory

### R2 Storage Buckets

| Bucket | Binding | Purpose | Retention |
|--------|---------|---------|-----------|
| `events` | `EVENTS_BUCKET` | Primary event lakehouse | Indefinite |
| `platform-events` | `PIPELINE_BUCKET` | Pipeline events | 90 days |
| `events-parquet-test` | `BENCHMARK_BUCKET` | Benchmark/testing | 7 days |

### R2 Storage Layout

```
events/
  events/{year}/{month}/{day}/{hour}/     # Time-partitioned event stream
    {ulid}.parquet                         # Raw event files
    _compact_{ulid}.parquet               # Compacted files
  cdc/{namespace}/{collection}/
    data.parquet                           # Current collection state
    manifest.json                          # Collection metadata
    deltas/
      001_YYYY-MM-DDTHH-MM.parquet        # Sequential delta files
    snapshots/
      YYYY-MM-DD.parquet                  # Daily snapshots
      YYYY-MM.parquet                      # Monthly snapshots
      manifest.json                        # Snapshot metadata
  dedup/{event-id}                        # Deduplication markers (24h TTL)
  dead-letter/                            # Failed message archive
```

### Durable Objects

| DO Class | Storage Type | Purpose | Backup Strategy |
|----------|--------------|---------|-----------------|
| `CatalogDO` | SQLite | Namespace/table metadata | CDC to R2 |
| `SubscriptionDO` | SQLite | Event subscriptions | CDC to R2 |
| `CDCProcessorDO` | SQLite | Collection state | Snapshots + deltas |
| `EventWriterDO` | In-memory | Event batching | Alarm-based flush |
| `RateLimiterDO` | SQLite | Rate limit state | Ephemeral (regenerates) |
| `SchemaRegistryDO` | SQLite | Event schemas | CDC to R2 |
| `ShardCoordinatorDO` | In-memory | Shard routing | Ephemeral |

### Snapshot Schedule

| Type | Frequency | Retention | Trigger |
|------|-----------|-----------|---------|
| Daily CDC Snapshot | Every 24 hours | 30 days | Scheduled (00:30 UTC) |
| Monthly CDC Snapshot | First of month | 12 months | Scheduled (01:00 UTC) |
| Event Stream Compaction | Hourly | N/A (compacted in place) | Scheduled (30 min past hour) |

---

## Recovery Procedures

### 1. Full System Recovery

**Scenario:** Complete loss of worker deployments and DO state

**Prerequisites:**
- Access to Cloudflare account (`b6641681fe423910342b9ffa1364c76d`)
- R2 buckets intact (independent of Workers)
- Wrangler CLI authenticated

**Procedure:**

```bash
# Step 1: Verify R2 data integrity
wrangler r2 object list events --prefix="events/" --limit=10
wrangler r2 object list events --prefix="cdc/" --limit=10

# Step 2: Deploy fresh worker
cd /path/to/events.do
pnpm install
pnpm deploy

# Step 3: Verify deployment
curl https://events.do/health

# Step 4: Restore DO migrations
# Migrations are applied automatically on first DO access
# The wrangler.jsonc migrations ensure DO classes are created

# Step 5: Verify event ingestion
curl -X POST https://events.do/ingest \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"test.recovery","data":{"status":"ok"}}'

# Step 6: Restore CDC collections from snapshots (for each collection)
# This is done programmatically - see PITR restore procedure below
```

**Estimated Recovery Time:** 30-60 minutes

---

### 2. Single DO Recovery

**Scenario:** Single Durable Object corruption or data loss

**Prerequisites:**
- DO ID known
- R2 snapshots available

**Procedure:**

```typescript
// Step 1: Identify the affected DO and collection
const doId = 'affected-do-id'
const namespace = 'myworker'
const collection = 'users'

// Step 2: List available snapshots
import { listSnapshots } from '@dotdo/events'

const snapshots = await listSnapshots(env.EVENTS_BUCKET, namespace, collection)
console.log('Available snapshots:')
for (const s of snapshots) {
  console.log(`  ${s.type}: ${s.path} (${s.rowCount} rows, ${s.timestamp.toISOString()})`)
}

// Step 3: Choose recovery point and restore
import { restoreSnapshot } from '@dotdo/events'

// Option A: Restore from latest snapshot
const latestSnapshot = snapshots[snapshots.length - 1]
const result = await restoreSnapshot(
  sql,  // DO's SQLite storage
  env.EVENTS_BUCKET,
  latestSnapshot.path
)
console.log(`Restored ${result.totalDocs} documents from ${latestSnapshot.path}`)

// Option B: Use PITR to restore to specific timestamp
import { reconstructState } from '@dotdo/events'

const targetTime = new Date('2026-02-01T14:30:00Z')
const state = await reconstructState(env.EVENTS_BUCKET, namespace, collection, targetTime)

// Manually apply reconstructed state to DO
for (const [id, doc] of state) {
  sql.exec(
    `INSERT OR REPLACE INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)`,
    collection, id, JSON.stringify(doc), new Date().toISOString()
  )
}
```

**Via Wrangler (for manual inspection):**

```bash
# List snapshot files
wrangler r2 object list events --prefix="cdc/myworker/users/snapshots/"

# Download a snapshot for inspection
wrangler r2 object get events "cdc/myworker/users/snapshots/2026-02-01.parquet" \
  --file=/tmp/snapshot.parquet

# Inspect with DuckDB (if installed locally)
duckdb -c "SELECT * FROM read_parquet('/tmp/snapshot.parquet') LIMIT 10"
```

**Estimated Recovery Time:** 15-60 minutes depending on data size

---

### 3. Data Corruption Recovery

**Scenario:** Corrupt data detected in collections or event stream

**Diagnosis:**

```bash
# Check recent events for anomalies
wrangler tail --search="error" --since=1h

# Query for corrupt records in R2
# Use DuckDB to analyze parquet files
```

**Procedure:**

```typescript
// Step 1: Identify corruption window
const corruptionStart = new Date('2026-02-01T10:00:00Z')
const corruptionEnd = new Date('2026-02-01T12:00:00Z')

// Step 2: Reconstruct state before corruption
import { reconstructState } from '@dotdo/events'

const beforeCorruption = new Date(corruptionStart.getTime() - 1)
const cleanState = await reconstructState(
  env.EVENTS_BUCKET,
  namespace,
  collection,
  beforeCorruption
)

// Step 3: Identify affected documents
const affectedIds = new Set<string>()
// Query deltas in corruption window
const deltasPrefix = `cdc/${namespace}/${collection}/deltas/`
const listed = await env.EVENTS_BUCKET.list({ prefix: deltasPrefix })

for (const obj of listed.objects) {
  // Check if delta falls in corruption window
  const deltaTimestamp = extractTimestampFromPath(obj.key)
  if (deltaTimestamp >= corruptionStart && deltaTimestamp <= corruptionEnd) {
    const delta = await env.EVENTS_BUCKET.get(obj.key)
    // Parse and collect affected IDs
    // ...
    affectedIds.add(/* pk from delta */)
  }
}

// Step 4: Restore affected documents from clean state
for (const id of affectedIds) {
  const cleanDoc = cleanState.get(id)
  if (cleanDoc) {
    // Restore document
    sql.exec(
      `INSERT OR REPLACE INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)`,
      collection, id, JSON.stringify(cleanDoc), new Date().toISOString()
    )
  } else {
    // Document didn't exist before corruption - delete it
    sql.exec(`DELETE FROM _collections WHERE collection = ? AND id = ?`, collection, id)
  }
}

// Step 5: Clean up corrupt delta files
for (const obj of listed.objects) {
  const deltaTimestamp = extractTimestampFromPath(obj.key)
  if (deltaTimestamp >= corruptionStart && deltaTimestamp <= corruptionEnd) {
    // Archive to dead-letter before deletion
    const content = await env.EVENTS_BUCKET.get(obj.key)
    await env.EVENTS_BUCKET.put(`dead-letter/corrupt/${obj.key}`, content.body)
    await env.EVENTS_BUCKET.delete(obj.key)
  }
}
```

**Estimated Recovery Time:** 1-4 hours depending on corruption scope

---

### 4. PITR (Point-in-Time Recovery) Procedure

**Scenario:** Need to restore collection state to exact point in time

**Prerequisites:**
- Target timestamp known
- Snapshots and deltas available for time period

**Step-by-Step Procedure:**

```typescript
import { reconstructState, listSnapshots } from '@dotdo/events'

// Step 1: Define recovery parameters
const namespace = 'myworker'
const collection = 'users'
const targetTime = new Date('2026-02-01T14:30:00Z')

// Step 2: Verify data availability
const snapshots = await listSnapshots(env.EVENTS_BUCKET, namespace, collection)
const snapshotsBefore = snapshots.filter(s => s.timestamp <= targetTime)

if (snapshotsBefore.length === 0) {
  console.warn('No snapshots before target time - using data.parquet as base')
}

// Step 3: Reconstruct state
const state = await reconstructState(
  env.EVENTS_BUCKET,
  namespace,
  collection,
  targetTime
)

console.log(`Reconstructed ${state.size} documents at ${targetTime.toISOString()}`)

// Step 4: Verify reconstruction (sample check)
for (const [id, doc] of Array.from(state.entries()).slice(0, 5)) {
  console.log(`Sample document ${id}:`, JSON.stringify(doc).slice(0, 100))
}

// Step 5: Apply to target DO (if restoring live system)
// WARNING: This will overwrite current state!
const confirmRestore = true // Set to true after verification

if (confirmRestore) {
  // Clear existing data
  sql.exec(`DELETE FROM _collections WHERE collection = ?`, collection)

  // Insert reconstructed state
  for (const [id, doc] of state) {
    const { _id, ...data } = doc as Record<string, unknown>
    sql.exec(
      `INSERT INTO _collections (collection, id, data, updated_at) VALUES (?, ?, ?, ?)`,
      collection,
      id,
      JSON.stringify(data),
      targetTime.toISOString()
    )
  }

  console.log(`Restored ${state.size} documents to ${collection}`)
}
```

**PITR Limitations:**
- Requires CDC events to have been emitted (use `CDCCollection` wrapper)
- SQLite bookmarks must be captured in events
- Deltas must not have been purged

**Estimated Recovery Time:** 30-60 minutes

---

## Testing Schedule and Procedures

### DR Test Schedule

| Test Type | Frequency | Duration | Owner |
|-----------|-----------|----------|-------|
| Backup Verification | Weekly | 15 min | Automated |
| Snapshot Restore Test | Monthly | 1 hour | On-call engineer |
| PITR Test | Monthly | 1 hour | On-call engineer |
| Full Recovery Drill | Quarterly | 4 hours | Engineering team |
| Tabletop Exercise | Semi-annually | 2 hours | All stakeholders |

### Weekly Backup Verification (Automated)

```bash
#!/bin/bash
# Run as scheduled job

# Verify R2 bucket accessibility
wrangler r2 object list events --prefix="events/" --limit=1 || exit 1
wrangler r2 object list events --prefix="cdc/" --limit=1 || exit 1

# Check recent snapshots exist
RECENT_SNAPSHOT=$(wrangler r2 object list events --prefix="cdc/" | grep "snapshots" | head -1)
if [ -z "$RECENT_SNAPSHOT" ]; then
  echo "WARNING: No recent snapshots found"
  exit 1
fi

# Verify health endpoint
curl -sf https://events.do/health || exit 1

echo "Backup verification passed"
```

### Monthly Snapshot Restore Test

**Procedure:**

1. Select a random collection for testing
2. Download latest snapshot
3. Verify snapshot integrity (parseable Parquet)
4. Create test DO and restore snapshot
5. Verify row counts match
6. Query sample documents
7. Document results

**Test Script:**

```typescript
async function monthlyRestoreTest(env: Env) {
  const testNs = 'test-restore'
  const testCollection = 'monthly-test'

  // 1. List production snapshots
  const prodSnapshots = await listSnapshots(env.EVENTS_BUCKET, 'production', 'users')
  const latestSnapshot = prodSnapshots[prodSnapshots.length - 1]

  // 2. Copy snapshot to test location
  const snapshotData = await env.EVENTS_BUCKET.get(latestSnapshot.path)
  await env.EVENTS_BUCKET.put(
    `${testNs}/${testCollection}/snapshots/test.parquet`,
    snapshotData.body
  )

  // 3. Verify parquet integrity
  const testSnapshot = await env.EVENTS_BUCKET.get(
    `${testNs}/${testCollection}/snapshots/test.parquet`
  )
  const buffer = await testSnapshot.arrayBuffer()
  const records = await parquetReadObjects({ file: createAsyncBuffer(buffer) })

  // 4. Validate record counts
  if (records.length !== latestSnapshot.rowCount) {
    throw new Error(`Row count mismatch: expected ${latestSnapshot.rowCount}, got ${records.length}`)
  }

  // 5. Cleanup
  await env.EVENTS_BUCKET.delete(`${testNs}/${testCollection}/snapshots/test.parquet`)

  console.log(`Monthly restore test passed: ${records.length} records verified`)
  return { success: true, recordCount: records.length }
}
```

### Monthly PITR Test

**Procedure:**

1. Select a collection with recent CDC activity
2. Pick a random timestamp within last 7 days
3. Run `reconstructState` for that timestamp
4. Verify state is non-empty and consistent
5. Compare with known state if available
6. Document results

### Quarterly Full Recovery Drill

**Scenario:** Simulate complete system loss

**Procedure:**

1. **Preparation (1 hour)**
   - Notify stakeholders
   - Create isolated test environment
   - Document current system state

2. **Simulation (2 hours)**
   - Deploy fresh worker to test environment
   - Restore all collections from R2 snapshots
   - Verify event ingestion works
   - Verify query functionality

3. **Validation (30 minutes)**
   - Compare restored data with production
   - Verify all endpoints functional
   - Test subscription delivery

4. **Documentation (30 minutes)**
   - Record RTO achieved
   - Document any issues
   - Update runbooks if needed

---

## Contact and Escalation

### Escalation Matrix

| Severity | Initial Response | Escalation (15 min) | Escalation (1 hour) |
|----------|-----------------|---------------------|---------------------|
| **P0** (Complete outage) | On-call engineer | Engineering lead | VP Engineering |
| **P1** (Major feature down) | On-call engineer | Engineering lead | - |
| **P2** (Degraded service) | On-call engineer | - | - |
| **P3** (Minor issue) | Next business day | - | - |

### Contact Information

| Role | Primary | Backup |
|------|---------|--------|
| On-call Engineer | PagerDuty rotation | #ops-alerts Slack |
| Engineering Lead | Direct contact | #engineering Slack |
| Cloudflare Support | support.cloudflare.com | Enterprise support ticket |

### Communication Channels

| Channel | Purpose |
|---------|---------|
| #incidents | Active incident coordination |
| #ops-alerts | Automated alerts |
| #engineering | Technical discussion |
| Status page | Customer-facing updates |

### External Support

| Provider | Contact | SLA |
|----------|---------|-----|
| Cloudflare Enterprise | Enterprise support portal | 1 hour response |
| Cloudflare Workers Discord | discord.gg/cloudflaredev | Community support |

---

## Runbook: Common Failure Scenarios

### Scenario 1: EventWriterDO Not Flushing

**Symptoms:**
- Events accepted via `/ingest` but not appearing in R2
- Increasing memory usage in DO metrics

**Diagnosis:**
```bash
# Check DO alarm state
wrangler tail --search="EventWriterDO"

# Check for errors
wrangler tail --search="alarm" --status=error
```

**Resolution:**
```bash
# Option 1: Force flush via alarm (automatic after 5s)
# Wait for alarm to trigger

# Option 2: Check R2 connectivity
wrangler r2 object list events --prefix="events/" --limit=5

# Option 3: Redeploy worker (resets DO state, may lose buffered events)
# WARNING: Buffered events will be lost
pnpm deploy
```

**Prevention:**
- Monitor DO memory usage
- Set up alerts for flush failures

---

### Scenario 2: CDC Snapshots Not Creating

**Symptoms:**
- Snapshot manifest not updating
- Old snapshots accumulating without new ones

**Diagnosis:**
```bash
# Check scheduled task logs
wrangler tail --search="SCHEDULED"

# Verify cron trigger
# Check wrangler.jsonc: "triggers": { "crons": ["30 * * * *"] }

# Check for lock contention
wrangler r2 object get events scheduler-lock.json --file=/tmp/lock.json
cat /tmp/lock.json
```

**Resolution:**
```bash
# Option 1: Clear stale lock
wrangler r2 object delete events scheduler-lock.json

# Option 2: Manually trigger snapshot
# Deploy with manual trigger endpoint (dev only)

# Option 3: Create snapshot programmatically
```

```typescript
import { createSnapshot } from '@dotdo/events'

await createSnapshot(env.EVENTS_BUCKET, namespace, collection, 'daily')
```

---

### Scenario 3: R2 Bucket Unavailable

**Symptoms:**
- Event ingestion failing
- Query endpoints returning errors
- R2 operations timing out

**Diagnosis:**
```bash
# Check R2 status
wrangler r2 bucket info events

# Check Cloudflare status page
# https://www.cloudflarestatus.com/
```

**Resolution:**
1. **Check Cloudflare status** for R2 outages
2. **Enable degraded mode** if available (queue events)
3. **Wait for recovery** (R2 is highly available)
4. **Replay queued events** after recovery

**Prevention:**
- Monitor R2 error rates
- Implement circuit breaker for R2 operations
- Consider multi-bucket redundancy for critical data

---

### Scenario 4: Durable Object Storage Limit Exceeded

**Symptoms:**
- DO operations failing with storage errors
- 10GB limit exceeded per DO instance

**Diagnosis:**
```bash
# Check DO storage in dashboard
# Cloudflare Dashboard > Workers > events-do > Durable Objects

wrangler tail --search="storage" --status=error
```

**Resolution:**
```typescript
// Option 1: Archive old data to R2
const oldRecords = sql.exec<{ id: string; data: string }>(
  `SELECT id, data FROM _collections WHERE collection = ? ORDER BY updated_at LIMIT 10000`,
  collection
).toArray()

// Archive to R2
await env.EVENTS_BUCKET.put(
  `archive/${doId}/${Date.now()}.json`,
  JSON.stringify(oldRecords)
)

// Delete archived records
for (const record of oldRecords) {
  sql.exec(`DELETE FROM _collections WHERE collection = ? AND id = ?`, collection, record.id)
}

// Option 2: Shard the DO (requires code changes)
// Split data across multiple DO instances by key prefix
```

**Prevention:**
- Monitor DO storage growth
- Implement automatic archival at 80% capacity
- Design sharding strategy for large collections

---

### Scenario 5: Queue Backlog Growing

**Symptoms:**
- Queue messages accumulating
- CDC/subscription delivery delayed
- Dead letters increasing

**Diagnosis:**
```bash
# Check queue stats
wrangler queues list

# Check consumer logs
wrangler tail --search="queue"
```

**Resolution:**
```bash
# Option 1: Scale consumer (increase batch size temporarily)
# Edit wrangler.jsonc:
# "max_batch_size": 100 -> 500

# Option 2: Clear DLQ after investigation
# Review dead letters first!

# Option 3: Pause non-critical consumers
```

**Prevention:**
- Monitor queue depth
- Set alerts for backlog thresholds
- Implement backpressure mechanisms

---

### Scenario 6: Deployment Rollback Required

**Symptoms:**
- New deployment causing errors
- Performance degradation after deploy
- Feature broken in production

**Resolution:**
```bash
# Step 1: Immediate rollback
wrangler rollback

# Step 2: Verify rollback successful
curl https://events.do/health

# Step 3: Check logs for resolution
wrangler tail --since=5m

# Step 4: Document the issue
# Create incident report
```

**Post-Rollback Actions:**
1. Identify root cause in development
2. Add tests to prevent recurrence
3. Deploy fix to staging first
4. Schedule maintenance window for re-deployment

---

### Scenario 7: Data Loss Detected

**Symptoms:**
- Customer reports missing data
- Query returns fewer results than expected
- Audit shows gaps in event stream

**Immediate Actions:**
1. **Preserve evidence** - Do not modify any data
2. **Identify scope** - Which collections/time ranges affected
3. **Check recent deployments** - Any changes that could cause data loss

**Recovery:**
```typescript
// Step 1: Identify the loss window
const lossStart = new Date('2026-02-01T10:00:00Z')
const lossEnd = new Date('2026-02-01T14:00:00Z')

// Step 2: Check what data exists in R2
const events = await queryR2Events(env.EVENTS_BUCKET, {
  start: lossStart,
  end: lossEnd,
})
console.log(`Found ${events.length} events in loss window`)

// Step 3: Check CDC deltas
const deltas = await listDeltas(env.EVENTS_BUCKET, namespace, collection, lossStart, lossEnd)
console.log(`Found ${deltas.length} delta files`)

// Step 4: Reconstruct and compare
const beforeState = await reconstructState(env.EVENTS_BUCKET, namespace, collection, lossStart)
const afterState = await reconstructState(env.EVENTS_BUCKET, namespace, collection, lossEnd)

// Step 5: Identify missing documents
const missing = []
for (const [id, doc] of afterState) {
  if (!currentState.has(id)) {
    missing.push({ id, doc })
  }
}

// Step 6: Restore missing documents
for (const { id, doc } of missing) {
  // Apply to current state
}
```

**Prevention:**
- Enable CDC for all critical collections
- Monitor event counts for anomalies
- Set up data integrity checks

---

## Appendix

### Recovery Verification Checklist

After any recovery procedure, verify:

- [ ] Health endpoint returns 200
- [ ] Event ingestion working (`POST /ingest`)
- [ ] Query endpoint returning data (`GET /events`)
- [ ] CDC events being captured
- [ ] Subscriptions delivering events
- [ ] Rate limiting functional
- [ ] All DO classes accessible
- [ ] R2 read/write operations working
- [ ] Queue processing normally

### Important File Locations

| Path | Purpose |
|------|---------|
| `/Users/nathanclevenger/projects/events/wrangler.jsonc` | Worker configuration |
| `/Users/nathanclevenger/projects/events/src/` | Worker source code |
| `/Users/nathanclevenger/projects/events/core/` | `@dotdo/events` package |
| `/Users/nathanclevenger/projects/events/docs/OPERATIONS.md` | Operational runbook |
| `/Users/nathanclevenger/projects/events/docs/LAKEHOUSE.md` | R2 storage documentation |

### Related Documentation

- [OPERATIONS.md](./OPERATIONS.md) - Day-to-day operational procedures
- [LAKEHOUSE.md](./LAKEHOUSE.md) - R2 storage architecture
- [GETTING_STARTED.md](./GETTING_STARTED.md) - Quick start guide
- [CLAUDE.md](/CLAUDE.md) - Project context

---

*Last Updated: 2026-02-02*
*Next Review: 2026-05-02*
