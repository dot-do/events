# Event Subscriptions

This guide covers the subscription system in events.do, which enables event-driven workflows by delivering events to your workers based on pattern matching.

## Table of Contents

1. [Overview](#overview)
2. [How Subscriptions Work](#how-subscriptions-work)
3. [Pattern Matching Syntax](#pattern-matching-syntax)
4. [Creating Subscriptions](#creating-subscriptions)
5. [Webhook Delivery](#webhook-delivery)
6. [Retry Behavior](#retry-behavior)
7. [Dead Letter Queue](#dead-letter-queue)
8. [Configuration Options](#configuration-options)
9. [Multi-Tenant Support](#multi-tenant-support)
10. [REST API Reference](#rest-api-reference)
11. [Code Examples](#code-examples)
12. [Monitoring and Debugging](#monitoring-and-debugging)

---

## Overview

The subscription system allows you to:

- Subscribe workers to specific event patterns (e.g., `webhook.github.*`, `collection.users.**`)
- Receive event deliveries via RPC (service bindings) or HTTP
- Automatically retry failed deliveries with exponential backoff
- Handle permanently failed deliveries via a dead letter queue
- Query subscription status and delivery history

### Architecture

```
                                 ┌─────────────────┐
                                 │  Event Source   │
                                 │  (EventEmitter) │
                                 └────────┬────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        events.do/ingest                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐    │
│  │   R2 Store   │   │ CDC Fanout   │   │ Subscription     │    │
│  │  (lakehouse) │   │              │   │ Fanout           │    │
│  └──────────────┘   └──────────────┘   └────────┬─────────┘    │
└─────────────────────────────────────────────────┼───────────────┘
                                                  │
                    ┌─────────────────────────────┼─────────────────┐
                    │                             │                 │
                    ▼                             ▼                 ▼
          ┌─────────────────┐         ┌─────────────────┐  ┌───────────────┐
          │ SubscriptionDO  │         │ SubscriptionDO  │  │SubscriptionDO │
          │ (collection)    │         │ (webhook)       │  │ (default)     │
          └────────┬────────┘         └────────┬────────┘  └───────┬───────┘
                   │                           │                   │
                   ▼                           ▼                   ▼
          ┌─────────────────┐         ┌─────────────────┐  ┌───────────────┐
          │  Your Worker    │         │  Your Worker    │  │ Your Worker   │
          │  (RPC/HTTP)     │         │  (RPC/HTTP)     │  │ (RPC/HTTP)    │
          └─────────────────┘         └─────────────────┘  └───────────────┘
```

---

## How Subscriptions Work

### Event Flow

1. **Event Ingestion**: Events arrive at events.do via the `/ingest` endpoint
2. **Pattern Matching**: Each event type is matched against active subscription patterns
3. **Delivery Creation**: A delivery record is created for each matching subscription
4. **Immediate Delivery**: events.do attempts to deliver the event to the subscriber
5. **Retry or Dead Letter**: Failed deliveries are retried with backoff or moved to dead letter queue

### Sharding

Subscriptions are sharded by the first segment of the event type (before the first `.`):

| Event Type | Shard |
|------------|-------|
| `collection.users.insert` | `collection` |
| `webhook.github.push` | `webhook` |
| `rpc.UserService.getUser` | `rpc` |
| `ws.connect` | `ws` |
| `custom.my-event` | `default` |

This distributes load across multiple Durable Object instances for better scalability.

### Known Shards

The system recognizes these standard shards:
- `collection` - CDC (Change Data Capture) events
- `rpc` - RPC method call events
- `do` - Durable Object lifecycle events
- `ws` - WebSocket events
- `webhook` - Webhook events
- `default` - Catch-all for unrecognized prefixes

---

## Pattern Matching Syntax

Subscription patterns use glob-style syntax with dot-separated segments.

### Pattern Types

| Pattern | Description | Example Matches |
|---------|-------------|-----------------|
| `exact.match` | Exact match only | `exact.match` |
| `prefix.*` | Single segment wildcard | `prefix.a`, `prefix.xyz` |
| `prefix.**` | Multi-segment wildcard | `prefix`, `prefix.a`, `prefix.a.b.c` |
| `*.middle.*` | Wildcards in any position | `a.middle.b`, `x.middle.y` |
| `**` | Match all events | Any event type |

### Single Wildcard (`*`)

The `*` wildcard matches exactly one segment:

```
Pattern: webhook.github.*
Matches: webhook.github.push, webhook.github.pull_request
Does NOT match: webhook.github.push.v1, webhook.github
```

### Double Wildcard (`**`)

The `**` wildcard matches zero or more segments:

```
Pattern: webhook.**
Matches: webhook, webhook.github, webhook.github.push, webhook.github.push.v1
```

### Complex Patterns

```
Pattern: webhook.**.push
Matches: webhook.push, webhook.github.push, webhook.github.v1.push
Does NOT match: webhook.github.pull
```

### Pattern Validation Rules

- Maximum length: 256 characters
- Maximum segments: 10 (split by `.`)
- Allowed characters: `a-z`, `A-Z`, `0-9`, `.`, `*`, `_`, `-`
- No consecutive asterisks beyond `**` (e.g., `***` is invalid)
- Cannot start or end with `.`
- Cannot have consecutive dots (`..`)

---

## Creating Subscriptions

### Via REST API

```bash
curl -X POST https://events.do/subscriptions/subscribe \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{
    "workerId": "my-worker",
    "pattern": "webhook.github.*",
    "rpcMethod": "handleGitHubEvent",
    "maxRetries": 5,
    "timeoutMs": 30000
  }'
```

Response:
```json
{
  "ok": true,
  "subscriptionId": "01HQXYZ123456789ABCDEF",
  "shard": "webhook",
  "namespace": null
}
```

### Via Code (SubscriptionDO RPC)

```typescript
// Get subscription DO stub
const subId = env.SUBSCRIPTIONS.idFromName('webhook')
const subscriptionDO = env.SUBSCRIPTIONS.get(subId)

// Create subscription
const result = await subscriptionDO.subscribe({
  workerId: 'my-worker',
  workerBinding: 'MY_HANDLER',  // Optional: use service binding instead of HTTP
  pattern: 'webhook.github.*',
  rpcMethod: 'handleGitHubEvent',
  maxRetries: 5,
  timeoutMs: 30000,
})

if (result.ok) {
  console.log('Subscription created:', result.subscriptionId)
} else {
  console.error('Failed:', result.error)
}
```

---

## Webhook Delivery

When an event matches a subscription, events.do delivers it to your worker.

### Delivery Methods

#### 1. Service Binding (RPC)

If `workerBinding` is specified and the binding exists in the environment, events.do calls the RPC method directly:

```typescript
// Your handler worker
export class MyHandler {
  async handleGitHubEvent(payload: unknown): Promise<{ processed: boolean }> {
    // Process the event
    console.log('Received event:', payload)
    return { processed: true }
  }
}
```

#### 2. HTTP Fallback

If no service binding is configured, events.do makes an HTTP POST request:

```
POST https://{workerId}.workers.dev/rpc/{rpcMethod}
Content-Type: application/json

{
  "id": "01HQXYZ...",
  "type": "webhook.github.push",
  "ts": "2026-01-15T10:30:00.000Z",
  "payload": { ... }
}
```

### Payload Structure

The delivered payload includes:

```typescript
interface DeliveryPayload {
  id: string           // Unique event ID (ULID)
  type: string         // Event type (e.g., "webhook.github.push")
  ts: string           // ISO 8601 timestamp
  payload: {
    // Original event data
    type: string
    ts: string
    do?: {
      id: string
      name: string
      class?: string
    }
    // ... additional event fields
    _namespace?: string  // Tenant namespace (if multi-tenant)
  }
}
```

---

## Retry Behavior

Failed deliveries are automatically retried with exponential backoff.

### Backoff Formula

```
delay = min(baseDelay * 2^(attempt-1), maxDelay) + random_jitter
```

Where:
- `baseDelay`: 1,000ms (1 second)
- `maxDelay`: 300,000ms (5 minutes)
- `jitter`: 0-1,000ms (random)

### Retry Schedule

| Attempt | Base Delay | With Max Jitter |
|---------|------------|-----------------|
| 1 | 1s | 1-2s |
| 2 | 2s | 2-3s |
| 3 | 4s | 4-5s |
| 4 | 8s | 8-9s |
| 5 | 16s | 16-17s |
| 6+ | 32s-5min | varies |

### Retry Triggers

Delivery is retried when:
- HTTP response status is not 2xx
- Request times out (default: 30 seconds)
- Network error occurs
- RPC method throws an error

### Retry Processing

Retries are processed via Durable Object alarms:
1. Failed delivery schedules alarm for `next_attempt_at`
2. Alarm fires and processes up to 100 pending deliveries
3. New alarms scheduled for remaining retries

---

## Dead Letter Queue

After `maxRetries` attempts (default: 5), failed deliveries are moved to the dead letter queue.

### Dead Letter Record

```typescript
interface DeadLetter {
  id: string              // Dead letter ID
  deliveryId: string      // Original delivery ID
  subscriptionId: string  // Subscription that failed
  eventId: string         // Original event ID
  eventPayload: string    // JSON-serialized event data
  reason: string          // "max_retries_exceeded"
  lastError: string       // Last error message
  createdAt: number       // Timestamp (ms)
}
```

### Querying Dead Letters

```bash
# List dead letters for a subscription
curl https://events.do/subscriptions/SUB_ID/dead-letters \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### Retrying Dead Letters

```bash
# Retry a dead letter (creates new delivery)
curl -X POST https://events.do/subscriptions/SUB_ID/dead-letters/DL_ID/retry \
  -H "Authorization: Bearer YOUR_API_KEY"
```

---

## Configuration Options

### Subscription Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `workerId` | string | required | Target worker identifier |
| `workerBinding` | string | null | Service binding name (for RPC) |
| `pattern` | string | required | Event pattern to match |
| `rpcMethod` | string | required | Method to call on delivery |
| `maxRetries` | number | 5 | Maximum delivery attempts |
| `timeoutMs` | number | 30000 | Delivery timeout (ms) |

### System Limits

| Limit | Value | Description |
|-------|-------|-------------|
| Pattern max length | 256 chars | Maximum pattern string length |
| Pattern max segments | 10 | Maximum dot-separated segments |
| Batch limit | 100 | Max deliveries per alarm |
| Max retry delay | 5 minutes | Exponential backoff cap |
| Base retry delay | 1 second | Initial retry delay |

---

## Multi-Tenant Support

Subscriptions support namespace isolation for multi-tenant deployments.

### How It Works

- Non-admin tenants have their subscriptions isolated by namespace prefix
- Shard keys become `{namespace}:{shard}` (e.g., `acme:webhook`)
- Admin tenants can access all subscriptions (no prefix)

### Creating Namespaced Subscriptions

The tenant context is extracted from the request (via middleware) and applied automatically:

```bash
# Request with tenant header
curl -X POST https://events.do/subscriptions/subscribe \
  -H "X-Tenant-Namespace: acme" \
  -H "Authorization: Bearer TENANT_API_KEY" \
  -d '{
    "workerId": "acme-handler",
    "pattern": "webhook.stripe.*",
    "rpcMethod": "handleStripe"
  }'

# Response includes namespace
{
  "ok": true,
  "subscriptionId": "01HQ...",
  "shard": "acme:webhook",
  "namespace": "acme"
}
```

---

## REST API Reference

### Create Subscription

```
POST /subscriptions/subscribe
```

Request body:
```json
{
  "workerId": "string (required)",
  "workerBinding": "string (optional)",
  "pattern": "string (required)",
  "rpcMethod": "string (required)",
  "maxRetries": "number (optional, default: 5)",
  "timeoutMs": "number (optional, default: 30000)"
}
```

### Deactivate Subscription

```
POST /subscriptions/unsubscribe
```

Request body:
```json
{
  "subscriptionId": "string (required)",
  "shard": "string (optional)"
}
```

### Reactivate Subscription

```
POST /subscriptions/reactivate
```

Request body:
```json
{
  "subscriptionId": "string (required)",
  "shard": "string (optional)"
}
```

### List Subscriptions

```
GET /subscriptions/list
```

Query parameters:
- `active` - Filter by active status (`true`/`false`)
- `workerId` - Filter by worker ID
- `patternPrefix` - Filter by pattern prefix
- `shard` - Query specific shard only
- `limit` - Maximum results
- `offset` - Pagination offset

### Get Subscription

```
GET /subscriptions/{subscriptionId}
```

### Update Subscription

```
PUT /subscriptions/{subscriptionId}
```

Request body:
```json
{
  "maxRetries": "number (optional)",
  "timeoutMs": "number (optional)",
  "rpcMethod": "string (optional)"
}
```

### Delete Subscription

```
DELETE /subscriptions/{subscriptionId}
```

### Get Subscription Status

```
GET /subscriptions/status/{subscriptionId}
```

Response includes delivery statistics:
```json
{
  "subscription": { ... },
  "stats": {
    "pendingDeliveries": 0,
    "failedDeliveries": 2,
    "deadLetters": 1,
    "successRate": 0.95,
    "totalDelivered": 100,
    "totalAttempts": 105
  }
}
```

### Find Matching Subscriptions

```
GET /subscriptions/match?eventType={eventType}
```

### Get Dead Letters

```
GET /subscriptions/{subscriptionId}/dead-letters
```

### Retry Dead Letter

```
POST /subscriptions/{subscriptionId}/dead-letters/{deadLetterId}/retry
```

### Get Delivery Logs

```
GET /subscriptions/deliveries/{deliveryId}/logs
```

---

## Code Examples

### Basic Subscription Setup

```typescript
import type { Env } from './env'

// Subscribe to GitHub webhook events
async function setupSubscription(env: Env): Promise<void> {
  const response = await fetch('https://events.do/subscriptions/subscribe', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${env.EVENTS_API_KEY}`,
    },
    body: JSON.stringify({
      workerId: 'github-handler',
      pattern: 'webhook.github.*',
      rpcMethod: 'handleGitHubWebhook',
      maxRetries: 3,
      timeoutMs: 10000,
    }),
  })

  const result = await response.json()
  console.log('Subscription created:', result)
}
```

### Webhook Handler Worker

```typescript
export interface Env {
  // Your environment bindings
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    // Handle RPC-style webhook delivery
    if (url.pathname.startsWith('/rpc/') && request.method === 'POST') {
      const method = url.pathname.replace('/rpc/', '')
      const payload = await request.json()

      try {
        switch (method) {
          case 'handleGitHubWebhook':
            await handleGitHubWebhook(payload)
            return Response.json({ processed: true })

          case 'handleStripeWebhook':
            await handleStripeWebhook(payload)
            return Response.json({ processed: true })

          default:
            return Response.json({ error: 'Unknown method' }, { status: 404 })
        }
      } catch (error) {
        console.error('Handler error:', error)
        return Response.json({ error: 'Processing failed' }, { status: 500 })
      }
    }

    return new Response('Not found', { status: 404 })
  },
}

async function handleGitHubWebhook(payload: unknown): Promise<void> {
  console.log('Processing GitHub event:', payload)
  // Your business logic here
}

async function handleStripeWebhook(payload: unknown): Promise<void> {
  console.log('Processing Stripe event:', payload)
  // Your business logic here
}
```

### CDC Event Subscription

```typescript
// Subscribe to all user collection changes
await subscriptionDO.subscribe({
  workerId: 'user-sync',
  pattern: 'collection.users.*',  // insert, update, delete
  rpcMethod: 'syncUserChange',
  maxRetries: 5,
})

// Handler for CDC events
export class UserSyncHandler {
  async syncUserChange(payload: {
    type: string
    collection: string
    docId: string
    doc: unknown
    prev?: unknown
  }): Promise<void> {
    switch (payload.type) {
      case 'collection.insert':
        await this.handleUserCreated(payload.docId, payload.doc)
        break
      case 'collection.update':
        await this.handleUserUpdated(payload.docId, payload.doc, payload.prev)
        break
      case 'collection.delete':
        await this.handleUserDeleted(payload.docId, payload.prev)
        break
    }
  }

  private async handleUserCreated(id: string, user: unknown): Promise<void> {
    // Sync to external system
  }

  private async handleUserUpdated(
    id: string,
    current: unknown,
    previous: unknown
  ): Promise<void> {
    // Handle user update with diff capability
  }

  private async handleUserDeleted(id: string, previous: unknown): Promise<void> {
    // Handle user deletion
  }
}
```

### Monitoring Subscription Health

```typescript
async function checkSubscriptionHealth(
  env: Env,
  subscriptionId: string
): Promise<void> {
  const response = await fetch(
    `https://events.do/subscriptions/status/${subscriptionId}`,
    {
      headers: {
        'Authorization': `Bearer ${env.EVENTS_API_KEY}`,
      },
    }
  )

  const { subscription, stats } = await response.json()

  console.log('Subscription:', subscription?.pattern)
  console.log('Success rate:', (stats.successRate * 100).toFixed(1) + '%')
  console.log('Pending deliveries:', stats.pendingDeliveries)
  console.log('Failed deliveries:', stats.failedDeliveries)
  console.log('Dead letters:', stats.deadLetters)

  // Alert on high failure rate
  if (stats.successRate < 0.9 && stats.totalAttempts > 10) {
    console.warn('Warning: Subscription success rate below 90%')
  }

  // Alert on growing dead letter queue
  if (stats.deadLetters > 100) {
    console.warn('Warning: Dead letter queue growing')
  }
}
```

---

## Monitoring and Debugging

### Delivery Logs

Each delivery attempt is logged with:
- Attempt number
- Status (success/failed)
- Duration (ms)
- Error message (if failed)
- Worker response (if successful)

Query logs for a specific delivery:

```bash
curl https://events.do/subscriptions/deliveries/DELIVERY_ID/logs \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### Common Issues

#### Deliveries Stuck in "Failed" Status

1. Check the last error message in delivery logs
2. Verify your worker is running and accessible
3. Check timeout settings (increase if needed)
4. Verify the RPC method exists and handles the payload correctly

#### High Dead Letter Count

1. Review dead letters to identify patterns
2. Check if target worker is experiencing issues
3. Increase `maxRetries` if transient failures are common
4. Implement better error handling in your worker

#### Pattern Not Matching

1. Verify pattern syntax is correct
2. Check if subscription is active
3. Ensure event type follows expected format
4. Use `/subscriptions/match?eventType=...` to test matching

### Cleanup

Old delivery records can be cleaned up to prevent unbounded growth:

```typescript
// Clean up records older than 30 days
const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000)
await subscriptionDO.cleanupOldData(thirtyDaysAgo)
```

---

## Next Steps

- [Getting Started Guide](./GETTING_STARTED.md) - Initial setup
- [Operations Guide](./OPERATIONS.md) - Monitoring and maintenance
- [API Reference](/core/README.md) - Detailed API documentation
