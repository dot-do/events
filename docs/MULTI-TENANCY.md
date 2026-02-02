# Multi-Tenancy and Namespace Isolation

This document describes how events.do implements multi-tenancy through namespace isolation, enabling secure separation of data between different tenants, teams, or applications.

## Table of Contents

1. [Overview](#overview)
2. [How Namespaces Work](#how-namespaces-work)
3. [API Key Formats](#api-key-formats)
4. [Configuration](#configuration)
5. [Isolation Guarantees](#isolation-guarantees)
6. [Code Examples](#code-examples)
7. [Best Practices](#best-practices)
8. [Migration Guide](#migration-guide)

---

## Overview

events.do provides multi-tenant isolation through **namespaces**. Each namespace represents an isolated tenant environment with:

- **Separate storage paths** in R2 (all data prefixed with `ns/<namespace>/`)
- **Scoped API keys** that can only access their assigned namespace
- **Query isolation** ensuring tenants cannot access each other's data
- **Durable Object sharding** with namespace-prefixed shard keys

### Architecture

```
                     ┌─────────────────────────────────────────────┐
                     │              events.do API                  │
                     └─────────────────────────────────────────────┘
                                        │
                     ┌──────────────────┼──────────────────┐
                     │                  │                  │
              ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
              │  Namespace  │    │  Namespace  │    │  Namespace  │
              │   "acme"    │    │   "beta"    │    │  "gamma"    │
              └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
                     │                  │                  │
              ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
              │ R2 Storage  │    │ R2 Storage  │    │ R2 Storage  │
              │ns/acme/...  │    │ns/beta/...  │    │ns/gamma/... │
              └─────────────┘    └─────────────┘    └─────────────┘
```

---

## How Namespaces Work

### Namespace Naming Rules

Namespaces must follow these rules:

- **Length:** 1-64 characters
- **Characters:** Lowercase alphanumeric, underscores (`_`), and hyphens (`-`)
- **Reserved names:** The following are reserved and cannot be used:
  - `admin`
  - `system`
  - `internal`
  - `default`
  - `public`
  - `global`

Valid examples:
- `acme-corp`
- `team_alpha`
- `prod-us-east-1`
- `customer123`

Invalid examples:
- `Acme` (uppercase)
- `my.namespace` (dots)
- `my namespace` (spaces)
- `admin` (reserved)

### Data Storage Layout

All tenant data is stored under a namespace-prefixed path in R2:

```
R2 Bucket
├── ns/
│   ├── acme/
│   │   ├── events/
│   │   │   └── 2026/01/15/12/abc123.parquet
│   │   ├── dedup/
│   │   │   └── batch-xyz.marker
│   │   └── cdc/
│   │       └── users/
│   │           ├── data.parquet
│   │           └── deltas/
│   └── beta/
│       ├── events/
│       └── cdc/
└── events/  (legacy admin data)
```

### Request Flow

1. **Authentication:** Request includes Bearer token in Authorization header
2. **Tenant Extraction:** Middleware parses the API key to determine namespace
3. **Namespace Scoping:** All operations are automatically scoped to the tenant's namespace
4. **Response:** Includes namespace in response for confirmation

```
Request                          Response
─────────────────────────────    ─────────────────────────────
POST /ingest                     {
Authorization: Bearer             "ok": true,
  ns_acme_abc123                   "received": 42,
                                   "namespace": "acme"
{events: [...]}                  }
```

---

## API Key Formats

### Namespace-Scoped API Keys

The recommended format for API keys that restrict access to a specific namespace:

```
ns_<namespace>_<token>
```

Examples:
- `ns_acme_abc123def456` - Access to `acme` namespace only
- `ns_production_xyz789` - Access to `production` namespace only
- `ns_team-alpha_secret123` - Access to `team-alpha` namespace only

### Legacy Admin Token

For backwards compatibility, a single `AUTH_TOKEN` can be used which grants admin access to all namespaces:

```
Bearer <AUTH_TOKEN>
```

Admin tokens:
- Can query all namespaces
- Use the `DEFAULT_NAMESPACE` for writes (defaults to `default`)
- Can access legacy (non-namespaced) data paths

### Generating API Keys

You can generate namespace-scoped API keys programmatically:

```typescript
import { generateNamespaceKey } from '@dotdo/events'

// Generate a new API key for the "acme" namespace
const token = crypto.randomUUID().replace(/-/g, '')
const apiKey = generateNamespaceKey('acme', token)
// Result: ns_acme_abc123def456...
```

---

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `AUTH_TOKEN` | Legacy admin token (grants access to all namespaces) | `secret-admin-token` |
| `NAMESPACE_API_KEYS` | JSON mapping of API keys to namespaces | `{"ns_acme_xxx": "acme"}` |
| `DEFAULT_NAMESPACE` | Default namespace for admin tokens | `production` |

### Setting Up Namespace API Keys

1. **Generate keys for each tenant:**

```bash
# Generate a random token
TOKEN=$(openssl rand -hex 16)
echo "ns_acme_${TOKEN}"
```

2. **Configure the mapping:**

```bash
# Create the JSON mapping
KEYS='{"ns_acme_abc123": "acme", "ns_beta_xyz789": "beta"}'

# Set as a secret (recommended)
wrangler secret put NAMESPACE_API_KEYS
# Paste the JSON when prompted

# Or set as a variable (less secure, visible in dashboard)
# In wrangler.jsonc:
# "vars": { "NAMESPACE_API_KEYS": "{...}" }
```

3. **Distribute keys to tenants:**

Each tenant receives only their namespace-scoped API key. They cannot access other namespaces.

### Wrangler Configuration Example

```jsonc
// wrangler.jsonc
{
  "vars": {
    "ENVIRONMENT": "production",
    "DEFAULT_NAMESPACE": "internal"
  }
  // AUTH_TOKEN and NAMESPACE_API_KEYS should be set via `wrangler secret put`
}
```

---

## Isolation Guarantees

### Data Isolation

| Layer | Mechanism | Guarantee |
|-------|-----------|-----------|
| **Storage** | R2 path prefix (`ns/<namespace>/`) | Data physically separated |
| **Queries** | Path filtering + payload._namespace check | Defense in depth |
| **DOs** | Namespace-prefixed shard keys | Separate DO instances |
| **API Keys** | Validated against NAMESPACE_API_KEYS | Cannot forge namespace |

### What is Isolated

1. **Events:** All events stored under `ns/<namespace>/events/`
2. **CDC Data:** Collection snapshots and deltas under `ns/<namespace>/cdc/`
3. **Deduplication Markers:** Under `ns/<namespace>/dedup/`
4. **Query Results:** Filtered by namespace at query time

### Security Boundaries

**Cross-namespace access is prevented at multiple layers:**

1. **Authentication:** API key must match the namespace being accessed
2. **Path Construction:** Paths are built server-side with namespace prefix
3. **Query Filtering:** WHERE clause includes namespace condition
4. **Payload Tagging:** Events include `_namespace` field for verification

```typescript
// Example: How queries are isolated
const pathPattern = `ns/${escapeSql(namespace)}/events/**/*.parquet`
const conditions = [
  ...otherConditions,
  `payload._namespace = '${escapeSql(namespace)}'`  // Defense in depth
]
```

---

## Code Examples

### Sending Events as a Tenant

```typescript
// Using curl
curl -X POST https://events.do/ingest \
  -H "Authorization: Bearer ns_acme_abc123def456" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "type": "custom.user_signup",
        "ts": "2026-01-15T12:00:00Z",
        "do": { "id": "user-123", "class": "UserDO" },
        "data": { "plan": "pro" }
      }
    ]
  }'

# Response:
# {
#   "ok": true,
#   "received": 1,
#   "namespace": "acme"
# }
```

### Querying Events as a Tenant

```typescript
// Tenant can only query their own namespace
curl -X POST https://events.do/query \
  -H "Authorization: Bearer ns_acme_abc123def456" \
  -H "Content-Type: application/json" \
  -d '{
    "dateRange": {
      "start": "2026-01-01T00:00:00Z",
      "end": "2026-01-31T23:59:59Z"
    },
    "eventTypes": ["custom.user_signup"],
    "limit": 100
  }'

# Query is automatically scoped to "acme" namespace
# Response includes SQL that filters by namespace
```

### Admin Cross-Namespace Query

```typescript
// Admin can query specific namespaces
curl -X POST https://events.do/query \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "acme",  // Explicit namespace
    "dateRange": { "start": "2026-01-01", "end": "2026-01-31" },
    "limit": 100
  }'

# Admin without namespace queries all data
curl -X POST https://events.do/query \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dateRange": { "start": "2026-01-01", "end": "2026-01-31" },
    "limit": 100
  }'
```

### Using EventEmitter with Namespace

```typescript
import { EventEmitter } from '@dotdo/events'

export class MyDO extends DurableObject<Env> {
  events = new EventEmitter(this.ctx, this.env, {
    cdc: true,
    // API key includes namespace (ns_acme_xxx)
    apiKey: this.env.EVENTS_API_KEY,
  })

  async doSomething() {
    // Events are automatically sent to the namespace
    // associated with the API key
    this.events.emit({
      type: 'custom.action',
      data: { userId: 'user-123' }
    })
  }
}
```

### Validating Namespace Access

```typescript
import {
  extractTenantContext,
  validateNamespace,
  buildNamespacedR2Path,
} from '@dotdo/events/middleware/tenant'

// In your handler:
export async function handleRequest(request: Request, env: Env) {
  const tenant = extractTenantContext(request, env)
  if (tenant instanceof Response) {
    return tenant // 401 Unauthorized
  }

  // Build namespace-isolated paths
  const path = buildNamespacedR2Path(tenant, 'events', '2026', '01', 'file.parquet')
  // For tenant: "ns/acme/events/2026/01/file.parquet"
  // For admin: "events/2026/01/file.parquet"

  // Use tenant context in your logic
  console.log(`Processing request for namespace: ${tenant.namespace}`)
  console.log(`Is admin: ${tenant.isAdmin}`)
}
```

---

## Best Practices

### 1. Always Use Namespace-Scoped Keys

Avoid using the legacy `AUTH_TOKEN` for tenant operations. Instead:

- Generate unique namespace-scoped keys for each tenant
- Store keys securely (e.g., in tenant's secrets manager)
- Rotate keys periodically

### 2. Validate Namespace Names

Before creating namespaces programmatically, validate the name:

```typescript
import { validateNamespace } from '@dotdo/events/middleware/tenant'

const error = validateNamespace(userProvidedNamespace)
if (error) {
  throw new Error(`Invalid namespace: ${error}`)
}
```

### 3. Use Meaningful Namespace Names

Choose namespace names that identify the tenant:

- `customer-acme-corp` - Customer identifier
- `team-engineering` - Team name
- `env-staging` - Environment name
- `project-foo` - Project identifier

### 4. Monitor Per-Namespace Usage

Track metrics per namespace to:

- Identify high-volume tenants
- Detect anomalies
- Plan capacity

### 5. Implement Tenant Provisioning

Create an admin endpoint for tenant provisioning:

```typescript
async function provisionTenant(namespace: string): Promise<string> {
  // 1. Validate namespace
  const error = validateNamespace(namespace)
  if (error) throw new Error(error)

  // 2. Generate API key
  const token = crypto.randomUUID().replace(/-/g, '')
  const apiKey = generateNamespaceKey(namespace, token)

  // 3. Store in NAMESPACE_API_KEYS (via KV or secrets rotation)
  // ...

  // 4. Return API key to caller
  return apiKey
}
```

### 6. Handle Legacy Data Migration

If migrating from single-tenant to multi-tenant:

1. Existing data remains at root paths (e.g., `events/`)
2. Admin token can still access this data
3. New tenant data goes to namespaced paths
4. Optionally migrate old data to a specific namespace

---

## Migration Guide

### From Single-Tenant to Multi-Tenant

1. **Set up NAMESPACE_API_KEYS:**

```bash
wrangler secret put NAMESPACE_API_KEYS
# Enter: {"ns_tenant1_xxx": "tenant1", "ns_tenant2_yyy": "tenant2"}
```

2. **Keep AUTH_TOKEN for admin access:**

The existing `AUTH_TOKEN` continues to work as an admin token.

3. **Distribute namespace-scoped keys:**

Give each tenant their specific API key.

4. **Update client applications:**

Change from:
```typescript
headers: { Authorization: `Bearer ${AUTH_TOKEN}` }
```

To:
```typescript
headers: { Authorization: `Bearer ${NAMESPACE_API_KEY}` }
```

5. **Verify isolation:**

Test that tenants can only access their own data.

### From Legacy to Namespaced Paths

Legacy data stored at root paths (`events/`, `cdc/`) is accessible only by admin tokens. To migrate:

1. Query old data with admin token
2. Re-ingest to the target namespace
3. Optionally delete old data

---

## API Reference

### Tenant Context Type

```typescript
interface TenantContext {
  /** The namespace this request is scoped to */
  namespace: string
  /** Whether this is an admin with cross-namespace access */
  isAdmin: boolean
  /** Original API key (masked for logging) */
  keyId: string
}
```

### Helper Functions

```typescript
// Parse a namespace-scoped API key
parseNamespaceKey(key: string): { namespace: string; token: string } | null

// Generate a namespace-scoped API key
generateNamespaceKey(namespace: string, token: string): string

// Validate a namespace string
validateNamespace(namespace: string): string | null  // null = valid

// Extract tenant context from request
extractTenantContext(request: Request, env: TenantEnv): TenantContext | Response

// Build a namespace-isolated R2 path
buildNamespacedR2Path(tenant: TenantContext, basePath: string, ...segments: string[]): string

// Validate a path belongs to tenant's namespace
validateNamespacePath(tenant: TenantContext, path: string): boolean

// Add namespace condition to SQL WHERE clauses
addNamespaceCondition(tenant: TenantContext, conditions: string[]): string[]

// Get namespace-prefixed R2 list prefix
getNamespacedListPrefix(tenant: TenantContext, basePrefix: string): string

// Get namespace-prefixed DO shard key
getNamespacedShardKey(tenant: TenantContext, baseKey: string): string
```

---

## Related Documentation

- [Getting Started](/docs/GETTING_STARTED.md) - Initial setup guide
- [Operations Runbook](/docs/OPERATIONS.md) - Deployment and monitoring
- [API Reference](/core/README.md) - Core package documentation
