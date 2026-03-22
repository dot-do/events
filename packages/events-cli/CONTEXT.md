---
name: events-cli-agent-skill
version: 1.0.0
metadata:
  requires:
    bins: ["events"]
---

# events CLI - Agent Skill File

The events CLI (`@dotdo/events-cli`) scaffolds, develops, deploys, and operates events.do projects -- event streaming, CDC, and lakehouse analytics for Cloudflare Durable Objects.

## Safety Invariants

### CRITICAL - Data Mutation

1. **`replay --all` retries ALL dead letter events.** This can trigger mass re-delivery of failed events to downstream subscribers. ALWAYS run `events replay --all --dry-run` first to preview what would be replayed. Use `--subscription-id` to scope replays to a single subscriber.

2. **`deploy` pushes to production Cloudflare Workers.** This replaces the running worker code. ALWAYS preview with `events deploy --dry-run` before deploying. Use `--env staging` to deploy to a non-production environment first.

3. **`query --sql` executes arbitrary SQL against the events store.** Never pass `DROP`, `DELETE`, `TRUNCATE`, or other DDL/DML statements. The query endpoint is intended for read-only analytics. Always include a `LIMIT` clause for exploratory queries.

### Secrets Handling

4. **Never store `EVENTS_API_KEY` in command arguments.** The CLI reads it from the `EVENTS_API_KEY` environment variable and passes it as a Bearer token in the Authorization header. Do not embed it in scripts, prompts, or shell history.

### Long-Running Commands

5. **`tail` blocks indefinitely** (SSE streaming). It will not exit until interrupted (Ctrl+C / SIGINT / SIGTERM). In automated contexts, always set an external timeout or use `timeout <seconds> events tail`.

6. **`dev` is a long-running local development server** (wraps `wrangler dev`). It blocks until interrupted.

### Read-Only Safety

7. **`status` is always safe.** It performs read-only HTTP requests to `/health` and `/shards` endpoints.

8. **`schema validate` is always safe.** It reads local files only, no network calls.

## Command Reference

### Safe / Read-Only Commands

| Command | Description | Notes |
|---------|-------------|-------|
| `events --help` | Show help | Always safe |
| `events --version` | Show version | Always safe |
| `events status` | Show system health and shard stats | Read-only HTTP GET |
| `events status --verbose` | Detailed shard config and per-shard stats | Read-only |
| `events schema validate` | Validate local JSON schema files | Local filesystem only |
| `events schema init` | Create example schema files | Local write, but non-destructive (skips existing) |
| `events schema generate` | Generate TypeScript types from schemas | Local write to `src/schema-types.ts` |
| `events query --sql "SELECT ..."` | Run analytics SQL query | Read-only if SQL is SELECT |

### Local Development Commands

| Command | Description | Notes |
|---------|-------------|-------|
| `events init` | Scaffold a new project (interactive prompts) | Creates directory; fails if exists |
| `events dev` | Start local wrangler dev server | Long-running; blocks |
| `events dev --local` | Dev server without remote resources | Local-only mode |

### Mutating / Production Commands (require caution)

| Command | Description | Risk Level |
|---------|-------------|------------|
| `events deploy` | Deploy to Cloudflare Workers | HIGH - replaces production code |
| `events deploy --env <env>` | Deploy to specific environment | MEDIUM - use for staging |
| `events deploy --dry-run` | Preview deployment | Safe |
| `events replay --dead-letter <id>` | Retry single dead letter | LOW - single event |
| `events replay --all` | Retry ALL dead letters | HIGH - mass re-delivery |
| `events replay --all --subscription-id <id>` | Retry dead letters for one subscriber | MEDIUM - scoped |
| `events replay --all --dry-run` | Preview replay | Safe |

### Long-Running / Streaming Commands

| Command | Description | Notes |
|---------|-------------|-------|
| `events tail` | Stream events via SSE | Blocks until Ctrl+C |
| `events tail --filter "collection.*"` | Stream filtered events | Glob pattern on event type |
| `events tail --namespace <ns>` | Stream events for namespace | Scoped stream |

### Dry-Run Pattern

For all mutating operations, always preview first:

```bash
# Preview deployment
events deploy --dry-run

# Preview dead letter replay
events replay --all --dry-run

# Preview scoped replay
events replay --all --subscription-id SUB_01XYZ789 --dry-run
```

## Output Parsing

- User-facing output goes to stdout with picocolors formatting.
- Error messages go to stderr, prefixed with `Error:`.
- Exit code 0 = success, 1 = failure.
- `query --format json` returns raw JSON (machine-parseable).
- `query --format table` returns formatted table output (default, human-readable).
- `status` output is structured text with labeled fields (Health, Shards sections).
- `tail` outputs events as they arrive, one per block, with timestamp, type, and metadata lines.

### Query Result Shape (JSON format)

```json
{
  "columns": ["id", "type", "ts"],
  "rows": [["evt_01", "collection.insert", "2026-03-21T10:00:00Z"]],
  "rowCount": 1,
  "executionTimeMs": 42
}
```

### Status Health Values

- `ok` - All systems operational
- `degraded` - Partial issues
- `down` - System unavailable

### Tail Event Color Coding (by type prefix)

| Prefix | Color | Category |
|--------|-------|----------|
| `collection.*` | green | CDC events |
| `rpc.*` | blue | RPC calls |
| `webhook.*` | magenta | Webhook events |
| `do.*` | yellow | Durable Object lifecycle |
| `ws.*` | cyan | WebSocket events |
| `tail.*` | gray | Tail events |

## Authentication

- Token: `EVENTS_API_KEY` environment variable
- Sent as: `Authorization: Bearer <token>` header
- No login/logout commands; all auth is via environment variable
- Commands work without auth for public endpoints, but most operations require it

## Environment Variables

| Variable | Purpose | Used By |
|----------|---------|---------|
| `EVENTS_API_KEY` | API key for events.do | `query`, `tail`, `status`, `replay` |
| `EVENTS_ENDPOINT` | API base URL (default: `https://events.do`) | `query`, `tail`, `status`, `replay` |

## CLI Options Quick Reference

### `init`
- `-n, --name <name>` - Project name
- `-t, --template <template>` - Template: `basic`, `cdc`, `analytics`
- `--no-typescript` - Use JavaScript
- `-p, --package-manager <pm>` - `npm`, `pnpm`, `yarn`
- `--skip-install` - Skip dependency installation

### `dev`
- `-p, --port <port>` - Port
- `-l, --local` - Local-only mode
- `--no-persist` - Disable persistent storage
- `-c, --config <file>` - Wrangler config path

### `deploy`
- `-e, --env <env>` - Target environment
- `--dry-run` - Preview only
- `-c, --config <file>` - Wrangler config path
- `--no-minify` - Disable minification

### `query`
- `-s, --sql <query>` - SQL query (REQUIRED)
- `-f, --format <format>` - Output: `json` or `table` (default: table)
- `-n, --namespace <ns>` - Namespace to query
- `-l, --limit <n>` - Max rows
- `-e, --endpoint <url>` - API endpoint

### `tail`
- `-f, --filter <pattern>` - Filter by event type (glob)
- `-n, --namespace <ns>` - Filter by namespace
- `-e, --endpoint <url>` - API endpoint

### `status`
- `-v, --verbose` - Detailed shard stats and config
- `-e, --endpoint <url>` - API endpoint

### `replay`
- `-d, --dead-letter <id>` - Single dead letter ID
- `-a, --all` - Replay all dead letters
- `-s, --subscription-id <id>` - Filter by subscription
- `--dry-run` - Preview only
- `-e, --endpoint <url>` - API endpoint

### `schema`
- `schema init [-d, --dir <dir>]` - Create example schemas
- `schema validate [-d, --dir <dir>]` - Validate schema files
- `schema generate [-d, --dir <dir>]` - Generate TypeScript types
