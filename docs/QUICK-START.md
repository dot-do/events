# Quick Start Guide

Get started with events.do in under 5 minutes.

## Prerequisites

- **Node.js** v18 or later - [nodejs.org](https://nodejs.org/)
- **Cloudflare account** - [dash.cloudflare.com/sign-up](https://dash.cloudflare.com/sign-up)

Verify your setup:

```bash
node --version  # Should be v18+
```

## 1. Install the CLI

```bash
npm install -g @dotdo/events-cli
```

## 2. Initialize Your Project

```bash
events init my-events
cd my-events
```

This creates a new project with:
- Pre-configured `wrangler.jsonc`
- Sample Durable Object with EventEmitter
- R2 bucket binding for lakehouse storage

## 3. Deploy to Cloudflare

```bash
events deploy
```

The CLI will prompt you to log in to Cloudflare if needed.

## 4. Send Your First Event

```bash
curl -X POST https://my-events.workers.dev/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "type": "custom.hello_world",
      "data": { "message": "Hello from events.do!" }
    }]
  }'
```

You should receive:

```json
{ "ok": true, "count": 1 }
```

## 5. View Events in Dashboard

Open your events dashboard:

```bash
events dashboard
```

Or visit [events.do/dashboard](https://events.do/dashboard) and connect your worker.

You will see your events streaming in real-time, with the ability to:
- Filter by event type, date range, and DO class
- Query historical events with DuckDB SQL
- View CDC (Change Data Capture) audit trails

## Next Steps

- **[Getting Started Guide](./GETTING_STARTED.md)** - Complete setup with EventEmitter and CDCCollection
- **[Lakehouse Analytics](./LAKEHOUSE.md)** - Query events with DuckDB
- **[Subscriptions](./SUBSCRIPTIONS.md)** - Event-driven workflows and webhooks
- **[Operations](./OPERATIONS.md)** - Monitoring, backups, and maintenance

## Quick Reference

| Command | Description |
|---------|-------------|
| `events init <name>` | Create new project |
| `events deploy` | Deploy to Cloudflare |
| `events dashboard` | Open events dashboard |
| `events logs` | Stream live event logs |
| `events query <sql>` | Run DuckDB query |

## Need Help?

- Documentation: [events.do/docs](https://events.do/docs)
- GitHub: [github.com/dotdo/events](https://github.com/dotdo/events)
