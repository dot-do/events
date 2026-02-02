import { Command } from 'commander'
import pc from 'picocolors'

export interface TailOptions {
  filter?: string
  namespace?: string
  endpoint?: string
}

/**
 * Tail events in real-time using Server-Sent Events (SSE)
 */
export async function runTail(options: TailOptions = {}): Promise<void> {
  const endpoint = options.endpoint || process.env.EVENTS_ENDPOINT || 'https://events.do'
  const apiKey = process.env.EVENTS_API_KEY

  console.log(pc.cyan('\n  Tailing events in real-time...\n'))

  if (options.filter) {
    console.log(pc.dim(`  Filter: ${options.filter}`))
  }
  if (options.namespace) {
    console.log(pc.dim(`  Namespace: ${options.namespace}`))
  }
  console.log(pc.dim(`  Endpoint: ${endpoint}`))
  console.log()

  // Build URL with query params
  const url = new URL('/events/stream', endpoint)
  if (options.filter) {
    url.searchParams.set('type', options.filter)
  }
  if (options.namespace) {
    url.searchParams.set('namespace', options.namespace)
  }

  const headers: Record<string, string> = {
    'Accept': 'text/event-stream',
    'Cache-Control': 'no-cache',
  }

  if (apiKey) {
    headers['Authorization'] = `Bearer ${apiKey}`
  }

  try {
    const response = await fetch(url.toString(), {
      headers,
    })

    if (!response.ok) {
      const text = await response.text()
      console.error(pc.red(`\n  Error: ${response.status} ${response.statusText}`))
      console.error(pc.dim(`  ${text}\n`))
      process.exit(1)
    }

    if (!response.body) {
      console.error(pc.red('\n  Error: No response body\n'))
      process.exit(1)
    }

    console.log(pc.green('  Connected! Waiting for events...\n'))
    console.log(pc.dim('  Press Ctrl+C to stop\n'))

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    // Handle graceful shutdown
    const cleanup = () => {
      console.log(pc.dim('\n\n  Disconnecting...\n'))
      reader.cancel()
      process.exit(0)
    }

    process.on('SIGINT', cleanup)
    process.on('SIGTERM', cleanup)

    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })

      // Process complete SSE messages
      const lines = buffer.split('\n')
      buffer = lines.pop() || ''

      let eventData = ''
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          eventData += line.slice(6)
        } else if (line === '' && eventData) {
          // End of event, parse and display
          try {
            const event = JSON.parse(eventData)
            displayEvent(event)
          } catch {
            // Skip malformed events
          }
          eventData = ''
        }
      }
    }
  } catch (err) {
    if ((err as Error).name === 'AbortError') {
      // Normal shutdown
      return
    }
    console.error(pc.red(`\n  Connection error: ${(err as Error).message}\n`))
    process.exit(1)
  }
}

/**
 * Display a single event with colored output
 */
function displayEvent(event: Record<string, unknown>): void {
  const ts = event.ts as string || new Date().toISOString()
  const type = event.type as string || 'unknown'
  const timestamp = new Date(ts).toLocaleTimeString()

  // Color code by event type prefix
  let typeColor = pc.white
  if (type.startsWith('collection.')) {
    typeColor = pc.green
  } else if (type.startsWith('rpc.')) {
    typeColor = pc.blue
  } else if (type.startsWith('webhook.')) {
    typeColor = pc.magenta
  } else if (type.startsWith('do.')) {
    typeColor = pc.yellow
  } else if (type.startsWith('ws.')) {
    typeColor = pc.cyan
  } else if (type.startsWith('tail.')) {
    typeColor = pc.gray
  }

  console.log(`${pc.dim(timestamp)} ${typeColor(type)}`)

  // Show DO info if present
  const doInfo = event.do as Record<string, unknown> | undefined
  if (doInfo) {
    const parts = []
    if (doInfo.class) parts.push(`class=${doInfo.class}`)
    if (doInfo.id) parts.push(`id=${String(doInfo.id).slice(0, 8)}...`)
    if (doInfo.colo) parts.push(`colo=${doInfo.colo}`)
    if (parts.length > 0) {
      console.log(pc.dim(`  ${parts.join(' ')}`))
    }
  }

  // Show collection info for CDC events
  if (type.startsWith('collection.')) {
    const collection = event.collection as string | undefined
    const docId = event.docId as string | undefined
    if (collection || docId) {
      console.log(pc.dim(`  collection=${collection || '?'} doc=${docId || '?'}`))
    }
  }

  // Show latency for RPC calls
  if (type.startsWith('rpc.') && event.latencyMs) {
    console.log(pc.dim(`  latency=${event.latencyMs}ms`))
  }

  console.log()
}

export const tailCommand = new Command('tail')
  .description('Tail events in real-time')
  .option('-f, --filter <pattern>', 'Filter by event type (glob pattern, e.g., "collection.*")')
  .option('-n, --namespace <namespace>', 'Filter by namespace')
  .option('-e, --endpoint <url>', 'Events.do API endpoint (default: https://events.do)')
  .action(async (opts) => {
    await runTail(opts)
  })
