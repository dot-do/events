import { Command } from 'commander'
import pc from 'picocolors'

export interface ReplayOptions {
  deadLetter?: string
  all?: boolean
  subscriptionId?: string
  endpoint?: string
  dryRun?: boolean
}

export interface DeadLetter {
  id: string
  deliveryId: string
  subscriptionId: string
  eventId: string
  eventPayload: string
  reason: string
  lastError?: string
  createdAt: number
}

export interface ReplayResult {
  ok: boolean
  deliveryId?: string
  error?: string
}

/**
 * Replay dead letter events
 */
export async function runReplay(options: ReplayOptions = {}): Promise<void> {
  const endpoint = options.endpoint || process.env.EVENTS_ENDPOINT || 'https://events.do'
  const apiKey = process.env.EVENTS_API_KEY

  if (!options.deadLetter && !options.all) {
    console.log(pc.red('\n  Error: Either --dead-letter <id> or --all is required\n'))
    console.log(pc.dim('  Examples:'))
    console.log(pc.dim('    events replay --dead-letter DL_01ABC123'))
    console.log(pc.dim('    events replay --all --subscription-id SUB_01XYZ789'))
    console.log(pc.dim('    events replay --all\n'))
    process.exit(1)
  }

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  }

  if (apiKey) {
    headers['Authorization'] = `Bearer ${apiKey}`
  }

  try {
    if (options.deadLetter) {
      // Replay a single dead letter
      await replaySingle(endpoint, headers, options.deadLetter, options.dryRun)
    } else if (options.all) {
      // Replay all dead letters (optionally filtered by subscription)
      await replayAll(endpoint, headers, options.subscriptionId, options.dryRun)
    }
  } catch (err) {
    console.error(pc.red(`\n  Error: ${(err as Error).message}\n`))
    process.exit(1)
  }
}

/**
 * Replay a single dead letter
 */
async function replaySingle(
  endpoint: string,
  headers: Record<string, string>,
  deadLetterId: string,
  dryRun?: boolean
): Promise<void> {
  console.log(pc.cyan(`\n  Replaying dead letter: ${deadLetterId}\n`))

  if (dryRun) {
    console.log(pc.yellow('  [DRY RUN] Would replay this dead letter\n'))

    // Fetch dead letter details
    const response = await fetch(`${endpoint}/subscriptions/dead-letters/${deadLetterId}`, {
      headers,
    })

    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: response.statusText }))
      throw new Error((error as { error?: string }).error || response.statusText)
    }

    const deadLetter = await response.json() as DeadLetter
    displayDeadLetter(deadLetter)
    return
  }

  const response = await fetch(`${endpoint}/subscriptions/dead-letters/${deadLetterId}/retry`, {
    method: 'POST',
    headers,
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }))
    throw new Error((error as { error?: string }).error || response.statusText)
  }

  const result = await response.json() as ReplayResult

  if (result.ok) {
    console.log(pc.green(`  Successfully queued for retry`))
    console.log(pc.dim(`  New delivery ID: ${result.deliveryId}\n`))
  } else {
    console.log(pc.red(`  Failed to replay: ${result.error}\n`))
  }
}

/**
 * Replay all dead letters
 */
async function replayAll(
  endpoint: string,
  headers: Record<string, string>,
  subscriptionId?: string,
  dryRun?: boolean
): Promise<void> {
  console.log(pc.cyan('\n  Fetching dead letters...\n'))

  // Build URL
  let url = `${endpoint}/subscriptions/dead-letters`
  if (subscriptionId) {
    url = `${endpoint}/subscriptions/${subscriptionId}/dead-letters`
  }

  const response = await fetch(url, { headers })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }))
    throw new Error((error as { error?: string }).error || response.statusText)
  }

  const deadLetters = await response.json() as DeadLetter[]

  if (!deadLetters || deadLetters.length === 0) {
    console.log(pc.green('  No dead letters found\n'))
    return
  }

  console.log(`  Found ${pc.bold(String(deadLetters.length))} dead letter(s)\n`)

  if (dryRun) {
    console.log(pc.yellow('  [DRY RUN] Would replay the following dead letters:\n'))
    for (const dl of deadLetters.slice(0, 10)) {
      displayDeadLetterSummary(dl)
    }
    if (deadLetters.length > 10) {
      console.log(pc.dim(`  ... and ${deadLetters.length - 10} more\n`))
    }
    return
  }

  // Confirm before replaying all
  if (!process.env.CI && deadLetters.length > 5) {
    console.log(pc.yellow(`  Warning: About to replay ${deadLetters.length} dead letters`))
    console.log(pc.dim('  Run with --dry-run first to preview\n'))

    // In a real CLI we'd prompt for confirmation here
    // For now, proceed but warn
  }

  console.log(pc.dim('  Replaying...\n'))

  let successCount = 0
  let failCount = 0

  for (const dl of deadLetters) {
    try {
      const retryResponse = await fetch(`${endpoint}/subscriptions/dead-letters/${dl.id}/retry`, {
        method: 'POST',
        headers,
      })

      if (retryResponse.ok) {
        const result = await retryResponse.json() as ReplayResult
        if (result.ok) {
          successCount++
          console.log(`  ${pc.green('OK')} ${dl.id} -> ${result.deliveryId}`)
        } else {
          failCount++
          console.log(`  ${pc.red('FAIL')} ${dl.id}: ${result.error}`)
        }
      } else {
        failCount++
        console.log(`  ${pc.red('FAIL')} ${dl.id}: HTTP ${retryResponse.status}`)
      }
    } catch (err) {
      failCount++
      console.log(`  ${pc.red('FAIL')} ${dl.id}: ${(err as Error).message}`)
    }
  }

  console.log()
  console.log(pc.bold('  Summary:'))
  console.log(`    Successful: ${pc.green(String(successCount))}`)
  console.log(`    Failed:     ${pc.red(String(failCount))}`)
  console.log()
}

/**
 * Display a dead letter with full details
 */
function displayDeadLetter(dl: DeadLetter): void {
  console.log(pc.bold('  Dead Letter Details:'))
  console.log(`    ID:           ${dl.id}`)
  console.log(`    Delivery ID:  ${dl.deliveryId}`)
  console.log(`    Subscription: ${dl.subscriptionId}`)
  console.log(`    Event ID:     ${dl.eventId}`)
  console.log(`    Reason:       ${dl.reason}`)
  if (dl.lastError) {
    console.log(`    Last Error:   ${pc.red(dl.lastError)}`)
  }
  console.log(`    Created:      ${new Date(dl.createdAt).toISOString()}`)

  if (dl.eventPayload) {
    try {
      const payload = JSON.parse(dl.eventPayload)
      console.log(`    Payload:`)
      console.log(pc.dim(`      ${JSON.stringify(payload, null, 2).split('\n').join('\n      ')}`))
    } catch {
      console.log(`    Payload: ${pc.dim(dl.eventPayload.slice(0, 100))}...`)
    }
  }
  console.log()
}

/**
 * Display a dead letter summary (one line)
 */
function displayDeadLetterSummary(dl: DeadLetter): void {
  const timestamp = new Date(dl.createdAt).toISOString()
  console.log(`  ${pc.dim(timestamp)} ${dl.id} ${pc.red(dl.reason)}`)
}

export const replayCommand = new Command('replay')
  .description('Replay dead letter events')
  .option('-d, --dead-letter <id>', 'Specific dead letter ID to replay')
  .option('-a, --all', 'Replay all dead letters')
  .option('-s, --subscription-id <id>', 'Filter by subscription ID (use with --all)')
  .option('--dry-run', 'Preview what would be replayed without actually replaying')
  .option('-e, --endpoint <url>', 'Events.do API endpoint (default: https://events.do)')
  .action(async (opts) => {
    await runReplay(opts)
  })
