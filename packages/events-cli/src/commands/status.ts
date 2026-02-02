import { Command } from 'commander'
import pc from 'picocolors'

export interface StatusOptions {
  endpoint?: string
  verbose?: boolean
}

export interface HealthResponse {
  status: 'ok' | 'degraded' | 'down'
  service: string
  ts: string
  env?: string
  cpuTimeMs?: number
  providers?: string[]
}

export interface ShardStats {
  activeShards: number[]
  totalShards: number
  eventsPerShard: Record<number, number>
  lastFlushTimes: Record<number, string>
  dynamicShardingEnabled: boolean
  config?: {
    minShards: number
    maxShards: number
    scaleUpThreshold: number
    scaleDownThreshold: number
    cooldownMs: number
    metricsWindowMs: number
    healthCheckIntervalMs: number
  }
  error?: string
  message?: string
  fallbackShards?: number[]
}

/**
 * Display system health and status
 */
export async function runStatus(options: StatusOptions = {}): Promise<void> {
  const endpoint = options.endpoint || process.env.EVENTS_ENDPOINT || 'https://events.do'
  const apiKey = process.env.EVENTS_API_KEY

  const headers: Record<string, string> = {}
  if (apiKey) {
    headers['Authorization'] = `Bearer ${apiKey}`
  }

  console.log(pc.cyan('\n  events.do System Status\n'))
  console.log(pc.dim(`  Endpoint: ${endpoint}\n`))

  // Fetch health and shard status in parallel
  const [healthResult, shardsResult] = await Promise.allSettled([
    fetchHealth(endpoint, headers),
    fetchShards(endpoint, headers),
  ])

  // Display health status
  console.log(pc.bold('  Health:'))
  if (healthResult.status === 'fulfilled') {
    const health = healthResult.value
    const statusColor = health.status === 'ok' ? pc.green : health.status === 'degraded' ? pc.yellow : pc.red
    console.log(`    Status:  ${statusColor(health.status.toUpperCase())}`)
    console.log(`    Service: ${pc.white(health.service)}`)
    if (health.env) {
      console.log(`    Env:     ${pc.dim(health.env)}`)
    }
    if (health.cpuTimeMs !== undefined) {
      console.log(`    CPU:     ${pc.dim(health.cpuTimeMs.toFixed(2) + 'ms')}`)
    }
    if (health.providers && health.providers.length > 0) {
      console.log(`    Providers: ${pc.dim(health.providers.join(', '))}`)
    }
  } else {
    console.log(`    ${pc.red('Error:')} ${healthResult.reason}`)
  }

  console.log()

  // Display shard status
  console.log(pc.bold('  Shards:'))
  if (shardsResult.status === 'fulfilled') {
    const shards = shardsResult.value

    if (shards.error) {
      console.log(`    ${pc.yellow('Warning:')} ${shards.message || shards.error}`)
      if (shards.fallbackShards) {
        console.log(`    Fallback Shards: ${pc.dim(shards.fallbackShards.join(', '))}`)
      }
    } else {
      const shardingStatus = shards.dynamicShardingEnabled ? pc.green('enabled') : pc.yellow('disabled')
      console.log(`    Dynamic Sharding: ${shardingStatus}`)
      console.log(`    Active Shards: ${pc.white(String(shards.activeShards?.length || 0))}`)
      console.log(`    Total Shards:  ${pc.white(String(shards.totalShards || 0))}`)

      if (shards.activeShards && shards.activeShards.length > 0) {
        console.log(`    Shard IDs: ${pc.dim(shards.activeShards.join(', '))}`)
      }

      // Show per-shard stats if verbose
      if (options.verbose && shards.eventsPerShard) {
        console.log()
        console.log(pc.bold('  Per-Shard Stats:'))
        for (const [shardId, eventCount] of Object.entries(shards.eventsPerShard)) {
          const lastFlush = shards.lastFlushTimes?.[Number(shardId)] || 'never'
          console.log(`    Shard ${shardId}: ${eventCount} events, last flush: ${pc.dim(lastFlush)}`)
        }
      }

      // Show config if verbose
      if (options.verbose && shards.config) {
        console.log()
        console.log(pc.bold('  Shard Config:'))
        console.log(`    Min Shards:       ${shards.config.minShards}`)
        console.log(`    Max Shards:       ${shards.config.maxShards}`)
        console.log(`    Scale Up:         ${shards.config.scaleUpThreshold} events/min`)
        console.log(`    Scale Down:       ${shards.config.scaleDownThreshold} events/min`)
        console.log(`    Cooldown:         ${shards.config.cooldownMs}ms`)
        console.log(`    Metrics Window:   ${shards.config.metricsWindowMs}ms`)
        console.log(`    Health Interval:  ${shards.config.healthCheckIntervalMs}ms`)
      }
    }
  } else {
    console.log(`    ${pc.red('Error:')} ${shardsResult.reason}`)
  }

  console.log()

  // Overall status summary
  const overallStatus = healthResult.status === 'fulfilled' &&
    healthResult.value.status === 'ok' &&
    shardsResult.status === 'fulfilled' &&
    !shardsResult.value.error

  if (overallStatus) {
    console.log(pc.green('  All systems operational\n'))
  } else {
    console.log(pc.yellow('  Some issues detected - see above for details\n'))
  }
}

/**
 * Fetch health endpoint
 */
async function fetchHealth(endpoint: string, headers: Record<string, string>): Promise<HealthResponse> {
  const response = await fetch(`${endpoint}/health`, { headers })
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`)
  }
  return response.json() as Promise<HealthResponse>
}

/**
 * Fetch shard status
 */
async function fetchShards(endpoint: string, headers: Record<string, string>): Promise<ShardStats> {
  const response = await fetch(`${endpoint}/shards`, { headers })
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`)
  }
  return response.json() as Promise<ShardStats>
}

export const statusCommand = new Command('status')
  .description('Show system health and shard status')
  .option('-v, --verbose', 'Show detailed shard statistics and configuration')
  .option('-e, --endpoint <url>', 'Events.do API endpoint (default: https://events.do)')
  .action(async (opts) => {
    await runStatus(opts)
  })
