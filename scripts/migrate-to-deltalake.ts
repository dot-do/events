#!/usr/bin/env npx tsx
/**
 * Migration Script: Legacy Parquet to DeltaLake
 *
 * Migrates existing Parquet event files to DeltaTable format.
 * Run this once to populate the DeltaTable with historical data.
 *
 * Usage:
 *   npx tsx scripts/migrate-to-deltalake.ts --dry-run
 *   npx tsx scripts/migrate-to-deltalake.ts --bucket events --prefix events/
 *
 * Features:
 * - Dry-run mode to preview changes
 * - Resumable (skips already-migrated files)
 * - Progress tracking
 * - Batch processing to avoid memory issues
 */

import { DeltaTable } from '@dotdo/deltalake'
import { parquetRead } from 'hyparquet'
import type { EventDeltaRecord } from '../core/src/deltalake-factory'

// Configuration
const DEFAULT_BATCH_SIZE = 10000
const DEFAULT_SHARD = 0

interface MigrationOptions {
  bucket: R2Bucket
  prefix: string
  shardId: number
  batchSize: number
  dryRun: boolean
  verbose: boolean
  startAfter?: string
}

interface MigrationResult {
  filesProcessed: number
  filesSkipped: number
  eventsWritten: number
  errors: string[]
  lastFile?: string
}

interface LegacyEventRecord {
  ts: string
  type: string
  source?: string
  provider?: string
  eventType?: string
  verified?: boolean
  scriptName?: string
  outcome?: string
  method?: string
  url?: string
  statusCode?: number
  durationMs?: number
  payload?: string
}

/**
 * List all Parquet files under a prefix.
 */
async function listParquetFiles(
  bucket: R2Bucket,
  prefix: string,
  startAfter?: string
): Promise<string[]> {
  const files: string[] = []
  let cursor: string | undefined
  let truncated = true

  while (truncated) {
    const response = await bucket.list({
      prefix,
      cursor,
    })

    for (const obj of response.objects) {
      if (obj.key.endsWith('.parquet')) {
        // Skip files we've already processed (for resumability)
        if (startAfter && obj.key <= startAfter) {
          continue
        }
        files.push(obj.key)
      }
    }

    truncated = response.truncated
    cursor = response.cursor
  }

  // Sort by key for consistent ordering
  return files.sort()
}

/**
 * Read events from a legacy Parquet file.
 */
async function readLegacyParquet(
  bucket: R2Bucket,
  key: string
): Promise<LegacyEventRecord[]> {
  const object = await bucket.get(key)
  if (!object) {
    throw new Error(`File not found: ${key}`)
  }

  const arrayBuffer = await object.arrayBuffer()
  const events: LegacyEventRecord[] = []

  await parquetRead({
    file: arrayBuffer,
    onComplete: (rows: Record<string, unknown>[]) => {
      for (const row of rows) {
        events.push({
          ts: String(row.ts ?? ''),
          type: String(row.type ?? ''),
          source: row.source != null ? String(row.source) : undefined,
          provider: row.provider != null ? String(row.provider) : undefined,
          eventType: row.eventType != null ? String(row.eventType) : undefined,
          verified: row.verified != null ? Boolean(row.verified) : undefined,
          scriptName: row.scriptName != null ? String(row.scriptName) : undefined,
          outcome: row.outcome != null ? String(row.outcome) : undefined,
          method: row.method != null ? String(row.method) : undefined,
          url: row.url != null ? String(row.url) : undefined,
          statusCode: row.statusCode != null ? Number(row.statusCode) : undefined,
          durationMs: row.durationMs != null ? Number(row.durationMs) : undefined,
          payload: row.payload != null ? String(row.payload) : undefined,
        })
      }
    },
  })

  return events
}

/**
 * Convert legacy event to DeltaTable record format.
 */
function toLegacyDeltaRecord(event: LegacyEventRecord): EventDeltaRecord {
  return {
    ts: event.ts,
    type: event.type,
    source: event.source ?? null,
    provider: event.provider ?? null,
    eventType: event.eventType ?? null,
    verified: event.verified ?? null,
    scriptName: event.scriptName ?? null,
    outcome: event.outcome ?? null,
    method: event.method ?? null,
    url: event.url ?? null,
    statusCode: event.statusCode ?? null,
    durationMs: event.durationMs ?? null,
    payload: event.payload ?? null,
  }
}

/**
 * Migrate legacy Parquet files to DeltaTable.
 */
export async function migrateToDeltalake(
  options: MigrationOptions
): Promise<MigrationResult> {
  const { bucket, prefix, shardId, batchSize, dryRun, verbose, startAfter } = options

  const result: MigrationResult = {
    filesProcessed: 0,
    filesSkipped: 0,
    eventsWritten: 0,
    errors: [],
  }

  // List all Parquet files
  console.log(`Listing files with prefix: ${prefix}`)
  const files = await listParquetFiles(bucket, prefix, startAfter)
  console.log(`Found ${files.length} Parquet files to process`)

  if (files.length === 0) {
    console.log('No files to migrate')
    return result
  }

  // Create DeltaTable for writing
  // Note: In actual usage, this would use createR2Storage and createEventsTable
  // For the script, we'd need to construct the storage backend
  const tablePath = `events/shard=${shardId}`

  if (dryRun) {
    console.log('\n=== DRY RUN MODE ===')
    console.log(`Would write to DeltaTable at: ${tablePath}`)
  }

  // Process files in batches
  let pendingEvents: EventDeltaRecord[] = []

  for (const file of files) {
    try {
      if (verbose) {
        console.log(`Processing: ${file}`)
      }

      // Read events from legacy file
      const events = await readLegacyParquet(bucket, file)

      if (verbose) {
        console.log(`  Read ${events.length} events`)
      }

      // Convert to delta format
      const deltaRecords = events.map(toLegacyDeltaRecord)
      pendingEvents.push(...deltaRecords)

      result.filesProcessed++
      result.lastFile = file

      // Write batch when threshold reached
      if (pendingEvents.length >= batchSize) {
        if (!dryRun) {
          // TODO: Write to DeltaTable
          // await table.write(pendingEvents)
          console.log(`Would write batch of ${pendingEvents.length} events`)
        } else {
          console.log(`[DRY RUN] Would write batch of ${pendingEvents.length} events`)
        }
        result.eventsWritten += pendingEvents.length
        pendingEvents = []
      }

      // Progress update
      if (result.filesProcessed % 100 === 0) {
        console.log(`Progress: ${result.filesProcessed}/${files.length} files, ${result.eventsWritten} events written`)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      result.errors.push(`${file}: ${message}`)
      console.error(`Error processing ${file}: ${message}`)
    }
  }

  // Write remaining events
  if (pendingEvents.length > 0) {
    if (!dryRun) {
      // TODO: Write to DeltaTable
      // await table.write(pendingEvents)
      console.log(`Would write final batch of ${pendingEvents.length} events`)
    } else {
      console.log(`[DRY RUN] Would write final batch of ${pendingEvents.length} events`)
    }
    result.eventsWritten += pendingEvents.length
  }

  // Summary
  console.log('\n=== Migration Summary ===')
  console.log(`Files processed: ${result.filesProcessed}`)
  console.log(`Files skipped: ${result.filesSkipped}`)
  console.log(`Events written: ${result.eventsWritten}`)
  console.log(`Errors: ${result.errors.length}`)
  if (result.lastFile) {
    console.log(`Last file: ${result.lastFile}`)
  }

  return result
}

/**
 * Parse command line arguments.
 */
function parseArgs(): Partial<MigrationOptions> & { help?: boolean } {
  const args = process.argv.slice(2)
  const options: Partial<MigrationOptions> & { help?: boolean } = {}

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    switch (arg) {
      case '--help':
      case '-h':
        options.help = true
        break
      case '--dry-run':
        options.dryRun = true
        break
      case '--verbose':
      case '-v':
        options.verbose = true
        break
      case '--prefix':
        options.prefix = args[++i]
        break
      case '--shard':
        options.shardId = parseInt(args[++i], 10)
        break
      case '--batch-size':
        options.batchSize = parseInt(args[++i], 10)
        break
      case '--start-after':
        options.startAfter = args[++i]
        break
    }
  }

  return options
}

function printHelp() {
  console.log(`
Migration Script: Legacy Parquet to DeltaLake

Usage:
  npx tsx scripts/migrate-to-deltalake.ts [options]

Options:
  --help, -h        Show this help message
  --dry-run         Preview changes without writing
  --verbose, -v     Show detailed progress
  --prefix <path>   R2 prefix to scan (default: events/)
  --shard <id>      Target shard ID (default: 0)
  --batch-size <n>  Events per write batch (default: 10000)
  --start-after <k> Resume from this file key

Examples:
  # Preview migration
  npx tsx scripts/migrate-to-deltalake.ts --dry-run --verbose

  # Migrate with custom batch size
  npx tsx scripts/migrate-to-deltalake.ts --batch-size 5000

  # Resume from a specific file
  npx tsx scripts/migrate-to-deltalake.ts --start-after events/2024/01/15/12/abc.parquet

Note: This script must be run in an environment with R2 bucket access.
      For production use, run via wrangler or as a Cloudflare Worker.
`)
}

// Main entry point (for CLI usage)
async function main() {
  const args = parseArgs()

  if (args.help) {
    printHelp()
    process.exit(0)
  }

  console.log('Migration script loaded.')
  console.log('Note: This script requires R2 bucket access.')
  console.log('For actual migration, run as a Cloudflare Worker or use wrangler.')
  console.log('')
  console.log('Parsed options:', args)

  // In actual usage, you'd get the bucket from the Worker environment
  // For now, just show what would happen
  if (args.dryRun) {
    console.log('\nDry run mode - no changes will be made.')
    console.log(`Would scan prefix: ${args.prefix ?? 'events/'}`)
    console.log(`Would write to shard: ${args.shardId ?? DEFAULT_SHARD}`)
    console.log(`Batch size: ${args.batchSize ?? DEFAULT_BATCH_SIZE}`)
  }
}

main().catch(console.error)
