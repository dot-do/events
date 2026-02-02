/**
 * Compatibility Reader for Legacy + DeltaLake
 *
 * Provides a unified query interface that reads from both legacy Parquet files
 * and DeltaTable during migration. This enables gradual rollout where:
 * - New writes go to DeltaTable
 * - Old data remains in legacy Parquet
 * - Queries transparently merge both sources
 *
 * After migration is complete and all data is in DeltaTable, this layer
 * can be removed in favor of direct DeltaTable queries.
 */

import { DeltaTable, type StorageBackend, type Filter } from '@dotdo/deltalake'
import { parquetRead } from 'hyparquet'
import { createEventsTable, type EventDeltaRecord } from './deltalake-factory'

// =============================================================================
// TYPES
// =============================================================================

/**
 * Query options for the compat reader.
 */
export interface CompatQueryOptions {
  /** Filter predicates (applied to both sources) */
  filter?: Filter
  /** Maximum number of results */
  limit?: number
  /** Skip legacy Parquet files (DeltaTable only) */
  skipLegacy?: boolean
  /** Skip DeltaTable (legacy only) */
  skipDelta?: boolean
  /** Time range start (ISO string) */
  startTime?: string
  /** Time range end (ISO string) */
  endTime?: string
}

/**
 * Query result with source information.
 */
export interface CompatQueryResult {
  /** Query results */
  records: EventDeltaRecord[]
  /** Number of records from legacy Parquet */
  legacyCount: number
  /** Number of records from DeltaTable */
  deltaCount: number
  /** Query execution time in ms */
  durationMs: number
}

/**
 * Reader configuration.
 */
export interface CompatReaderConfig {
  /** R2 bucket for storage */
  bucket: R2Bucket
  /** Storage backend for DeltaTable */
  storage: StorageBackend
  /** Shard ID for events table */
  shardId?: number
  /** Base path for legacy files */
  legacyPrefix?: string
}

// =============================================================================
// COMPAT READER
// =============================================================================

/**
 * Compatibility reader for querying both legacy and DeltaTable sources.
 */
export class CompatReader {
  private readonly bucket: R2Bucket
  private readonly storage: StorageBackend
  private readonly shardId: number
  private readonly legacyPrefix: string
  private deltaTable: DeltaTable<EventDeltaRecord> | null = null

  constructor(config: CompatReaderConfig) {
    this.bucket = config.bucket
    this.storage = config.storage
    this.shardId = config.shardId ?? 0
    this.legacyPrefix = config.legacyPrefix ?? 'events/'
  }

  /**
   * Get or create DeltaTable instance.
   */
  private getDeltaTable(): DeltaTable<EventDeltaRecord> {
    if (!this.deltaTable) {
      this.deltaTable = createEventsTable(this.storage, { shardId: this.shardId })
    }
    return this.deltaTable
  }

  /**
   * Query events from both legacy and DeltaTable sources.
   */
  async query(options: CompatQueryOptions = {}): Promise<CompatQueryResult> {
    const startTime = performance.now()
    const results: EventDeltaRecord[] = []
    let legacyCount = 0
    let deltaCount = 0

    // Query DeltaTable (newer data)
    if (!options.skipDelta) {
      try {
        const deltaRecords = await this.queryDeltaTable(options)
        results.push(...deltaRecords)
        deltaCount = deltaRecords.length
      } catch (error) {
        // DeltaTable might not exist yet during early migration
        console.warn('DeltaTable query failed (may not exist yet):', error)
      }
    }

    // Query legacy Parquet (older data)
    if (!options.skipLegacy) {
      try {
        const legacyRecords = await this.queryLegacyParquet(options)
        results.push(...legacyRecords)
        legacyCount = legacyRecords.length
      } catch (error) {
        console.warn('Legacy Parquet query failed:', error)
      }
    }

    // Sort by timestamp (newest first)
    results.sort((a, b) => {
      const tsA = new Date(a.ts).getTime()
      const tsB = new Date(b.ts).getTime()
      return tsB - tsA
    })

    // Apply limit after merge
    const limited = options.limit ? results.slice(0, options.limit) : results

    const durationMs = performance.now() - startTime

    return {
      records: limited,
      legacyCount,
      deltaCount,
      durationMs,
    }
  }

  /**
   * Query DeltaTable for events.
   */
  private async queryDeltaTable(options: CompatQueryOptions): Promise<EventDeltaRecord[]> {
    const table = this.getDeltaTable()

    // Build filter from options
    const filter: Filter = options.filter ?? {}

    // Add time range filter if specified
    if (options.startTime || options.endTime) {
      const tsFilter: Record<string, unknown> = {}
      if (options.startTime) {
        tsFilter.$gte = options.startTime
      }
      if (options.endTime) {
        tsFilter.$lte = options.endTime
      }
      filter.ts = tsFilter
    }

    // Query with filter
    const results = await table.query(filter)

    // Apply limit at query level if possible
    if (options.limit && results.length > options.limit) {
      return results.slice(0, options.limit)
    }

    return results
  }

  /**
   * Query legacy Parquet files for events.
   */
  private async queryLegacyParquet(options: CompatQueryOptions): Promise<EventDeltaRecord[]> {
    // Determine which files to scan based on time range
    const files = await this.listLegacyFiles(options.startTime, options.endTime)

    if (files.length === 0) {
      return []
    }

    const results: EventDeltaRecord[] = []
    const limit = options.limit ?? Infinity

    // Read files until we have enough results
    for (const file of files) {
      if (results.length >= limit) {
        break
      }

      try {
        const records = await this.readLegacyFile(file)

        // Apply filter
        const filtered = this.filterRecords(records, options)
        results.push(...filtered)
      } catch (error) {
        console.warn(`Failed to read legacy file ${file}:`, error)
      }
    }

    return results
  }

  /**
   * List legacy Parquet files, optionally filtered by time range.
   */
  private async listLegacyFiles(startTime?: string, endTime?: string): Promise<string[]> {
    const files: string[] = []
    let cursor: string | undefined
    let truncated = true

    // Parse time range to determine prefix filters
    const startDate = startTime ? new Date(startTime) : null
    const endDate = endTime ? new Date(endTime) : null

    while (truncated) {
      const response = await this.bucket.list({
        prefix: this.legacyPrefix,
        cursor,
      })

      for (const obj of response.objects) {
        if (!obj.key.endsWith('.parquet')) {
          continue
        }

        // Skip DeltaTable files (they have _delta_log in path)
        if (obj.key.includes('_delta_log') || obj.key.includes('shard=')) {
          continue
        }

        // Filter by time if specified (based on path structure: events/YYYY/MM/DD/HH/)
        if (startDate || endDate) {
          const pathDate = this.parseDateFromPath(obj.key)
          if (pathDate) {
            if (startDate && pathDate < startDate) continue
            if (endDate && pathDate > endDate) continue
          }
        }

        files.push(obj.key)
      }

      truncated = response.truncated
      cursor = response.cursor
    }

    // Sort by path (which is chronological due to date structure)
    return files.sort().reverse() // Newest first
  }

  /**
   * Parse date from legacy file path.
   * Expected format: events/{source}/YYYY/MM/DD/HH/{id}.parquet
   */
  private parseDateFromPath(path: string): Date | null {
    // Match patterns like /2024/01/15/12/
    const match = path.match(/\/(\d{4})\/(\d{2})\/(\d{2})\/(\d{2})\//)
    if (!match) {
      return null
    }

    const [, year, month, day, hour] = match
    return new Date(`${year}-${month}-${day}T${hour}:00:00.000Z`)
  }

  /**
   * Read events from a legacy Parquet file.
   */
  private async readLegacyFile(key: string): Promise<EventDeltaRecord[]> {
    const object = await this.bucket.get(key)
    if (!object) {
      throw new Error(`File not found: ${key}`)
    }

    const arrayBuffer = await object.arrayBuffer()
    const records: EventDeltaRecord[] = []

    await parquetRead({
      file: arrayBuffer,
      onComplete: (rows: Record<string, unknown>[]) => {
        for (const row of rows) {
          records.push({
            ts: String(row.ts ?? ''),
            type: String(row.type ?? ''),
            source: row.source != null ? String(row.source) : null,
            provider: row.provider != null ? String(row.provider) : null,
            eventType: row.eventType != null ? String(row.eventType) : null,
            verified: row.verified != null ? Boolean(row.verified) : null,
            scriptName: row.scriptName != null ? String(row.scriptName) : null,
            outcome: row.outcome != null ? String(row.outcome) : null,
            method: row.method != null ? String(row.method) : null,
            url: row.url != null ? String(row.url) : null,
            statusCode: row.statusCode != null ? Number(row.statusCode) : null,
            durationMs: row.durationMs != null ? Number(row.durationMs) : null,
            payload: row.payload != null ? String(row.payload) : null,
          })
        }
      },
    })

    return records
  }

  /**
   * Filter records based on query options.
   */
  private filterRecords(
    records: EventDeltaRecord[],
    options: CompatQueryOptions
  ): EventDeltaRecord[] {
    let filtered = records

    // Time range filter
    if (options.startTime) {
      const start = new Date(options.startTime).getTime()
      filtered = filtered.filter(r => new Date(r.ts).getTime() >= start)
    }
    if (options.endTime) {
      const end = new Date(options.endTime).getTime()
      filtered = filtered.filter(r => new Date(r.ts).getTime() <= end)
    }

    // Apply custom filter (simple equality matching)
    if (options.filter) {
      filtered = filtered.filter(r => this.matchesFilter(r, options.filter!))
    }

    return filtered
  }

  /**
   * Check if a record matches a filter (simple equality).
   */
  private matchesFilter(record: EventDeltaRecord, filter: Filter): boolean {
    for (const [key, value] of Object.entries(filter)) {
      const recordValue = (record as Record<string, unknown>)[key]

      if (typeof value === 'object' && value !== null) {
        // Handle comparison operators
        const ops = value as Record<string, unknown>
        if ('$eq' in ops && recordValue !== ops.$eq) return false
        if ('$ne' in ops && recordValue === ops.$ne) return false
        if ('$gt' in ops && !(recordValue > (ops.$gt as number))) return false
        if ('$gte' in ops && !(recordValue >= (ops.$gte as number))) return false
        if ('$lt' in ops && !(recordValue < (ops.$lt as number))) return false
        if ('$lte' in ops && !(recordValue <= (ops.$lte as number))) return false
        if ('$in' in ops && !Array.isArray(ops.$in)) return false
        if ('$in' in ops && !(ops.$in as unknown[]).includes(recordValue)) return false
      } else {
        // Simple equality
        if (recordValue !== value) return false
      }
    }
    return true
  }

  /**
   * Count events matching a filter.
   */
  async count(options: CompatQueryOptions = {}): Promise<{ total: number; legacy: number; delta: number }> {
    const result = await this.query({ ...options, limit: undefined })
    return {
      total: result.records.length,
      legacy: result.legacyCount,
      delta: result.deltaCount,
    }
  }

  /**
   * Get the latest events.
   */
  async latest(limit: number = 100): Promise<EventDeltaRecord[]> {
    const result = await this.query({ limit })
    return result.records
  }

  /**
   * Query events by type.
   */
  async byType(type: string, options: Omit<CompatQueryOptions, 'filter'> = {}): Promise<EventDeltaRecord[]> {
    const result = await this.query({
      ...options,
      filter: { type },
    })
    return result.records
  }

  /**
   * Query events by source.
   */
  async bySource(source: string, options: Omit<CompatQueryOptions, 'filter'> = {}): Promise<EventDeltaRecord[]> {
    const result = await this.query({
      ...options,
      filter: { source },
    })
    return result.records
  }

  /**
   * Query events in a time range.
   */
  async inTimeRange(
    startTime: string,
    endTime: string,
    options: Omit<CompatQueryOptions, 'startTime' | 'endTime'> = {}
  ): Promise<EventDeltaRecord[]> {
    const result = await this.query({
      ...options,
      startTime,
      endTime,
    })
    return result.records
  }
}

/**
 * Create a compat reader from R2 bucket and storage backend.
 */
export function createCompatReader(
  bucket: R2Bucket,
  storage: StorageBackend,
  options: Partial<CompatReaderConfig> = {}
): CompatReader {
  return new CompatReader({
    bucket,
    storage,
    ...options,
  })
}
