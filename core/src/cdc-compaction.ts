/**
 * CDC (Change Data Capture) Compaction Module
 *
 * Provides functions for compacting CDC delta files into a single data.parquet file.
 *
 * Compaction process:
 * 1. Read current data.parquet (if exists)
 * 2. Read all pending delta files in sequence order
 * 3. Apply deltas to current state (insert/update/delete)
 * 4. Write new data.parquet sorted by primary key
 * 5. Optionally archive or delete processed deltas
 * 6. Update collection manifest with stats
 */

import { readParquetRecords, writeCompactedParquet } from './compaction.js'
import { readDeltaFile } from './cdc-delta.js'
import type { DeltaRecord } from './cdc-delta.js'
import { validateNamespaceOrCollection, buildSafeR2Path } from './r2-path.js'
import {
  DEFAULT_COMPACTION_PARALLELISM,
  DEFAULT_COMPACTION_CHUNK_SIZE,
  MIN_DELTAS_FOR_PARALLEL,
  MAX_COMPACTION_PARALLELISM,
} from './config.js'

// ============================================================================
// Types
// ============================================================================

/**
 * A single CDC delta record representing a change operation (compaction-specific)
 */
export interface CompactionDeltaRecord {
  /** Operation type: insert, update, or delete */
  op: 'insert' | 'update' | 'delete'
  /** Document ID (primary key) */
  id: string
  /** Document data (required for insert/update, undefined for delete) */
  doc?: Record<string, unknown> | undefined
  /** Timestamp of the change */
  ts: number
  /** Sequence number for ordering */
  seq: number
}

/**
 * Options for the compaction operation
 */
export interface CompactionOptions {
  /** Archive delta files to processed/ folder after compaction */
  archiveDeltas?: boolean | undefined
  /** Delete delta files after compaction */
  deleteDeltas?: boolean | undefined
}

/**
 * Options for parallel compaction operation
 */
export interface ParallelCompactionOptions extends CompactionOptions {
  /**
   * Number of parallel workers to use for processing delta files.
   * Default: 4, Max: 16
   */
  parallelism?: number | undefined
  /**
   * Number of delta files to process per parallel chunk.
   * Default: 10
   */
  chunkSize?: number | undefined
  /**
   * Callback for progress updates during parallel processing.
   * Called after each chunk completes.
   */
  onProgress?: ((progress: ParallelCompactionProgress) => void) | undefined
}

/**
 * Progress information for parallel compaction
 */
export interface ParallelCompactionProgress {
  /** Total number of chunks being processed */
  totalChunks: number
  /** Number of chunks completed */
  completedChunks: number
  /** Total delta files to process */
  totalDeltas: number
  /** Number of delta files processed */
  processedDeltas: number
  /** Percentage complete (0-100) */
  percentComplete: number
  /** Any errors encountered so far */
  errors: string[]
}

/**
 * Result of a chunk processing operation (internal)
 */
interface ChunkResult {
  /** State map for this chunk */
  state: Map<string, Record<string, unknown>>
  /** Files processed in this chunk */
  processedFiles: string[]
  /** Highest delta sequence in this chunk */
  lastSequence: number
  /** Any errors encountered */
  errors: string[]
}

/**
 * Result of a compaction operation
 */
export interface CompactionResult {
  /** Whether compaction succeeded */
  success: boolean
  /** Path to the output data.parquet file */
  dataFile?: string | undefined
  /** Number of records in the compacted file */
  recordCount: number
  /** Size of the compacted file in bytes */
  fileSizeBytes?: number | undefined
  /** Number of delta files processed */
  deltasProcessed: number
  /** Paths of processed delta files */
  processedFiles: string[]
  /** Any errors encountered (non-fatal) */
  errors?: string[] | undefined
}

/**
 * Manifest tracking collection state
 */
export interface CollectionManifest {
  /** Path to the current data.parquet file */
  dataFile: string
  /** Number of records in the data file */
  recordCount: number
  /** Size of the data file in bytes */
  fileSizeBytes: number
  /** ISO timestamp of last compaction */
  lastCompactionAt?: string
  /** Number of compactions performed */
  compactionCount: number
  /** Last delta sequence number processed */
  lastDeltaSequence?: number
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Extracts sequence number from delta file path
 * Supports both formats:
 * - "ns/users/deltas/001_2024-01-15T12-30.parquet" -> 1
 * - "ns/users/deltas/000001.parquet" -> 1
 */
function extractSequenceNumber(path: string): number {
  const filename = path.split('/').pop() || ''
  // Match pattern: {seq}_{timestamp}.parquet (e.g., "001_2024-01-15T12-30.parquet")
  // or simpler: {seq}.parquet (e.g., "000001.parquet")
  const match = filename.match(/^(\d+)(?:_[^.]+)?\.parquet$/)
  return match?.[1] ? parseInt(match[1], 10) : 0
}

/**
 * Converts DeltaRecord to CompactionDeltaRecord format
 */
function deltaRecordToCompactionRecord(delta: DeltaRecord, seq: number): CompactionDeltaRecord {
  return {
    op: delta.op,
    id: delta.pk,
    doc: delta.data ?? undefined,
    ts: new Date(delta.ts).getTime(),
    seq,
  }
}

// ============================================================================
// Main Compaction Function
// ============================================================================

/**
 * Compacts CDC delta files into a single data.parquet file
 *
 * @param bucket - R2 bucket containing CDC data
 * @param ns - Namespace (e.g., DO class name or worker name)
 * @param collection - Collection name
 * @param options - Compaction options
 * @returns Compaction result with stats
 *
 * @example
 * ```typescript
 * const result = await compactCollection(bucket, 'myworker', 'users', {
 *   deleteDeltas: true
 * })
 * console.log(`Compacted ${result.recordCount} records`)
 * ```
 */
export async function compactCollection(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  options: CompactionOptions = {}
): Promise<CompactionResult> {
  // Validate namespace and collection to prevent path traversal
  const safeNs = validateNamespaceOrCollection(ns, 'namespace')
  const safeCollection = validateNamespaceOrCollection(collection, 'collection')

  // Path matches cdc-processor.ts: {ns}/{collection}/deltas/{seq}_{timestamp}.parquet
  const basePath = buildSafeR2Path(safeNs, safeCollection)
  const deltasPath = buildSafeR2Path(safeNs, safeCollection, 'deltas') + '/'
  const dataPath = buildSafeR2Path(safeNs, safeCollection, 'data.parquet')
  const manifestPath = buildSafeR2Path(safeNs, safeCollection, 'manifest.json')
  const processedPath = buildSafeR2Path(safeNs, safeCollection, 'processed') + '/'

  const errors: string[] = []
  const processedFiles: string[] = []

  // 1. List all delta files (Parquet format)
  const deltasList = await bucket.list({ prefix: deltasPath })
  const deltaFiles = deltasList.objects
    .filter((obj) => obj.key.endsWith('.parquet'))
    .sort((a, b) => a.key.localeCompare(b.key))

  // 2. Load existing data.parquet if it exists
  let currentState = new Map<string, Record<string, unknown>>()
  const existingData = await bucket.get(dataPath)
  if (existingData) {
    const buffer = await existingData.arrayBuffer()
    const records = await readParquetRecords(buffer)
    for (const record of records) {
      const id = record.id as string
      const doc = { ...record }
      delete doc.id
      currentState.set(id, doc)
    }
  }

  // 3. Read and apply all delta files in order (Parquet format)
  let lastDeltaSequence = 0
  for (const deltaFile of deltaFiles) {
    const obj = await bucket.get(deltaFile.key)
    if (!obj) continue

    try {
      const buffer = await obj.arrayBuffer()
      const deltaRecords = await readDeltaFile(buffer)
      const seq = extractSequenceNumber(deltaFile.key)

      // Convert DeltaRecords to CompactionDeltaRecords
      const deltas: CompactionDeltaRecord[] = deltaRecords.map((dr, i) =>
        deltaRecordToCompactionRecord(dr, seq * 1000 + i)
      )

      currentState = applyDeltasToState(currentState, deltas)
      processedFiles.push(deltaFile.key)

      if (seq > lastDeltaSequence) {
        lastDeltaSequence = seq
      }
    } catch (e) {
      errors.push(`Failed to read delta file ${deltaFile.key}: ${e instanceof Error ? e.message : String(e)}`)
    }
  }

  // 4. Write the compacted data.parquet
  const parquetBuffer = writeDataParquet(currentState)
  await bucket.put(dataPath, parquetBuffer)

  // 5. Handle delta cleanup based on options
  if (options.archiveDeltas) {
    for (const filePath of processedFiles) {
      const obj = await bucket.get(filePath)
      if (obj) {
        const content = await obj.arrayBuffer()
        const filename = filePath.split('/').pop()
        await bucket.put(`${processedPath}${filename}`, content)
        await bucket.delete(filePath)
      }
    }
  } else if (options.deleteDeltas) {
    if (processedFiles.length > 0) {
      await bucket.delete(processedFiles)
    }
  }

  // 6. Load existing manifest or create new one
  let manifest: CollectionManifest
  const existingManifest = await bucket.get(manifestPath)
  if (existingManifest) {
    manifest = (await existingManifest.json()) as CollectionManifest
    manifest.compactionCount += 1
  } else {
    manifest = {
      dataFile: dataPath,
      recordCount: 0,
      fileSizeBytes: 0,
      compactionCount: 1,
    }
  }

  // 7. Update manifest with new stats
  manifest.dataFile = dataPath
  manifest.recordCount = currentState.size
  manifest.fileSizeBytes = parquetBuffer.byteLength
  manifest.lastCompactionAt = new Date().toISOString()
  if (lastDeltaSequence > 0) {
    manifest.lastDeltaSequence = lastDeltaSequence
  }

  await bucket.put(manifestPath, JSON.stringify(manifest))

  return {
    success: true,
    dataFile: dataPath,
    recordCount: currentState.size,
    fileSizeBytes: parquetBuffer.byteLength,
    deltasProcessed: processedFiles.length,
    processedFiles,
    errors: errors.length > 0 ? errors : undefined,
  }
}

// ============================================================================
// Delta Processing
// ============================================================================

/**
 * Applies delta records to current state, returning new state
 *
 * Operations are applied in order:
 * - insert: Adds new record to state
 * - update: Replaces existing record (or upserts if not exists)
 * - delete: Removes record from state
 *
 * @param currentState - Current state as Map of id -> document
 * @param deltas - Array of compaction delta records to apply
 * @returns New state after applying all deltas
 *
 * @example
 * ```typescript
 * const state = new Map([['user-1', { name: 'Alice' }]])
 * const deltas: CompactionDeltaRecord[] = [
 *   { op: 'update', id: 'user-1', doc: { name: 'Alice Updated' }, ts: 123, seq: 1 },
 *   { op: 'insert', id: 'user-2', doc: { name: 'Bob' }, ts: 124, seq: 2 }
 * ]
 * const newState = applyDeltasToState(state, deltas)
 * // newState has user-1 updated and user-2 added
 * ```
 */
export function applyDeltasToState(
  currentState: Map<string, Record<string, unknown>>,
  deltas: CompactionDeltaRecord[]
): Map<string, Record<string, unknown>> {
  // Create a copy of the current state to avoid mutation
  const newState = new Map(currentState)

  for (const delta of deltas) {
    switch (delta.op) {
      case 'insert':
        // Insert requires a doc; skip if missing
        if (delta.doc) {
          newState.set(delta.id, delta.doc)
        }
        break

      case 'update':
        // Update acts as upsert - insert if not exists
        if (delta.doc) {
          newState.set(delta.id, delta.doc)
        }
        break

      case 'delete':
        // Delete removes the record (no-op if not exists)
        newState.delete(delta.id)
        break
    }
  }

  return newState
}

// ============================================================================
// Parquet Writing
// ============================================================================

/**
 * Writes state map to a Parquet buffer
 *
 * Records are sorted by id (primary key) in the output.
 * The id column is added from the map keys.
 *
 * @param state - State map of id -> document
 * @returns ArrayBuffer containing Parquet data
 *
 * @example
 * ```typescript
 * const state = new Map([
 *   ['user-1', { name: 'Alice', age: 30 }],
 *   ['user-2', { name: 'Bob', age: 25 }]
 * ])
 * const buffer = writeDataParquet(state)
 * // buffer contains sorted parquet data with id, name, age columns
 * ```
 */
export function writeDataParquet(state: Map<string, Record<string, unknown>>): ArrayBuffer {
  // Convert state map to array of records with id included
  const records: Record<string, unknown>[] = []

  for (const [id, doc] of state) {
    records.push({ id, ...doc })
  }

  // Sort by id (primary key)
  records.sort((a, b) => {
    const idA = String(a.id)
    const idB = String(b.id)
    return idA.localeCompare(idB)
  })

  // Use writeCompactedParquet from compaction.js
  return writeCompactedParquet(records)
}

// ============================================================================
// Parallel Compaction
// ============================================================================

/**
 * Splits an array into chunks of specified size
 *
 * @param array - Array to split
 * @param chunkSize - Maximum size of each chunk
 * @returns Array of chunks
 */
function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

/**
 * Processes a single chunk of delta files and returns the resulting state
 *
 * @param bucket - R2 bucket containing CDC data
 * @param deltaFiles - Delta files in this chunk
 * @returns ChunkResult with state and metadata
 */
async function processChunk(
  bucket: R2Bucket,
  deltaFiles: R2Object[]
): Promise<ChunkResult> {
  const state = new Map<string, Record<string, unknown>>()
  const processedFiles: string[] = []
  const errors: string[] = []
  let lastSequence = 0

  for (const deltaFile of deltaFiles) {
    const obj = await bucket.get(deltaFile.key)
    if (!obj) continue

    try {
      const buffer = await obj.arrayBuffer()
      const deltaRecords = await readDeltaFile(buffer)
      const seq = extractSequenceNumber(deltaFile.key)

      // Convert DeltaRecords to CompactionDeltaRecords
      const deltas: CompactionDeltaRecord[] = deltaRecords.map((dr, i) =>
        deltaRecordToCompactionRecord(dr, seq * 1000 + i)
      )

      // Apply deltas to this chunk's state
      for (const delta of deltas) {
        switch (delta.op) {
          case 'insert':
          case 'update':
            if (delta.doc) {
              state.set(delta.id, delta.doc)
            }
            break
          case 'delete':
            state.set(delta.id, { __deleted__: true })
            break
        }
      }

      processedFiles.push(deltaFile.key)

      if (seq > lastSequence) {
        lastSequence = seq
      }
    } catch (e) {
      errors.push(`Failed to read delta file ${deltaFile.key}: ${e instanceof Error ? e.message : String(e)}`)
    }
  }

  return {
    state,
    processedFiles,
    lastSequence,
    errors,
  }
}

/**
 * Merges multiple chunk states into a single state, respecting delta ordering
 *
 * States are merged in order (first chunk to last), so later chunks
 * will overwrite earlier chunks for the same keys. This maintains
 * the correct last-write-wins semantics.
 *
 * @param baseState - Initial state (from existing data.parquet)
 * @param chunkResults - Array of chunk results in sequence order
 * @returns Merged state map
 */
export function mergeChunkStates(
  baseState: Map<string, Record<string, unknown>>,
  chunkResults: ChunkResult[]
): Map<string, Record<string, unknown>> {
  // Start with a copy of the base state
  const mergedState = new Map(baseState)

  // Apply each chunk's state in order
  for (const chunk of chunkResults) {
    for (const [id, doc] of chunk.state) {
      if ('__deleted__' in doc && doc.__deleted__ === true) {
        // Handle deletion
        mergedState.delete(id)
      } else {
        mergedState.set(id, doc)
      }
    }
  }

  return mergedState
}

/**
 * Compacts CDC delta files into a single data.parquet file using parallel processing
 *
 * This function processes delta files in parallel chunks for better performance
 * with large datasets. Files are grouped into chunks that are processed concurrently,
 * then the results are merged in sequence order to maintain correctness.
 *
 * When to use parallel compaction:
 * - Large number of delta files (>100)
 * - Delta files contain many records
 * - Need to minimize compaction time
 *
 * @param bucket - R2 bucket containing CDC data
 * @param ns - Namespace (e.g., DO class name or worker name)
 * @param collection - Collection name
 * @param options - Parallel compaction options
 * @returns Compaction result with stats
 *
 * @example
 * ```typescript
 * const result = await compactCollectionParallel(bucket, 'myworker', 'users', {
 *   parallelism: 8,
 *   chunkSize: 20,
 *   deleteDeltas: true,
 *   onProgress: (p) => console.log(`${p.percentComplete}% complete`)
 * })
 * console.log(`Compacted ${result.recordCount} records from ${result.deltasProcessed} deltas`)
 * ```
 */
export async function compactCollectionParallel(
  bucket: R2Bucket,
  ns: string,
  collection: string,
  options: ParallelCompactionOptions = {}
): Promise<CompactionResult> {
  // Validate namespace and collection to prevent path traversal
  const safeNs = validateNamespaceOrCollection(ns, 'namespace')
  const safeCollection = validateNamespaceOrCollection(collection, 'collection')

  // Normalize parallelism options
  const parallelism = Math.min(
    Math.max(1, options.parallelism ?? DEFAULT_COMPACTION_PARALLELISM),
    MAX_COMPACTION_PARALLELISM
  )
  const chunkSize = Math.max(1, options.chunkSize ?? DEFAULT_COMPACTION_CHUNK_SIZE)

  // Build paths
  const deltasPath = buildSafeR2Path(safeNs, safeCollection, 'deltas') + '/'
  const dataPath = buildSafeR2Path(safeNs, safeCollection, 'data.parquet')
  const manifestPath = buildSafeR2Path(safeNs, safeCollection, 'manifest.json')
  const processedPath = buildSafeR2Path(safeNs, safeCollection, 'processed') + '/'

  const allErrors: string[] = []
  const allProcessedFiles: string[] = []

  // 1. List all delta files (Parquet format)
  const deltasList = await bucket.list({ prefix: deltasPath })
  const deltaFiles = deltasList.objects
    .filter((obj) => obj.key.endsWith('.parquet'))
    .sort((a, b) => a.key.localeCompare(b.key))

  // If there are few deltas, fall back to sequential processing
  if (deltaFiles.length < MIN_DELTAS_FOR_PARALLEL) {
    return compactCollection(bucket, ns, collection, options)
  }

  // 2. Load existing data.parquet if it exists
  let baseState = new Map<string, Record<string, unknown>>()
  const existingData = await bucket.get(dataPath)
  if (existingData) {
    const buffer = await existingData.arrayBuffer()
    const records = await readParquetRecords(buffer)
    for (const record of records) {
      const id = record.id as string
      const doc = { ...record }
      delete doc.id
      baseState.set(id, doc)
    }
  }

  // 3. Split delta files into chunks
  const chunks = chunkArray(deltaFiles, chunkSize)
  const totalChunks = chunks.length

  // Progress tracking
  let completedChunks = 0
  let processedDeltas = 0

  const reportProgress = () => {
    if (options.onProgress) {
      options.onProgress({
        totalChunks,
        completedChunks,
        totalDeltas: deltaFiles.length,
        processedDeltas,
        percentComplete: Math.round((processedDeltas / deltaFiles.length) * 100),
        errors: [...allErrors],
      })
    }
  }

  // 4. Process chunks in parallel with controlled concurrency
  const chunkResults: ChunkResult[] = new Array(chunks.length)

  // Process in batches of `parallelism` chunks at a time
  for (let batchStart = 0; batchStart < chunks.length; batchStart += parallelism) {
    const batchEnd = Math.min(batchStart + parallelism, chunks.length)
    const batchChunks = chunks.slice(batchStart, batchEnd)
    const batchIndices = Array.from({ length: batchEnd - batchStart }, (_, i) => batchStart + i)

    // Process this batch of chunks in parallel
    const batchPromises = batchChunks.map(async (chunk, i) => {
      const result = await processChunk(bucket, chunk)
      return { index: batchIndices[i], result }
    })

    const batchResults = await Promise.all(batchPromises)

    // Store results in order and update progress
    for (const { index, result } of batchResults) {
      chunkResults[index] = result
      completedChunks++
      processedDeltas += result.processedFiles.length
      allErrors.push(...result.errors)
      allProcessedFiles.push(...result.processedFiles)
      reportProgress()
    }
  }

  // 5. Merge all chunk states in order
  const finalState = mergeChunkStates(baseState, chunkResults)

  // Get the highest sequence number
  const lastDeltaSequence = Math.max(0, ...chunkResults.map((r) => r.lastSequence))

  // 6. Write the compacted data.parquet
  const parquetBuffer = writeDataParquet(finalState)
  await bucket.put(dataPath, parquetBuffer)

  // 7. Handle delta cleanup based on options
  if (options.archiveDeltas) {
    // Archive in parallel batches for efficiency
    const archiveChunks = chunkArray(allProcessedFiles, parallelism * 2)
    for (const archiveChunk of archiveChunks) {
      await Promise.all(
        archiveChunk.map(async (filePath) => {
          const obj = await bucket.get(filePath)
          if (obj) {
            const content = await obj.arrayBuffer()
            const filename = filePath.split('/').pop()
            await bucket.put(`${processedPath}${filename}`, content)
            await bucket.delete(filePath)
          }
        })
      )
    }
  } else if (options.deleteDeltas) {
    if (allProcessedFiles.length > 0) {
      await bucket.delete(allProcessedFiles)
    }
  }

  // 8. Load existing manifest or create new one
  let manifest: CollectionManifest
  const existingManifest = await bucket.get(manifestPath)
  if (existingManifest) {
    manifest = (await existingManifest.json()) as CollectionManifest
    manifest.compactionCount += 1
  } else {
    manifest = {
      dataFile: dataPath,
      recordCount: 0,
      fileSizeBytes: 0,
      compactionCount: 1,
    }
  }

  // 9. Update manifest with new stats
  manifest.dataFile = dataPath
  manifest.recordCount = finalState.size
  manifest.fileSizeBytes = parquetBuffer.byteLength
  manifest.lastCompactionAt = new Date().toISOString()
  if (lastDeltaSequence > 0) {
    manifest.lastDeltaSequence = lastDeltaSequence
  }

  await bucket.put(manifestPath, JSON.stringify(manifest))

  return {
    success: true,
    dataFile: dataPath,
    recordCount: finalState.size,
    fileSizeBytes: parquetBuffer.byteLength,
    deltasProcessed: allProcessedFiles.length,
    processedFiles: allProcessedFiles,
    errors: allErrors.length > 0 ? allErrors : undefined,
  }
}
