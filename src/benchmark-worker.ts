/**
 * Parquet Compression Benchmark Worker
 *
 * Tests real compression/decompression performance on Workers with:
 * - Realistic Wiktionary data
 * - Multiple batch sizes (1, 10, 100, 1K, 10K, 100K)
 * - Multiple codecs (UNCOMPRESSED, LZ4, ZSTD, SNAPPY)
 * - Both write (compress) and read (decompress) measurements
 * - CPU time and wall clock time
 */

import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'
import { parquetRead } from 'hyparquet'
import type { Compressors } from 'hyparquet'
import { compressors as hyparquetCompressors } from 'hyparquet-compressors'

import * as lz4Binding from 'lz4/lib/binding'

// Fixed LZ4 RAW decompression - hyparquet-compressors has a bug in match length extension
function lz4RawDecompress(input: Uint8Array, outputLength: number): Uint8Array {
  console.log(`[LZ4_DECOMPRESS] input=${input.length}B outputLength=${outputLength}`)
  const output = new Uint8Array(outputLength)
  let len = 0 // output position
  for (let i = 0; i < input.length;) {
    const token = input[i++]!

    let literals = token >> 4
    if (literals) {
      // literal length extension
      let byte = literals + 240
      while (byte === 255) literals += byte = input[i++]!
      // copy literals
      output.set(input.subarray(i, i + literals), len)
      len += literals
      i += literals
      if (i >= input.length) return output
    }

    const offset = (input[i++] ?? 0) | ((input[i++] ?? 0) << 8)
    if (!offset || offset > len) {
      throw new Error(`lz4 offset out of range ${offset}`)
    }

    // match length - FIX: use (token & 0xf), not matchLength which includes +4
    let matchLength = (token & 0xf) + 4 // minmatch 4
    let byte = (token & 0xf) + 240 // FIX: check nibble, not matchLength
    while (byte === 255) matchLength += byte = input[i++]!

    // copy match byte by byte (handles overlapping)
    let pos = len - offset
    const end = len + matchLength
    while (len < end) output[len++] = output[pos++]!
  }
  return output
}

// Compressors with fixed LZ4_RAW (and debug logging)
const compressors: Compressors = {
  ...hyparquetCompressors,
  LZ4_RAW: (input: Uint8Array, outputLength: number) => {
    console.log(`[COMPRESSORS.LZ4_RAW] called with input=${input.length}B outputLength=${outputLength}`)
    return lz4RawDecompress(input, outputLength)
  },
}

// LZ4 raw compression (Parquet-compatible)
// lz4/lib/binding returns 0 when no matches found - we need to encode as literals
function lz4Compress(input: Uint8Array): Uint8Array {
  const maxSize = lz4Binding.compressBound(input.length)
  const output = new Uint8Array(maxSize)
  const compressedSize = lz4Binding.compress(input, output)

  // If compression worked, return the compressed data
  if (compressedSize > 0 && compressedSize < input.length) {
    return output.slice(0, compressedSize)
  }

  // Compression failed or ineffective - encode as literal-only LZ4 block
  // LZ4 format: token (1 byte) + [extended literals length] + literals
  // Token high nibble = literals length (15 = extended), low nibble = match length (0)
  return encodeLz4Literals(input)
}

// Encode data as a literal-only LZ4 block (no matches)
function encodeLz4Literals(input: Uint8Array): Uint8Array {
  const literalsLen = input.length

  // Calculate output size: 1 (token) + extended length bytes + literals
  let extendedBytes = 0
  if (literalsLen >= 15) {
    let remaining = literalsLen - 15
    while (remaining >= 255) {
      extendedBytes++
      remaining -= 255
    }
    extendedBytes++ // Final byte < 255
  }

  const outputSize = 1 + extendedBytes + literalsLen
  const output = new Uint8Array(outputSize)
  let pos = 0

  // Token: high nibble = min(literalsLen, 15), low nibble = 0 (no match)
  const tokenLiterals = Math.min(literalsLen, 15)
  output[pos++] = tokenLiterals << 4

  // Extended literals length
  if (literalsLen >= 15) {
    let remaining = literalsLen - 15
    while (remaining >= 255) {
      output[pos++] = 255
      remaining -= 255
    }
    output[pos++] = remaining
  }

  // Copy literals
  output.set(input, pos)

  return output
}

import type { Env as FullEnv } from './env'

type Env = Pick<FullEnv, 'BENCHMARK_BUCKET' | 'PIPELINE_BUCKET'>

// Real Kaikki Wiktionary entry schema
interface KaikkiEntry {
  word: string
  pos: string
  lang: string
  lang_code: string
  senses: string // JSON stringified array
  etymology_texts: string | null // JSON stringified array
  sounds: string | null // JSON stringified array
  forms: string | null // JSON stringified array
  translations: string | null // JSON stringified array
}

// Benchmark result
interface BenchmarkResult {
  codec: string
  rowCount: number
  // Write metrics
  writeMs: number
  compressedBytes: number
  uncompressedBytes: number
  compressionRatio: number
  // Read metrics
  readMs: number
  readError: string | null
  // Derived
  writeRowsPerSec: number
  readRowsPerSec: number
  writeMBPerSec: number
  readMBPerSec: number
  _debug?: { writeStart: number; writeEnd: number }
}

const BATCH_SIZES = [1, 10, 100, 1000, 10000, 100000]
const CODECS = ['UNCOMPRESSED', 'LZ4_RAW', 'SNAPPY', 'ZSTD'] as const

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname === '/health') {
      return Response.json({ service: 'parquet-benchmark', status: 'ok' })
    }

    // Control timing test - use R2 I/O for real async
    if (url.pathname === '/timing-test') {
      // R2 head forces real I/O
      await env.BENCHMARK_BUCKET.head('__timing_test__')
      const t1 = Date.now()

      // Do some work - create and fill arrays
      const arr = new Uint8Array(10 * 1024 * 1024) // 10MB
      for (let i = 0; i < arr.length; i++) {
        arr[i] = i % 256
      }

      await env.BENCHMARK_BUCKET.head('__timing_test__')
      const t2 = Date.now()

      // Read it back
      let sum = 0
      for (let i = 0; i < arr.length; i++) {
        sum += arr[i]!
      }

      await env.BENCHMARK_BUCKET.head('__timing_test__')
      const t3 = Date.now()

      return Response.json({
        arraySize: arr.length,
        fillMs: t2 - t1,
        readMs: t3 - t2,
        totalMs: t3 - t1,
        sum,
        timestamps: { t1, t2, t3 },
      })
    }

    // Debug LZ4 compressor
    if (url.pathname === '/lz4-test') {
      const testData = new TextEncoder().encode('Hello World! '.repeat(1000))
      const maxSize = lz4Binding.compressBound(testData.length)
      const output = new Uint8Array(maxSize)
      const compressedSize = lz4Binding.compress(testData, output)
      const compressed = output.slice(0, compressedSize)

      // Try decompression with FIXED LZ4 decompressor
      let decompressResult: {
        success: boolean
        outputLength?: number
        error?: string
        firstDiff?: { index: number; expected: number; got: number }
        decompressedPreview?: number[]
      } = { success: false }
      try {
        // Use our fixed decompressor
        const decompressed = lz4RawDecompress(compressed, testData.length)

        // Compare byte by byte to find first difference
        let firstDiff: { index: number; expected: number; got: number } | undefined
        for (let i = 0; i < testData.length; i++) {
          if (testData[i] !== decompressed[i]) {
            firstDiff = { index: i, expected: testData[i]!, got: decompressed[i]! }
            break
          }
        }

        decompressResult = {
          success: firstDiff === undefined,
          outputLength: decompressed.length,
          error: firstDiff ? `Content mismatch at index ${firstDiff.index}` : undefined,
          firstDiff,
          decompressedPreview: Array.from(decompressed.slice(0, 30)),
        }
      } catch (e) {
        decompressResult = { success: false, error: String(e) }
      }

      return Response.json({
        inputSize: testData.length,
        maxSize,
        compressedSize,
        compressionRatio: testData.length / compressedSize,
        compressedPreview: Array.from(compressed.slice(0, 50)),
        decompressResult,
      })
    }

    // Run full benchmark suite
    if (url.pathname === '/run') {
      const maxRows = parseInt(url.searchParams.get('maxRows') ?? '10000')
      const codec = url.searchParams.get('codec') as typeof CODECS[number] | null

      // Load real Kaikki data
      const sampleData = await loadKaikkiData(env, maxRows)

      if (sampleData.length === 0) {
        return Response.json({ error: 'Failed to load Kaikki data' }, { status: 500 })
      }

      const results: BenchmarkResult[] = []
      const codecs = codec ? [codec] : CODECS
      const sizes = BATCH_SIZES.filter(s => s <= sampleData.length)

      for (const codecName of codecs) {
        for (const size of sizes) {
          const data = sampleData.slice(0, size)
          const result = await runBenchmark(data, codecName, env)
          results.push(result)

          // Log progress
          console.log(`[BENCH] ${codecName} @ ${size} rows: write=${result.writeMs.toFixed(1)}ms read=${result.readMs.toFixed(1)}ms ratio=${result.compressionRatio.toFixed(2)}x`)
        }
      }

      // Store results
      const resultKey = `benchmarks/${new Date().toISOString().replace(/[:.]/g, '-')}.json`
      await env.BENCHMARK_BUCKET.put(resultKey, JSON.stringify(results, null, 2))

      return Response.json({
        sampleDataRows: sampleData.length,
        benchmarkCount: results.length,
        resultKey,
        results,
      })
    }

    // Single benchmark test
    if (url.pathname === '/test') {
      const rows = parseInt(url.searchParams.get('rows') ?? '1000')
      const codec = (url.searchParams.get('codec') ?? 'UNCOMPRESSED') as typeof CODECS[number]
      const iterations = parseInt(url.searchParams.get('iterations') ?? '1')

      try {
        const sampleData = await loadKaikkiData(env, rows)
        if (sampleData.length === 0) {
          return Response.json({ error: 'Failed to load Kaikki data' }, { status: 500 })
        }

        // Run multiple iterations to get measurable time
        const results: BenchmarkResult[] = []
        const totalStart = Date.now()
        console.log(`[ITER] Start: ${totalStart}, iterations: ${iterations}`)

        for (let i = 0; i < iterations; i++) {
          const iterStart = Date.now()
          const result = await runBenchmark(sampleData.slice(0, rows), codec, env)
          results.push(result)
          console.log(`[ITER] ${i}: ${Date.now() - iterStart}ms`)
        }

        const totalEnd = Date.now()
        const totalMs = totalEnd - totalStart
        console.log(`[ITER] End: ${totalEnd}, total: ${totalMs}ms`)
        const avgWriteMs = totalMs / iterations
        const lastResult = results[results.length - 1]!

        return Response.json({
          ...lastResult,
          iterations,
          totalMs,
          avgWriteMs,
          effectiveRowsPerSec: avgWriteMs > 0 ? lastResult.rowCount / (avgWriteMs / 1000) : null,
          effectiveMBPerSec: avgWriteMs > 0 ? (lastResult.uncompressedBytes / 1024 / 1024) / (avgWriteMs / 1000) : null,
          _timing: { totalStart, totalEnd },
        })
      } catch (e) {
        return Response.json({ error: String(e), stack: e instanceof Error ? e.stack : undefined }, { status: 500 })
      }
    }

    // List previous benchmark results
    if (url.pathname === '/results') {
      const listed = await env.BENCHMARK_BUCKET.list({ prefix: 'benchmarks/', limit: 20 })
      const results = await Promise.all(
        listed.objects.map(async (obj) => {
          const data = await env.BENCHMARK_BUCKET.get(obj.key)
          return {
            key: obj.key,
            uploaded: obj.uploaded?.toISOString(),
            results: data ? await data.json() : null,
          }
        })
      )
      return Response.json({ benchmarks: results })
    }

    // Check available Wiktionary data
    if (url.pathname === '/data') {
      const listed = await env.PIPELINE_BUCKET.list({ prefix: 'test-wiktionary/', limit: 50 })
      const files = listed.objects.map(o => ({
        key: o.key,
        size: o.size,
        uploaded: o.uploaded?.toISOString(),
      }))
      return Response.json({
        fileCount: files.length,
        totalBytes: files.reduce((s, f) => s + f.size, 0),
        files
      })
    }

    // Debug: test Kaikki data fetch
    if (url.pathname === '/fetch-test') {
      try {
        const maxRows = parseInt(url.searchParams.get('rows') ?? '10')
        const data = await loadKaikkiData(env, maxRows)
        return Response.json({
          loaded: data.length,
          sample: data[0],
          totalBytes: JSON.stringify(data).length
        })
      } catch (e) {
        return Response.json({ error: String(e), stack: e instanceof Error ? e.stack : undefined }, { status: 500 })
      }
    }

    // Debug: try to read one parquet file
    if (url.pathname === '/debug-read') {
      const listed = await env.PIPELINE_BUCKET.list({ prefix: 'test-wiktionary/', limit: 5 })
      const results: { key: string; size: number; rows?: number; columns?: string[]; error?: string; sample?: unknown }[] = []

      for (const obj of listed.objects.slice(0, 3)) {
        if (!obj.key.endsWith('.parquet')) continue

        try {
          const data = await env.PIPELINE_BUCKET.get(obj.key)
          if (!data) {
            results.push({ key: obj.key, size: obj.size, error: 'No data' })
            continue
          }

          const buffer = await data.arrayBuffer()
          let rowCount = 0
          let columns: string[] = []
          let sample: unknown = null

          // hyparquet needs AsyncBuffer interface
          const arrayBuffer = buffer instanceof ArrayBuffer ? buffer : new Uint8Array(buffer).buffer
          await parquetRead({
            file: { byteLength: arrayBuffer.byteLength, slice: (start: number, end?: number) => arrayBuffer.slice(start, end) },
            compressors,
            rowFormat: 'object',
            onComplete: (rows: Record<string, unknown>[]) => {
              rowCount = rows.length
              if (rows[0]) {
                columns = Object.keys(rows[0])
                sample = rows[0]
              }
            },
          })

          results.push({ key: obj.key, size: obj.size, rows: rowCount, columns, sample })
        } catch (e) {
          results.push({ key: obj.key, size: obj.size, error: String(e) })
        }
      }

      return Response.json({ results })
    }

    // Debug Parquet file parsing
    if (url.pathname === '/debug-parquet') {
      const rows = parseInt(url.searchParams.get('rows') ?? '10')
      const codec = (url.searchParams.get('codec') ?? 'UNCOMPRESSED') as typeof CODECS[number]

      const sampleData = await loadKaikkiData(env, rows)
      const columnData = [
        { name: 'word', data: sampleData.map(d => d.word), type: 'STRING' as const },
        { name: 'pos', data: sampleData.map(d => d.pos), type: 'STRING' as const },
      ]

      let buffer: ArrayBuffer
      if (codec === 'LZ4_RAW') {
        buffer = parquetWriteBuffer({
          columnData,
          codec: 'LZ4_RAW',
          compressors: { LZ4_RAW: lz4Compress },
          statistics: true,
        })
      } else {
        buffer = parquetWriteBuffer({ columnData, statistics: true })
      }

      // Try to read back
      let readResult: { success: boolean; rows?: number; error?: string; errorStack?: string } = { success: false }
      try {
        let rowCount = 0
        await parquetRead({
          file: { byteLength: buffer.byteLength, slice: (start: number, end?: number) => buffer.slice(start, end) },
          compressors,
          onComplete: (r: unknown[]) => { rowCount = r.length },
        })
        readResult = { success: true, rows: rowCount }
      } catch (e) {
        readResult = {
          success: false,
          error: String(e),
          errorStack: e instanceof Error ? e.stack : undefined,
        }
      }

      return Response.json({
        codec,
        inputRows: rows,
        bufferSize: buffer.byteLength,
        // First few bytes (magic, etc)
        header: Array.from(new Uint8Array(buffer, 0, Math.min(20, buffer.byteLength))),
        // Last few bytes (footer)
        footer: Array.from(new Uint8Array(buffer, Math.max(0, buffer.byteLength - 20))),
        readResult,
      })
    }

    return Response.json({
      service: 'parquet-benchmark',
      endpoints: {
        '/run': 'Run full benchmark suite (maxRows, codec params)',
        '/test': 'Single benchmark (rows, codec params)',
        '/results': 'List previous benchmark results',
        '/data': 'Check available Wiktionary data',
        '/debug-parquet': 'Debug Parquet file structure',
      },
    })
  },
}

async function loadKaikkiData(env: Env, maxRows: number): Promise<KaikkiEntry[]> {
  // Check if we have cached chunks in R2
  const chunkKey = `kaikki-cache/cs-chunk-${maxRows}.json`

  // Try to load from R2 cache first
  const cached = await env.BENCHMARK_BUCKET.get(chunkKey)
  if (cached) {
    console.log(`[DATA] Loading from R2 cache: ${chunkKey}`)
    return await cached.json() as KaikkiEntry[]
  }

  // Stream from kaikki.org and only read what we need
  console.log(`[DATA] Streaming from kaikki.org (need ${maxRows} rows)...`)
  const response = await fetch('https://kaikki.org/dictionary/downloads/cs/cs-extract.jsonl')
  if (!response.ok || !response.body) {
    throw new Error(`Failed to fetch Kaikki data: ${response.status}`)
  }

  const entries: KaikkiEntry[] = []
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (entries.length < maxRows) {
    const { done, value } = await reader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })

    // Process complete lines
    const lines = buffer.split('\n')
    buffer = lines.pop() ?? '' // Keep incomplete line in buffer

    for (const line of lines) {
      if (entries.length >= maxRows) break
      if (!line.trim()) continue

      try {
        const raw = JSON.parse(line) as Record<string, unknown>
        entries.push({
          word: String(raw.word ?? ''),
          pos: String(raw.pos ?? ''),
          lang: String(raw.lang ?? ''),
          lang_code: String(raw.lang_code ?? ''),
          senses: JSON.stringify(raw.senses ?? []),
          etymology_texts: raw.etymology_texts ? JSON.stringify(raw.etymology_texts) : null,
          sounds: raw.sounds ? JSON.stringify(raw.sounds) : null,
          forms: raw.forms ? JSON.stringify(raw.forms) : null,
          translations: raw.translations ? JSON.stringify(raw.translations) : null,
        })
      } catch {
        // Skip malformed lines
      }
    }
  }

  // Cancel the stream early since we have enough data
  reader.cancel()

  console.log(`[DATA] Loaded ${entries.length} Kaikki entries`)

  // Cache this chunk for future runs
  if (entries.length > 0) {
    await env.BENCHMARK_BUCKET.put(chunkKey, JSON.stringify(entries), {
      customMetadata: {
        source: 'kaikki.org',
        rowCount: String(entries.length),
        fetchedAt: new Date().toISOString()
      }
    })
    console.log(`[DATA] Cached ${entries.length} entries to R2`)
  }

  return entries
}

async function runBenchmark(
  data: KaikkiEntry[],
  codec: typeof CODECS[number],
  env: Env
): Promise<BenchmarkResult> {
  const rowCount = data.length

  // Prepare column data matching real Kaikki schema
  const columnData = [
    { name: 'word', data: data.map(d => d.word), type: 'STRING' as const },
    { name: 'pos', data: data.map(d => d.pos), type: 'STRING' as const },
    { name: 'lang', data: data.map(d => d.lang), type: 'STRING' as const },
    { name: 'lang_code', data: data.map(d => d.lang_code), type: 'STRING' as const },
    { name: 'senses', data: data.map(d => d.senses), type: 'JSON' as const },
    { name: 'etymology_texts', data: data.map(d => d.etymology_texts), type: 'JSON' as const, nullable: true },
    { name: 'sounds', data: data.map(d => d.sounds), type: 'JSON' as const, nullable: true },
    { name: 'forms', data: data.map(d => d.forms), type: 'JSON' as const, nullable: true },
    { name: 'translations', data: data.map(d => d.translations), type: 'JSON' as const, nullable: true },
  ]

  // Calculate uncompressed size estimate
  const uncompressedBytes = data.reduce((sum, d) => {
    return sum +
      (d.word?.length ?? 0) +
      (d.pos?.length ?? 0) +
      (d.lang?.length ?? 0) +
      (d.lang_code?.length ?? 0) +
      (d.senses?.length ?? 0) +
      (d.etymology_texts?.length ?? 0) +
      (d.sounds?.length ?? 0)
  }, 0)

  // ========== WRITE BENCHMARK ==========
  // Workers freeze clock until real I/O - use R2 head to update clock
  await env.BENCHMARK_BUCKET.head('__clock_sync__')
  const writeStart = Date.now()

  let buffer: ArrayBuffer

  if (codec === 'UNCOMPRESSED') {
    buffer = parquetWriteBuffer({ columnData, statistics: true })
    console.log(`[BENCH] UNCOMPRESSED write done, buffer size: ${buffer.byteLength}`)
  } else if (codec === 'LZ4_RAW') {
    buffer = parquetWriteBuffer({
      columnData,
      codec: 'LZ4_RAW',
      compressors: { LZ4_RAW: lz4Compress },
      statistics: true,
    })
  } else if (codec === 'SNAPPY') {
    // SNAPPY requires hysnappy which uses WASM - may fail in some contexts
    try {
      const { snappyCompress } = await import('hysnappy')
      buffer = parquetWriteBuffer({
        columnData,
        codec: 'SNAPPY',
        compressors: { SNAPPY: snappyCompress },
        statistics: true,
      })
    } catch {
      // Fallback to uncompressed if SNAPPY not available
      buffer = parquetWriteBuffer({ columnData, statistics: true })
    }
  } else if (codec === 'ZSTD') {
    // ZSTD requires zstd-codec which uses WASM
    try {
      const { ZstdCodec } = await import('zstd-codec')
      const zstd = await new Promise<{ compress: (data: Uint8Array, level?: number) => Uint8Array }>((resolve) => {
        ZstdCodec.run((binding) => {
          resolve(new binding.Simple())
        })
      })
      buffer = parquetWriteBuffer({
        columnData,
        codec: 'ZSTD',
        compressors: { ZSTD: (data: Uint8Array) => zstd.compress(data, 3) },
        statistics: true,
      })
    } catch {
      // Fallback to uncompressed if ZSTD not available
      buffer = parquetWriteBuffer({ columnData, statistics: true })
    }
  } else {
    buffer = parquetWriteBuffer({ columnData, statistics: true })
  }

  // Force clock update after write
  await env.BENCHMARK_BUCKET.head('__clock_sync__')
  const writeEnd = Date.now()
  const writeMs = writeEnd - writeStart
  const compressedBytes = buffer.byteLength

  // ========== READ BENCHMARK ==========
  let readMs = 0
  let readError: string | null = null

  try {
    await env.BENCHMARK_BUCKET.head('__clock_sync__')
    const readStart = Date.now()

    let readRowCount = 0
    await parquetRead({
      file: { byteLength: buffer.byteLength, slice: (start: number, end?: number) => buffer.slice(start, end) },
      compressors,
      onComplete: (rows: unknown[]) => {
        readRowCount = rows.length
      },
    })

    await env.BENCHMARK_BUCKET.head('__clock_sync__')
    readMs = Date.now() - readStart

    if (readRowCount !== rowCount) {
      readError = `Row count mismatch: expected ${rowCount}, got ${readRowCount}`
    }
  } catch (e) {
    readError = String(e)
  }

  // Calculate derived metrics
  const compressionRatio = uncompressedBytes / compressedBytes
  const writeRowsPerSec = writeMs > 0 ? rowCount / (writeMs / 1000) : 0
  const readRowsPerSec = readMs > 0 ? rowCount / (readMs / 1000) : 0
  const writeMBPerSec = writeMs > 0 ? (uncompressedBytes / 1024 / 1024) / (writeMs / 1000) : 0
  const readMBPerSec = readMs > 0 ? (uncompressedBytes / 1024 / 1024) / (readMs / 1000) : 0

  return {
    codec,
    rowCount,
    writeMs,
    compressedBytes,
    uncompressedBytes,
    compressionRatio,
    readMs,
    readError,
    writeRowsPerSec,
    readRowsPerSec,
    writeMBPerSec,
    readMBPerSec,
    _debug: { writeStart, writeEnd },
  }
}
