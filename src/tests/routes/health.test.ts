/**
 * Health Route Handler Tests
 *
 * Unit tests for src/routes/health.ts
 * Tests health check, pipeline check, and benchmark endpoints.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleHealth, handlePipelineCheck, handleBenchmark } from '../../routes/health'

// ============================================================================
// Mock Types
// ============================================================================

interface MockR2Object {
  key: string
  size: number
  uploaded?: Date
  etag?: string
  customMetadata?: Record<string, string>
}

interface MockR2Bucket {
  list: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  head: ReturnType<typeof vi.fn>
}

interface MockEnv {
  ENVIRONMENT: string
  PIPELINE_BUCKET: MockR2Bucket
  BENCHMARK_BUCKET: MockR2Bucket
  ALLOWED_ORIGINS?: string
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockR2Bucket(objects: MockR2Object[] = []): MockR2Bucket {
  return {
    list: vi.fn().mockResolvedValue({ objects, truncated: false }),
    get: vi.fn(),
    head: vi.fn().mockImplementation((key: string) => {
      const obj = objects.find(o => o.key === key)
      return Promise.resolve(obj ? { customMetadata: obj.customMetadata } : null)
    }),
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    ENVIRONMENT: 'test',
    PIPELINE_BUCKET: createMockR2Bucket(),
    BENCHMARK_BUCKET: createMockR2Bucket(),
    ...overrides,
  }
}

function createRequest(url: string, options: RequestInit = {}): Request {
  return new Request(url, { method: 'GET', ...options })
}

// ============================================================================
// handleHealth Tests
// ============================================================================

describe('handleHealth', () => {
  describe('route matching', () => {
    it('handles /health endpoint', () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)

      expect(result).not.toBeNull()
      expect(result?.status).toBe(200)
    })

    it('handles / endpoint', () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)

      expect(result).not.toBeNull()
      expect(result?.status).toBe(200)
    })

    it('returns null for non-health endpoints', () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/ingest')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)

      expect(result).toBeNull()
    })

    it('returns null for /events endpoint', () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/events')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)

      expect(result).toBeNull()
    })
  })

  describe('response content', () => {
    it('returns correct service name for events.do domain', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)
      const json = await result?.json()

      expect(json.status).toBe('ok')
      expect(json.service).toBe('events.do')
    })

    it('returns correct service name for webhooks.do domain', async () => {
      const env = createMockEnv()
      const request = createRequest('https://webhooks.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, true, 0)
      const json = await result?.json()

      expect(json.status).toBe('ok')
      expect(json.service).toBe('webhooks.do')
    })

    it('includes environment in response', async () => {
      const env = createMockEnv({ ENVIRONMENT: 'production' })
      const request = createRequest('https://events.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)
      const json = await result?.json()

      expect(json.env).toBe('production')
    })

    it('includes timestamp in response', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)
      const json = await result?.json()

      expect(json.ts).toBeDefined()
      expect(new Date(json.ts).toISOString()).toBe(json.ts)
    })

    it('includes CPU time in response', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, performance.now() - 5)
      const json = await result?.json()

      expect(json.cpuTimeMs).toBeDefined()
      expect(typeof json.cpuTimeMs).toBe('number')
      expect(json.cpuTimeMs).toBeGreaterThanOrEqual(0)
    })

    it('includes providers for webhooks.do domain', async () => {
      const env = createMockEnv()
      const request = createRequest('https://webhooks.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, true, 0)
      const json = await result?.json()

      expect(json.providers).toBeDefined()
      expect(json.providers).toContain('github')
      expect(json.providers).toContain('stripe')
      expect(json.providers).toContain('workos')
      expect(json.providers).toContain('slack')
      expect(json.providers).toContain('linear')
      expect(json.providers).toContain('svix')
    })

    it('does not include providers for events.do domain', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/health')
      const url = new URL(request.url)

      const result = handleHealth(request, env as any, url, false, 0)
      const json = await result?.json()

      expect(json.providers).toBeUndefined()
    })
  })
})

// ============================================================================
// handlePipelineCheck Tests
// ============================================================================

describe('handlePipelineCheck', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('query parameters', () => {
    it('uses default prefix when not provided', async () => {
      const bucket = createMockR2Bucket()
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline')

      await handlePipelineCheck(request, env as any)

      expect(bucket.list).toHaveBeenCalledWith({
        prefix: 'test-wiktionary/',
        limit: 20,
      })
    })

    it('uses custom prefix from query param', async () => {
      const bucket = createMockR2Bucket()
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline?prefix=custom/')

      await handlePipelineCheck(request, env as any)

      expect(bucket.list).toHaveBeenCalledWith({
        prefix: 'custom/',
        limit: 20,
      })
    })

    it('uses default limit when not provided', async () => {
      const bucket = createMockR2Bucket()
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline')

      await handlePipelineCheck(request, env as any)

      expect(bucket.list).toHaveBeenCalledWith({
        prefix: 'test-wiktionary/',
        limit: 20,
      })
    })

    it('uses custom limit from query param', async () => {
      const bucket = createMockR2Bucket()
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline?limit=50')

      await handlePipelineCheck(request, env as any)

      expect(bucket.list).toHaveBeenCalledWith({
        prefix: 'test-wiktionary/',
        limit: 50,
      })
    })
  })

  describe('response formatting', () => {
    it('returns file information correctly', async () => {
      const objects: MockR2Object[] = [
        { key: 'test-wiktionary/file1.parquet', size: 1024, uploaded: new Date('2024-01-01'), etag: 'abc123' },
        { key: 'test-wiktionary/file2.parquet', size: 2048, uploaded: new Date('2024-01-02'), etag: 'def456' },
      ]
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline')

      const response = await handlePipelineCheck(request, env as any)
      const json = await response.json()

      expect(json.bucket).toBe('platform-events')
      expect(json.prefix).toBe('test-wiktionary/')
      expect(json.fileCount).toBe(2)
      expect(json.totalSize).toBe(3072)
      expect(json.files).toHaveLength(2)
      expect(json.files[0].key).toBe('test-wiktionary/file1.parquet')
      expect(json.files[0].size).toBe(1024)
    })

    it('includes root prefixes in response', async () => {
      const objects: MockR2Object[] = [
        { key: 'prefix1/file.parquet', size: 100 },
        { key: 'prefix2/file.parquet', size: 100 },
      ]
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline')

      const response = await handlePipelineCheck(request, env as any)
      const json = await response.json()

      expect(json.rootPrefixes).toBeDefined()
      expect(Array.isArray(json.rootPrefixes)).toBe(true)
    })

    it('includes truncated flag in response', async () => {
      const bucket = createMockR2Bucket()
      bucket.list.mockResolvedValue({ objects: [], truncated: true })
      const env = createMockEnv({ PIPELINE_BUCKET: bucket })
      const request = createRequest('https://events.do/pipeline')

      const response = await handlePipelineCheck(request, env as any)
      const json = await response.json()

      expect(json.truncated).toBe(true)
    })
  })

  describe('CORS headers', () => {
    it('includes CORS headers in response', async () => {
      const env = createMockEnv()
      const request = createRequest('https://events.do/pipeline', {
        headers: { 'Origin': 'https://dashboard.do' },
      })

      const response = await handlePipelineCheck(request, env as any)

      expect(response.headers.get('Access-Control-Allow-Origin')).toBeDefined()
    })
  })
})

// ============================================================================
// handleBenchmark Tests
// ============================================================================

describe('handleBenchmark', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('query parameters', () => {
    it('uses default prefix when not provided', async () => {
      const bucket = createMockR2Bucket()
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark')

      await handleBenchmark(request, env as any)

      expect(bucket.list).toHaveBeenCalledWith({
        prefix: 'benchmark/',
        limit: 100,
      })
    })

    it('uses custom prefix from query param', async () => {
      const bucket = createMockR2Bucket()
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark?prefix=custom/')

      await handleBenchmark(request, env as any)

      expect(bucket.list).toHaveBeenCalledWith({
        prefix: 'custom/',
        limit: 100,
      })
    })
  })

  describe('codec statistics', () => {
    it('groups files by codec', async () => {
      const objects: MockR2Object[] = [
        { key: 'benchmark/file1.parquet', size: 1000, customMetadata: { codec: 'snappy', cpuMs: '10', events: '100' } },
        { key: 'benchmark/file2.parquet', size: 2000, customMetadata: { codec: 'snappy', cpuMs: '20', events: '200' } },
        { key: 'benchmark/file3.parquet', size: 500, customMetadata: { codec: 'zstd', cpuMs: '5', events: '50' } },
      ]
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark')

      const response = await handleBenchmark(request, env as any)
      const json = await response.json()

      expect(json.summary).toBeDefined()
      expect(Array.isArray(json.summary)).toBe(true)

      const snappySummary = json.summary.find((s: any) => s.codec === 'snappy')
      expect(snappySummary).toBeDefined()
      expect(snappySummary.files).toBe(2)
      expect(snappySummary.totalEvents).toBe(300)
      expect(snappySummary.totalBytes).toBe(3000)

      const zstdSummary = json.summary.find((s: any) => s.codec === 'zstd')
      expect(zstdSummary).toBeDefined()
      expect(zstdSummary.files).toBe(1)
    })

    it('handles missing codec metadata', async () => {
      const objects: MockR2Object[] = [
        { key: 'benchmark/file1.parquet', size: 1000, customMetadata: {} },
      ]
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark')

      const response = await handleBenchmark(request, env as any)
      const json = await response.json()

      const unknownSummary = json.summary.find((s: any) => s.codec === 'unknown')
      expect(unknownSummary).toBeDefined()
    })

    it('calculates average CPU time per file', async () => {
      const objects: MockR2Object[] = [
        { key: 'benchmark/file1.parquet', size: 1000, customMetadata: { codec: 'snappy', cpuMs: '10', events: '100' } },
        { key: 'benchmark/file2.parquet', size: 2000, customMetadata: { codec: 'snappy', cpuMs: '20', events: '200' } },
      ]
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark')

      const response = await handleBenchmark(request, env as any)
      const json = await response.json()

      const snappySummary = json.summary.find((s: any) => s.codec === 'snappy')
      expect(snappySummary.avgCpuMsPerFile).toBe('15.00')
    })

    it('calculates average bytes per event', async () => {
      const objects: MockR2Object[] = [
        { key: 'benchmark/file1.parquet', size: 1000, customMetadata: { codec: 'snappy', cpuMs: '10', events: '100' } },
      ]
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark')

      const response = await handleBenchmark(request, env as any)
      const json = await response.json()

      const snappySummary = json.summary.find((s: any) => s.codec === 'snappy')
      expect(snappySummary.avgBytesPerEvent).toBe(10) // 1000 bytes / 100 events
    })
  })

  describe('response formatting', () => {
    it('limits files in response to 20', async () => {
      const objects: MockR2Object[] = Array.from({ length: 30 }, (_, i) => ({
        key: `benchmark/file${i}.parquet`,
        size: 100,
        customMetadata: { codec: 'snappy', cpuMs: '1', events: '10' },
      }))
      const bucket = createMockR2Bucket(objects)
      const env = createMockEnv({ BENCHMARK_BUCKET: bucket })
      const request = createRequest('https://events.do/benchmark')

      const response = await handleBenchmark(request, env as any)
      const json = await response.json()

      expect(json.files).toHaveLength(20)
      expect(json.fileCount).toBe(30)
    })
  })
})
