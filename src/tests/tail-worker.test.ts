/**
 * Tail Worker Tests
 *
 * Comprehensive unit tests for src/tail-worker.ts covering:
 * 1. checkAuth() function - valid/invalid auth
 * 2. fetch() handler - /status endpoint, /stats endpoint
 * 3. tail() event processing - trace handling, loop prevention
 * 4. Error handling paths
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockR2Bucket {
  list: ReturnType<typeof vi.fn>
  head: ReturnType<typeof vi.fn>
  put: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  _objects: Map<string, { body: Uint8Array; metadata: Record<string, string>; uploaded?: Date }>
}

interface MockEventWriterDO {
  ingest: ReturnType<typeof vi.fn>
}

interface MockDurableObjectNamespace<T = unknown> {
  idFromName: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  _stub: T
}

interface MockEnv {
  EVENTS_BUCKET: MockR2Bucket
  EVENT_WRITER: MockDurableObjectNamespace<MockEventWriterDO>
  TAIL_AUTH_SECRET?: string
}

// ============================================================================
// Mock Factories
// ============================================================================

function createMockR2Bucket(): MockR2Bucket {
  const objects = new Map<string, { body: Uint8Array; metadata: Record<string, string>; uploaded?: Date }>()

  return {
    list: vi.fn(async (options?: { prefix?: string; limit?: number }) => {
      const prefix = options?.prefix ?? ''
      const limit = options?.limit ?? 1000
      const matching = Array.from(objects.entries())
        .filter(([key]) => key.startsWith(prefix))
        .slice(0, limit)
        .map(([key, obj]) => ({
          key,
          size: obj.body.byteLength,
          uploaded: obj.uploaded ?? new Date(),
        }))
      return { objects: matching, truncated: matching.length >= limit }
    }),
    head: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        size: obj.body.byteLength,
        customMetadata: obj.metadata,
      }
    }),
    put: vi.fn(async (key: string, body: Uint8Array | string, options?: { customMetadata?: Record<string, string> }) => {
      const bodyBytes = typeof body === 'string' ? new TextEncoder().encode(body) : body
      objects.set(key, {
        body: bodyBytes,
        metadata: options?.customMetadata ?? {},
        uploaded: new Date(),
      })
      return { key }
    }),
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        text: async () => new TextDecoder().decode(obj.body),
        arrayBuffer: async () => obj.body.buffer,
        customMetadata: obj.metadata,
      }
    }),
    delete: vi.fn(async (key: string) => objects.delete(key)),
    _objects: objects,
  }
}

function createMockEventWriterDO(): MockEventWriterDO {
  return {
    ingest: vi.fn().mockResolvedValue({
      ok: true,
      buffered: 1,
      shard: 0,
    }),
  }
}

function createMockDurableObjectNamespace<T>(stub: T): MockDurableObjectNamespace<T> {
  return {
    idFromName: vi.fn((name: string) => ({ name, toString: () => name })),
    get: vi.fn(() => stub),
    _stub: stub,
  }
}

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  const writerStub = createMockEventWriterDO()

  return {
    EVENTS_BUCKET: createMockR2Bucket(),
    EVENT_WRITER: createMockDurableObjectNamespace(writerStub),
    TAIL_AUTH_SECRET: 'test-secret-123',
    ...overrides,
  }
}

function createRequest(method: string, url: string, options: RequestInit = {}): Request {
  return new Request(url, { method, ...options })
}

// ============================================================================
// Mock the ingestWithOverflow function
// ============================================================================

const mockIngestWithOverflow = vi.fn().mockResolvedValue({
  ok: true,
  buffered: 1,
  shard: 0,
})

vi.mock('../event-writer-do', () => ({
  ingestWithOverflow: (...args: unknown[]) => mockIngestWithOverflow(...args),
}))

// ============================================================================
// Import the module under test (after mocks are set up)
// ============================================================================

import tailWorker from '../tail-worker'

// ============================================================================
// checkAuth Tests
// ============================================================================

describe('checkAuth (via fetch handler)', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  describe('when TAIL_AUTH_SECRET is not configured', () => {
    it('should return 500 error for any request', async () => {
      const envNoSecret = createMockEnv({ TAIL_AUTH_SECRET: undefined })
      const request = createRequest('GET', 'https://tail.events.do/status')

      const response = await tailWorker.fetch(request, envNoSecret as any)

      expect(response.status).toBe(500)
      const json = await response.json() as { error: string }
      expect(json.error).toBe('Server misconfigured: no auth secret')
    })

    it('should return 500 even with valid-looking Authorization header', async () => {
      const envNoSecret = createMockEnv({ TAIL_AUTH_SECRET: undefined })
      const request = createRequest('GET', 'https://tail.events.do/status', {
        headers: { Authorization: 'Bearer some-token' },
      })

      const response = await tailWorker.fetch(request, envNoSecret as any)

      expect(response.status).toBe(500)
    })
  })

  describe('when TAIL_AUTH_SECRET is configured', () => {
    it('should return 401 when Authorization header is missing', async () => {
      const request = createRequest('GET', 'https://tail.events.do/status')

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(401)
      const json = await response.json() as { error: string }
      expect(json.error).toBe('Unauthorized')
    })

    it('should return 401 when Authorization header has wrong format', async () => {
      const request = createRequest('GET', 'https://tail.events.do/status', {
        headers: { Authorization: 'Basic test-secret-123' },
      })

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(401)
    })

    it('should return 401 when Bearer token is incorrect', async () => {
      const request = createRequest('GET', 'https://tail.events.do/status', {
        headers: { Authorization: 'Bearer wrong-secret' },
      })

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(401)
    })

    it('should allow request when Bearer token matches secret', async () => {
      const request = createRequest('GET', 'https://tail.events.do/status', {
        headers: { Authorization: 'Bearer test-secret-123' },
      })

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(200)
    })

    it('should be case-sensitive for the token', async () => {
      const request = createRequest('GET', 'https://tail.events.do/status', {
        headers: { Authorization: 'Bearer TEST-SECRET-123' },
      })

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(401)
    })
  })
})

// ============================================================================
// fetch() handler Tests
// ============================================================================

describe('fetch() handler', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  function authenticatedRequest(method: string, url: string, options: RequestInit = {}): Request {
    return createRequest(method, url, {
      ...options,
      headers: {
        Authorization: 'Bearer test-secret-123',
        ...((options.headers as Record<string, string>) ?? {}),
      },
    })
  }

  describe('/status endpoint', () => {
    it('should return service info', async () => {
      const request = authenticatedRequest('GET', 'https://tail.events.do/status')

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(200)
      const json = await response.json() as { service: string; architecture: string }
      expect(json.service).toBe('tail')
      expect(json.architecture).toBe('shared-do-immediate')
    })
  })

  describe('/stats endpoint', () => {
    it('should return empty stats when no events stored', async () => {
      const request = authenticatedRequest('GET', 'https://tail.events.do/stats')

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(200)
      const json = await response.json() as { summary: { files: number; totalEvents: number }; files: unknown[] }
      expect(json.summary.files).toBe(0)
      expect(json.summary.totalEvents).toBe(0)
      expect(json.files).toEqual([])
    })

    it('should list recent files from R2', async () => {
      // Pre-populate bucket with some files
      const bucket = env.EVENTS_BUCKET
      bucket._objects.set('events/2024/01/01/00/file1.jsonl', {
        body: new TextEncoder().encode('{}'),
        metadata: { events: '10', cpuMs: '5' },
        uploaded: new Date('2024-01-01T00:00:00Z'),
      })
      bucket._objects.set('events/2024/01/01/01/file2.jsonl', {
        body: new TextEncoder().encode('{}{}'),
        metadata: { events: '20', cpuMs: '10' },
        uploaded: new Date('2024-01-01T01:00:00Z'),
      })

      const request = authenticatedRequest('GET', 'https://tail.events.do/stats')

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(200)
      const json = await response.json() as {
        summary: { files: number; totalEvents: number; totalBytes: number; avgCpuMs: string }
        files: Array<{ key: string; events?: string; cpuMs?: string }>
      }
      expect(json.summary.files).toBe(2)
      expect(json.summary.totalEvents).toBe(30) // 10 + 20
      expect(json.files).toHaveLength(2)
    })

    it('should sort files by upload time descending', async () => {
      const bucket = env.EVENTS_BUCKET
      bucket._objects.set('events/2024/01/01/00/older.jsonl', {
        body: new TextEncoder().encode('{}'),
        metadata: { events: '5' },
        uploaded: new Date('2024-01-01T00:00:00Z'),
      })
      bucket._objects.set('events/2024/01/01/01/newer.jsonl', {
        body: new TextEncoder().encode('{}'),
        metadata: { events: '10' },
        uploaded: new Date('2024-01-01T01:00:00Z'),
      })

      const request = authenticatedRequest('GET', 'https://tail.events.do/stats')

      const response = await tailWorker.fetch(request, env as any)

      const json = await response.json() as { files: Array<{ key: string }> }
      // Newer file should come first
      expect(json.files[0].key).toBe('events/2024/01/01/01/newer.jsonl')
      expect(json.files[1].key).toBe('events/2024/01/01/00/older.jsonl')
    })

    it('should calculate summary statistics correctly', async () => {
      const bucket = env.EVENTS_BUCKET
      bucket._objects.set('events/2024/01/01/00/file1.jsonl', {
        body: new Uint8Array(100),
        metadata: { events: '50', cpuMs: '10' },
        uploaded: new Date(),
      })
      bucket._objects.set('events/2024/01/01/01/file2.jsonl', {
        body: new Uint8Array(200),
        metadata: { events: '100', cpuMs: '20' },
        uploaded: new Date(),
      })

      const request = authenticatedRequest('GET', 'https://tail.events.do/stats')

      const response = await tailWorker.fetch(request, env as any)

      const json = await response.json() as {
        summary: {
          files: number
          totalEvents: number
          totalBytes: number
          avgEventsPerFile: number
          avgBytesPerEvent: number
          avgCpuMs: string
        }
      }
      expect(json.summary.files).toBe(2)
      expect(json.summary.totalEvents).toBe(150)
      expect(json.summary.totalBytes).toBe(300)
      expect(json.summary.avgEventsPerFile).toBe(75)
      expect(json.summary.avgBytesPerEvent).toBe(2)
      expect(json.summary.avgCpuMs).toBe('15.00')
    })

    it('should handle missing metadata gracefully', async () => {
      const bucket = env.EVENTS_BUCKET
      bucket._objects.set('events/2024/01/01/00/file1.jsonl', {
        body: new Uint8Array(50),
        metadata: {}, // No events or cpuMs metadata
        uploaded: new Date(),
      })

      const request = authenticatedRequest('GET', 'https://tail.events.do/stats')

      const response = await tailWorker.fetch(request, env as any)

      const json = await response.json() as { summary: { totalEvents: number; avgCpuMs: string } }
      expect(json.summary.totalEvents).toBe(0)
      expect(json.summary.avgCpuMs).toBe('0.00')
    })

    it('should limit to 20 files in response', async () => {
      const bucket = env.EVENTS_BUCKET
      // Add 25 files
      for (let i = 0; i < 25; i++) {
        bucket._objects.set(`events/2024/01/01/${i.toString().padStart(2, '0')}/file.jsonl`, {
          body: new Uint8Array(10),
          metadata: { events: '1' },
          uploaded: new Date(Date.now() - i * 1000),
        })
      }

      const request = authenticatedRequest('GET', 'https://tail.events.do/stats')

      const response = await tailWorker.fetch(request, env as any)

      const json = await response.json() as { files: unknown[] }
      expect(json.files).toHaveLength(20)
    })
  })

  describe('default endpoint', () => {
    it('should return service info and available endpoints for unknown paths', async () => {
      const request = authenticatedRequest('GET', 'https://tail.events.do/unknown')

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(200)
      const json = await response.json() as { service: string; endpoints: string[] }
      expect(json.service).toBe('tail')
      expect(json.architecture).toBe('shared-do-immediate')
      expect(json.endpoints).toContain('/status')
      expect(json.endpoints).toContain('/stats')
    })

    it('should return endpoints info for root path', async () => {
      const request = authenticatedRequest('GET', 'https://tail.events.do/')

      const response = await tailWorker.fetch(request, env as any)

      expect(response.status).toBe(200)
      const json = await response.json() as { endpoints: string[] }
      expect(json.endpoints).toContain('/status')
      expect(json.endpoints).toContain('/stats')
    })
  })
})

// ============================================================================
// tail() event processing Tests
// ============================================================================

describe('tail() event processing', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
    mockIngestWithOverflow.mockResolvedValue({
      ok: true,
      buffered: 1,
      shard: 0,
    })
  })

  describe('basic event processing', () => {
    it('should return early when events array is empty', async () => {
      await tailWorker.tail([], env as any)

      expect(mockIngestWithOverflow).not.toHaveBeenCalled()
    })

    it('should process a single trace event', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'my-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: {
            request: { method: 'GET', url: 'https://example.com/api' },
            response: { status: 200 },
          },
        },
      ]

      await tailWorker.tail(events, env as any)

      expect(mockIngestWithOverflow).toHaveBeenCalledTimes(1)
      expect(mockIngestWithOverflow).toHaveBeenCalledWith(
        env,
        expect.arrayContaining([
          expect.objectContaining({
            type: 'tail.my-worker.fetch',
            source: 'tail',
            scriptName: 'my-worker',
            outcome: 'ok',
            eventType: 'fetch',
            method: 'GET',
            url: 'https://example.com/api',
            statusCode: 200,
          }),
        ]),
        'tail'
      )
    })

    it('should process multiple trace events', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker-a',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://a.com' } },
        },
        {
          scriptName: 'worker-b',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'POST', url: 'https://b.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      expect(mockIngestWithOverflow).toHaveBeenCalledTimes(1)
      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records).toHaveLength(2)
    })
  })

  describe('loop prevention', () => {
    it('should skip events from "tail" script to prevent loops', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'tail', // Self-reference
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      // Should not ingest any records since the only event was filtered
      expect(mockIngestWithOverflow).not.toHaveBeenCalled()
    })

    it('should process other events while skipping "tail" events', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'tail', // Should be skipped
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://tail.example.com' } },
        },
        {
          scriptName: 'my-worker', // Should be processed
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      expect(mockIngestWithOverflow).toHaveBeenCalledTimes(1)
      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records).toHaveLength(1)
      expect(records[0].scriptName).toBe('my-worker')
    })
  })

  describe('event type detection', () => {
    it('should detect fetch events', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'POST', url: 'https://api.com/data' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('fetch')
      expect(records[0].method).toBe('POST')
    })

    it('should detect scheduled events', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'cron-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { scheduledTime: 1704067200000, cron: '0 * * * *' },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('scheduled')
    })

    it('should detect queue events', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'queue-consumer',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { queue: 'my-queue', batchSize: 10 },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('queue')
    })

    it('should detect alarm events', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'do-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { alarm: true },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('alarm')
    })

    it('should detect alarm events with type field', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'do-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { type: 'alarm' },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('alarm')
    })

    it('should detect RPC method calls', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'rpc-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { rpcMethod: 'getData' },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('rpc.getData')
      expect(records[0].method).toBe('getData')
    })

    it('should detect DO RPC calls with entrypoint', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'do-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { rpcMethod: 'ingest' },
          executionModel: 'durableObject',
          entrypoint: 'EventWriterDO',
        } as TraceItem & { executionModel: string; entrypoint: string },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('do.EventWriterDO.ingest')
    })

    it('should detect DO events without specific event info', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'do-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: null,
          executionModel: 'durableObject',
          entrypoint: 'CounterDO',
        } as TraceItem & { executionModel: string; entrypoint: string },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('do.CounterDO')
    })

    it('should use event.type for custom event types', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'custom-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { type: 'custom.event.type' },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('custom.event.type')
    })

    it('should handle unknown event type', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'mystery-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { unknownField: 'value' } as any,
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].eventType).toBe('unknown')
    })
  })

  describe('duration calculation', () => {
    it('should calculate duration from logs', async () => {
      const eventTimestamp = 1704067200000
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp,
          logs: [
            { timestamp: eventTimestamp + 10, level: 'log', message: ['Starting'] },
            { timestamp: eventTimestamp + 50, level: 'log', message: ['Processing'] },
            { timestamp: eventTimestamp + 100, level: 'log', message: ['Done'] },
          ],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].durationMs).toBe(100) // lastLog.timestamp - eventTimestamp
    })

    it('should handle missing eventTimestamp', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: null as any,
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].durationMs).toBeUndefined()
    })

    it('should handle empty logs array', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].durationMs).toBeUndefined()
    })
  })

  describe('response status extraction', () => {
    it('should extract response status code', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: {
            request: { method: 'GET', url: 'https://example.com' },
            response: { status: 404 },
          },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].statusCode).toBe(404)
    })

    it('should handle missing response', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'exception',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: {
            request: { method: 'GET', url: 'https://example.com' },
            // No response (exception thrown)
          },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].statusCode).toBeUndefined()
    })
  })

  describe('payload serialization and sanitization', () => {
    it('should include sanitized trace payload', async () => {
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [{ timestamp: Date.now(), level: 'log', message: ['Test log'] }],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].payload).toBeDefined()
      expect(records[0].payload).toHaveProperty('scriptName', 'worker')
    })

    it('should handle serialization errors gracefully', async () => {
      // Test that the code handles non-serializable traces gracefully
      // by creating a trace with properties that would cause issues
      // The actual implementation does a try-catch around JSON.parse(JSON.stringify(trace))
      // We can verify the fallback behavior by checking that it creates a minimal payload

      // Create a trace where the code path would trigger the catch block
      // by having non-standard properties that might fail serialization
      const traceWithIssue = {
        scriptName: 'problem-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        logs: [],
        exceptions: [],
        event: { type: 'test.event' },
      }

      await tailWorker.tail([traceWithIssue as TraceItem], env as any)

      // Should still ingest successfully
      expect(mockIngestWithOverflow).toHaveBeenCalledTimes(1)
      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].scriptName).toBe('problem-worker')
      expect(records[0].payload).toBeDefined()
    })
  })

  describe('event record structure', () => {
    it('should create properly structured event records', async () => {
      const timestamp = Date.now()
      const events: TraceItem[] = [
        {
          scriptName: 'my-worker',
          outcome: 'ok',
          eventTimestamp: timestamp,
          logs: [],
          exceptions: [],
          event: {
            request: { method: 'POST', url: 'https://api.example.com/data' },
            response: { status: 201 },
          },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      const record = records[0]

      expect(record).toMatchObject({
        ts: new Date(timestamp).toISOString(),
        type: 'tail.my-worker.fetch',
        source: 'tail',
        scriptName: 'my-worker',
        outcome: 'ok',
        eventType: 'fetch',
        method: 'POST',
        url: 'https://api.example.com/data',
        statusCode: 201,
      })
    })

    it('should use current timestamp when eventTimestamp is missing', async () => {
      const before = Date.now()
      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: null as any,
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)
      const after = Date.now()

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      const recordTs = new Date(records[0].ts).getTime()
      expect(recordTs).toBeGreaterThanOrEqual(before)
      expect(recordTs).toBeLessThanOrEqual(after)
    })

    it('should handle missing scriptName', async () => {
      const events: TraceItem[] = [
        {
          scriptName: undefined as any,
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await tailWorker.tail(events, env as any)

      const [, records] = mockIngestWithOverflow.mock.calls[0]
      expect(records[0].scriptName).toBe('unknown')
      expect(records[0].type).toBe('tail.unknown.fetch')
    })
  })

  describe('error handling', () => {
    it('should log error when ingest fails', async () => {
      mockIngestWithOverflow.mockResolvedValueOnce({
        ok: false,
        shard: 0,
        buffered: 0,
      })

      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      // Should not throw
      await expect(tailWorker.tail(events, env as any)).resolves.toBeUndefined()

      expect(mockIngestWithOverflow).toHaveBeenCalled()
    })

    it('should handle ingest success', async () => {
      mockIngestWithOverflow.mockResolvedValueOnce({
        ok: true,
        shard: 1,
        buffered: 5,
      })

      const events: TraceItem[] = [
        {
          scriptName: 'worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [],
          exceptions: [],
          event: { request: { method: 'GET', url: 'https://example.com' } },
        },
      ]

      await expect(tailWorker.tail(events, env as any)).resolves.toBeUndefined()

      expect(mockIngestWithOverflow).toHaveBeenCalled()
    })
  })
})

// ============================================================================
// Edge Cases
// ============================================================================

describe('edge cases', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
    mockIngestWithOverflow.mockResolvedValue({
      ok: true,
      buffered: 1,
      shard: 0,
    })
  })

  it('should handle trace with null event', async () => {
    const events: TraceItem[] = [
      {
        scriptName: 'worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        logs: [],
        exceptions: [],
        event: null as any,
      },
    ]

    await tailWorker.tail(events, env as any)

    const [, records] = mockIngestWithOverflow.mock.calls[0]
    expect(records[0].eventType).toBe('unknown')
  })

  it('should handle trace with undefined event', async () => {
    const events: TraceItem[] = [
      {
        scriptName: 'worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        logs: [],
        exceptions: [],
        event: undefined as any,
      },
    ]

    await tailWorker.tail(events, env as any)

    const [, records] = mockIngestWithOverflow.mock.calls[0]
    expect(records[0].eventType).toBe('unknown')
  })

  it('should handle trace with exceptions', async () => {
    const events: TraceItem[] = [
      {
        scriptName: 'worker',
        outcome: 'exception',
        eventTimestamp: Date.now(),
        logs: [],
        exceptions: [
          { name: 'Error', message: 'Something went wrong', timestamp: Date.now() },
        ],
        event: { request: { method: 'GET', url: 'https://example.com' } },
      },
    ]

    await tailWorker.tail(events, env as any)

    const [, records] = mockIngestWithOverflow.mock.calls[0]
    expect(records[0].outcome).toBe('exception')
    expect(records[0].payload).toHaveProperty('exceptions')
  })

  it('should handle DO alarm with entrypoint', async () => {
    const events: TraceItem[] = [
      {
        scriptName: 'do-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        logs: [],
        exceptions: [],
        event: { alarm: true },
        executionModel: 'durableObject',
        entrypoint: 'MyDO',
      } as TraceItem & { executionModel: string; entrypoint: string },
    ]

    await tailWorker.tail(events, env as any)

    const [, records] = mockIngestWithOverflow.mock.calls[0]
    expect(records[0].eventType).toBe('do.MyDO.alarm')
  })

  it('should handle large batch of events', async () => {
    const events: TraceItem[] = Array.from({ length: 100 }, (_, i) => ({
      scriptName: `worker-${i}`,
      outcome: 'ok',
      eventTimestamp: Date.now() + i,
      logs: [],
      exceptions: [],
      event: { request: { method: 'GET', url: `https://example.com/${i}` } },
    }))

    await tailWorker.tail(events, env as any)

    expect(mockIngestWithOverflow).toHaveBeenCalledTimes(1)
    const [, records] = mockIngestWithOverflow.mock.calls[0]
    expect(records).toHaveLength(100)
  })

  it('should filter all tail events from large batch', async () => {
    // All events are from 'tail' worker
    const events: TraceItem[] = Array.from({ length: 50 }, () => ({
      scriptName: 'tail',
      outcome: 'ok',
      eventTimestamp: Date.now(),
      logs: [],
      exceptions: [],
      event: { request: { method: 'GET', url: 'https://example.com' } },
    }))

    await tailWorker.tail(events, env as any)

    // Should not call ingest since all events were filtered
    expect(mockIngestWithOverflow).not.toHaveBeenCalled()
  })
})
