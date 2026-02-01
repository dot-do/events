/**
 * Shared Mocks for @dotdo/events Tests
 *
 * Provides mock implementations for:
 * - DurableObjectState (ctx)
 * - DurableObjectStorage
 * - DurableObjectId
 * - R2Bucket
 * - Request with Cloudflare properties
 */

import { vi } from 'vitest'

// ============================================================================
// DurableObject Mocks
// ============================================================================

/**
 * Creates a mock DurableObjectId
 */
export function createMockDurableObjectId(id = 'test-do-id', name?: string): DurableObjectId {
  return {
    toString: () => id,
    equals: (other: DurableObjectId) => other.toString() === id,
    name,
  } as DurableObjectId
}

/**
 * Creates a mock DurableObjectStorage with in-memory Map backend
 */
export function createMockStorage() {
  const storage = new Map<string, unknown>()
  let alarm: number | null = null

  return {
    get: vi.fn(async <T>(key: string): Promise<T | undefined> => {
      return storage.get(key) as T | undefined
    }),
    put: vi.fn(async (key: string, value: unknown) => {
      storage.set(key, value)
    }),
    delete: vi.fn(async (key: string | string[]) => {
      if (Array.isArray(key)) {
        let deleted = 0
        for (const k of key) {
          if (storage.delete(k)) deleted++
        }
        return deleted
      }
      return storage.delete(key)
    }),
    list: vi.fn(async () => storage),
    getAlarm: vi.fn(async () => alarm),
    setAlarm: vi.fn(async (time: number | Date) => {
      alarm = typeof time === 'number' ? time : time.getTime()
    }),
    deleteAlarm: vi.fn(async () => {
      alarm = null
    }),
    // Expose internal storage for test assertions
    _storage: storage,
    _getAlarm: () => alarm,
    // SQLite bookmark mock
    getCurrentBookmark: vi.fn(() => 'bookmark-123'),
  }
}

/**
 * Creates a mock DurableObjectState (ctx)
 */
export function createMockCtx(options: { id?: string; name?: string } = {}) {
  const storage = createMockStorage()

  return {
    id: createMockDurableObjectId(options.id ?? 'test-do-id', options.name),
    storage,
    waitUntil: vi.fn((promise: Promise<unknown>) => promise),
    blockConcurrencyWhile: vi.fn(<T>(fn: () => Promise<T>) => fn()),
  } as unknown as DurableObjectState & { storage: ReturnType<typeof createMockStorage> }
}

// ============================================================================
// R2 Mocks
// ============================================================================

export interface MockR2Object {
  body: string
  metadata: R2HTTPMetadata
  customMetadata: Record<string, string>
}

/**
 * Creates a mock R2Bucket with in-memory storage
 */
export function createMockR2Bucket() {
  const objects = new Map<string, MockR2Object>()

  return {
    put: vi.fn(async (key: string, body: string | ReadableStream, options?: R2PutOptions) => {
      objects.set(key, {
        body: typeof body === 'string' ? body : '',
        metadata: options?.httpMetadata ?? {},
        customMetadata: options?.customMetadata ?? {},
      })
      return {
        key,
        version: 'v1',
        size: typeof body === 'string' ? body.length : 0,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        httpMetadata: options?.httpMetadata ?? {},
        customMetadata: options?.customMetadata ?? {},
        checksums: { md5: new ArrayBuffer(16) },
      } as R2Object
    }),
    get: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        version: 'v1',
        size: obj.body.length,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        httpMetadata: obj.metadata,
        customMetadata: obj.customMetadata,
        checksums: { md5: new ArrayBuffer(16) },
        body: new ReadableStream(),
        text: async () => obj.body,
        json: async () => JSON.parse(obj.body),
        arrayBuffer: async () => new TextEncoder().encode(obj.body).buffer,
        blob: async () => new Blob([obj.body]),
      } as R2ObjectBody
    }),
    delete: vi.fn(async (key: string) => objects.delete(key)),
    list: vi.fn(async () => ({
      objects: Array.from(objects.keys()).map(key => ({ key })),
    })),
    head: vi.fn(async (key: string) => {
      const obj = objects.get(key)
      if (!obj) return null
      return {
        key,
        version: 'v1',
        size: obj.body.length,
        etag: 'test-etag',
        httpEtag: '"test-etag"',
        uploaded: new Date(),
        httpMetadata: obj.metadata,
        customMetadata: obj.customMetadata,
        checksums: { md5: new ArrayBuffer(16) },
      } as R2Object
    }),
    // Expose internal storage for test assertions
    _objects: objects,
  } as unknown as R2Bucket & { _objects: Map<string, MockR2Object> }
}

// ============================================================================
// Request Mocks
// ============================================================================

export interface MockRequestOptions {
  url?: string
  method?: string
  colo?: string
  worker?: string
  doClass?: string
  headers?: Record<string, string>
}

/**
 * Creates a mock Request with Cloudflare (CF) properties
 */
export function createMockRequest(options: MockRequestOptions = {}): Request {
  const headers = new Headers(options.headers)
  if (options.worker) headers.set('cf-worker', options.worker)
  if (options.doClass) headers.set('X-DO-Class', options.doClass)

  const request = new Request(options.url ?? 'https://example.com/test', {
    method: options.method ?? 'GET',
    headers,
  })

  // Add CF properties (simulating Cloudflare edge)
  ;(request as any).cf = {
    colo: options.colo ?? 'SFO',
    country: 'US',
    city: 'San Francisco',
    continent: 'NA',
    latitude: '37.7749',
    longitude: '-122.4194',
    postalCode: '94102',
    region: 'California',
    regionCode: 'CA',
    timezone: 'America/Los_Angeles',
    asn: 13335,
    asOrganization: 'Cloudflare, Inc.',
  } as IncomingRequestCfProperties

  return request
}

// ============================================================================
// Collection Mocks (for CDC)
// ============================================================================

import type { Collection } from '../cdc.js'

/**
 * Creates a mock Collection (compatible with @dotdo/rpc collections)
 */
export function createMockCollection<T extends Record<string, unknown>>(): Collection<T> & {
  _data: Map<string, T>
} {
  const data = new Map<string, T>()

  return {
    _data: data,
    get: vi.fn((id: string): T | null => data.get(id) ?? null),
    put: vi.fn((id: string, doc: T): void => {
      data.set(id, doc)
    }),
    delete: vi.fn((id: string): boolean => data.delete(id)),
    has: vi.fn((id: string): boolean => data.has(id)),
    find: vi.fn((_filter?: Record<string, unknown>, _options?: Record<string, unknown>): T[] =>
      Array.from(data.values())
    ),
    count: vi.fn((_filter?: Record<string, unknown>): number => data.size),
    list: vi.fn((_options?: Record<string, unknown>): T[] => Array.from(data.values())),
    keys: vi.fn((): string[] => Array.from(data.keys())),
    clear: vi.fn((): number => {
      const count = data.size
      data.clear()
      return count
    }),
  }
}

// ============================================================================
// EventEmitter Mocks
// ============================================================================

import type { EventEmitter } from '../emitter.js'

/**
 * Creates a mock EventEmitter for testing components that depend on it
 */
export function createMockEmitter() {
  return {
    emit: vi.fn(),
    emitChange: vi.fn(),
    flush: vi.fn().mockResolvedValue(undefined),
    handleAlarm: vi.fn().mockResolvedValue(undefined),
    enrichFromRequest: vi.fn(),
    persistBatch: vi.fn().mockResolvedValue(undefined),
    pendingCount: 0,
    doIdentity: { id: 'mock-do' },
  } as unknown as EventEmitter & {
    emitChange: ReturnType<typeof vi.fn>
    emit: ReturnType<typeof vi.fn>
    flush: ReturnType<typeof vi.fn>
    handleAlarm: ReturnType<typeof vi.fn>
    enrichFromRequest: ReturnType<typeof vi.fn>
    persistBatch: ReturnType<typeof vi.fn>
  }
}

// ============================================================================
// Fetch Mock Helper
// ============================================================================

/**
 * Creates a mock fetch function that can be configured to return different responses
 */
export function createMockFetch() {
  const mockFn = vi.fn()

  // Default: return success
  mockFn.mockResolvedValue(new Response('OK', { status: 200 }))

  return {
    fn: mockFn,
    /** Configure mock to return success response */
    success: (body = 'OK', status = 200) => {
      mockFn.mockResolvedValue(new Response(body, { status }))
    },
    /** Configure mock to return error response */
    error: (status = 500, body = 'Error') => {
      mockFn.mockResolvedValue(new Response(body, { status }))
    },
    /** Configure mock to throw network error */
    networkError: (message = 'Network error') => {
      mockFn.mockRejectedValue(new Error(message))
    },
    /** Configure mock to return success once then error */
    successThenError: () => {
      mockFn
        .mockResolvedValueOnce(new Response('OK', { status: 200 }))
        .mockResolvedValue(new Response('Error', { status: 500 }))
    },
    /** Configure mock to return error once then success */
    errorThenSuccess: () => {
      mockFn
        .mockResolvedValueOnce(new Response('Error', { status: 500 }))
        .mockResolvedValue(new Response('OK', { status: 200 }))
    },
    /** Get call details */
    getCalls: () => mockFn.mock.calls,
    /** Get last call body parsed as JSON */
    getLastBody: () => {
      const lastCall = mockFn.mock.calls[mockFn.mock.calls.length - 1]
      if (!lastCall) return null
      return JSON.parse(lastCall[1]?.body)
    },
  }
}
