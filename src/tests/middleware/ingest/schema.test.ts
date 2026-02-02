/**
 * Ingest Schema Validation Middleware Tests
 *
 * Unit tests for src/middleware/ingest/schema.ts
 * Tests schema validation against registered JSON schemas
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  validateEventsAgainstSchemas,
  schemaValidationMiddleware,
  getSchemaRegistryCircuitBreaker,
} from '../../../middleware/ingest/schema'
import type { IngestContext, BatchValidationResult } from '../../../middleware/ingest/types'
import type { Env } from '../../../env'

// ============================================================================
// Mock Types
// ============================================================================

interface MockSchemaRegistry {
  validateEvents: ReturnType<typeof vi.fn>
}

interface MockDurableObjectId {
  toString(): string
}

interface MockDurableObjectNamespace {
  idFromName: ReturnType<typeof vi.fn<[string], MockDurableObjectId>>
  get: ReturnType<typeof vi.fn<[MockDurableObjectId], MockSchemaRegistry>>
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockSchemaRegistry(
  validateResult: BatchValidationResult = { valid: true, results: [] }
): MockSchemaRegistry {
  return {
    validateEvents: vi.fn().mockResolvedValue(validateResult),
  }
}

function createMockNamespace(registry: MockSchemaRegistry): MockDurableObjectNamespace {
  return {
    idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
    get: vi.fn().mockReturnValue(registry),
  }
}

function createMockEnv(
  options: {
    enableSchemaValidation?: boolean
    schemaRegistry?: MockDurableObjectNamespace
  } = {}
): Partial<Env> {
  return {
    ENABLE_SCHEMA_VALIDATION: options.enableSchemaValidation ? 'true' : undefined,
    SCHEMA_REGISTRY: options.schemaRegistry as unknown as Env['SCHEMA_REGISTRY'],
    ANALYTICS: undefined,
  }
}

function createMockContext(overrides: Partial<IngestContext> = {}): IngestContext {
  return {
    request: new Request('https://events.do/ingest', { method: 'POST' }),
    env: createMockEnv() as any,
    ctx: {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as any,
    tenant: {
      namespace: 'test-namespace',
      isAdmin: false,
      keyId: 'test-key',
    },
    batch: {
      events: [
        { type: 'user.created', ts: '2024-01-01T00:00:00Z', name: 'John' },
        { type: 'user.updated', ts: '2024-01-01T00:00:01Z', name: 'Jane' },
      ],
    },
    startTime: performance.now(),
    ...overrides,
  }
}

// ============================================================================
// validateEventsAgainstSchemas Tests
// ============================================================================

describe('validateEventsAgainstSchemas', () => {
  describe('when schema validation is disabled', () => {
    it('returns null when ENABLE_SCHEMA_VALIDATION is not set', async () => {
      const env = createMockEnv({ enableSchemaValidation: false })
      const events = [{ type: 'event', data: 'any' }]

      const result = await validateEventsAgainstSchemas(events, 'namespace', env as Env)

      expect(result).toBeNull()
    })

    it('returns null when SCHEMA_REGISTRY is not configured', async () => {
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: undefined,
      })
      const events = [{ type: 'event', data: 'any' }]

      const result = await validateEventsAgainstSchemas(events, 'namespace', env as Env)

      expect(result).toBeNull()
    })
  })

  describe('when schema validation is enabled', () => {
    it('calls schema registry with events and namespace', async () => {
      const registry = createMockSchemaRegistry()
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [
        { type: 'user.created', name: 'John' },
        { type: 'user.deleted', userId: '123' },
      ]

      await validateEventsAgainstSchemas(events, 'my-namespace', env as Env)

      expect(namespace.idFromName).toHaveBeenCalledWith('my-namespace')
      expect(registry.validateEvents).toHaveBeenCalledWith(events, 'my-namespace')
    })

    it('returns null when all events pass validation', async () => {
      const registry = createMockSchemaRegistry({ valid: true, results: [] })
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [{ type: 'valid.event', data: 'correct' }]

      const result = await validateEventsAgainstSchemas(events, 'namespace', env as Env)

      expect(result).toBeNull()
    })

    it('returns validation errors for invalid events', async () => {
      const registry = createMockSchemaRegistry({
        valid: false,
        results: [
          {
            index: 0,
            eventType: 'user.created',
            valid: false,
            errors: [{ path: '/name', message: 'required field missing' }],
          },
          {
            index: 1,
            eventType: 'user.updated',
            valid: true,
            errors: [],
          },
          {
            index: 2,
            eventType: 'user.created',
            valid: false,
            errors: [{ path: '/email', message: 'invalid email format' }],
          },
        ],
      })
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [
        { type: 'user.created' },
        { type: 'user.updated', name: 'valid' },
        { type: 'user.created', email: 'invalid' },
      ]

      const result = await validateEventsAgainstSchemas(events, 'namespace', env as Env)

      expect(result).toHaveLength(2)
      expect(result?.[0].index).toBe(0)
      expect(result?.[0].eventType).toBe('user.created')
      expect(result?.[0].errors[0].message).toBe('required field missing')
      expect(result?.[1].index).toBe(2)
    })
  })

  describe('error handling', () => {
    it('returns null when schema registry throws', async () => {
      const registry = {
        validateEvents: vi.fn().mockRejectedValue(new Error('Registry unavailable')),
      }
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [{ type: 'event' }]
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      const result = await validateEventsAgainstSchemas(events, 'namespace', env as Env)

      expect(result).toBeNull()
      expect(consoleSpy).toHaveBeenCalled()
      consoleSpy.mockRestore()
    })
  })

  describe('timeout protection', () => {
    beforeEach(() => {
      vi.useFakeTimers()
      getSchemaRegistryCircuitBreaker().reset()
    })

    afterEach(() => {
      vi.useRealTimers()
      getSchemaRegistryCircuitBreaker().reset()
    })

    it('returns null when schema validation times out', async () => {
      const registry = {
        validateEvents: vi.fn().mockImplementation(
          () => new Promise((resolve) => setTimeout(() => resolve({ valid: true, results: [] }), 5000))
        ),
      }
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [{ type: 'event' }]
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      // Call with a 100ms timeout
      const resultPromise = validateEventsAgainstSchemas(events, 'namespace', env as Env, 100)
      vi.advanceTimersByTime(100)
      const result = await resultPromise

      expect(result).toBeNull()
      consoleSpy.mockRestore()
    })

    it('uses custom timeout when provided', async () => {
      const registry = {
        validateEvents: vi.fn().mockImplementation(
          () => new Promise((resolve) => setTimeout(() => resolve({ valid: true, results: [] }), 200))
        ),
      }
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [{ type: 'event' }]

      // Call with a 500ms timeout - should succeed
      const resultPromise = validateEventsAgainstSchemas(events, 'namespace', env as Env, 500)
      vi.advanceTimersByTime(200)
      const result = await resultPromise

      expect(result).toBeNull() // null means validation passed
    })
  })

  describe('circuit breaker protection', () => {
    beforeEach(() => {
      getSchemaRegistryCircuitBreaker().reset()
    })

    afterEach(() => {
      getSchemaRegistryCircuitBreaker().reset()
    })

    it('returns null when circuit breaker is open', async () => {
      // Trip the circuit breaker
      getSchemaRegistryCircuitBreaker().trip(new Error('test'))

      const registry = createMockSchemaRegistry({ valid: true, results: [] })
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [{ type: 'event' }]
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

      const result = await validateEventsAgainstSchemas(events, 'namespace', env as Env)

      expect(result).toBeNull()
      expect(registry.validateEvents).not.toHaveBeenCalled()
      consoleSpy.mockRestore()
    })

    it('opens circuit after repeated failures', async () => {
      const registry = {
        validateEvents: vi.fn().mockRejectedValue(new Error('Registry error')),
      }
      const namespace = createMockNamespace(registry)
      const env = createMockEnv({
        enableSchemaValidation: true,
        schemaRegistry: namespace,
      })
      const events = [{ type: 'event' }]
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      // Call 5 times to trip the circuit breaker (default threshold)
      for (let i = 0; i < 5; i++) {
        await validateEventsAgainstSchemas(events, 'namespace', env as Env)
      }

      expect(getSchemaRegistryCircuitBreaker().getState()).toBe('open')
      consoleSpy.mockRestore()
    })
  })
})

// ============================================================================
// schemaValidationMiddleware Tests
// ============================================================================

describe('schemaValidationMiddleware', () => {
  describe('when validation passes', () => {
    it('returns continue: true when schema validation is disabled', async () => {
      const context = createMockContext({
        env: createMockEnv({ enableSchemaValidation: false }) as any,
      })

      const result = await schemaValidationMiddleware(context)

      expect(result.continue).toBe(true)
    })

    it('returns continue: true when all events are valid', async () => {
      const registry = createMockSchemaRegistry({ valid: true, results: [] })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)

      expect(result.continue).toBe(true)
    })
  })

  describe('when validation fails', () => {
    it('returns error response for invalid events', async () => {
      const registry = createMockSchemaRegistry({
        valid: false,
        results: [
          {
            index: 0,
            eventType: 'user.created',
            valid: false,
            errors: [{ path: '/name', message: 'required' }],
          },
        ],
      })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(400)
    })

    it('includes validation errors in response body', async () => {
      const registry = createMockSchemaRegistry({
        valid: false,
        results: [
          {
            index: 1,
            eventType: 'order.placed',
            valid: false,
            errors: [
              { path: '/amount', message: 'must be positive number' },
              { path: '/currency', message: 'invalid currency code' },
            ],
          },
        ],
      })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)
      const json = await result.response?.json()

      expect(json.error).toBe('Schema validation failed')
      expect(json.details.validationErrors).toHaveLength(1)
      expect(json.details.validationErrors[0].eventType).toBe('order.placed')
    })

    it('includes error summary in response', async () => {
      const registry = createMockSchemaRegistry({
        valid: false,
        results: [
          {
            index: 0,
            eventType: 'user.created',
            valid: false,
            errors: [{ path: '/email', message: 'invalid format' }],
          },
        ],
      })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)
      const json = await result.response?.json()

      expect(json.details.summary).toBeDefined()
      expect(json.details.summary[0]).toContain('Event[0]')
      expect(json.details.summary[0]).toContain('user.created')
    })

    it('limits validation errors to first 10', async () => {
      const manyErrors = Array(15)
        .fill(null)
        .map((_, i) => ({
          index: i,
          eventType: `event.${i}`,
          valid: false,
          errors: [{ path: '/field', message: 'error' }],
        }))
      const registry = createMockSchemaRegistry({
        valid: false,
        results: manyErrors,
      })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)
      const json = await result.response?.json()

      expect(json.details.validationErrors.length).toBeLessThanOrEqual(10)
    })

    it('limits summary to first 5 events', async () => {
      const manyErrors = Array(10)
        .fill(null)
        .map((_, i) => ({
          index: i,
          eventType: `event.${i}`,
          valid: false,
          errors: [{ path: '/field', message: 'error' }],
        }))
      const registry = createMockSchemaRegistry({
        valid: false,
        results: manyErrors,
      })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)
      const json = await result.response?.json()

      expect(json.details.summary).toHaveLength(5)
    })

    it('includes CORS headers in error response', async () => {
      const registry = createMockSchemaRegistry({
        valid: false,
        results: [
          {
            index: 0,
            eventType: 'event',
            valid: false,
            errors: [{ message: 'error' }],
          },
        ],
      })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      const result = await schemaValidationMiddleware(context)

      expect(result.response?.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })
  })

  describe('namespace isolation', () => {
    it('validates against correct namespace schema registry', async () => {
      const registry = createMockSchemaRegistry({ valid: true, results: [] })
      const namespace = createMockNamespace(registry)
      const context = createMockContext({
        tenant: { namespace: 'custom-ns', isAdmin: false, keyId: 'key' },
        env: createMockEnv({
          enableSchemaValidation: true,
          schemaRegistry: namespace,
        }) as any,
      })

      await schemaValidationMiddleware(context)

      expect(namespace.idFromName).toHaveBeenCalledWith('custom-ns')
    })
  })
})
