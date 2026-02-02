/**
 * Ingest Middleware Composition Tests
 *
 * Unit tests for src/middleware/ingest/compose.ts
 * Tests middleware composition, error handling, and conditional execution
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  composeMiddleware,
  withErrorHandling,
  conditionalMiddleware,
  fromSimpleFunction,
} from '../../../middleware/ingest/compose'
import type {
  IngestContext,
  IngestMiddleware,
  MiddlewareResult,
} from '../../../middleware/ingest/types'

// ============================================================================
// Helper Functions
// ============================================================================

function createMockContext(overrides: Partial<IngestContext> = {}): IngestContext {
  return {
    request: new Request('https://events.do/ingest', { method: 'POST' }),
    env: {} as any,
    ctx: {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as any,
    tenant: {
      namespace: 'test',
      isAdmin: false,
      keyId: 'test-key',
    },
    startTime: performance.now(),
    ...overrides,
  }
}

function createPassMiddleware(): IngestMiddleware {
  return vi.fn().mockResolvedValue({ continue: true })
}

function createStopMiddleware(status: number = 400): IngestMiddleware {
  return vi.fn().mockResolvedValue({
    continue: false,
    response: new Response('error', { status }),
  })
}

function createThrowingMiddleware(error: Error): IngestMiddleware {
  return vi.fn().mockRejectedValue(error)
}

// ============================================================================
// composeMiddleware Tests
// ============================================================================

describe('composeMiddleware', () => {
  describe('sequential execution', () => {
    it('runs all middleware in sequence when all pass', async () => {
      const middleware1 = createPassMiddleware()
      const middleware2 = createPassMiddleware()
      const middleware3 = createPassMiddleware()

      const pipeline = composeMiddleware([middleware1, middleware2, middleware3])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(true)
      expect(middleware1).toHaveBeenCalledTimes(1)
      expect(middleware2).toHaveBeenCalledTimes(1)
      expect(middleware3).toHaveBeenCalledTimes(1)
    })

    it('passes context to each middleware', async () => {
      const context = createMockContext({ tenant: { namespace: 'custom', isAdmin: true, keyId: 'key' } })
      const middleware1 = vi.fn().mockImplementation(async (ctx) => {
        expect(ctx.tenant.namespace).toBe('custom')
        return { continue: true }
      })
      const middleware2 = vi.fn().mockImplementation(async (ctx) => {
        expect(ctx.tenant.namespace).toBe('custom')
        return { continue: true }
      })

      const pipeline = composeMiddleware([middleware1, middleware2])

      await pipeline(context)

      expect(middleware1).toHaveBeenCalledWith(context)
      expect(middleware2).toHaveBeenCalledWith(context)
    })

    it('allows middleware to modify context for subsequent middleware', async () => {
      const context = createMockContext()
      const middleware1 = vi.fn().mockImplementation(async (ctx) => {
        ctx.rawBody = { modified: true }
        return { continue: true }
      })
      const middleware2 = vi.fn().mockImplementation(async (ctx) => {
        expect(ctx.rawBody).toEqual({ modified: true })
        return { continue: true }
      })

      const pipeline = composeMiddleware([middleware1, middleware2])

      await pipeline(context)

      expect(context.rawBody).toEqual({ modified: true })
    })
  })

  describe('short-circuit behavior', () => {
    it('stops execution when middleware returns continue: false', async () => {
      const middleware1 = createPassMiddleware()
      const middleware2 = createStopMiddleware(403)
      const middleware3 = createPassMiddleware()

      const pipeline = composeMiddleware([middleware1, middleware2, middleware3])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(403)
      expect(middleware1).toHaveBeenCalledTimes(1)
      expect(middleware2).toHaveBeenCalledTimes(1)
      expect(middleware3).not.toHaveBeenCalled()
    })

    it('returns response from stopping middleware', async () => {
      const stopResponse = Response.json({ error: 'Custom error' }, { status: 422 })
      const stoppingMiddleware: IngestMiddleware = vi.fn().mockResolvedValue({
        continue: false,
        response: stopResponse,
      })

      const pipeline = composeMiddleware([createPassMiddleware(), stoppingMiddleware])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(false)
      expect(result.response).toBe(stopResponse)
    })

    it('first middleware can stop pipeline', async () => {
      const middleware1 = createStopMiddleware(401)
      const middleware2 = createPassMiddleware()

      const pipeline = composeMiddleware([middleware1, middleware2])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(false)
      expect(middleware2).not.toHaveBeenCalled()
    })
  })

  describe('empty pipeline', () => {
    it('returns continue: true for empty middleware array', async () => {
      const pipeline = composeMiddleware([])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(true)
    })
  })

  describe('single middleware', () => {
    it('handles single passing middleware', async () => {
      const middleware = createPassMiddleware()
      const pipeline = composeMiddleware([middleware])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(true)
      expect(middleware).toHaveBeenCalledTimes(1)
    })

    it('handles single stopping middleware', async () => {
      const middleware = createStopMiddleware(500)
      const pipeline = composeMiddleware([middleware])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(500)
    })
  })
})

// ============================================================================
// withErrorHandling Tests
// ============================================================================

describe('withErrorHandling', () => {
  describe('without custom error handler', () => {
    it('passes through successful result', async () => {
      const middleware = createPassMiddleware()
      const wrapped = withErrorHandling(middleware)
      const context = createMockContext()

      const result = await wrapped(context)

      expect(result.continue).toBe(true)
    })

    it('re-throws error when no handler provided', async () => {
      const error = new Error('Test error')
      const middleware = createThrowingMiddleware(error)
      const wrapped = withErrorHandling(middleware)
      const context = createMockContext()

      await expect(wrapped(context)).rejects.toThrow('Test error')
    })
  })

  describe('with custom error handler', () => {
    it('calls error handler when middleware throws', async () => {
      const error = new Error('Test error')
      const middleware = createThrowingMiddleware(error)
      const errorHandler = vi.fn().mockResolvedValue({
        continue: false,
        response: new Response('handled', { status: 500 }),
      })
      const wrapped = withErrorHandling(middleware, errorHandler)
      const context = createMockContext()

      const result = await wrapped(context)

      expect(errorHandler).toHaveBeenCalledWith(error, context)
      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(500)
    })

    it('passes context to error handler', async () => {
      const error = new Error('Test')
      const middleware = createThrowingMiddleware(error)
      const context = createMockContext({ tenant: { namespace: 'custom', isAdmin: false, keyId: 'k' } })
      const errorHandler = vi.fn().mockImplementation(async (err, ctx) => {
        expect(ctx.tenant.namespace).toBe('custom')
        return { continue: false, response: new Response('error', { status: 500 }) }
      })
      const wrapped = withErrorHandling(middleware, errorHandler)

      await wrapped(context)

      expect(errorHandler).toHaveBeenCalledWith(error, context)
    })

    it('error handler can allow continuation', async () => {
      const middleware = createThrowingMiddleware(new Error('recoverable'))
      const errorHandler = vi.fn().mockResolvedValue({ continue: true })
      const wrapped = withErrorHandling(middleware, errorHandler)
      const context = createMockContext()

      const result = await wrapped(context)

      expect(result.continue).toBe(true)
    })
  })
})

// ============================================================================
// conditionalMiddleware Tests
// ============================================================================

describe('conditionalMiddleware', () => {
  describe('condition evaluation', () => {
    it('runs middleware when condition returns true', async () => {
      const middleware = createPassMiddleware()
      const condition = vi.fn().mockReturnValue(true)
      const conditional = conditionalMiddleware(condition, middleware)
      const context = createMockContext()

      const result = await conditional(context)

      expect(condition).toHaveBeenCalledWith(context)
      expect(middleware).toHaveBeenCalledTimes(1)
      expect(result.continue).toBe(true)
    })

    it('skips middleware when condition returns false', async () => {
      const middleware = createPassMiddleware()
      const condition = vi.fn().mockReturnValue(false)
      const conditional = conditionalMiddleware(condition, middleware)
      const context = createMockContext()

      const result = await conditional(context)

      expect(condition).toHaveBeenCalledWith(context)
      expect(middleware).not.toHaveBeenCalled()
      expect(result.continue).toBe(true)
    })
  })

  describe('middleware execution', () => {
    it('returns middleware result when condition is true', async () => {
      const middleware = createStopMiddleware(429)
      const condition = () => true
      const conditional = conditionalMiddleware(condition, middleware)
      const context = createMockContext()

      const result = await conditional(context)

      expect(result.continue).toBe(false)
      expect(result.response?.status).toBe(429)
    })

    it('condition can use context values', async () => {
      const middleware = createPassMiddleware()
      const condition = (ctx: IngestContext) => ctx.tenant.namespace === 'enabled'
      const conditional = conditionalMiddleware(condition, middleware)

      const enabledContext = createMockContext({ tenant: { namespace: 'enabled', isAdmin: false, keyId: 'k' } })
      const disabledContext = createMockContext({ tenant: { namespace: 'disabled', isAdmin: false, keyId: 'k' } })

      await conditional(enabledContext)
      await conditional(disabledContext)

      expect(middleware).toHaveBeenCalledTimes(1)
    })
  })

  describe('common patterns', () => {
    it('can check env variables', async () => {
      const middleware = createPassMiddleware()
      const condition = (ctx: IngestContext) => ctx.env.ENABLE_FEATURE === 'true'
      const conditional = conditionalMiddleware(condition, middleware)

      const enabledContext = createMockContext({ env: { ENABLE_FEATURE: 'true' } as any })
      const disabledContext = createMockContext({ env: { ENABLE_FEATURE: 'false' } as any })

      await conditional(enabledContext)
      await conditional(disabledContext)

      expect(middleware).toHaveBeenCalledTimes(1)
    })

    it('can check admin status', async () => {
      const middleware = createPassMiddleware()
      const condition = (ctx: IngestContext) => !ctx.tenant.isAdmin
      const conditional = conditionalMiddleware(condition, middleware)

      const adminContext = createMockContext({ tenant: { namespace: 'ns', isAdmin: true, keyId: 'k' } })
      const userContext = createMockContext({ tenant: { namespace: 'ns', isAdmin: false, keyId: 'k' } })

      await conditional(adminContext)
      await conditional(userContext)

      expect(middleware).toHaveBeenCalledTimes(1)
    })
  })
})

// ============================================================================
// fromSimpleFunction Tests
// ============================================================================

describe('fromSimpleFunction', () => {
  describe('successful execution', () => {
    it('returns continue: true when function succeeds', async () => {
      const fn = vi.fn().mockResolvedValue(undefined)
      const middleware = fromSimpleFunction(fn)
      const context = createMockContext()

      const result = await middleware(context)

      expect(result.continue).toBe(true)
      expect(fn).toHaveBeenCalledWith(context)
    })

    it('function can modify context', async () => {
      const fn = vi.fn().mockImplementation(async (ctx) => {
        ctx.rawBody = { processed: true }
      })
      const middleware = fromSimpleFunction(fn)
      const context = createMockContext()

      await middleware(context)

      expect(context.rawBody).toEqual({ processed: true })
    })
  })

  describe('error handling', () => {
    it('propagates errors from function', async () => {
      const error = new Error('Function error')
      const fn = vi.fn().mockRejectedValue(error)
      const middleware = fromSimpleFunction(fn)
      const context = createMockContext()

      await expect(middleware(context)).rejects.toThrow('Function error')
    })
  })

  describe('integration with compose', () => {
    it('works in composed pipeline', async () => {
      const fn1 = vi.fn().mockImplementation(async (ctx) => {
        ctx.rawBody = { step1: true }
      })
      const fn2 = vi.fn().mockImplementation(async (ctx) => {
        ctx.rawBody = { ...ctx.rawBody, step2: true }
      })

      const pipeline = composeMiddleware([
        fromSimpleFunction(fn1),
        fromSimpleFunction(fn2),
      ])
      const context = createMockContext()

      const result = await pipeline(context)

      expect(result.continue).toBe(true)
      expect(context.rawBody).toEqual({ step1: true, step2: true })
    })
  })
})

// ============================================================================
// Integration Tests
// ============================================================================

describe('middleware integration', () => {
  it('composes conditional and error-handled middleware', async () => {
    const innerMiddleware = vi.fn().mockResolvedValue({ continue: true })
    const errorHandler = vi.fn()

    const wrapped = withErrorHandling(innerMiddleware, errorHandler)
    const conditional = conditionalMiddleware((ctx) => ctx.tenant.isAdmin, wrapped)
    const pipeline = composeMiddleware([conditional])

    const adminContext = createMockContext({ tenant: { namespace: 'ns', isAdmin: true, keyId: 'k' } })
    const userContext = createMockContext({ tenant: { namespace: 'ns', isAdmin: false, keyId: 'k' } })

    await pipeline(adminContext)
    await pipeline(userContext)

    expect(innerMiddleware).toHaveBeenCalledTimes(1)
  })

  it('handles complex middleware chains', async () => {
    const context = createMockContext()
    const executionOrder: string[] = []

    const middleware1: IngestMiddleware = async (ctx) => {
      executionOrder.push('1')
      return { continue: true }
    }

    const middleware2: IngestMiddleware = async (ctx) => {
      executionOrder.push('2')
      return { continue: true }
    }

    const conditionalMiddleware3 = conditionalMiddleware(
      () => true,
      async (ctx) => {
        executionOrder.push('3-conditional')
        return { continue: true }
      }
    )

    const middleware4: IngestMiddleware = async (ctx) => {
      executionOrder.push('4')
      return { continue: false, response: new Response('done') }
    }

    const middleware5: IngestMiddleware = async (ctx) => {
      executionOrder.push('5-should-not-run')
      return { continue: true }
    }

    const pipeline = composeMiddleware([
      middleware1,
      middleware2,
      conditionalMiddleware3,
      middleware4,
      middleware5,
    ])

    await pipeline(context)

    expect(executionOrder).toEqual(['1', '2', '3-conditional', '4'])
  })
})
