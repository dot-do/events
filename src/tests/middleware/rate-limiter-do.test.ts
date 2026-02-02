/**
 * RateLimiterDO Tests - Token bucket, rate limiting, burst handling, reset timing, concurrency
 */
import { describe, it, expect, vi } from 'vitest'
import { RateLimiterDO } from '../../middleware/rate-limiter-do'

function createMockSqlStorage() {
  const windows = new Map<number, { requestCount: number; eventCount: number }>()
  return {
    exec: vi.fn(<T>(sql: string, ...params: unknown[]): { [Symbol.iterator]: () => Iterator<T> } => {
      const results: T[] = []
      if (sql.includes('CREATE TABLE')) return { [Symbol.iterator]: () => [][Symbol.iterator]() }
      if (sql.includes('SELECT') && sql.includes('window_start IN')) {
        for (const ws of params.slice(0, 2) as number[]) {
          const r = windows.get(ws); if (r) results.push({ windowStart: ws, ...r } as T)
        }
      } else if (sql.includes('INSERT INTO rate_limit_windows')) {
        const [ws, ec] = params as [number, number], ex = windows.get(ws)
        ex ? (ex.requestCount++, ex.eventCount += ec) : windows.set(ws, { requestCount: 1, eventCount: ec })
      } else if (sql.includes('DELETE') && sql.includes('WHERE')) {
        for (const k of windows.keys()) if (k < (params[0] as number)) windows.delete(k)
      } else if (sql.includes('DELETE')) windows.clear()
      else if (sql.includes('COUNT')) results.push({ count: windows.size } as T)
      else if (sql.includes('MIN')) {
        const ks = [...windows.keys()]
        results.push({ minWindow: ks.length ? Math.min(...ks) : null, maxWindow: ks.length ? Math.max(...ks) : null } as T)
      } else if (sql.includes('SUM')) {
        let tr = 0, te = 0; for (const r of windows.values()) { tr += r.requestCount; te += r.eventCount }
        results.push({ totalRequests: tr, totalEvents: te } as T)
      }
      return { [Symbol.iterator]: () => results[Symbol.iterator]() }
    }), _windows: windows
  }
}
const createCtx = () => ({ storage: { sql: createMockSqlStorage() }, id: { toString: () => 'test-id' } }) as unknown as DurableObjectState & { storage: { sql: ReturnType<typeof createMockSqlStorage> } }

describe('RateLimiterDO - Token Bucket Initialization', () => {
  it('creates table on first use', async () => {
    const ctx = createCtx(), rl = new RateLimiterDO(ctx, {})
    await rl.checkAndIncrement(100, 1000, 1)
    expect(ctx.storage.sql.exec).toHaveBeenCalledWith(expect.stringContaining('CREATE TABLE'))
  })
  it('initializes only once', async () => {
    const ctx = createCtx(), rl = new RateLimiterDO(ctx, {})
    await rl.checkAndIncrement(100, 1000, 1); await rl.checkAndIncrement(100, 1000, 1)
    expect(ctx.storage.sql.exec.mock.calls.filter(c => c[0].includes('CREATE TABLE'))).toHaveLength(1)
  })
  it('creates index for cleanup', async () => {
    const ctx = createCtx(), rl = new RateLimiterDO(ctx, {})
    await rl.checkAndIncrement(100, 1000, 1)
    expect(ctx.storage.sql.exec).toHaveBeenCalledWith(expect.stringContaining('CREATE INDEX'))
  })
})

describe('RateLimiterDO - Rate Limit Per Tenant', () => {
  it('allows requests within limit', async () => {
    const r = await new RateLimiterDO(createCtx(), {}).checkAndIncrement(100, 1000, 10)
    expect(r.allowed).toBe(true); expect(r.requestsRemaining).toBe(99); expect(r.eventsRemaining).toBe(990)
  })
  it('blocks when request limit exceeded', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    for (let i = 0; i < 10; i++) await rl.checkAndIncrement(10, 1000, 1)
    const r = await rl.checkAndIncrement(10, 1000, 1)
    expect(r.allowed).toBe(false); expect(r.requestsRemaining).toBe(0)
  })
  it('blocks when event limit exceeded', async () => {
    const r = await new RateLimiterDO(createCtx(), {}).checkAndIncrement(1000, 100, 101)
    expect(r.allowed).toBe(false); expect(r.eventsRemaining).toBeLessThanOrEqual(100)
  })
  it('returns retry-after when blocked', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    for (let i = 0; i < 5; i++) await rl.checkAndIncrement(5, 1000, 1)
    const r = await rl.checkAndIncrement(5, 1000, 1)
    expect(r.allowed).toBe(false); expect(r.retryAfterSeconds).toBeGreaterThanOrEqual(1)
  })
})

describe('RateLimiterDO - Burst Handling', () => {
  it('handles burst in single request', async () => {
    const r = await new RateLimiterDO(createCtx(), {}).checkAndIncrement(100, 500, 250)
    expect(r.allowed).toBe(true); expect(r.eventsRemaining).toBe(250)
  })
  it('blocks burst exceeding limit', async () => {
    expect((await new RateLimiterDO(createCtx(), {}).checkAndIncrement(100, 100, 150)).allowed).toBe(false)
  })
  it('tracks cumulative events', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    await rl.checkAndIncrement(100, 100, 30); await rl.checkAndIncrement(100, 100, 30); await rl.checkAndIncrement(100, 100, 30)
    expect((await rl.checkAndIncrement(100, 100, 20)).allowed).toBe(false)
  })
})

describe('RateLimiterDO - Reset Timing', () => {
  it('returns reset aligned to minute', async () => {
    const r = await new RateLimiterDO(createCtx(), {}).checkAndIncrement(100, 1000, 1)
    expect(r.resetAt % 60000).toBe(0); expect(r.resetAt).toBeGreaterThan(Date.now())
  })
  it('resets counters via reset()', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    for (let i = 0; i < 5; i++) await rl.checkAndIncrement(5, 1000, 1)
    await rl.reset(); expect((await rl.checkAndIncrement(5, 1000, 1)).allowed).toBe(true)
  })
  it('getStatus does not increment', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    await rl.checkAndIncrement(100, 1000, 10)
    const s1 = await rl.getStatus(100, 1000), s2 = await rl.getStatus(100, 1000)
    expect(s1.requestsRemaining).toBe(s2.requestsRemaining)
  })
})

describe('RateLimiterDO - Concurrent Requests', () => {
  it('handles multiple concurrent requests', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    const rs = await Promise.all([rl.checkAndIncrement(100, 1000, 10), rl.checkAndIncrement(100, 1000, 10), rl.checkAndIncrement(100, 1000, 10)])
    expect(rs.every(r => r.allowed)).toBe(true)
  })
  it('limits at boundary', async () => {
    const rl = new RateLimiterDO(createCtx(), {})
    for (let i = 0; i < 8; i++) await rl.checkAndIncrement(10, 1000, 1)
    const rs = await Promise.all([rl.checkAndIncrement(10, 1000, 1), rl.checkAndIncrement(10, 1000, 1), rl.checkAndIncrement(10, 1000, 1)])
    expect(rs.filter(r => r.allowed).length).toBeLessThanOrEqual(2)
  })
})

describe('RateLimiterDO - HTTP Handler', () => {
  it('returns health on /health', async () => {
    const rl = new RateLimiterDO(createCtx(), {}); await rl.checkAndIncrement(100, 1000, 5)
    const res = await rl.fetch(new Request('https://rl.do/health')), h = await res.json() as any
    expect(res.status).toBe(200); expect(h.status).toBe('healthy'); expect(h.currentWindow).toBeDefined()
  })
  it('returns 404 for unknown', async () => {
    expect((await new RateLimiterDO(createCtx(), {}).fetch(new Request('https://rl.do/x'))).status).toBe(404)
  })
  it('returns diagnostics on /diagnostics', async () => {
    expect((await new RateLimiterDO(createCtx(), {}).fetch(new Request('https://rl.do/diagnostics'))).status).toBe(200)
  })
})
