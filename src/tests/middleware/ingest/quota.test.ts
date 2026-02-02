/**
 * Ingest Quota Middleware Tests
 */
import { describe, it, expect, vi } from 'vitest'
import { quotaMiddleware, trackEventUsage } from '../../../middleware/ingest/quota'
import type { IngestContext } from '../../../middleware/ingest/types'

const mockCatalog = (overrides = {}) => ({
  checkEventQuota: vi.fn().mockResolvedValue({ allowed: true }),
  incrementEventCount: vi.fn().mockResolvedValue(undefined),
  ...overrides,
})
const mockCatalogNS = (c: ReturnType<typeof mockCatalog>) => ({ idFromName: vi.fn().mockReturnValue('id'), get: vi.fn().mockReturnValue(c) })
const ctx = (o: Partial<IngestContext> = {}): IngestContext => ({
  request: new Request('https://events.do/ingest', { method: 'POST' }),
  env: { CATALOG: mockCatalogNS(mockCatalog()) } as any,
  ctx: { waitUntil: vi.fn(), passThroughOnException: vi.fn() } as any,
  tenant: { namespace: 'ns', isAdmin: false, keyId: 'k' },
  batch: { events: [{ type: 'e', ts: '2024-01-01T00:00:00Z' }] },
  startTime: performance.now(),
  ...o,
})

describe('quotaMiddleware', () => {
  it('continues when quota check passes', async () => {
    const c = mockCatalog()
    const context = ctx({ env: { CATALOG: mockCatalogNS(c) } as any })
    expect((await quotaMiddleware(context)).continue).toBe(true)
    expect(context.quotaCheckPassed).toBe(true)
  })

  it('calls checkEventQuota with namespace and count', async () => {
    const c = mockCatalog()
    await quotaMiddleware(ctx({ env: { CATALOG: mockCatalogNS(c) } as any, tenant: { namespace: 'acme', isAdmin: false, keyId: 'k' }, batch: { events: [{type:'a',ts:'2024-01-01T00:00:00Z'},{type:'b',ts:'2024-01-01T00:00:01Z'}] } }))
    expect(c.checkEventQuota).toHaveBeenCalledWith('acme', 2)
  })

  it('returns 429 when quota exceeded', async () => {
    const c = mockCatalog({ checkEventQuota: vi.fn().mockResolvedValue({ allowed: false, reason: 'Limit' }) })
    const r = await quotaMiddleware(ctx({ env: { CATALOG: mockCatalogNS(c) } as any }))
    expect(r.continue).toBe(false)
    expect(r.response?.status).toBe(429)
  })

  it('includes QUOTA_EXCEEDED code and reason', async () => {
    const c = mockCatalog({ checkEventQuota: vi.fn().mockResolvedValue({ allowed: false, reason: 'Daily limit' }) })
    const json = await (await quotaMiddleware(ctx({ env: { CATALOG: mockCatalogNS(c) } as any }))).response?.json()
    expect(json.code).toBe('QUOTA_EXCEEDED')
    expect(json.reason).toBe('Daily limit')
  })

  it('includes Retry-After and CORS headers', async () => {
    const c = mockCatalog({ checkEventQuota: vi.fn().mockResolvedValue({ allowed: false }) })
    const r = await quotaMiddleware(ctx({ env: { CATALOG: mockCatalogNS(c) } as any }))
    expect(r.response?.headers.get('Retry-After')).toBe('3600')
    expect(r.response?.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })

  it('continues when CATALOG undefined (graceful degradation)', async () => {
    const context = ctx({ env: { CATALOG: undefined } as any })
    expect((await quotaMiddleware(context)).continue).toBe(true)
    expect(context.quotaCheckPassed).toBeUndefined()
  })

  it('continues when checkEventQuota throws (fail-open)', async () => {
    const c = mockCatalog({ checkEventQuota: vi.fn().mockRejectedValue(new Error('DO down')) })
    expect((await quotaMiddleware(ctx({ env: { CATALOG: mockCatalogNS(c) } as any }))).continue).toBe(true)
  })

  it('continues when batch undefined', async () => {
    expect((await quotaMiddleware(ctx({ batch: undefined }))).continue).toBe(true)
  })
})

describe('trackEventUsage', () => {
  it('calls incrementEventCount after quota passes', async () => {
    const c = mockCatalog()
    await trackEventUsage(ctx({ env: { CATALOG: mockCatalogNS(c) } as any, tenant: { namespace: 'team', isAdmin: false, keyId: 'k' }, batch: { events: [{type:'e1',ts:'2024-01-01T00:00:00Z'},{type:'e2',ts:'2024-01-01T00:00:01Z'}] }, quotaCheckPassed: true }))
    expect(c.incrementEventCount).toHaveBeenCalledWith('team', 2)
  })

  it('skips when quotaCheckPassed is false', async () => {
    const c = mockCatalog()
    await trackEventUsage(ctx({ env: { CATALOG: mockCatalogNS(c) } as any, quotaCheckPassed: false }))
    expect(c.incrementEventCount).not.toHaveBeenCalled()
  })

  it('skips when CATALOG unavailable', async () => {
    await expect(trackEventUsage(ctx({ env: { CATALOG: undefined } as any, quotaCheckPassed: true }))).resolves.toBeUndefined()
  })

  it('handles incrementEventCount errors gracefully', async () => {
    const c = mockCatalog({ incrementEventCount: vi.fn().mockRejectedValue(new Error('fail')) })
    await expect(trackEventUsage(ctx({ env: { CATALOG: mockCatalogNS(c) } as any, quotaCheckPassed: true }))).resolves.toBeUndefined()
  })
})
