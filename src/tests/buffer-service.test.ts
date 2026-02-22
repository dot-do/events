// .do/events/src/tests/buffer-service.test.ts
import { describe, it, expect, vi } from 'vitest'

// Mock cloudflare:workers since we're running outside workerd pool
vi.mock('cloudflare:workers', () => ({
  WorkerEntrypoint: class {
    env: unknown
    constructor(ctx: unknown, env: unknown) {
      this.env = env
    }
  },
}))

describe('BufferService', () => {
  it('exports a BufferService class', async () => {
    const mod = await import('../buffer-service')
    expect(mod.BufferService).toBeDefined()
  })

  it('BufferService has an ingest method', async () => {
    const { BufferService } = await import('../buffer-service')
    expect(BufferService.prototype.ingest).toBeDefined()
  })
})
