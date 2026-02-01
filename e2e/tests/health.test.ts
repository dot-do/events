/**
 * Health Endpoint Tests
 *
 * Tests GET /health and GET / endpoints for the events.do receiver.
 */

import { SELF } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'

/** Response type for /health endpoint */
interface HealthResponse {
  status: string
  service: string
  ts: string
  env: string
}

describe('GET /health', () => {
  it('returns status ok', async () => {
    const response = await SELF.fetch('http://localhost/health')
    expect(response.status).toBe(200)

    const data = await response.json() as HealthResponse
    expect(data.status).toBe('ok')
  })

  it('returns service name events.do', async () => {
    const response = await SELF.fetch('http://localhost/health')
    const data = await response.json() as HealthResponse
    expect(data.service).toBe('events.do')
  })

  it('returns ISO timestamp', async () => {
    const response = await SELF.fetch('http://localhost/health')
    const data = await response.json() as HealthResponse

    expect(data.ts).toBeDefined()
    // Verify it's a valid ISO timestamp
    const parsed = new Date(data.ts)
    expect(parsed.toISOString()).toBe(data.ts)
  })

  it('returns environment from env vars', async () => {
    const response = await SELF.fetch('http://localhost/health')
    const data = await response.json() as HealthResponse

    expect(data.env).toBeDefined()
    expect(typeof data.env).toBe('string')
  })
})

describe('GET /', () => {
  it('returns same health response as /health', async () => {
    const response = await SELF.fetch('http://localhost/')
    expect(response.status).toBe(200)

    const data = await response.json() as HealthResponse
    expect(data.status).toBe('ok')
    expect(data.service).toBe('events.do')
  })
})
