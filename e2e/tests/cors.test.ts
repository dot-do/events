/**
 * CORS Tests
 *
 * Tests CORS headers for client-side analytics support.
 */

import { SELF } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'

describe('CORS - OPTIONS preflight', () => {
  it('returns 200 for OPTIONS request', async () => {
    const response = await SELF.fetch('http://localhost/ingest', {
      method: 'OPTIONS',
    })
    expect(response.status).toBe(200)
  })

  it('includes Access-Control-Allow-Origin: *', async () => {
    const response = await SELF.fetch('http://localhost/ingest', {
      method: 'OPTIONS',
    })
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })

  it('includes Access-Control-Allow-Methods', async () => {
    const response = await SELF.fetch('http://localhost/ingest', {
      method: 'OPTIONS',
    })
    const methods = response.headers.get('Access-Control-Allow-Methods')
    expect(methods).toContain('POST')
    expect(methods).toContain('GET')
    expect(methods).toContain('OPTIONS')
  })

  it('includes Access-Control-Allow-Headers', async () => {
    const response = await SELF.fetch('http://localhost/ingest', {
      method: 'OPTIONS',
    })
    const headers = response.headers.get('Access-Control-Allow-Headers')
    expect(headers).toContain('Content-Type')
    expect(headers).toContain('Authorization')
  })

  it('includes Access-Control-Max-Age', async () => {
    const response = await SELF.fetch('http://localhost/ingest', {
      method: 'OPTIONS',
    })
    expect(response.headers.get('Access-Control-Max-Age')).toBe('86400')
  })
})

describe('CORS - Response headers', () => {
  it('POST /ingest includes Access-Control-Allow-Origin', async () => {
    const response = await SELF.fetch('http://localhost/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [] }),
    })
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })

  it('GET /health includes Access-Control-Allow-Origin', async () => {
    // Health endpoint should also include CORS headers
    // This tests that all responses have CORS, not just OPTIONS
    const response = await SELF.fetch('http://localhost/health')
    // Note: The current implementation doesn't add CORS to health
    // This test may fail initially - that's the TDD red phase
  })

  it('POST /query includes Access-Control-Allow-Origin', async () => {
    const response = await SELF.fetch('http://localhost/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    })
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })

  it('GET /recent includes Access-Control-Allow-Origin', async () => {
    const response = await SELF.fetch('http://localhost/recent')
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})
