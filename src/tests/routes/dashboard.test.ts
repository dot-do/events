/**
 * Dashboard Route Handler Tests
 *
 * Unit tests for src/routes/dashboard.ts
 * Tests the admin dashboard HTML generation and route handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleDashboard } from '../../routes/dashboard'

// ============================================================================
// Mock Types
// ============================================================================

interface MockEnv {
  ALLOWED_ORIGINS?: string
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    ...overrides,
  }
}

function createRequest(url: string, options: RequestInit = {}): Request {
  return new Request(url, { method: 'GET', ...options })
}

// ============================================================================
// Route Matching Tests
// ============================================================================

describe('handleDashboard - Route Matching', () => {
  it('handles /dashboard endpoint', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result).not.toBeNull()
    expect(result?.status).toBe(200)
  })

  it('returns null for non-dashboard endpoints', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/events')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result).toBeNull()
  })

  it('returns null for /dashboard/subpath', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard/users')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result).toBeNull()
  })
})

// ============================================================================
// HTTP Method Tests
// ============================================================================

describe('handleDashboard - HTTP Methods', () => {
  it('allows GET requests', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard', { method: 'GET' })
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result?.status).toBe(200)
  })

  it('rejects POST requests with 405', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard', { method: 'POST' })
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result?.status).toBe(405)
  })

  it('rejects PUT requests with 405', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard', { method: 'PUT' })
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result?.status).toBe(405)
  })

  it('rejects DELETE requests with 405', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard', { method: 'DELETE' })
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result?.status).toBe(405)
  })
})

// ============================================================================
// Response Headers Tests
// ============================================================================

describe('handleDashboard - Response Headers', () => {
  it('returns HTML content type', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result?.headers.get('Content-Type')).toBe('text/html; charset=utf-8')
  })

  it('sets no-cache headers', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)

    expect(result?.headers.get('Cache-Control')).toBe('no-cache, no-store, must-revalidate')
  })
})

// ============================================================================
// HTML Content Tests
// ============================================================================

describe('handleDashboard - HTML Content', () => {
  it('returns valid HTML document', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('<!DOCTYPE html>')
    expect(html).toContain('<html')
    expect(html).toContain('</html>')
  })

  it('includes page title', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('<title>events.do - Admin Dashboard</title>')
  })

  it('includes meta viewport for responsiveness', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('viewport')
    expect(html).toContain('width=device-width')
  })

  it('includes inline CSS styles', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('<style>')
    expect(html).toContain('</style>')
  })

  it('includes inline JavaScript', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('<script>')
    expect(html).toContain('</script>')
  })
})

// ============================================================================
// Dashboard Components Tests
// ============================================================================

describe('handleDashboard - Dashboard Components', () => {
  it('includes events stats card', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('Events (24h)')
    expect(html).toContain('id="events-stats"')
  })

  it('includes subscriptions stats card', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('Subscriptions')
    expect(html).toContain('id="subscriptions-stats"')
  })

  it('includes CDC/Catalog stats card', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('CDC / Catalog')
    expect(html).toContain('id="catalog-stats"')
  })

  it('includes webhooks stats card', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('Webhooks')
    expect(html).toContain('id="webhooks-stats"')
  })

  it('includes recent events table', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('Recent Events')
    expect(html).toContain('id="events-body"')
    expect(html).toContain('<table')
    expect(html).toContain('<thead>')
  })

  it('includes source filter tabs', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('data-source="all"')
    expect(html).toContain('data-source="events"')
    expect(html).toContain('data-source="tail"')
  })

  it('includes refresh button', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('id="refresh-btn"')
    expect(html).toContain('Refresh')
  })

  it('includes user info display', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('id="user-info"')
  })
})

// ============================================================================
// API Integration Tests
// ============================================================================

describe('handleDashboard - API Integration', () => {
  it('uses correct origin for API calls', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain("const API_BASE = 'https://events.do'")
  })

  it('fetches from /events endpoint', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain("'/events")
  })

  it('fetches from /subscriptions/list endpoint', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain("'/subscriptions/list'")
  })

  it('fetches from /catalog/namespaces endpoint', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain("'/catalog/namespaces'")
  })

  it('fetches from /me endpoint', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain("'/me'")
  })

  it('includes credentials in fetch calls', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain("credentials: 'include'")
  })
})

// ============================================================================
// Auto-refresh Tests
// ============================================================================

describe('handleDashboard - Auto-refresh', () => {
  it('configures 30 second auto-refresh interval', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('30000')
    expect(html).toContain('setInterval')
  })

  it('checks document visibility before refresh', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('document.hidden')
  })
})

// ============================================================================
// Security Tests
// ============================================================================

describe('handleDashboard - Security', () => {
  it('escapes HTML in event details', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('escapeHtml')
    expect(html).toContain('&amp;')
    expect(html).toContain('&lt;')
    expect(html).toContain('&gt;')
  })

  it('uses template literals safely', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    // Should escape HTML before inserting into templates
    expect(html).toContain('escapeHtml(')
  })
})

// ============================================================================
// CSS Theme Tests
// ============================================================================

describe('handleDashboard - CSS Theme', () => {
  it('uses dark theme by default', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain('--bg: #0a0a0a')
  })

  it('includes CSS variables for theming', async () => {
    const env = createMockEnv()
    const request = createRequest('https://events.do/dashboard')
    const url = new URL(request.url)

    const result = await handleDashboard(request, env as any, url)
    const html = await result?.text()

    expect(html).toContain(':root')
    expect(html).toContain('--bg-card')
    expect(html).toContain('--text')
    expect(html).toContain('--accent')
    expect(html).toContain('--success')
    expect(html).toContain('--warning')
    expect(html).toContain('--error')
  })
})
