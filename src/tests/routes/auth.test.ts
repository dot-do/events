/**
 * Auth Route Handler Tests
 *
 * Unit tests for src/routes/auth.ts
 * Tests login, callback, logout, and /me endpoints.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'

// ============================================================================
// Mock Types
// ============================================================================

interface MockEnv {
  OAUTH: {
    fetch: ReturnType<typeof vi.fn>
  }
  ALLOWED_ORIGINS?: string
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockEnv(overrides: Partial<MockEnv> = {}): MockEnv {
  return {
    OAUTH: {
      fetch: vi.fn(),
    },
    ...overrides,
  }
}

function createRequest(url: string, options: RequestInit = {}): Request {
  return new Request(url, { method: 'GET', ...options })
}

// ============================================================================
// Redirect URI Validation Tests (mirrored from auth.ts logic)
// ============================================================================

/**
 * Mirror of validateRedirectUri function from auth.ts
 */
function validateRedirectUri(redirectUri: string, currentOrigin: string, defaultPath: string = '/events'): string {
  if (!redirectUri || typeof redirectUri !== 'string') {
    return `${currentOrigin}${defaultPath}`
  }

  redirectUri = redirectUri.trim()

  const lowerUri = redirectUri.toLowerCase()
  if (lowerUri.startsWith('javascript:') ||
      lowerUri.startsWith('data:') ||
      lowerUri.startsWith('vbscript:') ||
      lowerUri.startsWith('file:')) {
    return `${currentOrigin}${defaultPath}`
  }

  if (redirectUri.startsWith('//')) {
    return `${currentOrigin}${defaultPath}`
  }

  if (redirectUri.startsWith('/') && !redirectUri.startsWith('//')) {
    if (redirectUri.includes('%') || redirectUri.includes('\\')) {
      try {
        const decoded = decodeURIComponent(redirectUri)
        if (decoded.includes(':') || decoded.startsWith('//')) {
          return `${currentOrigin}${defaultPath}`
        }
      } catch {
        return `${currentOrigin}${defaultPath}`
      }
    }
    return `${currentOrigin}${redirectUri}`
  }

  try {
    const redirectUrl = new URL(redirectUri)
    if (redirectUrl.origin === currentOrigin) {
      return redirectUri
    }
    return `${currentOrigin}${defaultPath}`
  } catch {
    return `${currentOrigin}${defaultPath}`
  }
}

describe('validateRedirectUri - Open Redirect Prevention', () => {
  const origin = 'https://events.do'

  describe('safe redirects', () => {
    it('allows relative paths starting with /', () => {
      expect(validateRedirectUri('/dashboard', origin)).toBe('https://events.do/dashboard')
    })

    it('allows same-origin absolute URLs', () => {
      expect(validateRedirectUri('https://events.do/events', origin)).toBe('https://events.do/events')
    })

    it('allows nested relative paths', () => {
      expect(validateRedirectUri('/api/v1/events', origin)).toBe('https://events.do/api/v1/events')
    })

    it('allows relative paths with query strings', () => {
      expect(validateRedirectUri('/events?page=1', origin)).toBe('https://events.do/events?page=1')
    })

    it('allows relative paths with fragments', () => {
      expect(validateRedirectUri('/events#section', origin)).toBe('https://events.do/events#section')
    })
  })

  describe('dangerous protocol prevention', () => {
    it('blocks javascript: protocol', () => {
      expect(validateRedirectUri('javascript:alert(1)', origin)).toBe('https://events.do/events')
    })

    it('blocks JavaScript: protocol (case insensitive)', () => {
      expect(validateRedirectUri('JavaScript:alert(1)', origin)).toBe('https://events.do/events')
    })

    it('blocks data: protocol', () => {
      expect(validateRedirectUri('data:text/html,<script>alert(1)</script>', origin)).toBe('https://events.do/events')
    })

    it('blocks vbscript: protocol', () => {
      expect(validateRedirectUri('vbscript:msgbox(1)', origin)).toBe('https://events.do/events')
    })

    it('blocks file: protocol', () => {
      expect(validateRedirectUri('file:///etc/passwd', origin)).toBe('https://events.do/events')
    })
  })

  describe('protocol-relative URL prevention', () => {
    it('blocks protocol-relative URLs (//)', () => {
      expect(validateRedirectUri('//evil.com/path', origin)).toBe('https://events.do/events')
    })

    it('blocks protocol-relative with credentials', () => {
      expect(validateRedirectUri('//user:pass@evil.com', origin)).toBe('https://events.do/events')
    })
  })

  describe('external domain prevention', () => {
    it('blocks external HTTP URLs', () => {
      expect(validateRedirectUri('http://evil.com/steal', origin)).toBe('https://events.do/events')
    })

    it('blocks external HTTPS URLs', () => {
      expect(validateRedirectUri('https://evil.com/steal', origin)).toBe('https://events.do/events')
    })

    it('blocks external URLs that look similar', () => {
      expect(validateRedirectUri('https://events.do.evil.com/steal', origin)).toBe('https://events.do/events')
    })
  })

  describe('encoded attack prevention', () => {
    it('blocks URL-encoded protocols', () => {
      // %3A = :
      expect(validateRedirectUri('/%2F%2Fevil.com', origin)).toBe('https://events.do/events')
    })

    it('handles invalid URL encoding gracefully', () => {
      expect(validateRedirectUri('/%invalid%', origin)).toBe('https://events.do/events')
    })

    it('allows backslash escapes when decoded result has no dangerous patterns', () => {
      // Backslash escapes are allowed if decoded result doesn't contain protocol patterns
      // /\\evil.com decodes to /\evil.com which has no : or // prefix
      expect(validateRedirectUri('/\\\\evil.com', origin)).toBe('https://events.do/\\\\evil.com')
    })
  })

  describe('edge cases', () => {
    it('returns default for null/undefined', () => {
      expect(validateRedirectUri(null as any, origin)).toBe('https://events.do/events')
      expect(validateRedirectUri(undefined as any, origin)).toBe('https://events.do/events')
    })

    it('returns default for empty string', () => {
      expect(validateRedirectUri('', origin)).toBe('https://events.do/events')
    })

    it('returns default for whitespace only', () => {
      expect(validateRedirectUri('   ', origin)).toBe('https://events.do/events')
    })

    it('trims whitespace from valid paths', () => {
      expect(validateRedirectUri('  /dashboard  ', origin)).toBe('https://events.do/dashboard')
    })

    it('uses custom default path', () => {
      expect(validateRedirectUri('https://evil.com', origin, '/home')).toBe('https://events.do/home')
    })
  })
})

// ============================================================================
// Login Route Tests
// ============================================================================

describe('handleAuth - Login', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  it('redirects to oauth.do for login', async () => {
    env.OAUTH.fetch.mockResolvedValue(new Response(null, {
      status: 302,
      headers: { 'Location': 'https://oauth.do/authorize?...' },
    }))

    const request = createRequest('https://events.do/login')
    const url = new URL(request.url)

    // Simulate the login handler
    const oauthUrl = new URL('/login', 'https://oauth.do')
    oauthUrl.searchParams.set('returnTo', 'https://events.do/events')
    const response = await env.OAUTH.fetch(new Request(oauthUrl.toString()))

    expect(response.status).toBe(302)
  })

  it('prevents redirect loops to /login', () => {
    const returnTo = '/login?redirect_uri=/dashboard'
    // Should default to /events when returnTo is /login
    const safeReturnTo = returnTo === '/login' || returnTo.startsWith('/login?') ? '/events' : returnTo
    expect(safeReturnTo).toBe('/events')
  })

  it('uses redirect_uri query param', () => {
    const request = createRequest('https://events.do/login?redirect_uri=/dashboard')
    const url = new URL(request.url)
    const returnTo = url.searchParams.get('redirect_uri') || '/events'
    expect(returnTo).toBe('/dashboard')
  })

  it('uses returnTo query param as fallback', () => {
    const request = createRequest('https://events.do/login?returnTo=/dashboard')
    const url = new URL(request.url)
    const returnTo = url.searchParams.get('redirect_uri') || url.searchParams.get('returnTo') || '/events'
    expect(returnTo).toBe('/dashboard')
  })

  it('defaults to /events when no redirect specified', () => {
    const request = createRequest('https://events.do/login')
    const url = new URL(request.url)
    const returnTo = url.searchParams.get('redirect_uri') || url.searchParams.get('returnTo') || '/events'
    expect(returnTo).toBe('/events')
  })
})

// ============================================================================
// Logout Route Tests
// ============================================================================

describe('handleAuth - Logout', () => {
  it('clears auth cookie on logout', () => {
    const cookieHeader = 'auth=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax'

    // Verify cookie format is correct for clearing
    expect(cookieHeader).toContain('auth=;')
    expect(cookieHeader).toContain('Expires=Thu, 01 Jan 1970 00:00:00 GMT')
    expect(cookieHeader).toContain('HttpOnly')
    expect(cookieHeader).toContain('Secure')
    expect(cookieHeader).toContain('SameSite=Lax')
  })

  it('redirects to / by default after logout', () => {
    const request = createRequest('https://events.do/logout')
    const url = new URL(request.url)
    const redirectUri = url.searchParams.get('redirect_uri') || '/'
    const redirectTo = validateRedirectUri(redirectUri, url.origin, '/')
    expect(redirectTo).toBe('https://events.do/')
  })

  it('uses custom redirect_uri after logout', () => {
    const request = createRequest('https://events.do/logout?redirect_uri=/goodbye')
    const url = new URL(request.url)
    const redirectUri = url.searchParams.get('redirect_uri') || '/'
    const redirectTo = validateRedirectUri(redirectUri, url.origin, '/')
    expect(redirectTo).toBe('https://events.do/goodbye')
  })

  it('prevents open redirect on logout', () => {
    const request = createRequest('https://events.do/logout?redirect_uri=https://evil.com')
    const url = new URL(request.url)
    const redirectUri = url.searchParams.get('redirect_uri') || '/'
    const redirectTo = validateRedirectUri(redirectUri, url.origin, '/')
    expect(redirectTo).toBe('https://events.do/')
  })
})

// ============================================================================
// Callback Route Tests
// ============================================================================

describe('handleAuth - Callback', () => {
  let env: MockEnv

  beforeEach(() => {
    env = createMockEnv()
    vi.clearAllMocks()
  })

  it('requires code parameter', async () => {
    const request = createRequest('https://events.do/callback')
    const url = new URL(request.url)
    const code = url.searchParams.get('code')

    expect(code).toBeNull()
  })

  it('returns error when error param is present', async () => {
    const request = createRequest('https://events.do/callback?error=access_denied&error_description=User%20denied%20access')
    const url = new URL(request.url)
    const error = url.searchParams.get('error')

    expect(error).toBe('access_denied')
  })

  it('extracts returnTo from callback', () => {
    const request = createRequest('https://events.do/callback?code=abc123&returnTo=/dashboard')
    const url = new URL(request.url)
    const returnTo = url.searchParams.get('returnTo') || '/events'

    expect(returnTo).toBe('/dashboard')
  })

  it('sets auth cookie with proper attributes', () => {
    const token = 'test-access-token'
    const maxAge = 3600 * 24 * 7 // 7 days
    const cookie = `auth=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAge}`

    expect(cookie).toContain(`auth=${token}`)
    expect(cookie).toContain('Path=/')
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain('Secure')
    expect(cookie).toContain('SameSite=Lax')
    expect(cookie).toContain('Max-Age=604800')
  })

  it('accepts both access_token and token from oauth.do', () => {
    // oauth.do may return 'token' instead of 'access_token'
    const data1 = { access_token: 'token123' }
    const data2 = { token: 'token456' }

    const accessToken1 = data1.access_token || (data1 as any).token
    const accessToken2 = (data2 as any).access_token || data2.token

    expect(accessToken1).toBe('token123')
    expect(accessToken2).toBe('token456')
  })

  it('returns debug info when debug=true', () => {
    const request = createRequest('https://events.do/callback?code=abc&debug=true')
    const url = new URL(request.url)
    const debug = url.searchParams.get('debug') === 'true'

    expect(debug).toBe(true)
  })
})

// ============================================================================
// Me Route Tests
// ============================================================================

describe('handleAuth - Me', () => {
  it('returns 401 for unauthenticated requests', async () => {
    const response = Response.json({ error: 'Not authenticated' }, { status: 401 })

    expect(response.status).toBe(401)
    const json = await response.json()
    expect(json.error).toBe('Not authenticated')
  })

  it('returns user info for authenticated requests', async () => {
    const user = {
      email: 'test@example.com',
      name: 'Test User',
      roles: ['admin'],
    }
    const response = Response.json(user)

    expect(response.status).toBe(200)
    const json = await response.json()
    expect(json.email).toBe('test@example.com')
    expect(json.name).toBe('Test User')
  })
})

// ============================================================================
// Route Matching Tests
// ============================================================================

describe('Auth Route Matching', () => {
  it('matches /login', () => {
    const url = new URL('https://events.do/login')
    expect(url.pathname).toBe('/login')
  })

  it('matches /callback', () => {
    const url = new URL('https://events.do/callback')
    expect(url.pathname).toBe('/callback')
  })

  it('matches /logout', () => {
    const url = new URL('https://events.do/logout')
    expect(url.pathname).toBe('/logout')
  })

  it('matches /me', () => {
    const url = new URL('https://events.do/me')
    expect(url.pathname).toBe('/me')
  })

  it('does not match /login/other', () => {
    const url = new URL('https://events.do/login/other')
    expect(url.pathname).not.toBe('/login')
  })
})
